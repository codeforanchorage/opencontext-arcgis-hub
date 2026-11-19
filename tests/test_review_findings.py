"""Four findings from the 2026-11 review, each a call that succeeded
(or failed) for a reason the caller could not see.

- The spanning tool joined rows to their values on a field named
  OBJECTID. On a layer whose object-id field has another name nothing
  joined, and with a narrowed out_fields nothing joined on any layer.
- coverage_by_polygon gave a building to the first target containing
  it, so every other record stacked on the same ground read 0%.
- coverage_by_polygon queried overlays 50 targets at a time, and 50
  detailed targets exceeded the caller-facing filter caps.
- Geometry helpers measured longitude differences the long way round
  the globe at the antimeridian.
"""

import math
from unittest.mock import AsyncMock, Mock, patch

import pytest

from tests.profile import ExampleConfig, ExamplePlugin

LAYER = "https://services.arcgis.com/org123/arcgis/rest/services/X/FeatureServer/0"


@pytest.fixture
def plugin():
    cfg = {
        "portal_base_url": "https://example.maps.arcgis.com/sharing/rest",
        "gallery_group_id": "abc123",
        "org_id": "org123",
        "city_name": "Municipality of Anchorage",
        "gallery_url": "https://example.com/gallery",
        "timeout": 20,
    }
    p = ExamplePlugin(cfg)
    p.plugin_config = ExampleConfig(**cfg)
    return p


def _box(x0, y0, x1, y1):
    return {
        "type": "Polygon",
        "coordinates": [[[x0, y0], [x0, y1], [x1, y1], [x1, y0], [x0, y0]]],
    }


# ── the object-id field ────────────────────────────────────────────────


class TestOidFieldName:
    def test_layer_metadata_declares_it(self):
        assert ExamplePlugin._oid_field_name({"objectIdField": "FID"}) == "FID"

    def test_query_response_declares_it(self):
        assert (
            ExamplePlugin._oid_field_name({"objectIdFieldName": "ObjectId"})
            == "ObjectId"
        )

    def test_falls_back_to_the_field_of_oid_type(self):
        meta = {
            "fields": [
                {"name": "NAME", "type": "esriFieldTypeString"},
                {"name": "OBJECTID_1", "type": "esriFieldTypeOID"},
            ]
        }
        assert ExamplePlugin._oid_field_name(meta) == "OBJECTID_1"

    @pytest.mark.parametrize("meta", [None, {}, {"fields": []}, "x"])
    def test_unknown_is_none(self, meta):
        assert ExamplePlugin._oid_field_name(meta) is None


CLS_POLYS = [
    {"geometry": _box(0, 0, 1, 1), "properties": {"ZONE_CODE": "R-1"}},
    {"geometry": _box(1, 0, 2, 1), "properties": {"ZONE_CODE": "R-2M"}},
]


async def _run_spanning(plugin, source_meta, attr_rows, args=None, oid=778):
    """Run the spanning tool with one feature in both zones.

    Returns (text, structured, posts): `posts` is every POST body.
    """
    posts = []
    gets = []

    async def fake_post(url, data=None, **kw):
        posts.append(dict(data or {}))
        resp = Mock()
        resp.status_code = 200
        resp.raise_for_status = Mock()
        if (data or {}).get("objectIds"):
            resp.json.return_value = attr_rows
        else:
            resp.json.return_value = {"objectIds": [oid]}
        return resp

    async def fake_get(url, params=None, **kw):
        gets.append(dict(params or {}))
        resp = Mock()
        resp.status_code = 200
        resp.raise_for_status = Mock()
        resp.json.return_value = {"features": []}
        return resp

    with patch.object(
        plugin,
        "_resolve_layer_url",
        AsyncMock(
            side_effect=[
                "https://example.com/source/0",
                "https://example.com/cls/0",
            ]
        ),
    ), patch.object(
        plugin,
        "_fetch_layer_meta",
        AsyncMock(
            side_effect=[
                {
                    "geometryType": "esriGeometryPolygon",
                    "fields": [{"name": "ZONE_CODE"}],
                },
                source_meta,
            ]
        ),
    ), patch.object(
        plugin, "_get_record_count", AsyncMock(return_value=1)
    ), patch.object(
        plugin, "_paged_geojson_fetch", AsyncMock(return_value=CLS_POLYS)
    ):
        plugin.client = Mock()
        plugin.client.post = fake_post
        plugin.client.get = fake_get
        base = {
            "source_item_id": "a" * 32,
            "classification_item_id": "b" * 32,
            "classification_field": "ZONE_CODE",
        }
        base.update(args or {})
        text, structured = (
            await plugin._find_features_spanning_classifications(base)
        )
    return text, structured, posts, gets


class TestSpanningJoinsOnTheRealOidField:
    FID_META = {
        "geometryType": "esriGeometryPolygon",
        "objectIdField": "FID",
        "fields": [
            {"name": "FID", "type": "esriFieldTypeOID"},
            {"name": "Parcel_ID", "type": "esriFieldTypeString"},
        ],
    }

    @pytest.mark.asyncio
    async def test_layer_with_another_oid_name_joins(self, plugin):
        meta = {
            "geometryType": "esriGeometryPolygon",
            "objectIdField": "OBJECTID_1",
            "fields": [
                {"name": "OBJECTID_1", "type": "esriFieldTypeOID"},
                {"name": "Parcel_ID", "type": "esriFieldTypeString"},
            ],
        }
        rows = {
            "objectIdFieldName": "OBJECTID_1",
            "features": [
                {"attributes": {"OBJECTID_1": 778, "Parcel_ID": "07502103000"}}
            ],
        }
        text, s, _, gets = await _run_spanning(plugin, meta, rows)
        assert "**`07502103000`**" in text
        (row,) = s["rows"]
        assert row["attributes"] == {"Parcel_ID": "07502103000"}
        # Joined in one request: no per-row fallback.
        assert gets == []

    @pytest.mark.asyncio
    async def test_narrowed_out_fields_still_fetches_the_oid(self, plugin):
        meta = {
            "geometryType": "esriGeometryPolygon",
            "objectIdField": "OBJECTID",
            "fields": [
                {"name": "OBJECTID", "type": "esriFieldTypeOID"},
                {"name": "Parcel_ID", "type": "esriFieldTypeString"},
            ],
        }
        rows = {
            "features": [
                {"attributes": {"OBJECTID": 778, "Parcel_ID": "07502103000"}}
            ]
        }
        text, s, posts, gets = await _run_spanning(
            plugin, meta, rows, args={"out_fields": "Parcel_ID"}
        )
        attr_post = next(p for p in posts if p.get("objectIds"))
        assert attr_post["outFields"] == "Parcel_ID,OBJECTID"
        assert gets == []
        (row,) = s["rows"]
        # The id rode along for the join; it is not shown as data.
        assert row["attributes"] == {"Parcel_ID": "07502103000"}

    @pytest.mark.asyncio
    async def test_object_id_zero_is_a_real_row(self, plugin):
        rows = {
            "features": [
                {"attributes": {"FID": 0, "Parcel_ID": "00100100000"}}
            ]
        }
        text, s, _, gets = await _run_spanning(
            plugin, self.FID_META, rows, oid=0
        )
        assert "**`00100100000`**" in text
        assert gets == []

    @pytest.mark.asyncio
    async def test_fallback_filters_on_the_real_field(self, plugin):
        # Bulk fetch errors: the per-row fallback must not ask for
        # OBJECTID on a layer that has none.
        _, _, _, gets = await _run_spanning(
            plugin, self.FID_META, {"error": {"code": 400, "message": "x"}}
        )
        assert gets and all("FID=778" in g["where"] for g in gets)
        assert not any("OBJECTID" in g["where"] for g in gets)


# ── coverage: stacked targets ──────────────────────────────────────────


def _cov_meta():
    return {
        "geometryType": "esriGeometryPolygon",
        "extent": {"spatialReference": {"wkid": 3857, "latestWkid": 3857}},
        "fields": [
            {"name": "PID", "type": "esriFieldTypeString"},
            {"name": "Shape__Area", "type": "esriFieldTypeDouble"},
        ],
    }


async def _run_cov(plugin, targets, overlays, calls=None):
    async def paged(url, where, out_fields, limit, extra_params=None,
                    method="get"):
        if extra_params is None:
            return targets
        if calls is not None:
            calls.append(extra_params)
        return overlays

    metas = iter([_cov_meta(), _cov_meta()])

    async def meta(url, require_fields=()):
        return next(metas)

    with patch.object(
        plugin, "_resolve_layer_url", AsyncMock(return_value=LAYER)
    ), patch.object(plugin, "_fetch_layer_meta", new=meta), patch.object(
        plugin, "_paged_geojson_fetch", new=paged
    ), patch.object(
        plugin, "_safe_layer_meta", AsyncMock(return_value={})
    ):
        return await plugin._coverage_by_polygon(
            {
                "target_item_id": "a" * 32,
                "overlay_item_id": "b" * 32,
                "target_id_field": "PID",
            }
        )


def _target(pid, geom, area=1000.0, oid=None):
    return {
        "id": oid,
        "geometry": geom,
        "properties": {"PID": pid, "Shape__Area": area},
    }


BUILDING = {
    "id": 10,
    "geometry": _box(-149.9995, 61.1998, -149.9990, 61.2002),
    "properties": {"Shape__Area": 400.0},
}
LOT = _box(-150.0, 61.199, -149.998, 61.201)


class TestCoverageStackedTargets:
    @pytest.mark.asyncio
    async def test_every_unit_on_the_same_ground_is_credited(self, plugin):
        """Three condominium units share one polygon. Only the first
        used to get the building; the other two read 0% -- vacant."""
        targets = [_target(f"U{i}", LOT, oid=i) for i in (1, 2, 3)]
        _, s = await _run_cov(plugin, targets, [BUILDING])
        pct = {r["id"]: r["coverage_pct"] for r in s["rows"]}
        assert pct == {"U1": 40.0, "U2": 40.0, "U3": 40.0}
        stacked = [c for c in s["caveats"] if c["code"] == "stacked_targets"]
        assert stacked and stacked[0]["count"] == 3
        assert "do not add" in stacked[0]["message"]

    @pytest.mark.asyncio
    async def test_adjacent_lots_are_unchanged(self, plugin):
        """Lots that tile: the building is on one of them only, and no
        stacking is reported."""
        west = _target("W", LOT, oid=1)
        east = _target("E", _box(-149.998, 61.199, -149.996, 61.201), oid=2)
        _, s = await _run_cov(plugin, [west, east], [BUILDING])
        pct = {r["id"]: r["coverage_pct"] for r in s["rows"]}
        assert pct == {"W": 40.0, "E": 0.0}
        assert not [c for c in s["caveats"] if c["code"] == "stacked_targets"]


# ── coverage: batches that fit the filter caps ─────────────────────────


def _detailed(x0, n):
    """A thin polygon with about `n` vertices, starting at lon x0."""
    top = [[x0 + i * 1e-6, 61.201] for i in range(n // 2)]
    bottom = [[x0 + i * 1e-6, 61.199] for i in reversed(range(n // 2))]
    ring = top + bottom + [top[0]]
    return {"type": "Polygon", "coordinates": [ring]}


class TestCoverageBatches:
    def _parcels(self, geoms):
        return [
            {"geom": g, "bbox": ExamplePlugin._geometry_bbox(g)}
            for g in geoms
        ]

    def test_small_targets_keep_the_fixed_batch_size(self, plugin):
        parcels = self._parcels([LOT] * 120)
        sizes = [len(b) for b, _ in plugin._coverage_batches(parcels)]
        assert sizes == [50, 50, 20]

    def test_detailed_targets_close_the_batch_early(self, plugin):
        # 30 targets x 600 vertices = 18,000: over the 10,000 cap.
        parcels = self._parcels(
            [_detailed(-150 + i * 0.01, 600) for i in range(30)]
        )
        batches = plugin._coverage_batches(parcels)
        assert sum(len(b) for b, _ in batches) == 30
        assert len(batches) > 1
        for _, polys in batches:
            coords = sum(len(r) for poly in polys for r in poly)
            assert coords <= plugin.MAX_FILTER_COORDS
            # and the conversion the tool runs next accepts it
            plugin._geojson_to_esri_polygon(
                {"type": "MultiPolygon", "coordinates": polys}
            )

    def test_one_target_over_the_cap_is_queried_by_its_box(self, plugin):
        big = _detailed(-150, plugin.MAX_FILTER_COORDS + 2000)
        parcels = self._parcels([LOT, big, LOT])
        batches = plugin._coverage_batches(parcels)
        assert [len(b) for b, _ in batches] == [1, 1, 1]
        _, polys = batches[1]
        (ring,) = polys[0]
        assert len(ring) == 5
        bb = ExamplePlugin._geometry_bbox(big)
        assert {tuple(pt) for pt in ring} == {
            (bb[0], bb[1]), (bb[2], bb[1]), (bb[2], bb[3]), (bb[0], bb[3])
        }

    @pytest.mark.asyncio
    async def test_detailed_targets_no_longer_fail_the_call(self, plugin):
        """This used to raise "filter_geometry has N coordinates" at a
        caller who had passed no filter_geometry."""
        targets = [
            _target(f"T{i}", _detailed(-150 + i * 0.01, 600), oid=i)
            for i in range(30)
        ]
        calls = []
        _, s = await _run_cov(plugin, targets, [], calls=calls)
        assert s["summary"]["targets_measured"] == 30
        assert len(calls) > 1


# ── the antimeridian ───────────────────────────────────────────────────
#
# ArcGIS cuts geometry at +/-180 degrees, so the western Aleutians come
# back as parts on both sides of the line (checked against the State of
# Alaska's layers: no ring crosses it, and no longitude exceeds 180).


class TestAntimeridian:
    EAST = _box(179.0, 51.0, 180.0, 52.0)  # ends at the line
    WEST = _box(-180.0, 51.0, -179.0, 52.0)  # begins at it

    def test_lon_delta_takes_the_short_way_round(self):
        d = ExamplePlugin._lon_delta
        assert d(-179.9, 179.9) == pytest.approx(0.2)
        assert d(179.9, -179.9) == pytest.approx(-0.2)
        assert d(-149.0, -150.0) == pytest.approx(1.0)
        assert d(10.0, 10.0) == 0.0

    def test_distance_across_the_line_is_short(self):
        # 0.05 degrees of longitude at 51.5N is about 3.5 km, not the
        # ~25,000 km of going the long way round.
        m = ExamplePlugin._point_to_geometry_distance_m(
            self.WEST, (179.95, 51.5)
        )
        assert 3000 < m < 4000

    def test_distance_away_from_the_line_is_unchanged(self):
        box = _box(-150.0, 61.0, -149.0, 62.0)
        m = ExamplePlugin._point_to_geometry_distance_m(box, (-150.1, 61.5))
        expected = 0.1 * 111195.0 * math.cos(math.radians(61.5))
        assert m == pytest.approx(expected, rel=1e-6)

    def test_prefilter_admits_a_point_across_the_line(self):
        bb = ExamplePlugin._geometry_bbox(self.WEST)
        ex = ExamplePlugin._bbox_excludes
        assert ex(bb, 179.95, 51.5) is True  # no buffer: not inside
        assert ex(bb, 179.95, 51.5, pad_lon=0.1, pad_lat=0.1) is False
        assert ex(bb, 170.0, 51.5, pad_lon=0.1, pad_lat=0.1) is True
        assert ex(bb, -179.5, 51.5) is False
        assert ex(bb, -179.5, 60.0, pad_lon=0.1, pad_lat=0.1) is True

    def test_centroid_of_parts_on_both_sides_stays_in_the_aleutians(self):
        geom = {
            "type": "MultiPolygon",
            "coordinates": [
                self.EAST["coordinates"],
                self.WEST["coordinates"],
            ],
        }
        x, y = ExamplePlugin._geometry_centroid(geom)
        assert abs(abs(x) - 180.0) < 0.01, x  # was ~0: the Atlantic
        assert y == pytest.approx(51.5)

    def test_centroid_leans_to_the_larger_side(self):
        geom = {
            "type": "MultiPolygon",
            "coordinates": [
                _box(177.0, 51.0, 180.0, 52.0)["coordinates"],
                self.WEST["coordinates"],
            ],
        }
        x, _ = ExamplePlugin._geometry_centroid(geom)
        assert 178.0 < x <= 180.0

    def test_centroid_elsewhere_is_unchanged(self):
        geom = {
            "type": "MultiPolygon",
            "coordinates": [
                _box(-150.0, 61.0, -149.0, 62.0)["coordinates"],
                _box(-148.0, 61.0, -147.0, 62.0)["coordinates"],
            ],
        }
        x, y = ExamplePlugin._geometry_centroid(geom)
        assert x == pytest.approx(-148.5)
        assert y == pytest.approx(61.5)

    def test_line_centroid_of_parts_on_both_sides(self):
        lines = [
            [[179.0, 51.0], [180.0, 51.0]],
            [[-180.0, 51.0], [-179.0, 51.0]],
        ]
        x, _ = ExamplePlugin._multilinestring_centroid(lines)
        assert abs(abs(x) - 180.0) < 0.01

    @pytest.mark.asyncio
    async def test_buffer_reaches_a_polygon_across_the_line(self, plugin):
        """A point 3.5 km east of a polygon that starts at the line is
        within 5 km of it; it used to be 'unmatched'."""
        polygons = [
            {"group": "Attu side", "geometry": self.WEST},
        ]
        source = [
            {
                "id": 1,
                "geometry": {"type": "Point", "coordinates": [179.95, 51.5]},
                "properties": {"OBJECTID": 1},
            }
        ]
        with patch.object(
            plugin, "_fetch_aggregation_polygons", AsyncMock(return_value=polygons)
        ), patch.object(
            plugin, "_resolve_layer_url", AsyncMock(return_value=LAYER)
        ), patch.object(
            plugin,
            "_fetch_layer_meta",
            AsyncMock(
                return_value={
                    "geometryType": "esriGeometryPoint",
                    "fields": [{"name": "OBJECTID", "type": "esriFieldTypeOID"}],
                }
            ),
        ), patch.object(
            plugin, "_paged_geojson_fetch", AsyncMock(return_value=source)
        ), patch.object(
            plugin, "_safe_layer_meta", AsyncMock(return_value={})
        ):
            _, s = await plugin._aggregate_by_polygon(
                {
                    "source_item_id": "a" * 32,
                    "aggregation_item_id": "b" * 32,
                    "group_by_field": "NAME",
                    "buffer_distance": 5,
                    "buffer_units": "kilometers",
                }
            )
        assert s["summary"]["unmatched"] == 0
        assert s["rows"][0]["group"] == "Attu side"
        assert s["rows"][0]["count"] == 1
