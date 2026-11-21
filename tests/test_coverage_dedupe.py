"""coverage_by_polygon must count an overlay feature once.

Targets are queried in batches, and a building that touches targets in two
batches comes back from both queries. The de-duplication keyed on the
GeoJSON feature `id` -- which ArcGIS sends only when the object-id field is
among outFields. Only the area field was requested, so every id was None
and nothing was ever de-duplicated: on the deployed Anchorage server one
parcel read 38% covered in a 400-target call and 19% queried alone.

The upstream stub below behaves as the real service does: a feature gets an
`id` only if the object-id field was asked for.
"""

from unittest.mock import AsyncMock, patch

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


def _meta(oid_field="OBJECTID"):
    fields = [
        {"name": "PID", "type": "esriFieldTypeString"},
        {"name": "Shape__Area", "type": "esriFieldTypeDouble"},
    ]
    meta = {
        "geometryType": "esriGeometryPolygon",
        "extent": {"spatialReference": {"wkid": 3857, "latestWkid": 3857}},
        "fields": fields,
    }
    if oid_field:
        meta["objectIdField"] = oid_field
        fields.append({"name": oid_field, "type": "esriFieldTypeOID"})
    return meta


# Two adjacent lots; the building straddles their shared edge with its
# centre in lot A, so each lot's overlay query returns it.
LOT_A = _box(-150.000, 61.199, -149.998, 61.201)
LOT_B = _box(-149.998, 61.199, -149.996, 61.201)
BUILDING_GEOM = _box(-149.9990, 61.1998, -149.9978, 61.2002)


def _one_batch_per_target(parcels):
    return [([p], [p["geom"]["coordinates"]]) for p in parcels]


async def _run(plugin, oid_field="OBJECTID", buildings=None):
    """Run coverage over lots A and B, one overlay query per lot."""
    out_fields_seen = []
    buildings = buildings or [(10, BUILDING_GEOM, 400.0)]

    async def paged(url, where, out_fields, limit, extra_params=None, method="get"):
        if extra_params is None:
            return [
                {"geometry": LOT_A, "properties": {"PID": "A", "Shape__Area": 1000.0}},
                {"geometry": LOT_B, "properties": {"PID": "B", "Shape__Area": 1000.0}},
            ]
        out_fields_seen.append(out_fields)
        asked = set(out_fields.split(","))
        feats = []
        for oid, geom, area in buildings:
            feat = {"geometry": geom, "properties": {"Shape__Area": area}}
            if oid_field and oid_field in asked:
                feat["id"] = oid
                feat["properties"][oid_field] = oid
            feats.append(feat)
        return feats

    metas = iter([_meta(), _meta(oid_field)])

    async def meta(url, require_fields=()):
        return next(metas)

    with (
        patch.object(plugin, "_resolve_layer_url", AsyncMock(return_value=LAYER)),
        patch.object(plugin, "_fetch_layer_meta", new=meta),
        patch.object(plugin, "_paged_geojson_fetch", new=paged),
        patch.object(plugin, "_safe_layer_meta", AsyncMock(return_value={})),
        patch.object(plugin, "_coverage_batches", new=_one_batch_per_target),
    ):
        _, structured = await plugin._coverage_by_polygon(
            {
                "target_item_id": "a" * 32,
                "overlay_item_id": "b" * 32,
                "target_id_field": "PID",
            }
        )
    pct = {row["id"]: row["coverage_pct"] for row in structured["rows"]}
    return pct, out_fields_seen


class TestAnOverlaySeenByTwoBatchesIsCountedOnce:
    @pytest.mark.asyncio
    async def test_a_building_on_a_batch_boundary(self, plugin):
        pct, out_fields = await _run(plugin)
        assert len(out_fields) == 2  # the building really was fetched twice
        # 400 of 1,000, not 800 of 1,000.
        assert pct["A"] == 40.0
        assert pct["B"] == 0.0

    @pytest.mark.asyncio
    async def test_the_object_id_field_is_requested(self, plugin):
        _, out_fields = await _run(plugin, oid_field="FID")
        assert all(f == "Shape__Area,FID" for f in out_fields)

    @pytest.mark.asyncio
    async def test_a_layer_with_no_object_id_is_still_deduplicated(self, plugin):
        pct, out_fields = await _run(plugin, oid_field=None)
        assert all(f == "Shape__Area" for f in out_fields)
        assert pct["A"] == 40.0

    @pytest.mark.asyncio
    async def test_two_different_buildings_are_both_counted(self, plugin):
        second = _box(-149.9996, 61.1993, -149.9992, 61.1996)
        pct, _ = await _run(
            plugin,
            buildings=[(10, BUILDING_GEOM, 400.0), (11, second, 100.0)],
        )
        assert pct["A"] == 50.0

    @pytest.mark.asyncio
    async def test_same_footprints_without_ids_are_not_merged_if_distinct(self, plugin):
        # No object id at all, two buildings of the same area in different
        # places: the geometric fallback must keep both.
        second = _box(-149.9996, 61.1993, -149.9992, 61.1996)
        pct, _ = await _run(
            plugin,
            oid_field=None,
            buildings=[(None, BUILDING_GEOM, 400.0), (None, second, 400.0)],
        )
        assert pct["A"] == 80.0


class TestPagingStallGuardWithoutFeatureIds:
    @pytest.mark.asyncio
    async def test_a_repeated_page_stops_the_fetch(self, plugin):
        """A server that ignores resultOffset returns page one again. The
        guard keyed on `id`, which is absent unless the object-id field is
        requested, so it never fired and the page was collected until the
        limit."""
        page = {
            "features": [
                {"geometry": LOT_A, "properties": {"Shape__Area": 1.0}},
                {"geometry": LOT_B, "properties": {"Shape__Area": 2.0}},
            ],
            "exceededTransferLimit": True,
        }
        request = AsyncMock(return_value=page)
        with patch.object(plugin, "_request_json_with_retry", new=request):
            features = await plugin._paged_geojson_fetch(
                LAYER, where="1=1", out_fields="Shape__Area", limit=1000
            )
        assert request.await_count == 2
        assert len(features) == 2
