"""Results that used to be wrong without saying so.

Each class pins one defect found in the 2026-11 review. What they have
in common: the call succeeded, the output looked ordinary, and the
number in it was wrong or partial with nothing to show for it.

  * spatial queries cut at `limit` reported `truncated: false`;
  * a buffered aggregation gave each feature to whichever polygon the
    server listed first;
  * the centroid of a building-sized polygon landed blocks away;
  * a `..` in a service URL walked out of the tenant allowlist;
  * NaN, zero, negative and null arguments reached the upstream query.
"""

import json
import math
from unittest.mock import AsyncMock, Mock, patch

import pytest
from jsonschema import Draft202012Validator

from core.interfaces import ToolInputError
from tests.profile import ExampleConfig, ExamplePlugin

ORG = "Ce3DhLRthdwbHlfF"
ITEM = "0123456789abcdef0123456789abcdef"
LAYER = (
    "https://services.arcgis.com/Ce3DhLRthdwbHlfF/arcgis/rest/services/"
    "X/FeatureServer/0"
)
BOX = {
    "type": "Polygon",
    "coordinates": [
        [[-150, 61], [-150, 61.1], [-149.9, 61.1], [-149.9, 61], [-150, 61]]
    ],
}


@pytest.fixture
def plugin():
    cfg = {
        "portal_base_url": "https://muniorg.maps.arcgis.com/sharing/rest",
        "gallery_group_id": "c34ed10758ec4f4eb8aa6826ee5be3ff",
        "org_id": ORG,
        "city_name": "Municipality of Anchorage",
        "gallery_url": "https://example.org/gallery",
        "timeout": 20,
    }
    p = ExamplePlugin(cfg)
    p.plugin_config = ExampleConfig(**cfg)
    p.ARCGIS_RETRY_BACKOFF_S = 0
    p.client = Mock()
    p.get_dataset = AsyncMock(
        return_value={"url": LAYER, "type": "Feature Service"}
    )
    p._safe_layer_meta = AsyncMock(return_value={})
    return p


def _resp(payload):
    r = Mock()
    r.status_code = 200
    r.text = json.dumps(payload)
    r.json.return_value = payload
    r.headers = {"content-type": "application/json"}
    return r


def _rows(n):
    return {"features": [{"attributes": {"OBJECTID": i}} for i in range(n)]}


def _validate(schema, instance):
    errors = list(Draft202012Validator(schema).iter_errors(instance))
    assert not errors, [e.message for e in errors]


# ── spatial queries cut at `limit` ────────────────────────────────────


class TestSpatialQueriesSayWhenTheyAreCut:
    @pytest.mark.asyncio
    async def test_full_page_is_counted_and_flagged(self, plugin):
        plugin._send = AsyncMock(
            side_effect=[_resp(_rows(3)), _resp({"count": 412})]
        )
        result = await plugin.execute_tool(
            "spatial_query_polygon",
            {"item_id": ITEM, "filter_geometry": BOX, "limit": 3},
        )
        assert result.success
        s = result.structured_content
        _validate(ExamplePlugin.QUERY_RESULT_OUTPUT_SCHEMA, s)
        assert s["summary"] == {
            "returned": 3,
            "total_count": 412,
            "truncated": True,
            "complete": False,
        }
        assert [c["code"] for c in s["caveats"]] == ["result_truncated"]
        assert "TRUNCATED" in result.content[0]["text"]
        assert "412" in result.content[0]["text"]

    @pytest.mark.asyncio
    async def test_count_twin_asks_the_same_question(self, plugin):
        plugin._send = AsyncMock(
            side_effect=[_resp(_rows(3)), _resp({"count": 412})]
        )
        await plugin.execute_tool(
            "spatial_query_polygon",
            {
                "item_id": ITEM,
                "filter_geometry": BOX,
                "limit": 3,
                "where": "ZONE = 'R1'",
                "distance": 0.5,
                "units": "miles",
            },
        )
        page, count = (c.kwargs["data"] for c in plugin._send.call_args_list)
        assert count["returnCountOnly"] == "true"
        assert "resultRecordCount" not in count
        assert "outFields" not in count
        for key in ("where", "geometry", "spatialRel", "distance", "units"):
            assert count[key] == page[key], key

    @pytest.mark.asyncio
    async def test_short_page_costs_no_second_request(self, plugin):
        plugin._send = AsyncMock(side_effect=[_resp(_rows(2))])
        result = await plugin.execute_tool(
            "spatial_query_polygon",
            {"item_id": ITEM, "filter_geometry": BOX, "limit": 3},
        )
        s = result.structured_content
        assert plugin._send.call_count == 1
        assert s["summary"]["truncated"] is False
        assert s["summary"]["total_count"] is None
        assert s["caveats"] == []

    @pytest.mark.asyncio
    async def test_exactly_limit_matches_is_not_truncated(self, plugin):
        plugin._send = AsyncMock(
            side_effect=[_resp(_rows(3)), _resp({"count": 3})]
        )
        result = await plugin.execute_tool(
            "spatial_query_polygon",
            {"item_id": ITEM, "filter_geometry": BOX, "limit": 3},
        )
        s = result.structured_content
        assert s["summary"]["truncated"] is False
        assert s["caveats"] == []

    @pytest.mark.asyncio
    async def test_failed_count_still_flags_the_sample(self, plugin):
        plugin._send = AsyncMock(
            side_effect=[
                _resp(_rows(3)),
                _resp({"error": {"code": 400, "message": "no"}}),
            ]
        )
        result = await plugin.execute_tool(
            "spatial_query_polygon",
            {"item_id": ITEM, "filter_geometry": BOX, "limit": 3},
        )
        assert result.success
        s = result.structured_content
        _validate(ExamplePlugin.QUERY_RESULT_OUTPUT_SCHEMA, s)
        assert s["summary"]["truncated"] is True
        assert s["summary"]["total_count"] is None
        assert [c["code"] for c in s["caveats"]] == ["limit_reached"]
        assert "TRUNCATED" in result.content[0]["text"]

    @pytest.mark.asyncio
    async def test_server_flag_on_a_short_page(self, plugin):
        """maxRecordCount below `limit`: the page is short AND cut."""
        page = dict(_rows(2), exceededTransferLimit=True)
        plugin._send = AsyncMock(
            side_effect=[_resp(page), _resp({"count": 900})]
        )
        result = await plugin.execute_tool(
            "spatial_query_polygon",
            {"item_id": ITEM, "filter_geometry": BOX, "limit": 100},
        )
        assert result.structured_content["summary"]["total_count"] == 900
        assert result.structured_content["summary"]["truncated"] is True

    @pytest.mark.asyncio
    async def test_point_query_is_flagged_too(self, plugin):
        plugin._get_meta_json = AsyncMock(
            return_value={"geometryType": "esriGeometryPolygon"}
        )
        plugin._send = AsyncMock(
            side_effect=[_resp(_rows(10)), _resp({"count": 64})]
        )
        result = await plugin.execute_tool(
            "spatial_query_point",
            {"item_id": ITEM, "lon": -149.9, "lat": 61.2},
        )
        s = result.structured_content
        assert s["summary"]["total_count"] == 64
        assert s["summary"]["truncated"] is True
        # The successful path echoes the query, as the empty path does.
        assert s["query"]["item_id"] == ITEM

    @pytest.mark.asyncio
    async def test_filter_by_polygon_is_flagged_too(self, plugin):
        overflow_seen = {}

        async def fake_spq(*args, info=None, **kwargs):
            info["total_count"] = 5000
            overflow_seen.update(kwargs)
            return [{"OBJECTID": i} for i in range(100)]

        plugin.spatial_query_polygon = fake_spq
        plugin._resolve_layer_url = AsyncMock(return_value=LAYER)
        plugin._fetch_layer_meta = AsyncMock(
            return_value={
                "geometryType": "esriGeometryPolygon",
                "fields": [{"name": "NAME"}],
            }
        )
        # The container count; the source query itself is faked above.
        plugin._send = AsyncMock(side_effect=[_resp({"count": 1})])
        text, s = await plugin._filter_by_polygon(
            {
                "source_item_id": ITEM,
                "container_item_id": "f" * 32,
                "container_where": "NAME = 'Fairview'",
            }
        )
        assert s["summary"]["total_count"] == 5000
        assert s["summary"]["truncated"] is True
        assert "TRUNCATED" in text


# ── inline MultiPolygon filters ───────────────────────────────────────


class TestInlineMultiPolygonIsUnioned:
    @pytest.mark.asyncio
    async def test_overlapping_parts_are_merged(self, plugin):
        a = [[[0, 0], [0, 2], [2, 2], [2, 0], [0, 0]]]
        b = [[[1, 1], [1, 3], [3, 3], [3, 1], [1, 1]]]
        plugin._send = AsyncMock(side_effect=[_resp(_rows(0))])
        await plugin.spatial_query_polygon(
            ITEM,
            filter_geometry={"type": "MultiPolygon", "coordinates": [a, b]},
        )
        sent = json.loads(plugin._send.call_args.kwargs["data"]["geometry"])
        # Two overlapping squares are one L-shaped ring of area 7, not
        # two rings whose 1x1 overlap an even-odd fill reads as a hole.
        assert len(sent["rings"]) == 1
        ring = sent["rings"][0]
        area = abs(
            sum(
                ring[i][0] * ring[i + 1][1] - ring[i + 1][0] * ring[i][1]
                for i in range(len(ring) - 1)
            )
            / 2
        )
        assert area == pytest.approx(7.0)


# ── buffered aggregation ──────────────────────────────────────────────


def _square(x0, y0, size=0.001):
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [x0, y0],
                [x0, y0 + size],
                [x0 + size, y0 + size],
                [x0 + size, y0],
                [x0, y0],
            ]
        ],
    }


def _pt(x, y, **props):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [x, y]},
        "properties": props,
    }


def _agg(plugin, polygons, features, geometry_type="esriGeometryPoint",
         fields=("POP",)):
    meta = {
        "geometryType": geometry_type,
        "fields": [
            {"name": f, "type": "esriFieldTypeInteger"} for f in fields
        ],
    }
    return (
        patch.object(
            plugin,
            "_fetch_aggregation_polygons",
            AsyncMock(return_value=polygons),
        ),
        patch.object(
            plugin, "_resolve_layer_url", AsyncMock(return_value=LAYER)
        ),
        patch.object(
            plugin, "_fetch_layer_meta", AsyncMock(return_value=meta)
        ),
        patch.object(
            plugin, "_paged_geojson_fetch", AsyncMock(return_value=features)
        ),
    )


def _args(**over):
    base = {
        "source_item_id": "a" * 32,
        "aggregation_item_id": "b" * 32,
        "group_by_field": "NAME",
        "sum_fields": ["POP"],
    }
    base.update(over)
    return base


async def _run(plugin, polygons, features, args, **kw):
    p1, p2, p3, p4 = _agg(plugin, polygons, features, **kw)
    with p1, p2, p3, p4:
        return await plugin._aggregate_by_polygon(args)


# Two parks about 220 m apart at the equator (0.001 deg ~ 111 m); a
# point midway is within 200 m of both, a second sits inside Park B.
PARKS = [
    {"group": "Park A", "geometry": _square(0.000, 0.0)},
    {"group": "Park B", "geometry": _square(0.003, 0.0)},
]
BETWEEN = _pt(0.002, 0.0005, POP=1000)
IN_B = _pt(0.0035, 0.0005, POP=500)


class TestBufferedAggregationIsPerPolygon:
    @pytest.mark.asyncio
    async def test_default_counts_a_feature_for_every_polygon_in_range(
        self, plugin
    ):
        _, s = await _run(
            plugin,
            PARKS,
            [BETWEEN, IN_B],
            _args(buffer_distance=200, buffer_units="meters"),
        )
        _validate(ExamplePlugin.AGGREGATE_OUTPUT_SCHEMA, s)
        rows = {r["group"]: r for r in s["rows"]}
        assert rows["Park A"]["sums"]["POP"] == 1000
        assert rows["Park B"]["sums"]["POP"] == 1500
        assert s["query"]["overlap_policy"] == "all_matches"
        codes = {c["code"]: c for c in s["caveats"]}
        assert codes["counted_in_multiple_buckets"]["count"] == 1

    @pytest.mark.asyncio
    async def test_explicit_first_match_is_kept_and_explained(self, plugin):
        text, s = await _run(
            plugin,
            PARKS,
            [BETWEEN, IN_B],
            _args(
                buffer_distance=200,
                buffer_units="meters",
                overlap_policy="first_match",
            ),
        )
        assert s["query"]["overlap_policy"] == "first_match"
        codes = [c["code"] for c in s["caveats"]]
        assert "first_match_with_buffer" in codes
        assert "not the nearest" in text

    @pytest.mark.asyncio
    async def test_no_buffer_still_defaults_to_first_match(self, plugin):
        _, s = await _run(plugin, PARKS, [IN_B], _args())
        assert s["query"]["overlap_policy"] == "first_match"
        codes = [c["code"] for c in s["caveats"]]
        assert "counted_in_multiple_buckets" not in codes
        assert "first_match_with_buffer" not in codes

    @pytest.mark.asyncio
    async def test_same_group_twice_counts_the_feature_once(self, plugin):
        """Two polygons of one group (a multi-part park stored as two
        rows) both in range: one feature, one count."""
        polys = [
            {"group": "Park A", "geometry": _square(0.000, 0.0)},
            {"group": "Park A", "geometry": _square(0.003, 0.0)},
        ]
        _, s = await _run(
            plugin,
            polys,
            [BETWEEN],
            _args(buffer_distance=200, buffer_units="meters"),
        )
        assert s["rows"] == [
            {
                "group": "Park A",
                "sums": {"POP": 1000.0},
                "count": 1,
                "small_sample": True,
            }
        ]
        assert "counted_in_multiple_buckets" not in [
            c["code"] for c in s["caveats"]
        ]


class TestAggregationBookkeeping:
    @pytest.mark.asyncio
    async def test_duplicate_sum_fields_are_summed_once(self, plugin):
        _, s = await _run(
            plugin, PARKS, [IN_B], _args(sum_fields=["POP", "POP"])
        )
        assert s["rows"][0]["sums"] == {"POP": 500.0}
        assert s["query"]["sum_fields"] == ["POP"]

    @pytest.mark.asyncio
    async def test_a_field_named_count_does_not_touch_the_count(
        self, plugin
    ):
        feats = [_pt(0.0035, 0.0005, count=5), _pt(0.0036, 0.0005, count=5)]
        _, s = await _run(
            plugin,
            PARKS,
            feats,
            _args(sum_fields=["count"]),
            fields=("count",),
        )
        _validate(ExamplePlugin.AGGREGATE_OUTPUT_SCHEMA, s)
        assert s["rows"][0]["count"] == 2
        assert s["rows"][0]["sums"] == {"count": 10.0}

    @pytest.mark.asyncio
    async def test_features_without_geometry_are_reported(self, plugin):
        feats = [
            IN_B,
            {"type": "Feature", "geometry": None, "properties": {"POP": 9}},
        ]
        text, s = await _run(plugin, PARKS, feats, _args())
        codes = {c["code"]: c for c in s["caveats"]}
        assert codes["source_features_without_geometry"]["count"] == 1
        assert "no usable geometry" in text

    @pytest.mark.asyncio
    async def test_line_sources_say_they_are_assigned_whole(self, plugin):
        line = {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [[0.0031, 0.0005], [0.0039, 0.0005]],
            },
            "properties": {"POP": 3},
        }
        text, s = await _run(
            plugin,
            PARKS,
            [line],
            _args(),
            geometry_type="esriGeometryPolyline",
        )
        codes = [c["code"] for c in s["caveats"]]
        assert "whole_feature_assignment" in codes
        assert "midpoint" in text

    @pytest.mark.asyncio
    async def test_point_sources_carry_no_assignment_caveat(self, plugin):
        _, s = await _run(plugin, PARKS, [IN_B], _args())
        assert "whole_feature_assignment" not in [
            c["code"] for c in s["caveats"]
        ]


# ── centroid precision ────────────────────────────────────────────────


class TestCentroidOfSmallPolygons:
    @pytest.mark.parametrize("size_m", [3, 10, 30, 100, 1000])
    @pytest.mark.parametrize(
        "lon,lat", [(-149.899958, 61.200001), (-148.9, 60.95), (179.5, 52)]
    )
    def test_square_centroid_is_its_centre(self, size_m, lon, lat):
        dlat = size_m / 111320.0
        dlon = size_m / (111320.0 * math.cos(math.radians(lat)))
        ring = [
            [lon, lat],
            [lon + dlon, lat],
            [lon + dlon, lat + dlat],
            [lon, lat + dlat],
            [lon, lat],
        ]
        cx, cy = ExamplePlugin._ring_centroid(ring)
        err_m = math.hypot(
            (cx - (lon + dlon / 2)) * 111320 * math.cos(math.radians(lat)),
            (cy - (lat + dlat / 2)) * 111320,
        )
        # Was a median 146 m off (and outside the ring) for a 10 m
        # square at Anchorage.
        assert err_m < 0.01

    def test_representative_point_is_inside_a_building(self):
        lon, lat = -149.899958, 61.200001
        ring = [
            [lon, lat],
            [lon + 0.000186, lat],
            [lon + 0.000186, lat + 0.00009],
            [lon, lat + 0.00009],
            [lon, lat],
        ]
        geom = {"type": "Polygon", "coordinates": [ring]}
        x, y = ExamplePlugin._feature_to_point(geom, "auto")
        # Strictly inside, not the first-vertex fallback on the corner.
        assert lon < x < lon + 0.000186
        assert lat < y < lat + 0.00009


# ── argument hygiene ──────────────────────────────────────────────────


class TestArguments:
    @pytest.mark.parametrize("bad", [0, -2, "0", -1])
    def test_limits_have_a_floor(self, bad):
        with pytest.raises(ToolInputError, match="at least 1"):
            ExamplePlugin._int_arg({"limit": bad}, "limit", 10)

    @pytest.mark.parametrize("bad", [float("inf"), float("nan"), True, "x"])
    def test_non_integers_are_refused_by_name(self, bad):
        with pytest.raises(ToolInputError, match="limit must be an integer"):
            ExamplePlugin._int_arg({"limit": bad}, "limit", 10)

    def test_null_means_the_default(self):
        assert ExamplePlugin._int_arg({"limit": None}, "limit", 10) == 10
        assert ExamplePlugin._float_arg({"d": None}, "d", 1.5) == 1.5

    def test_layer_index_may_be_zero(self):
        assert ExamplePlugin._int_arg({"layer_index": 0}, "layer_index") == 0
        with pytest.raises(ToolInputError, match="at least 0"):
            ExamplePlugin._int_arg({"layer_index": -1}, "layer_index")

    @pytest.mark.parametrize("bad", ["nan", "inf", "-inf", float("nan")])
    def test_non_finite_floats_are_refused(self, bad):
        with pytest.raises(ToolInputError, match="finite"):
            ExamplePlugin._float_arg({"d": bad}, "d")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad", ["nan", "inf"])
    async def test_non_finite_distance_never_reaches_upstream(
        self, plugin, bad
    ):
        plugin._send = AsyncMock()
        with pytest.raises(ToolInputError, match="finite"):
            await plugin.spatial_query_polygon(
                ITEM, filter_geometry=BOX, distance=bad
            )
        plugin._send.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name", ["max_coverage_pct", "min_coverage_pct"]
    )
    async def test_non_finite_coverage_band_is_refused(self, plugin, name):
        with pytest.raises(ToolInputError, match="finite"):
            await plugin._coverage_by_polygon(
                {
                    "target_item_id": "a" * 32,
                    "overlay_item_id": "b" * 32,
                    "target_id_field": "ID",
                    name: "nan",
                }
            )

    @pytest.mark.asyncio
    async def test_non_finite_buffer_is_refused(self, plugin):
        with pytest.raises(ToolInputError, match="finite"):
            await _run(plugin, PARKS, [IN_B], _args(buffer_distance="inf"))

    def test_null_arguments_are_dropped(self, plugin):
        cleaned = plugin._clean_arguments(
            "get_layer_schema", {"item_id": ITEM, "service_url": None}
        )
        assert cleaned == {"item_id": ITEM}

    def test_a_list_of_field_names_is_joined(self, plugin):
        cleaned = plugin._clean_arguments(
            "query_data", {"item_id": ITEM, "out_fields": ["A", "B"]}
        )
        assert cleaned["out_fields"] == "A,B"

    @pytest.mark.asyncio
    async def test_a_wrong_typed_string_argument_is_named(self, plugin):
        result = await plugin.execute_tool("query_data", {"item_id": 12345})
        assert not result.success
        assert result.error_message == "item_id must be a string (got int)"

    @pytest.mark.asyncio
    async def test_explicit_null_keyword_does_not_crash(self, plugin):
        plugin._run_search = AsyncMock(return_value=[])
        plugin._search_gallery = AsyncMock(return_value=[])
        result = await plugin.execute_tool(
            "browse_gallery", {"keyword": None, "limit": None}
        )
        assert "NoneType" not in (result.error_message or "")
        assert "strip" not in (result.error_message or "")

    @pytest.mark.asyncio
    async def test_zero_limit_is_an_error_not_an_empty_answer(self, plugin):
        plugin._run_search = AsyncMock(return_value=[])
        result = await plugin.execute_tool(
            "find_gis_content", {"topic": "parks", "limit": 0}
        )
        assert not result.success
        assert "limit must be at least 1" in result.error_message


# ── paging and caps ───────────────────────────────────────────────────


def _features(start, n):
    return [
        {"type": "Feature", "id": i, "geometry": None, "properties": {}}
        for i in range(start, start + n)
    ]


class TestPagedFetch:
    @pytest.mark.asyncio
    async def test_short_flagged_pages_are_followed_to_the_end(
        self, plugin
    ):
        """A layer whose maxRecordCount (500) is under the page size:
        every page is 'short', each flagged exceededTransferLimit."""
        pages = [
            {"features": _features(0, 500), "exceededTransferLimit": True},
            {
                "features": _features(500, 500),
                "properties": {"exceededTransferLimit": True},
            },
            {"features": _features(1000, 300)},
        ]
        plugin._request_json_with_retry = AsyncMock(side_effect=pages)
        got = await plugin._paged_geojson_fetch(LAYER, "1=1", "*", 5000)
        assert len(got) == 1300
        offsets = [
            c.kwargs["params"]["resultOffset"]
            for c in plugin._request_json_with_retry.call_args_list
        ]
        assert offsets == ["0", "500", "1000"]

    @pytest.mark.asyncio
    async def test_unflagged_short_page_still_ends_the_fetch(self, plugin):
        plugin._request_json_with_retry = AsyncMock(
            side_effect=[{"features": _features(0, 40)}]
        )
        got = await plugin._paged_geojson_fetch(LAYER, "1=1", "*", 5000)
        assert len(got) == 40

    @pytest.mark.asyncio
    async def test_a_server_that_ignores_the_offset_is_not_looped(
        self, plugin
    ):
        same = {"features": _features(0, 500), "exceededTransferLimit": True}
        plugin._request_json_with_retry = AsyncMock(return_value=same)
        got = await plugin._paged_geojson_fetch(LAYER, "1=1", "*", 5000)
        assert len(got) == 500
        assert plugin._request_json_with_retry.call_count == 2


class TestCapsAreLoud:
    @pytest.mark.asyncio
    async def test_too_many_aggregation_polygons_is_refused(self, plugin):
        plugin._resolve_layer_url = AsyncMock(return_value=LAYER)
        plugin._fetch_layer_meta = AsyncMock(
            return_value={
                "geometryType": "esriGeometryPolygon",
                "fields": [{"name": "NAME"}],
            }
        )
        full = [
            {
                "type": "Feature",
                "geometry": _square(0, 0),
                "properties": {"NAME": str(i)},
            }
            for i in range(plugin.AGG_SOURCE_LIMIT)
        ]
        plugin._paged_geojson_fetch = AsyncMock(return_value=full)
        with pytest.raises(ToolInputError, match="Narrow agg_where"):
            await plugin._fetch_aggregation_polygons("b" * 32, "NAME", "1=1")
        # Nothing partial was cached for the next caller.
        assert not plugin._agg_layer_cache

    @pytest.mark.asyncio
    async def test_distinct_values_cut_page_with_a_null_is_flagged(
        self, plugin
    ):
        plugin._resolve_layer_url = AsyncMock(return_value=LAYER)
        plugin._fetch_layer_meta = AsyncMock(
            return_value={
                "fields": [{"name": "ZONE", "type": "esriFieldTypeString"}]
            }
        )
        rows = [{"attributes": {"ZONE": v}} for v in (None, "A", "B", "C", "D")]
        plugin._send = AsyncMock(side_effect=[_resp({"features": rows})])
        text, s = await plugin._get_distinct_values(
            {"item_id": ITEM, "field": "ZONE", "limit": 5}
        )
        assert s["summary"] == {"returned": 4, "truncated": True}
        assert "truncated to limit" in text

    @pytest.mark.asyncio
    async def test_distinct_values_below_the_cap_is_complete(self, plugin):
        plugin._resolve_layer_url = AsyncMock(return_value=LAYER)
        plugin._fetch_layer_meta = AsyncMock(
            return_value={
                "fields": [{"name": "ZONE", "type": "esriFieldTypeString"}]
            }
        )
        rows = [{"attributes": {"ZONE": v}} for v in (None, "A", "B")]
        plugin._send = AsyncMock(side_effect=[_resp({"features": rows})])
        _, s = await plugin._get_distinct_values(
            {"item_id": ITEM, "field": "ZONE", "limit": 5}
        )
        assert s["summary"] == {"returned": 2, "truncated": False}


class TestSearchLayersByFieldCountsWhatItRead:
    def _item(self, url):
        return {"id": ITEM, "title": "Zoning", "url": url}

    @pytest.mark.asyncio
    async def test_item_url_that_is_already_a_layer(self, plugin):
        """.../FeatureServer/0 is the layer itself; asking for .../0/0
        returned an error body that read as 'no matching fields'."""
        plugin._run_search = AsyncMock(return_value=[self._item(LAYER)])
        layer_json = {
            "id": 0,
            "name": "Zoning",
            "fields": [{"name": "ZONING", "alias": None}],
        }
        plugin._send = AsyncMock(side_effect=[_resp(layer_json)])
        _, s = await plugin._search_layers_by_field(
            {"field_keyword": "zoning"}
        )
        assert plugin._send.call_count == 1
        assert s["summary"]["layers_matched"] == 1
        assert s["summary"]["services_failed"] == 0

    @pytest.mark.asyncio
    async def test_error_body_is_a_failure_not_a_clean_miss(self, plugin):
        root = LAYER.rsplit("/", 1)[0]
        plugin._run_search = AsyncMock(return_value=[self._item(root)])
        plugin._send = AsyncMock(
            side_effect=[
                _resp({"error": {"code": 499, "message": "Token Required"}})
            ]
        )
        _, s = await plugin._search_layers_by_field(
            {"field_keyword": "zoning"}
        )
        assert s["summary"]["layers_matched"] == 0
        assert s["summary"]["services_failed"] == 1


# ── coverage across two projections ───────────────────────────────────


def _cov_meta(wkid):
    return {
        "geometryType": "esriGeometryPolygon",
        "extent": {"spatialReference": {"wkid": wkid, "latestWkid": wkid}},
        "fields": [
            {"name": "PID", "type": "esriFieldTypeString"},
            {"name": "Shape__Area", "type": "esriFieldTypeDouble"},
        ],
    }


def _box(x0, y0, x1, y1):
    return {
        "type": "Polygon",
        "coordinates": [[[x0, y0], [x0, y1], [x1, y1], [x1, y0], [x0, y0]]],
    }


async def _run_cov(plugin, targets, overlays, tmeta, ometa, args):
    async def paged(url, where, out_fields, limit, extra_params=None,
                    method="get"):
        return targets if extra_params is None else overlays

    metas = iter([tmeta, ometa])

    async def meta(url, require_fields=()):
        return next(metas)

    with patch.object(
        plugin, "_resolve_layer_url", AsyncMock(return_value=LAYER)
    ), patch.object(plugin, "_fetch_layer_meta", new=meta), patch.object(
        plugin, "_paged_geojson_fetch", new=paged
    ):
        base = {
            "target_item_id": "a" * 32,
            "overlay_item_id": "b" * 32,
            "target_id_field": "PID",
        }
        base.update(args)
        return await plugin._coverage_by_polygon(base)


class _AlbersPlugin(ExamplePlugin):
    """A deployment that declares Alaska Albers as equal-area metres."""

    EQUAL_AREA_METRE_WKIDS = frozenset({3338})


class TestCoverageAcrossProjections:
    LAT = 61.2
    INFLATION = 1 / math.cos(math.radians(61.2)) ** 2

    @pytest.fixture
    def plugin(self, plugin):
        albers = _AlbersPlugin(plugin.config)
        albers.plugin_config = plugin.plugin_config
        return albers

    def _layers(self, target_area, overlay_area):
        lat = self.LAT
        targets = [
            {
                "id": 1,
                "geometry": _box(-150, lat - 0.001, -149.998, lat + 0.001),
                "properties": {"PID": "P1", "Shape__Area": target_area},
            }
        ]
        overlays = [
            {
                "id": 10,
                "geometry": _box(
                    -149.9995, lat - 0.0002, -149.999, lat + 0.0002
                ),
                "properties": {"Shape__Area": overlay_area},
            }
        ]
        return targets, overlays

    @pytest.mark.asyncio
    async def test_mercator_target_with_equal_area_overlay(self, plugin):
        """True coverage 50%: 1,000 m2 lot (stored inflated, Web
        Mercator) and a 500 m2 building (stored true, Alaska Albers).
        The raw ratio said 11.6% and put the lot in an under-40% band."""
        targets, overlays = self._layers(1000.0 * self.INFLATION, 500.0)
        _, s = await _run_cov(
            plugin,
            targets,
            overlays,
            _cov_meta(3857),
            _cov_meta(3338),
            {"max_coverage_pct": 40},
        )
        assert s["summary"]["in_band"] == 0
        codes = {c["code"]: c["message"] for c in s["caveats"]}
        assert "different projections" in codes["area_units_corrected"]
        assert "unaffected" not in codes["area_units_corrected"]

        _, s = await _run_cov(
            plugin,
            targets,
            overlays,
            _cov_meta(3857),
            _cov_meta(3338),
            {},
        )
        assert s["rows"][0]["coverage_pct"] == pytest.approx(50.0, abs=0.1)

    @pytest.mark.asyncio
    async def test_same_projection_is_unchanged(self, plugin):
        targets, overlays = self._layers(
            1000.0 * self.INFLATION, 500.0 * self.INFLATION
        )
        _, s = await _run_cov(
            plugin, targets, overlays, _cov_meta(3857), _cov_meta(3857), {}
        )
        assert s["rows"][0]["coverage_pct"] == pytest.approx(50.0, abs=0.1)
        codes = {c["code"]: c["message"] for c in s["caveats"]}
        assert "unaffected" in codes["area_units_corrected"]

    @pytest.mark.asyncio
    async def test_unconvertible_mix_is_called_unreliable(self, plugin):
        targets, overlays = self._layers(1000.0 * self.INFLATION, 500.0)
        _, s = await _run_cov(
            plugin, targets, overlays, _cov_meta(3857), _cov_meta(26935), {}
        )
        codes = [c["code"] for c in s["caveats"]]
        assert "mixed_projections" in codes
        # ...and nothing alongside it claims the percentage is sound.
        assert "area_units_corrected" not in codes
