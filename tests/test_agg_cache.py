"""The aggregation-layer cache, now that it outlives a request.

Until opencontext-core 1.5.0 a plugin object lived for one Lambda request,
so this cache was never reused and nothing about it could go wrong. Since
1.5.0 one object serves a warm container for hours. A review of the
deployed Anchorage GIS server then found a cached boundary set served for
24 hours with no freshness check, and a cache bounded by entry count but
not by size (one ~5,000-polygon set measured ~9 MB in memory).
"""

import json
from unittest.mock import AsyncMock, patch

import pytest

from tests.profile import ExampleConfig, ExamplePlugin

_AGG_ID = "b" * 32


@pytest.fixture
def plugin():
    cfg = {
        "portal_base_url": "https://example.maps.arcgis.com/sharing/rest",
        "gallery_group_id": "abc123",
        "org_id": "Ce3DhLRthdwbHlfF",
        "city_name": "Municipality of Anchorage",
        "gallery_url": "https://example.com/gallery",
        "timeout": 20,
    }
    p = ExamplePlugin(cfg)
    p.plugin_config = ExampleConfig(**cfg)
    return p

URL = "https://services.arcgis.com/Ce3DhLRthdwbHlfF/FeatureServer/0"


def _meta(edit_date=None):
    meta = {
        "geometryType": "esriGeometryPolygon",
        "fields": [{"name": "COUNCIL", "type": "esriFieldTypeString"}],
    }
    if edit_date is not None:
        meta["editingInfo"] = {"lastEditDate": edit_date}
    return meta


def _features(name="Midtown", vertices=5):
    ring = [[i, i % 2] for i in range(vertices - 1)] + [[0, 0]]
    return [
        {
            "geometry": {"type": "Polygon", "coordinates": [ring]},
            "properties": {"COUNCIL": name},
        }
    ]


def _patched(plugin, meta, paged):
    return (
        patch.object(plugin, "_resolve_layer_url", new=AsyncMock(return_value=URL)),
        patch.object(plugin, "_fetch_layer_meta", new=meta),
        patch.object(plugin, "_paged_geojson_fetch", new=paged),
    )


class TestACachedSetIsOnlyReusedWhileTheLayerIsUnchanged:
    @pytest.mark.asyncio
    async def test_an_edited_layer_is_refetched(self, plugin):
        meta = AsyncMock(side_effect=[_meta(1000), _meta(1000), _meta(2000)])
        paged = AsyncMock(
            side_effect=[_features("Old"), _features("New")]
        )
        a, b, c = _patched(plugin, meta, paged)
        with a, b, c:
            first = await plugin._fetch_aggregation_polygons(_AGG_ID, "COUNCIL", "1=1")
            again = await plugin._fetch_aggregation_polygons(_AGG_ID, "COUNCIL", "1=1")
            after_edit = await plugin._fetch_aggregation_polygons(
                _AGG_ID, "COUNCIL", "1=1"
            )
        assert first[0]["group"] == "Old" and again is first
        # The layer's edit date moved: the old boundaries are not served.
        assert after_edit[0]["group"] == "New"
        assert paged.await_count == 2
        assert len(plugin._agg_layer_cache) == 1

    @pytest.mark.asyncio
    async def test_a_layer_with_no_edit_date_gets_the_short_ttl(self, plugin):
        a, b, c = _patched(
            plugin, AsyncMock(return_value=_meta()), AsyncMock(return_value=_features())
        )
        with a, b, c, patch("arcgis_hub.plugin.time.time", return_value=1_000_000.0):
            await plugin._fetch_aggregation_polygons(_AGG_ID, "COUNCIL", "1=1")
        (expires, _polygons, stamp, _size), = plugin._agg_layer_cache.values()
        assert stamp is None
        assert expires == 1_000_000.0 + plugin.AGG_CACHE_TTL_NO_EDIT_DATE_SECONDS
        assert plugin.AGG_CACHE_TTL_NO_EDIT_DATE_SECONDS < plugin.AGG_CACHE_TTL_SECONDS

    @pytest.mark.asyncio
    async def test_a_layer_with_an_edit_date_keeps_the_long_ttl(self, plugin):
        a, b, c = _patched(
            plugin,
            AsyncMock(return_value=_meta(1000)),
            AsyncMock(return_value=_features()),
        )
        with a, b, c, patch("arcgis_hub.plugin.time.time", return_value=1_000_000.0):
            await plugin._fetch_aggregation_polygons(_AGG_ID, "COUNCIL", "1=1")
        (expires, _polygons, stamp, _size), = plugin._agg_layer_cache.values()
        assert stamp == 1000
        assert expires == 1_000_000.0 + plugin.AGG_CACHE_TTL_SECONDS


class TestTheCacheIsBoundedBySize:
    @pytest.mark.asyncio
    async def test_total_size_evicts_least_recently_used(self, plugin):
        features = _features(vertices=400)
        one = len(
            json.dumps(
                [{"group": "Midtown", "geometry": features[0]["geometry"]}],
                separators=(",", ":"),
            )
        )
        plugin.AGG_CACHE_MAX_BYTES = one * 3 + 10  # room for three sets
        a, b, c = _patched(
            plugin,
            AsyncMock(return_value=_meta(1000)),
            AsyncMock(return_value=features),
        )
        with a, b, c:
            for i in range(6):
                await plugin._fetch_aggregation_polygons(
                    _AGG_ID, "COUNCIL", f"OBJECTID<>{i}"
                )
        # Far below the 32-entry cap, yet bounded.
        assert len(plugin._agg_layer_cache) == 3
        wheres = [key[2] for key in plugin._agg_layer_cache]
        assert wheres == ["OBJECTID<>3", "OBJECTID<>4", "OBJECTID<>5"]
        assert (
            sum(entry[3] for entry in plugin._agg_layer_cache.values())
            <= plugin.AGG_CACHE_MAX_BYTES
        )

    @pytest.mark.asyncio
    async def test_a_set_larger_than_the_cap_is_returned_but_not_kept(self, plugin):
        plugin.AGG_CACHE_MAX_BYTES = 50
        paged = AsyncMock(return_value=_features(vertices=400))
        a, b, c = _patched(plugin, AsyncMock(return_value=_meta(1000)), paged)
        with a, b, c:
            first = await plugin._fetch_aggregation_polygons(_AGG_ID, "COUNCIL", "1=1")
            await plugin._fetch_aggregation_polygons(_AGG_ID, "COUNCIL", "1=1")
        assert len(first) == 1  # the caller still gets the polygons
        assert not plugin._agg_layer_cache
        assert paged.await_count == 2

    @pytest.mark.asyncio
    async def test_an_oversized_set_does_not_evict_the_others(self, plugin):
        small = _features(vertices=5)
        big = _features(vertices=4000)
        small_size = len(
            json.dumps(
                [{"group": "Midtown", "geometry": small[0]["geometry"]}],
                separators=(",", ":"),
            )
        )
        plugin.AGG_CACHE_MAX_BYTES = small_size * 4
        paged = AsyncMock(side_effect=[small, small, big])
        a, b, c = _patched(plugin, AsyncMock(return_value=_meta(1000)), paged)
        with a, b, c:
            await plugin._fetch_aggregation_polygons(_AGG_ID, "COUNCIL", "OBJECTID<>1")
            await plugin._fetch_aggregation_polygons(_AGG_ID, "COUNCIL", "OBJECTID<>2")
            await plugin._fetch_aggregation_polygons(_AGG_ID, "COUNCIL", "OBJECTID<>3")
        assert [key[2] for key in plugin._agg_layer_cache] == [
            "OBJECTID<>1",
            "OBJECTID<>2",
        ]
