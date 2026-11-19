"""Errors a live probe of the tools found unclear or silent (2026-11-19).

- A layer index the service does not have came back as "Tool execution
  failed (RuntimeError)": ArcGIS sends an empty message for it and puts
  the reason in `details`.
- An inline polygon in projected coordinates matched nothing and was
  reported as "no features".
- An item of another account with no orgId was said to belong to
  "org None".
- Two enum arguments accepted any value.
"""

from unittest.mock import AsyncMock, Mock

import pytest

from core.interfaces import ToolInputError
from tests.profile import ExampleConfig, ExamplePlugin

SERVICE = "https://services.arcgis.com/org123/arcgis/rest/services/X/FeatureServer"


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


def _resp(payload):
    r = Mock()
    r.status_code = 200
    r.raise_for_status = Mock()
    r.json = Mock(return_value=payload)
    return r


MISSING_LAYER = {
    "error": {
        "code": 400,
        "message": "",
        "details": ["The requested layer (layerId: 9) was not found."],
    }
}


class TestMissingLayerIndex:
    @pytest.mark.asyncio
    async def test_metadata_error_with_empty_message_uses_details(self, plugin):
        plugin._send = AsyncMock(return_value=_resp(MISSING_LAYER))
        with pytest.raises(RuntimeError) as e:
            await plugin._get_meta_json(f"{SERVICE}/9")
        assert "was not found" in str(e.value)

    @pytest.mark.asyncio
    async def test_metadata_error_with_a_message_is_unchanged(self, plugin):
        plugin._send = AsyncMock(
            return_value=_resp({"error": {"code": 400, "message": "Item does not exist"}})
        )
        with pytest.raises(RuntimeError) as e:
            await plugin._get_meta_json(f"{SERVICE}/0")
        assert str(e.value) == "Item does not exist"

    @pytest.mark.asyncio
    async def test_schema_names_the_layers_that_exist(self, plugin):
        root = {
            "layers": [{"id": 0, "name": "Parcels"}],
            "tables": [{"id": 3, "name": "Owners"}],
        }

        async def send(method, url, **kw):
            return _resp(MISSING_LAYER if url.endswith("/9") else root)

        plugin._send = AsyncMock(side_effect=send)
        with pytest.raises(ToolInputError) as e:
            await plugin._get_layer_schema(
                {"service_url": SERVICE, "layer_index": 9}
            )
        text = str(e.value)
        assert "layer_index 9 does not exist" in text
        assert "0 (Parcels)" in text and "3 (Owners)" in text

    @pytest.mark.asyncio
    async def test_schema_still_explains_when_the_root_is_unreadable(self, plugin):
        plugin._send = AsyncMock(return_value=_resp(MISSING_LAYER))
        with pytest.raises(ToolInputError) as e:
            await plugin._get_layer_schema(
                {"service_url": SERVICE, "layer_index": 9}
            )
        assert "layer_index 9 does not exist" in str(e.value)

    @pytest.mark.asyncio
    async def test_other_metadata_failures_are_not_relabelled(self, plugin):
        plugin._send = AsyncMock(
            return_value=_resp({"error": {"code": 499, "message": "Token Required"}})
        )
        with pytest.raises(RuntimeError) as e:
            await plugin._get_layer_schema(
                {"service_url": SERVICE, "layer_index": 0}
            )
        assert not isinstance(e.value, ToolInputError)
        assert "Token Required" in str(e.value)


def _ring(*pts):
    return [list(p) for p in pts]


class TestInlinePolygonIsLonLat:
    def test_lon_lat_passes(self):
        ExamplePlugin._check_lonlat_rings(
            [_ring((-149.9, 61.2), (-149.8, 61.2), (-149.8, 61.25), (-149.9, 61.2))]
        )

    def test_antimeridian_edge_passes(self):
        ExamplePlugin._check_lonlat_rings(
            [_ring((179.5, 51.0), (180.0, 51.0), (180.0, 52.0), (179.5, 51.0))]
        )

    def test_web_mercator_is_refused(self):
        with pytest.raises(ToolInputError) as e:
            ExamplePlugin._check_lonlat_rings(
                [_ring((-16686000, 8660000), (-16680000, 8660000),
                       (-16680000, 8665000), (-16686000, 8660000))]
            )
        assert "EPSG:4326" in str(e.value)

    def test_latitude_first_is_refused(self):
        with pytest.raises(ToolInputError):
            ExamplePlugin._check_lonlat_rings(
                [_ring((61.2, -149.9), (61.2, -149.8), (61.25, -149.8), (61.2, -149.9))]
            )

    def test_short_ring_is_refused(self):
        with pytest.raises(ToolInputError) as e:
            ExamplePlugin._check_lonlat_rings([_ring((-149.9, 61.2), (-149.8, 61.2))])
        assert "at least 4" in str(e.value)

    @pytest.mark.parametrize("pos", [["a", "b"], [1], None, [True, 2], [float("nan"), 1]])
    def test_non_numeric_position_is_refused(self, pos):
        ring = [[-149.9, 61.2], [-149.8, 61.2], pos, [-149.9, 61.2]]
        with pytest.raises(ToolInputError):
            ExamplePlugin._check_lonlat_rings([ring])

    @pytest.mark.asyncio
    async def test_spatial_query_polygon_refuses_before_any_request(self, plugin):
        plugin._send = AsyncMock()
        plugin._resolve_layer_url = AsyncMock(return_value=f"{SERVICE}/0")
        geom = {
            "type": "Polygon",
            "coordinates": [_ring((-16686000, 8660000), (-16680000, 8660000),
                                  (-16680000, 8665000), (-16686000, 8660000))],
        }
        with pytest.raises(ToolInputError):
            await plugin.spatial_query_polygon(
                "a" * 32, filter_geometry=geom
            )


class TestForeignItemMessage:
    def test_names_the_org_when_known(self, plugin):
        text = plugin._foreign_org_message({"id": "x", "orgId": "other"})
        assert "belongs to org 'other'" in text

    def test_no_org_id_does_not_say_org_none(self, plugin):
        text = plugin._foreign_org_message({"id": "x"})
        assert "None" not in text
        assert "another ArcGIS account" in text


class TestEnumArguments:
    @pytest.mark.asyncio
    async def test_layer_type_is_checked(self, plugin):
        with pytest.raises(ToolInputError) as e:
            await plugin._search_spatial_layers("parks", "maps", 5)
        assert "layer_type" in str(e.value)

    @pytest.mark.asyncio
    async def test_date_format_is_checked(self, plugin):
        plugin._resolve_layer_url = AsyncMock(return_value=f"{SERVICE}/0")
        result = await plugin.execute_tool(
            "query_data", {"item_id": "a" * 32, "date_format": "iso"}
        )
        assert result.success is False
        assert "date_format" in (result.error_message or "")
