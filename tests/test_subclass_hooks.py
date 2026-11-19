"""The seams a deployment customises the base class through.

Everything else in this suite runs against the bare base. These tests
subclass it the way the Anchorage and Alaska deployments do -- extra
tools, wording, partner organisations, several gallery groups, an
equal-area projection, an error explanation -- and check each hook is
actually consulted.
"""

from typing import List
from unittest.mock import AsyncMock

import pytest
from pydantic import Field

from arcgis_hub.config_schema import HubConfigBase, PartnerOrg
from arcgis_hub.plugin import ArcGISHubPlugin
from core.interfaces import ToolDefinition, ToolInputError
from tests.profile import ExampleConfig, ExamplePlugin

ORG = "Ce3DhLRthdwbHlfF"
PARTNER = "r4A0V7UzH9fcLVvv"
GROUP_A = "a" * 32
GROUP_B = "b" * 32

BASE_CFG = {
    "portal_base_url": "https://example.maps.arcgis.com/sharing/rest",
    "org_id": ORG,
    "city_name": "Example County",
    "gallery_url": "https://example.maps.arcgis.com/apps/gallery",
    "timeout": 30,
}


class MultiConfig(HubConfigBase):
    gallery_group_ids: List[str] = Field(..., min_length=1)
    partner_orgs: List[PartnerOrg] = Field(default_factory=list)


class RegionalPlugin(ArcGISHubPlugin):
    plugin_name = "regional"
    CONFIG_CLASS = MultiConfig
    REGION_NAME = "Exampleland"
    EQUAL_AREA_METRE_WKIDS = frozenset({3338})
    TEXT = {
        **ArcGISHubPlugin.TEXT,
        "limited_coverage_hint_short": " -- regional layers are partial",
        "limited_coverage_hint_long": " -- a regional layer is partial",
    }
    TOOL_TITLES = {**ArcGISHubPlugin.TOOL_TITLES, "say_hello": "Say Hello"}
    TOOL_OUTPUT_SCHEMAS = {
        **ArcGISHubPlugin.TOOL_OUTPUT_SCHEMAS,
        "say_hello": {"type": "object"},
    }

    def _tool_definitions(self):
        tools = super()._tool_definitions()
        tools.append(
            ToolDefinition(
                name="say_hello",
                description="Greets.",
                input_schema={"type": "object", "properties": {}},
            )
        )
        return tools

    async def _execute_extra_tool(self, tool_name, arguments):
        if tool_name == "say_hello":
            if arguments.get("rude"):
                raise ToolInputError("be polite")
            return "hello", {"greeting": "hello"}
        return None

    @classmethod
    def _explain_unresolved_error(cls, full, item_arg, where_clause):
        if "Unable to complete" in full:
            return f"explained: {where_clause} / {item_arg}"
        return None


def _regional(partners=True, groups=(GROUP_A, GROUP_B)):
    cfg = dict(BASE_CFG, gallery_group_ids=list(groups))
    if partners:
        cfg["partner_orgs"] = [
            {
                "org_id": PARTNER,
                "name": "Partner Borough",
                "hosts": ["maps.partner.example"],
            }
        ]
    p = RegionalPlugin(cfg)
    p.plugin_config = MultiConfig(**cfg)
    return p


def _example():
    cfg = dict(BASE_CFG, gallery_group_id=GROUP_A)
    p = ExamplePlugin(cfg)
    p.plugin_config = ExampleConfig(**cfg)
    return p


class TestExtraTools:
    def test_base_lists_only_the_shared_tools(self):
        names = [t.name for t in _example().get_tools()]
        assert len(names) == 14
        assert "find_parcel" not in names
        assert "footprint_for_parcel" not in names

    def test_extra_tool_is_listed_and_decorated_like_the_rest(self):
        tools = {t.name: t for t in _regional().get_tools()}
        assert len(tools) == 15
        hello = tools["say_hello"]
        assert hello.title == "Say Hello"
        assert hello.output_schema == {"type": "object"}
        assert hello.annotations == {
            "readOnlyHint": True,
            "openWorldHint": True,
        }

    @pytest.mark.asyncio
    async def test_extra_tool_is_dispatched_with_structured_content(self):
        result = await _regional().execute_tool("say_hello", {})
        assert result.success
        assert result.structured_content == {"greeting": "hello"}
        assert result.content[0]["text"].startswith("hello")

    @pytest.mark.asyncio
    async def test_extra_tool_input_error_is_a_tool_error(self):
        result = await _regional().execute_tool("say_hello", {"rude": True})
        assert not result.success
        assert result.error_message == "be polite"

    @pytest.mark.asyncio
    async def test_unknown_tool_is_still_unknown(self):
        for plugin in (_example(), _regional()):
            result = await plugin.execute_tool("nope", {})
            assert not result.success
            assert result.error_message == "Unknown tool: nope"


class TestGalleryGroups:
    @pytest.mark.asyncio
    async def test_single_group_is_a_bare_clause(self):
        p = _example()
        p._run_search = AsyncMock(return_value=[])
        await p._search_gallery("trails", 5)
        assert p._run_search.await_args.args[0] == (
            f"group:{GROUP_A} AND (trails)"
        )

    @pytest.mark.asyncio
    async def test_an_or_in_the_topic_stays_inside_the_group(self):
        """Bare, `group:X AND parks OR trails` matched every item about
        trails, in the gallery group or not (104 vs 9 on a live portal).
        The caller's own parentheses must not be able to undo that."""
        p = _example()
        p._run_search = AsyncMock(return_value=[])
        await p._search_gallery("parks OR trails", 5)
        assert p._run_search.await_args.args[0] == (
            f"group:{GROUP_A} AND (parks OR trails)"
        )
        await p._search_gallery("parks) OR (title:x", 5)
        assert p._run_search.await_args.args[0] == (
            f"group:{GROUP_A} AND (parks  OR  title:x)"
        )

    @pytest.mark.asyncio
    async def test_several_groups_are_ored_in_one_query(self):
        p = _regional()
        p._run_search = AsyncMock(return_value=[])
        await p._search_gallery("", 5)
        assert p._run_search.await_args.args[0] == (
            f"(group:{GROUP_A} OR group:{GROUP_B})"
        )


class TestPartnerOrgs:
    def test_no_partners_means_one_allowed_org_and_no_labels(self):
        p = _example()
        assert p._allowed_org_ids == {ORG.lower(): "Example County"}
        assert p._partner_hosts == frozenset()
        assert p._org_label({"orgId": ORG}) is None
        assert "[" not in p._format_summary(
            {"title": "T", "type": "Feature Service", "id": "e" * 32}
        ).split("\n")[0]

    @pytest.mark.asyncio
    async def test_single_org_search_clause_is_unchanged(self):
        p = _example()
        p._run_search = AsyncMock(return_value=[])
        await p._search_org_layers("roads", ["Feature Service"], 5)
        assert p._run_search.await_args.args[0] == (
            f'orgid:{ORG} AND (type:"Feature Service") AND (roads)'
        )

    @pytest.mark.asyncio
    async def test_partner_orgs_widen_the_search_and_the_post_filter(self):
        p = _regional()
        p._run_search = AsyncMock(
            return_value=[
                {"id": "1", "orgId": ORG},
                {"id": "2", "orgId": PARTNER},
                {"id": "3", "orgId": "SomeOtherOrg"},
                {"id": "4"},
            ]
        )
        kept = await p._search_org_layers("roads", ["Feature Service"], 5)
        assert p._run_search.await_args.args[0].startswith(
            f"(orgid:{ORG} OR orgid:{PARTNER}) AND "
        )
        assert [k["id"] for k in kept] == ["1", "2", "4"]

    def test_ownership_check_accepts_partners_only(self):
        p = _regional()
        p._assert_owned_by_configured_org({"id": "x", "orgId": PARTNER})
        with pytest.raises(ToolInputError):
            p._assert_owned_by_configured_org({"id": "x", "orgId": "Other"})
        with pytest.raises(ToolInputError):
            p._assert_owned_by_configured_org({"id": "x"})
        with pytest.raises(ToolInputError):
            _example()._assert_owned_by_configured_org(
                {"id": "x", "orgId": PARTNER}
            )

    def test_service_url_allowlist_follows_the_partner_list(self):
        p = _regional()
        ok = f"https://services.arcgis.com/{PARTNER}/arcgis/rest/services/x/FeatureServer/0"
        assert p._validate_service_url(ok) == ok
        host = "https://maps.partner.example/arcgis/rest/services/x/MapServer/0"
        assert p._validate_service_url(host) == host
        with pytest.raises(ToolInputError):
            p._validate_service_url(
                "https://services.arcgis.com/SomeoneElse/arcgis/rest/services/x/FeatureServer/0"
            )
        # The org id must be a whole path segment, not a prefix of one.
        with pytest.raises(ToolInputError):
            p._validate_service_url(
                f"https://services.arcgis.com/{PARTNER}extra/arcgis/rest/services/x/FeatureServer/0"
            )
        with pytest.raises(ToolInputError):
            _example()._validate_service_url(ok)
        with pytest.raises(ToolInputError):
            _example()._validate_service_url(host)

    def test_results_from_a_partner_are_labelled(self):
        p = _regional()
        item = {
            "title": "Roads",
            "type": "Feature Service",
            "id": "e" * 32,
            "orgId": PARTNER,
        }
        assert p._org_label(item) == "Partner Borough"
        assert "[Partner Borough]" in p._format_summary(item).split("\n")[0]
        own = dict(item, orgId=ORG)
        assert "[" not in p._format_summary(own).split("\n")[0]


class TestRegionWording:
    def _limited(self, plugin):
        return plugin._format_query_results(
            [{"OBJECTID": 1, "NAME": "a"}],
            25,
            1,
            None,
            coverage_pct=0.2,
        )

    def test_region_name_falls_back_to_the_configured_name(self):
        text, structured = self._limited(_example())
        assert "covers ~20% of Example County." in text
        message = [
            c for c in structured["caveats"] if c["code"] == "limited_coverage"
        ][0]["message"]
        assert message.endswith("inside the layer's coverage.")

    def test_region_name_and_hints_come_from_the_subclass(self):
        text, structured = self._limited(_regional())
        assert "covers ~20% of Exampleland." in text
        assert "layer's coverage -- regional layers are partial." in text
        message = [
            c for c in structured["caveats"] if c["code"] == "limited_coverage"
        ][0]["message"]
        assert message.endswith("coverage -- a regional layer is partial.")

    def test_base_has_no_region_so_no_coverage_is_computed(self):
        extent = {
            "xmin": -150.0, "ymin": 61.0, "xmax": -149.0, "ymax": 61.5,
            "spatialReference": {"wkid": 4326},
        }
        assert ArcGISHubPlugin._region_coverage_pct(extent) is None

    def test_text_keys_are_complete_in_a_subclass(self):
        assert set(RegionalPlugin.TEXT) == set(ArcGISHubPlugin.TEXT)


class TestAreaAndErrors:
    def test_equal_area_projection_passes_the_stored_area_through(self):
        assert RegionalPlugin._true_area_m2(1234.5, 61.0, 3338) == 1234.5
        # Not declared on the base: an unknown projection is refused.
        assert ArcGISHubPlugin._true_area_m2(1234.5, 61.0, 3338) is None

    def test_unresolved_error_hook_sees_the_where_clause(self):
        out = RegionalPlugin._rewrite_arcgis_error(
            "Unable to complete operation.",
            [],
            resource_id="e" * 32,
            has_where=True,
            where_clause="Depth > 0",
        )
        assert out == f"explained: Depth > 0 / item_id='{'e' * 32}'"

    def test_without_the_hook_the_raw_error_passes_through(self):
        out = ArcGISHubPlugin._rewrite_arcgis_error(
            "Unable to complete operation.", [], where_clause="Depth > 0"
        )
        assert out == "Unable to complete operation."

    def test_query_banner_defaults_to_none(self):
        assert _example()._query_result_banner("e" * 32) is None
