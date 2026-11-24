"""find_gis_content falls through to field names when no item matches.

The portal search reads titles, tags and descriptions, never field
names, so a topic that is an attribute ("speed limit" is a field on the
streets layer) used to come back as "nothing found" and get reported to
the user as missing data. Seen in production 2026-11-18.
"""

import pytest
from jsonschema import Draft202012Validator

from tests.profile import ExampleConfig, ExamplePlugin

ITEM_SEARCH = ExamplePlugin.ITEM_SEARCH_OUTPUT_SCHEMA
SVC = "https://services.arcgis.com/org123/arcgis/rest/services"


@pytest.fixture
def plugin():
    config = {
        "portal_base_url": "https://example.maps.arcgis.com/sharing/rest",
        "gallery_group_id": "abc123",
        "org_id": "org123",
        "city_name": "Municipality of Anchorage",
        "gallery_url": "https://example.com/gallery",
        "timeout": 30,
    }
    p = ExamplePlugin(config)
    p.plugin_config = ExampleConfig(**config)
    return p


def _service(name, item_id):
    return {
        "id": item_id,
        "title": name,
        "type": "Feature Service",
        "url": f"{SVC}/{name}/FeatureServer",
    }


def _wire(plugin, services, layers, item_hits=()):
    """Item search returns ``item_hits``; the most-viewed list returns
    ``services``; ``layers`` maps a layer URL to its field names."""
    calls = []

    async def run_search(q, limit, sort_field="relevance"):
        calls.append(sort_field)
        if sort_field == "numViews":
            return list(services)
        return list(item_hits)

    async def meta(url, validate=None):
        if url in layers:
            return {
                "name": url.rsplit("/", 2)[-2],
                "fields": [
                    {"name": n, "alias": n.replace("_", " ").title()}
                    for n in layers[url]
                ],
            }
        if url.endswith("FeatureServer"):
            ids = sorted(
                int(k.rsplit("/", 1)[1])
                for k in layers
                if k.startswith(url + "/")
            )
            if ids:
                return {"layers": [{"id": i} for i in ids]}
        raise RuntimeError("unreachable")

    plugin._run_search = run_search
    plugin._get_meta_json = meta
    return calls


def _validate(instance):
    errors = list(Draft202012Validator(ITEM_SEARCH).iter_errors(instance))
    assert not errors, [e.message for e in errors]


class TestFieldNameFallback:
    @pytest.mark.asyncio
    async def test_attribute_topic_lands_on_the_layer(self, plugin):
        calls = _wire(
            plugin,
            [_service("Parks", "1" * 32), _service("Streets", "2" * 32)],
            {
                f"{SVC}/Parks/FeatureServer/0": ["NAME", "ACRES"],
                f"{SVC}/Streets/FeatureServer/0": ["NAME", "SPEED_LIMIT"],
            },
        )
        text, structured = await plugin._find_gis_content(
            {"topic": "speed limit"}
        )
        _validate(structured)
        assert calls.count("numViews") == 1
        (item,) = structured["items"]
        assert item["item_id"] == "2" * 32
        assert item["source"] == "field_match"
        assert item["queryable"] is True
        assert item["matching_fields"] == [
            {
                "name": "SPEED_LIMIT",
                "alias": "Speed Limit",
                "layer_index": 0,
                "layer_name": "FeatureServer",
            }
        ]
        assert structured["caveats"][0]["code"] == "matched_by_field_name"
        assert "SPEED_LIMIT" in text and "2" * 32 in text
        assert "most-viewed" in text

    @pytest.mark.asyncio
    async def test_every_topic_word_must_match_one_field(self, plugin):
        _wire(
            plugin,
            [_service("Sidewalks", "3" * 32)],
            {f"{SVC}/Sidewalks/FeatureServer/0": ["Ownership", "Street"]},
        )
        text, structured = await plugin._find_gis_content(
            {"topic": "road ownership"}
        )
        _validate(structured)
        assert structured["items"] == []
        assert structured["caveats"][0]["code"] == "no_results"
        assert text.startswith("No Municipality of Anchorage GIS content")

    @pytest.mark.asyncio
    async def test_not_run_when_the_item_search_finds_something(
        self, plugin
    ):
        calls = _wire(
            plugin,
            [_service("Streets", "2" * 32)],
            {f"{SVC}/Streets/FeatureServer/0": ["SPEED_LIMIT"]},
            item_hits=[_service("Speed Limits", "4" * 32)],
        )
        _, structured = await plugin._find_gis_content(
            {"topic": "speed limit"}
        )
        assert "numViews" not in calls
        assert {i["source"] for i in structured["items"]} == {
            "gallery",
            "layers",
        }

    @pytest.mark.asyncio
    async def test_unreadable_layers_fall_back_to_plain_no_results(
        self, plugin
    ):
        _wire(plugin, [_service("Streets", "2" * 32)], {})
        _, structured = await plugin._find_gis_content(
            {"topic": "speed limit"}
        )
        _validate(structured)
        assert structured["items"] == []
        assert structured["caveats"][0]["code"] == "no_results"

    @pytest.mark.asyncio
    async def test_failed_candidate_search_is_not_an_error(self, plugin):
        calls = []

        async def run_search(q, limit, sort_field="relevance"):
            calls.append(sort_field)
            if sort_field == "numViews":
                raise RuntimeError("portal down")
            return []

        plugin._run_search = run_search
        _, structured = await plugin._find_gis_content({"topic": "speed"})
        assert "numViews" in calls
        assert structured["caveats"][0]["code"] == "no_results"

    @pytest.mark.asyncio
    async def test_subclass_can_disable_it(self, plugin):
        calls = _wire(
            plugin,
            [_service("Streets", "2" * 32)],
            {f"{SVC}/Streets/FeatureServer/0": ["SPEED_LIMIT"]},
        )
        plugin.FIELD_FALLBACK_SERVICES = 0
        _, structured = await plugin._find_gis_content({"topic": "speed"})
        assert "numViews" not in calls
        assert structured["items"] == []

    @pytest.mark.asyncio
    async def test_hits_are_capped_at_limit(self, plugin):
        services = [_service(f"S{i}", f"{i:032d}") for i in range(5)]
        _wire(
            plugin,
            services,
            {f"{SVC}/S{i}/FeatureServer/0": ["SPEED"] for i in range(5)},
        )
        _, structured = await plugin._find_gis_content(
            {"topic": "speed", "limit": 2}
        )
        assert [i["title"] for i in structured["items"]] == ["S0", "S1"]

    def test_tokens(self):
        tokens = ExamplePlugin._field_topic_tokens
        assert tokens("Speed Limits") == ["speed", "limit"]
        assert tokens("a of class") == ["class"]
        assert tokens("bus bus") == ["bus"]
        assert tokens("--") == []
