# opencontext-arcgis-hub

The GIS plugin shared by the OpenContext MCP servers that front an ArcGIS
Online organisation or Hub site. It is the part every such deployment has
in common:

- discovery of an organisation's gallery and layers (`find_gis_content`,
  `browse_gallery`, `search_spatial_layers`, `get_item_details`,
  `search_layers_by_field`)
- schema and values (`get_layer_schema`, `get_distinct_values`)
- attribute and spatial queries (`query_data`, `spatial_query_point`,
  `spatial_query_polygon`, with server-side buffers)
- polygon analytics (`aggregate_by_polygon`, `coverage_by_polygon`,
  `filter_by_polygon`, `find_features_spanning_classifications`)
- the plumbing under them: a tenant allowlist, WHERE-clause validation, a
  per-call time budget with bounded retry, a metadata cache, true-area
  correction for Web Mercator, and structured output for every tool.

It is a library, not a server. A deployment runs
[`opencontext-core`](https://github.com/codeforanchorage/opencontext-core)
and keeps a small subclass of `ArcGISHubPlugin` in its own repository.

Used by: Anchorage GIS (`anchorage-gis-mcp`) and the Alaska Geoportal
(`alaska-geoportal-mcp`).

## Using it in a deployment

Pin a tag in `requirements.txt` and `pyproject.toml`:

```
opencontext-arcgis-hub @ git+https://github.com/codeforanchorage/opencontext-arcgis-hub@v1.0.0
```

Then subclass in `plugins/<name>/plugin.py`:

```python
from arcgis_hub.plugin import ArcGISHubPlugin
from plugins.my_region.config_schema import MyRegionConfig


class MyRegionPlugin(ArcGISHubPlugin):
    plugin_name = "my_region"
    plugin_version = "1.0.0"
    CONFIG_CLASS = MyRegionConfig
    DISPLAY_NAME = "My Region GIS"
    REGION_NAME = "My Region"
    ONPREM_HOST_SUFFIXES = (".myregion.gov",)
```

The core's plugin loader picks the class defined in that module, so
importing the base there is safe.

`scripts/deploy.sh` in a deployment installs this package into the Lambda
zip beside `plugins/`; its dependencies (httpx, pydantic, pyclipper) must
be in the deployment's own `requirements.txt`, because the git packages
are installed with `--no-deps`.

### What a subclass can set

| Member | Purpose |
|---|---|
| `plugin_name`, `plugin_version`, `DISPLAY_NAME` | identity and log lines |
| `CONFIG_CLASS` | pydantic model for the plugin's config section; build it on `HubConfigBase` or `ArcGISHubPluginConfig` from `arcgis_hub.config_schema` |
| `REGION_NAME` | name used in coverage caveats (default: the configured `city_name`) |
| `ONPREM_HOST_SUFFIXES` | the organisation's own ArcGIS Server hosts, accepted by suffix |
| `TEXT` | region-specific wording; override entries with `{**ArcGISHubPlugin.TEXT, "key": "..."}` |
| `NATURAL_ID_FIELD_PRIORITY`, `PARCEL_INDICATOR_FIELDS` | field-name conventions of the region's layers |
| `EQUAL_AREA_METRE_WKIDS` | projections whose stored area is already square metres |
| `_region_coverage_pct(extent)` | share of the region a layer covers; `None` (the default) silences the coverage caveat |
| `_tool_definitions()`, `_execute_extra_tool()`, `TOOL_TITLES`, `TOOL_OUTPUT_SCHEMAS` | add tools of the deployment's own |
| `_query_result_banner(item_id)` | text put in front of `query_data` results for one layer |
| `_foreign_org_message()`, `_foreign_tenant_message()` | refusal wording for other tenants |
| `_explain_unresolved_error()` | explain an ArcGIS error the generic cases do not cover |

Config may give one `gallery_group_id` or a list of `gallery_group_ids`,
and optional `partner_orgs` (other ArcGIS organisations whose items and
hosts are also allowed). With no partners and one group, the portal
queries are exactly the single-organisation form.

## Changing shared behaviour

Change it here, not in a deployment:

1. Edit, add a test, push `main`, wait for CI to go green.
2. Bump `version` in `pyproject.toml` and `__version__` in
   `arcgis_hub/__init__.py`, tag `vX.Y.Z`, push the tag.
3. Move the pin in each deployment when it should pick the change up,
   run that deployment's tests and smoke script, deploy.

Nothing changes for any deployment until it moves its pin.

To work on this package and a deployment together:
`pip install -e ../opencontext-arcgis-hub` in the deployment's
environment.

## Development

```
pip install -e ".[dev]"
pytest -n auto
ruff check arcgis_hub tests
```

The suite runs against `tests/profile.py`, the smallest possible
subclass, so it exercises the base's own behaviour.
`tests/test_subclass_hooks.py` covers the extension points. Wording,
coverage boxes and extra tools that belong to one deployment are tested
in that deployment's repository.

Do not run `ruff format` across the repo; the source is hand-wrapped and
not format-clean.

## Releases

**1.0.0** — extracted from the Anchorage GIS deployment
(`anchorage-gis-mcp` at `7897032`), generalised with the Alaska
Geoportal's additions: several gallery groups, partner organisations and
their on-prem hosts, result labelling by publishing agency, equal-area
projections, and CAST type names in the WHERE validator.

## Origin

Built on [OpenContext](https://github.com/CityOfBoston/OpenContext) (City
of Boston Department of Innovation and Technology, MIT).
