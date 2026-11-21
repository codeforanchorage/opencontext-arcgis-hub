"""Region-neutral ArcGIS Online / Hub plugin base for OpenContext.

`ArcGISHubPlugin` holds everything a deployment shares: discovery of an
organisation's gallery and layers through the ArcGIS Portal REST API,
schema inspection, attribute and spatial queries, the polygon analytics,
upstream resilience (per-call budget, bounded retry, metadata cache) and
structured output for every tool.

A deployment subclasses it in its own `plugins/<name>/plugin.py` and
supplies what is specific to its region: `plugin_name`, a config class,
wording (`TEXT`, `REGION_NAME`), a coverage bounding box
(`_region_coverage_pct`) and any extra tools (`_tool_definitions`,
`_execute_extra_tool`). Extracted from the Anchorage GIS deployment.
"""

import asyncio
import contextvars
import copy
import json
import logging
import math
import re
import time
from collections import Counter, OrderedDict, defaultdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

import httpx
import pyclipper

from core.interfaces import (
    DataPlugin,
    PluginType,
    ToolDefinition,
    ToolInputError,
    ToolResult,
)
from arcgis_hub.config_schema import ArcGISHubPluginConfig
from arcgis_hub.where_validator import (
    OrderByValidator,
    OutFieldsValidator,
    WhereValidator,
)

logger = logging.getLogger(__name__)

# ── Upstream resilience ─────────────────────────────────────────────────
#
# One tool call can make many upstream requests (item lookup, layer
# metadata, a count, then paged queries). Each is capped at the plugin
# `timeout` (20s), so without a shared budget two slow ones run past the
# Lambda's 28s and it is killed mid-flight -- the opaque 502 the timeout
# ladder exists to prevent. Every request in a call therefore draws from ONE
# budget, kept under lambda_timeout with room to format and return.
# tests/test_gis_resilience.py (TestBudgetFitsDeployment) pins it below
# lambda_timeout.
TOOL_CALL_BUDGET_S = 24.0

# A transient failure (connection reset, DNS blip, upstream 5xx) is retried
# only while this much of the budget is left: a retry that cannot finish
# just converts a readable error into a budget-exhausted one. A TIMEOUT is
# never retried -- it already spent a full per-request timeout.
RETRY_MIN_REMAINING_S = 4.0

# Deadline (monotonic seconds) of the tool call currently executing. A
# contextvar, not an attribute: the local server runs calls concurrently on
# one plugin instance, and gather()ed subtasks inherit it.
_call_deadline: contextvars.ContextVar[Optional[float]] = (
    contextvars.ContextVar("arcgis_hub_call_deadline", default=None)
)


class UpstreamTimeoutError(RuntimeError):
    """ArcGIS did not answer in time. Never retried."""


class ArcGISHubPlugin(DataPlugin):
    """Base plugin for one organisation's ArcGIS Online / Hub content.

    Uses the ArcGIS Portal REST API to search the curated public gallery
    and the organization's spatial layers, retrieve item details, inspect
    Feature Service schemas, and query Feature Service records.
    """

    plugin_name = "arcgis_hub"
    plugin_type = PluginType.OPEN_DATA
    plugin_version = "1.2.0"

    # ── What a deployment overrides ──────────────────────────────────────
    # Pydantic model the plugin's config section is validated with.
    CONFIG_CLASS = ArcGISHubPluginConfig
    # Name used in log lines.
    DISPLAY_NAME = "ArcGIS Hub"
    # Name of the area the coverage caveats measure against ("Anchorage",
    # "Alaska"). None falls back to the configured city_name.
    REGION_NAME: Optional[str] = None
    # WKIDs of equal-area projections in metres: a stored area in one of
    # these already IS true square metres (see _true_area_m2).
    EQUAL_AREA_METRE_WKIDS: frozenset = frozenset()
    # Region-specific wording. A subclass replaces entries with
    #   TEXT = {**ArcGISHubPlugin.TEXT, "key": "..."}
    TEXT: Dict[str, str] = {
        # Appended (before the full stop) to the LIMITED COVERAGE caveat:
        # structured message and text line respectively.
        "limited_coverage_hint_long": "",
        "limited_coverage_hint_short": "",
        # Body of the AMBIGUITY WARNING shown when several queryable
        # layers match one topic.
        "ambiguity_warning": (
            "\n> **AMBIGUITY WARNING:** multiple queryable "
            "layers match this topic. They may be maintained "
            "by different agencies or cover different subsets or "
            "regions. For 'how "
            "many?' / 'list all' questions, do NOT silently "
            "pick the first one -- either (a) query each "
            "layer with `limit=1` and report a breakdown of "
            "totals, or (b) ask the user which subset they "
            "mean. The titles below hint at scope (look for "
            "agency or region prefixes).\n\n"
        ),
        # Heading (and optional note) of the gallery section of
        # find_gis_content.
        "gallery_section_title": "Maps, Apps & Viewers",
        "gallery_section_note": "",
        # Footer of browse_gallery, before the "Full gallery" link.
        "gallery_footer": (
            "**These are VIEWERS -- not directly queryable.** Web "
            "Maps, Dashboards, and Apps cannot be passed to "
            "`query_data` for record counts or filtered lists. If "
            "the user asked 'how many?' or 'list X', call "
            "`find_gis_content(topic=...)` to find the underlying "
            "Feature Service instead.\n"
        ),
        # Extra WHERE-clause advice appended in get_layer_schema.
        "where_tip_extra": "",
        # Why source features fell outside every aggregation polygon.
        "unmatched_diagnosis": (
            "This usually means the aggregation layer does not cover "
            "those features, or stray coordinates."
        ),
        # Parenthetical after "inflates area by sec^2(latitude)".
        "mercator_inflation": "the factor grows with latitude",
        # How to verify one target when coverage looks like a centroid
        # artefact.
        "coverage_verify_hint": (
            "Verify individual targets with "
            "spatial_query_polygon on the overlay layer, "
            "filtered to that one target polygon."
        ),
        # Why targets outnumber distinct ids, and what to do about it.
        "stacked_records_note": (
            "Cadastral layers hold multipart parcels plus "
            "stacked lease / condo records. Check "
            "get_layer_schema for a record-category field "
            "and narrow target_where to one row per parcel."
        ),
        # Example layers for a 'parcels spanning zones' question.
        "spanning_example": (
            "should be a parcels layer and classification should be a "
            "zoning or land-status layer. Use "
        ),
        # Parenthetical naming parcel layers that carry a parcel id.
        "parcel_layer_examples": "",
        # coverage_by_polygon input-schema notes on Shape__Area.
        "shape_area_note": (
            "NOTE: Shape__Area on ArcGIS Online hosted layers is "
            "WEB MERCATOR, not a real area -- it is inflated by "
            "sec^2(latitude). The "
        ),
        "hosted_layers": "ArcGIS Online hosted layers",
    }

    GALLERY_APP_TYPES = [
        "Web Experience",
        "Web Mapping Application",
        "StoryMap",
        "Dashboard",
        "Hub Site Application",
        "Instant App",
    ]
    LAYER_TYPES = [
        "Feature Service",
        "Map Service",
        "Image Service",
        "Tile Layer",
        "Vector Tile Service",
        "WFS",
        "WMS",
    ]
    DATA_TYPES = [
        "CSV",
        "GeoJSON",
        "Shapefile",
        "File Geodatabase",
        "Web Map",
        "Web Scene",
    ]
    QUERYABLE_TYPES = {
        "Feature Service",
        "Map Service",
        "Feature Layer",
        "Table",
    }

    # The org's on-prem hosts we'll proxy without further checks (a
    # deployment sets e.g. (".muni.org",)). ArcGIS Online
    # hosts (*.arcgis.com) are handled separately in _validate_service_url:
    # they must either be this org's portal or carry the configured org_id
    # in the URL path, so we can't be coerced into proxying other tenants.
    ONPREM_HOST_SUFFIXES: Tuple[str, ...] = ()

    ITEM_ID_RE = re.compile(r"^[0-9a-fA-F]{32}$")

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self.plugin_config: Optional[ArcGISHubPluginConfig] = None
        self.client: Optional[httpx.AsyncClient] = None
        # LRU {(item_id, group_by_field, agg_where):
        #      (expires_epoch, polygons, layer_edit_stamp, approx_bytes)}.
        # Bounded by entry count AND by total size, to prevent memory
        # exhaustion via agg_where variants that miss the cache
        # (e.g. "1=1 AND 1=1" vs "1=1 AND 2=2"). See AGG_CACHE_MAX_BYTES.
        self._agg_layer_cache: "OrderedDict[Tuple[str, str, str], Tuple[float, List[Dict[str, Any]], Any, int]]" = OrderedDict()
        # LRU {key: (expires_monotonic, json)} for portal item records and
        # layer/service ?f=json metadata. See META_CACHE_TTL_S.
        self._meta_cache: "OrderedDict[str, Tuple[float, Dict[str, Any]]]" = OrderedDict()

    async def initialize(self) -> bool:
        try:
            self.plugin_config = self.CONFIG_CLASS(**self.config)
            self.client = httpx.AsyncClient(
                timeout=self.plugin_config.timeout,
            )

            # Test connectivity with a minimal search
            resp = await self.client.get(
                f"{self.plugin_config.portal_base_url}/search",
                params={
                    "q": f"orgid:{self.plugin_config.org_id}",
                    "f": "json",
                    "num": "1",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                raise RuntimeError(
                    data["error"].get("message", str(data["error"]))
                )

            self._initialized = True
            logger.info(
                f"{self.DISPLAY_NAME} plugin initialized successfully for "
                f"{self.plugin_config.city_name}"
            )
            return True

        except Exception as e:
            logger.error(
                f"Failed to initialize {self.DISPLAY_NAME} plugin: {e}", exc_info=True
            )
            return False

    async def shutdown(self) -> None:
        if self.client:
            await self.client.aclose()
            self.client = None
        self._initialized = False
        logger.info(f"{self.DISPLAY_NAME} plugin shut down")

    # ── Portal search helpers ─────────────────────────────────────────────

    async def _run_search(self, q: str, limit: int) -> List[Dict[str, Any]]:
        """Run a search against the ArcGIS Portal REST API."""
        params = {
            "q": q,
            "f": "json",
            "num": str(limit),
            "sortField": "relevance",
            "sortOrder": "desc",
        }
        resp = await self._send("get",
            f"{self.plugin_config.portal_base_url}/search",
            params=params,
        )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(
                data["error"].get("message", str(data["error"]))
            )
        return data.get("results", [])

    @staticmethod
    def _user_query_clause(query: str) -> str:
        """The caller's search text as one parenthesised clause.

        The text is ANDed onto the scope filters (group, org, item
        type). Left bare, an OR in it binds looser than those ANDs:
        ``group:X AND parks OR trails`` matches every item about
        trails, in the group or not. Wrapped, it stays inside the
        scope. The caller's own parentheses are dropped so they cannot
        close the wrapper early.
        """
        cleaned = query.replace("(", " ").replace(")", " ").strip()
        return f"({cleaned})" if cleaned else ""

    async def _search_gallery(
        self, query: str, limit: int
    ) -> List[Dict[str, Any]]:
        """Search within the configured public content group(s).

        A Hub catalog can be a union of several ArcGIS groups (one per
        publishing division), so the group filter is an OR across every
        configured id in a single portal query.
        """
        group_ids = self._gallery_group_ids
        group_clause = " OR ".join(f"group:{gid}" for gid in group_ids)
        clauses = [
            f"({group_clause})" if len(group_ids) > 1 else group_clause
        ]
        if query and self._user_query_clause(query):
            clauses.append(self._user_query_clause(query))
        return await self._run_search(" AND ".join(clauses), limit)

    async def _search_org_layers(
        self, query: str, item_types: List[str], limit: int
    ) -> List[Dict[str, Any]]:
        """Search the organization's spatial layers.

        We scope the upstream query with ``orgid:<org_id>`` (ORed with
        each configured partner org, when there are any); the
        post-filter below is defense-in-depth in case Esri ever returns
        results that don't honor that filter (e.g. shared items, cross-
        org content, indexing quirks).

        Items with ``orgId`` unset (None / empty) are kept -- many
        legitimate items in this tenant (FEMA-imported feeds, older
        items predating the field) have a null ``orgId`` in the
        response, even though the upstream ``orgid:`` query confirmed
        they belong to this org. The post-filter only rejects items
        whose ``orgId`` is set to a *different* organization, which is
        the actual cross-org leak we care about.
        """
        type_filter = " OR ".join(f'type:"{t}"' for t in item_types)
        org_ids = [self.plugin_config.org_id] + [
            p.org_id for p in self._partner_orgs
        ]
        org_filter = " OR ".join(f"orgid:{o}" for o in org_ids)
        clauses = [
            f"({org_filter})" if len(org_ids) > 1 else org_filter,
            f"({type_filter})",
        ]
        if query and self._user_query_clause(query):
            clauses.append(self._user_query_clause(query))
        results = await self._run_search(" AND ".join(clauses), limit)
        allowed = self._allowed_org_ids
        kept: List[Dict[str, Any]] = []
        for r in results:
            item_org = (r.get("orgId") or "").lower()
            if not item_org or item_org in allowed:
                kept.append(r)
        return kept

    # ── Organisations this server proxies ─────────────────────────────────

    @property
    def _gallery_group_ids(self) -> List[str]:
        """Configured gallery group ids: `gallery_group_ids` when the
        config class has it, else the single `gallery_group_id`."""
        cfg = self.plugin_config
        many = getattr(cfg, "gallery_group_ids", None)
        if many:
            return list(many)
        return [cfg.gallery_group_id]

    @property
    def _partner_orgs(self) -> List[Any]:
        """Partner organisations from config (empty when the config
        class does not define `partner_orgs`)."""
        return list(getattr(self.plugin_config, "partner_orgs", None) or [])

    @property
    def _allowed_org_ids(self) -> Dict[str, str]:
        """Lowercased org id -> display name for every org this server
        proxies: the configured org plus each partner org."""
        cfg = self.plugin_config
        if not cfg:
            return {}
        allowed = {(cfg.org_id or "").lower(): cfg.city_name}
        for partner in self._partner_orgs:
            allowed[partner.org_id.lower()] = partner.name
        return allowed

    @property
    def _partner_hosts(self) -> frozenset:
        """Exact on-prem hostnames declared by partner orgs."""
        return frozenset(h for p in self._partner_orgs for h in p.hosts)

    def _org_label(self, item: Dict[str, Any]) -> Optional[str]:
        """Best-effort publishing-agency label for a search result.

        Search results often omit ``orgId``, so fall back to the org id
        in a ``*.arcgis.com/<org>/`` service URL or to a partner's
        on-prem host. Returns None when nothing identifies the org, and
        always when no partner orgs are configured: with a single org
        there is nothing to tell apart.
        """
        partners = self._partner_orgs
        if not partners:
            return None
        allowed = self._allowed_org_ids
        org = (item.get("orgId") or "").lower()
        if org in allowed:
            return allowed[org]
        url = item.get("url") or ""
        if url:
            parsed = urlparse(url)
            host = (parsed.hostname or "").lower()
            if host == "arcgis.com" or host.endswith(".arcgis.com"):
                first = parsed.path.lower().strip("/").split("/")[0]
                if first in allowed:
                    return allowed[first]
            for partner in partners:
                if host in partner.hosts:
                    return partner.name
            if any(
                host == suffix.lstrip(".") or host.endswith(suffix)
                for suffix in self.ONPREM_HOST_SUFFIXES
            ):
                # The org's own on-prem servers host several agencies:
                # label by host.
                return host
        return None

    @property
    def _region_name(self) -> str:
        if self.REGION_NAME:
            return self.REGION_NAME
        return self.plugin_config.city_name if self.plugin_config else ""

    def _foreign_org_message(self, item: Dict[str, Any]) -> str:
        """Refusal text for an item owned by an org we do not serve."""
        org = item.get("orgId")
        owner = (
            f"belongs to org {org!r}"
            if org
            else "belongs to another ArcGIS account"
        )
        return (
            f"Item {item.get('id')!r} {owner}, not the configured org "
            f"{self.plugin_config.org_id!r}; this MCP only serves "
            f"{self.plugin_config.city_name} data. Use find_gis_content "
            f"to get the item ids this server can query."
        )

    def _foreign_tenant_message(
        self, host: str, path: str, org_id: str
    ) -> str:
        """Refusal text for an ArcGIS Online URL of another tenant."""
        return (
            f"service URL host {host!r} (path {path!r}) is not "
            f"scoped to org {org_id!r}; refusing to proxy other "
            f"ArcGIS Online tenants"
        )

    def _query_result_banner(self, item_id: str) -> Optional[str]:
        """Text put in FRONT of a query_data response for one layer
        (known traps in that layer's data). None for no banner."""
        return None

    def _apply_result_banner(
        self, item_id: str, text: str, structured: Any
    ) -> str:
        """Put a layer's banner at the front of BOTH result channels.

        The banner used to be prepended to the markdown only, after the
        structured half was built, so clients that render
        structuredContent alone never saw the traps it warns about.
        """
        banner = self._query_result_banner(item_id)
        if not banner:
            return text
        if isinstance(structured, dict) and isinstance(
            structured.get("caveats"), list
        ):
            structured["caveats"].insert(
                0, {"code": "layer_notes", "message": banner}
            )
        # Front of the response: these notes change how the numbers
        # below must be read, so they are useless appended after them.
        return banner + "\n\n" + text

    async def _execute_extra_tool(
        self, tool_name: str, arguments: Dict[str, Any]
    ) -> Optional[Tuple[str, Dict[str, Any]]]:
        """Run a tool the base class does not define.

        Return (text, structured_content), or None when the name is not
        one of the subclass's tools. Errors are raised as in any other
        tool: ToolInputError for a caller mistake.
        """
        return None

    # ── Formatters ────────────────────────────────────────────────────────

    @property
    def _portal_home(self) -> str:
        """Portal home URL (without /sharing/rest)."""
        return self.plugin_config.portal_base_url.replace("/sharing/rest", "")

    def _item_portal_url(self, item: Dict[str, Any]) -> str:
        url = item.get("url", "")
        item_type = item.get("type", "")
        item_id = item.get("id", "")
        if url and item_type in self.GALLERY_APP_TYPES:
            return url
        return f"{self._portal_home}/home/item.html?id={item_id}"

    def _format_summary(self, item: Dict[str, Any]) -> str:
        title = item.get("title", "Untitled")
        item_type = item.get("type", "Unknown")
        snippet = (item.get("snippet") or "").strip()
        tags = item.get("tags", [])
        item_id = item.get("id", "")
        url = self._item_portal_url(item)

        label = self._org_label(item)
        agency = (
            f"  [{label}]"
            if label and label != self.plugin_config.city_name
            else ""
        )
        lines = [f"**{title}**  _{item_type}_{agency} -- ID: `{item_id}`"]
        if snippet:
            lines.append(snippet)
        if tags:
            lines.append(f"Tags: {', '.join(tags[:6])}")
        lines.append(url)
        lines.append("")
        return "\n".join(lines)

    @staticmethod
    def _ms_to_date(ms: Any) -> str:
        if ms:
            try:
                return datetime.fromtimestamp(
                    int(ms) / 1000, tz=timezone.utc
                ).strftime("%Y-%m-%d")
            except (ValueError, TypeError, OSError):
                pass
        return "Unknown"

    @staticmethod
    def _with_retrieved_footer(text: str) -> str:
        # Stamp every tool response with a UTC retrieval timestamp so
        # models can tell stale outputs from fresh ones. Skip if the
        # text already carries a Retrieved: line (query_data emits one
        # in its provenance header) to avoid double-stamping.
        if not text or "Retrieved:" in text:
            return text
        retrieved_at = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        return f"{text}\n\n_Retrieved: {retrieved_at}_"

    @staticmethod
    def _ms_to_iso_smart(ms: Any) -> Any:
        # Midnight UTC -> date-only (preserves output for true date fields
        # like effective/inspection dates). Non-midnight -> full ISO so we
        # don't silently drop the time-of-day on real datetime fields.
        # On failure, return the raw value rather than a placeholder --
        # losing the data is worse than showing the epoch ms.
        if ms is None or ms == "":
            return ms
        try:
            dt = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
        except (ValueError, TypeError, OSError):
            return ms
        if dt.hour == dt.minute == dt.second == dt.microsecond == 0:
            return dt.strftime("%Y-%m-%d")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _format_details(self, item: Dict[str, Any]) -> str:
        title = item.get("title", "Untitled")
        item_type = item.get("type", "Unknown")
        item_id = item.get("id", "")
        snippet = (item.get("snippet") or "").strip()
        description = (
            item.get("description") or "No description available."
        ).strip()
        tags = item.get("tags", [])
        categories = item.get("categories", [])
        owner = item.get("owner", "")
        access = item.get("access", "")
        url = item.get("url", "")
        num_views = item.get("numViews", 0)
        created = self._ms_to_date(item.get("created"))
        modified = self._ms_to_date(item.get("modified"))

        extent = item.get("extent") or []
        extent_str = ""
        if len(extent) == 2:
            try:
                extent_str = (
                    f"SW {extent[0][1]:.4f}\u00b0N {extent[0][0]:.4f}\u00b0E  "
                    f"NE {extent[1][1]:.4f}\u00b0N {extent[1][0]:.4f}\u00b0E"
                )
            except (IndexError, TypeError):
                pass

        lines = [
            f"## {title}",
            f"**Type:** {item_type}  |  **ID:** `{item_id}`",
            f"**Owner:** {owner}  |  **Access:** {access}  |  **Views:** {num_views:,}",
            f"**Created:** {created}  |  **Modified:** {modified}",
            "",
            "### Summary",
            snippet or "_No summary._",
            "",
            "### Description",
            description,
        ]
        if tags:
            lines += ["", f"**Tags:** {', '.join(tags)}"]
        if categories:
            lines += [f"**Categories:** {', '.join(categories)}"]
        if extent_str:
            lines += [f"**Spatial Extent:** {extent_str}"]
        if url:
            lines += [f"**Service/App URL:** {url}"]
        lines += [
            f"**Portal Page:** {self._portal_home}/home/item.html?id={item_id}"
        ]
        queryable = item_type in (
            "Feature Service",
            "Feature Layer",
            "Map Service",
            "Table",
        )
        lines += ["", "---"]
        if queryable:
            lines += [
                "**NEXT STEPS:** this is a queryable layer. Use "
                f"`query_data('{item_id}', limit=1)` to count records "
                f"(read the TOTAL COUNT line); "
                f"`get_layer_schema('{item_id}')` to see field names; "
                f"`query_data('{item_id}', where=..., limit=N)` to list."
            ]
        else:
            lines += [
                f"**NOTE:** type '{item_type}' is not directly queryable. "
                "It may bundle queryable layers -- open the Portal Page "
                "above to inspect, or use `find_gis_content` to find a "
                "related Feature Service."
            ]
        return "\n".join(lines)

    # ── Structured records for the discovery tools ───────────────────

    @staticmethod
    def _str_list(raw: Any) -> List[str]:
        """A portal list field (tags, categories) as strings, or []."""
        if not isinstance(raw, list):
            return []
        return [str(v) for v in raw if v is not None]

    def _item_record(
        self, item: Dict[str, Any], source: Optional[str] = None
    ) -> Dict[str, Any]:
        """One portal item as an ITEM_SEARCH_OUTPUT_SCHEMA `items` entry."""
        item_id = str(item.get("id") or "")
        record: Dict[str, Any] = {
            "item_id": item_id,
            "title": str(item.get("title") or "Untitled"),
            "type": str(item.get("type") or "Unknown"),
            "queryable": item.get("type") in self.QUERYABLE_TYPES,
            "snippet": (str(item.get("snippet") or "")).strip() or None,
            "tags": self._str_list(item.get("tags")),
            "url": str(item["url"]) if item.get("url") else None,
            "portal_url": (
                f"{self._portal_home}/home/item.html?id={item_id}"
            ),
        }
        if source is not None:
            record["source"] = source
        return record

    def _item_details_record(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """get_item_details' structured half (ITEM_DETAILS_OUTPUT_SCHEMA)."""
        record = self._item_record(item)

        def _date(ms: Any) -> Optional[str]:
            rendered = self._ms_to_date(ms)
            return None if rendered == "Unknown" else rendered

        extent = None
        raw_extent = item.get("extent") or []
        if len(raw_extent) == 2:
            try:
                extent = {
                    "west": float(raw_extent[0][0]),
                    "south": float(raw_extent[0][1]),
                    "east": float(raw_extent[1][0]),
                    "north": float(raw_extent[1][1]),
                }
            except (IndexError, TypeError, ValueError):
                extent = None

        num_views = item.get("numViews")
        description = item.get("description")
        record.update(
            {
                "owner": item.get("owner") or None,
                "access": item.get("access") or None,
                "description": (
                    str(description).strip() or None if description else None
                ),
                "categories": self._str_list(item.get("categories")),
                "num_views": (
                    num_views
                    if isinstance(num_views, int)
                    and not isinstance(num_views, bool)
                    else None
                ),
                "created": _date(item.get("created")),
                "modified": _date(item.get("modified")),
                "extent": extent,
            }
        )
        caveats: List[Dict[str, Any]] = []
        if not record["queryable"]:
            caveats.append(
                {
                    "code": "not_queryable",
                    "message": (
                        f"Type '{record['type']}' is not directly "
                        "queryable: do not pass it to query_data. It "
                        "may bundle queryable layers -- open portal_url "
                        "to inspect, or use find_gis_content to find a "
                        "related Feature Service."
                    ),
                }
            )
        return {"item": record, "caveats": caveats}

    @staticmethod
    def _item_list_caveats(
        records: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """The judgement calls the prose makes about an item list.

        Mirrors the AMBIGUITY WARNING and the QUERYABLE / OTHER split
        of _format_layer_section, which a structured-only client would
        otherwise never see.
        """
        caveats: List[Dict[str, Any]] = []
        queryable = sum(1 for r in records if r["queryable"])
        if queryable >= 2:
            caveats.append(
                {
                    "code": "multiple_queryable_layers",
                    "count": queryable,
                    "message": (
                        "Multiple queryable layers match. They may be "
                        "maintained by different agencies (municipal vs "
                        "state vs federal) or cover different subsets "
                        "(all trails vs nordic trails only). For 'how "
                        "many?' / 'list all' questions do NOT silently "
                        "pick the first: query each with limit=1 and "
                        "report a breakdown, or ask the user which "
                        "subset they mean. Titles hint at scope (agency "
                        "prefixes like ADNR, USFS, ParksRec)."
                    ),
                }
            )
        other = len(records) - queryable
        if other:
            caveats.append(
                {
                    "code": "non_queryable_items",
                    "count": other,
                    "message": (
                        "Items with queryable=false are web maps, apps, "
                        "viewers or downloadable files. They cannot be "
                        "passed to query_data for counts or lists; use "
                        "get_item_details or find_gis_content to reach "
                        "the underlying Feature Service."
                    ),
                }
            )
        return caveats

    def _item_search_result(
        self,
        records: List[Dict[str, Any]],
        *,
        text: Optional[str],
        limit: int,
        layer_type: Optional[str] = None,
        no_results_message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Assemble an ITEM_SEARCH_OUTPUT_SCHEMA payload."""
        if records:
            caveats = self._item_list_caveats(records)
        else:
            caveats = [
                {
                    "code": "no_results",
                    "message": no_results_message or "No items matched.",
                }
            ]
        return {
            "query": {
                "text": text or None,
                "layer_type": layer_type,
                "limit": limit,
            },
            "summary": {
                "returned": len(records),
                "queryable": sum(1 for r in records if r["queryable"]),
            },
            "items": records,
            "caveats": caveats,
        }

    @staticmethod
    def _field_record(field: Dict[str, Any]) -> Dict[str, Any]:
        """One layer field as a _FIELD_RECORD_SCHEMA entry."""
        name = str(field.get("name") or "")
        record: Dict[str, Any] = {
            "name": name,
            "alias": str(field.get("alias") or name),
            "type": str(field.get("type") or "").replace(
                "esriFieldType", ""
            ),
        }
        domain = field.get("domain")
        if isinstance(domain, dict) and domain.get("type") == "codedValue":
            coded = []
            for c in domain.get("codedValues") or []:
                if not isinstance(c, dict):
                    continue
                code = c.get("code")
                if not isinstance(code, (str, int, float, bool)):
                    code = None if code is None else str(code)
                label = c.get("name")
                coded.append(
                    {
                        "code": code,
                        "name": None if label is None else str(label),
                    }
                )
            if coded:
                record["coded_values"] = coded
        return record

    # Upper bound on how many chars of a single feature's geometry we
    # dump into the response. Simplified polygons are usually well
    # under this; anything larger gets truncated with a clear marker.
    GEOMETRY_STR_MAX = 600

    # Devil's-advocate thresholds.
    STALENESS_THRESHOLD_DAYS = 365  # layer unchanged > 1 yr -> freshness note
    COVERAGE_THRESHOLD = 0.5  # < 50% muni overlap -> coverage note
    SMALL_SAMPLE_THRESHOLD = 10  # total_count < 10 (and > 1) -> small-N note

    # Above this many records, _format_query_results switches from
    # per-record blocks to a compact pipe-delimited table -- the block
    # format costs ~3x the bytes of the data it carries. Results that
    # carry geometry keep the block format: a multi-hundred-char
    # GeoJSON cell would dominate any table row.
    COMPACT_FORMAT_THRESHOLD = 20

    # Small-table rule: when query_data is called WITHOUT an explicit
    # `limit` and the matching set has at most this many rows, every row
    # comes back in one response instead of the default 25. Observed in
    # prod (Nov 2026): a 39-row lookup table returned "25 of 39,
    # TRUNCATED", and claude.ai sessions read it as 8 OBJECTID-range
    # chunks instead. An explicit limit is always honoured -- limit=1 is
    # the documented counting idiom and must stay one row.
    SMALL_TABLE_ROW_THRESHOLD = 100
    # Hard per-call row cap of query_data (its inputSchema maximum).
    QUERY_MAX_LIMIT = 1000

    def _render_field_value(
        self,
        key: str,
        value: Any,
        date_fields: Optional[set],
        coded_domains: Optional[Dict[str, Dict[Any, str]]],
    ) -> Any:
        """Date + coded-domain rendering shared by the per-record block
        and compact table formats."""
        if date_fields and key in date_fields and value is not None:
            return self._ms_to_iso_smart(value)
        if (
            coded_domains
            and key in coded_domains
            and value is not None
            and value in coded_domains[key]
        ):
            # Render coded values as "<code> (<label>)" so the model
            # doesn't have to guess what e.g. R1A means. Skip the
            # parenthetical when code and label are the same string to
            # avoid noise like "Status: Open (Open)".
            label = coded_domains[key][value]
            if str(value) != str(label):
                return f"{value} ({label})"
        return value

    @staticmethod
    def _table_cell(value: Any) -> str:
        if value is None:
            return ""
        return str(value).replace("|", "\\|")

    @staticmethod
    def _empty_query_result(
        *,
        item_id: Optional[str],
        where: Optional[str],
        out_fields: Optional[str],
        limit: int,
    ) -> Dict[str, Any]:
        """A conforming QUERY_RESULT payload for a zero-row answer.

        The spatial_query_* tools short-circuit before
        _format_query_results when nothing matches, so they cannot get
        their structured result from it. They still declare an
        outputSchema, and the MCP spec makes a declared schema binding on
        every path -- so "no matches" is a valid RESULT, not an excuse to
        omit structuredContent.

        total_count is 0 rather than null here: zero matches is a known,
        complete count, not the "this tool does not paginate" case null
        is reserved for.
        """
        return {
            "query": {
                "item_id": item_id,
                "service_url": None,
                "where": where,
                "out_fields": out_fields,
                "limit": limit,
                "geometry_type": None,
            },
            "summary": {
                "returned": 0,
                "total_count": 0,
                "truncated": False,
                "complete": True,
            },
            "rows": [],
            "caveats": [],
        }

    def _format_query_results(
        self,
        records: List[Dict[str, Any]],
        limit: int,
        total_count: Optional[int] = None,
        date_fields: Optional[set] = None,
        geometry_type: Optional[str] = None,
        name_field: Optional[str] = None,
        item_id: Optional[str] = None,
        service_url: Optional[str] = None,
        where: Optional[str] = None,
        out_fields: Optional[str] = None,
        coded_domains: Optional[Dict[str, Dict[Any, str]]] = None,
        last_edit_date: Optional[int] = None,
        coverage_pct: Optional[float] = None,
        auto_expanded_from: Optional[int] = None,
        limit_reached: bool = False,
    ) -> Tuple[str, Dict[str, Any]]:
        """Render a record set, and return its structured twin.

        Returns (text, structured) where structured conforms to
        QUERY_RESULT_OUTPUT_SCHEMA. Shared by query_data,
        spatial_query_point, spatial_query_polygon and
        filter_by_polygon, so all four get structured output from this
        one place.

        ``auto_expanded_from`` is the default limit the small-table rule
        lifted (see SMALL_TABLE_ROW_THRESHOLD), so the response can say
        why it holds more rows than the default.

        ``limit_reached`` is for the spatial callers: the page came back
        full (or the server flagged more) and no total could be had, so
        the rows are a sample of unknown size. With a ``total_count``
        the ordinary truncation banner covers it and this is ignored.
        """
        provenance: List[str] = []
        # Structured twin of the caveats appended to `provenance` below.
        # Both are produced here so a caveat can never appear in one and
        # not the other.
        caveats: List[Dict[str, Any]] = []
        if service_url:
            provenance.append(f"Source: {service_url}")
        if where is not None or out_fields is not None:
            parts = []
            if where is not None:
                parts.append(f"where={where!r}")
            if out_fields is not None:
                parts.append(f"outFields={out_fields!r}")
            parts.append(f"resultRecordCount={limit}")
            provenance.append(f"Query: {', '.join(parts)}")
        if provenance:
            retrieved_at = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            provenance.append(f"Retrieved: {retrieved_at}")

        # Truncation banner: make it impossible to miss that the listed
        # records are a sample. Fires when total_count proves the cap.
        # The spatial callers pass a total only when their page was cut
        # (see _spatial_overflow); when even that count failed they
        # pass limit_reached, handled just below.
        truncated = (
            total_count is not None
            and len(records) > 0
            and total_count > len(records)
        )
        unknown_overflow = (
            limit_reached and total_count is None and len(records) > 0
        )
        if unknown_overflow:
            caveats.append(
                {
                    "code": "limit_reached",
                    "count": len(records),
                    "limit": limit,
                    "message": (
                        f"Returned {len(records)} records, which is "
                        f"the limit ({limit}); more match, and the "
                        f"total could not be counted. The listed "
                        f"records are a SAMPLE -- do not count or sum "
                        f"them as if they were the whole set. Raise "
                        f"`limit`, or narrow `where`."
                    ),
                }
            )
            provenance.append(
                f"**TRUNCATED:** returned {len(records)} records, "
                f"which is the limit ({limit}); more match, and the "
                f"total could not be counted. The records below are a "
                f"SAMPLE -- do not count or sum them as if they were "
                f"the whole set. Raise `limit`, or narrow `where`."
            )
        complete = (
            total_count is not None and len(records) == total_count
        )
        if truncated:
            # Name the cheapest next step outright. The old hint ("narrow
            # the WHERE clause to get a complete listing") read as advice
            # to filter, and models turned it into OBJECTID-range paging:
            # many calls where one larger `limit` would do.
            has_geometry_rows = any(
                r.get("__geometry__") is not None for r in records
            )
            one_call_cap = (
                self.GEOMETRY_LIMIT_CAP
                if has_geometry_rows
                else self.QUERY_MAX_LIMIT
            )
            if total_count <= one_call_cap:
                next_step = (
                    f"To get ALL {total_count:,} rows, repeat this "
                    f"call ONCE with limit={total_count} (add "
                    f"out_fields=<only the fields you need> to keep it "
                    f"small). Do not page by OBJECTID ranges."
                )
            else:
                next_step = (
                    f"{total_count:,} rows exceed the {one_call_cap:,}"
                    f"-row cap of one call"
                    + (
                        " with return_geometry=true (drop geometry to "
                        f"raise it to {self.QUERY_MAX_LIMIT:,})"
                        if has_geometry_rows
                        else ""
                    )
                    + ". Narrow `where` (e.g. on a category field), or "
                    "use get_distinct_values / aggregate_by_polygon for "
                    "summaries. Do not page by OBJECTID ranges."
                )
            caveats.append(
                {
                    "code": "result_truncated",
                    "count": total_count,
                    "limit": limit,
                    "message": (
                        f"Returned {len(records)} of {total_count:,} "
                        f"matching records (limit={limit}). The listed "
                        f"records are a SAMPLE -- do not generalize "
                        f"counts or percentages from them. Use "
                        f"total_count for 'how many?' questions. "
                        f"{next_step}"
                    ),
                }
            )
            provenance.append(
                f"**TRUNCATED:** returned {len(records)} of "
                f"{total_count:,} matching records (limit={limit}). "
                f"The records below are a SAMPLE -- do not generalize "
                f"counts or percentages from them. Use the TOTAL COUNT "
                f"line below for 'how many?' questions. {next_step}"
            )

        # Single-record / small-sample caveats: fire only when
        # total_count was actually computed and the result is small
        # enough that a model could mistakenly extrapolate a trend.
        if total_count == 1:
            caveats.append(
                {
                    "code": "single_record",
                    "count": 1,
                    "message": (
                        "Only 1 matching record. Do not report this as "
                        "a trend, pattern, or distribution -- it is an "
                        "N=1 anecdote."
                    ),
                }
            )
            provenance.append(
                "**SINGLE-RECORD CLAIM:** only 1 matching record. Do "
                "not report this as a trend, pattern, or "
                "distribution -- it is an N=1 anecdote."
            )
        elif (
            total_count is not None
            and 1 < total_count < self.SMALL_SAMPLE_THRESHOLD
        ):
            caveats.append(
                {
                    "code": "small_sample",
                    "count": total_count,
                    "limit": self.SMALL_SAMPLE_THRESHOLD,
                    "message": (
                        f"Only {total_count} matching records. "
                        f"Percentages, distributions, and trend claims "
                        f"drawn from this set are weak -- name the "
                        f"sample size in any summary."
                    ),
                }
            )
            provenance.append(
                f"**SMALL SAMPLE:** only {total_count} matching "
                f"records. Percentages, distributions, and trend "
                f"claims drawn from this set are weak -- name the "
                f"sample size in any summary."
            )

        # Staleness caveat: layer hasn't been edited in a long time.
        # The threshold is intentionally a year (not days) -- false
        # positives here are cheap (informational), false negatives
        # (calling old data "current") are expensive.
        if last_edit_date:
            try:
                edit_dt = datetime.fromtimestamp(
                    int(last_edit_date) / 1000, tz=timezone.utc
                )
                age_days = (datetime.now(timezone.utc) - edit_dt).days
                if age_days > self.STALENESS_THRESHOLD_DAYS:
                    age_years = age_days / 365.25
                    caveats.append(
                        {
                            "code": "stale_layer",
                            "count": age_days,
                            "limit": self.STALENESS_THRESHOLD_DAYS,
                            "message": (
                                f"Layer last edited "
                                f"{edit_dt.strftime('%Y-%m-%d')} "
                                f"({age_years:.1f} years ago). Confirm "
                                f"this matches the recency the "
                                f"question needs."
                            ),
                        }
                    )
                    provenance.append(
                        f"**DATA FRESHNESS:** layer last edited "
                        f"{edit_dt.strftime('%Y-%m-%d')} "
                        f"({age_years:.1f} years ago). Confirm this "
                        f"matches the recency the question needs."
                    )
            except (ValueError, TypeError, OSError):
                pass

        # Coverage caveat: layer's spatial extent covers a small slice
        # of the region. Skip when coverage_pct is None (unhandled SR, or
        # a deployment with no region box); honest silence beats a
        # guessed flag.
        region = self._region_name
        if coverage_pct is not None:
            if coverage_pct == 0.0:
                caveats.append(
                    {
                        "code": "no_coverage",
                        "message": (
                            "This layer's spatial extent does not "
                            f"overlap {region} at all. Confirm this is "
                            "the right layer."
                        ),
                    }
                )
                provenance.append(
                    "**COVERAGE:** this layer's spatial extent does "
                    f"not overlap {region} at all. Confirm this is "
                    "the right layer."
                )
            elif coverage_pct < self.COVERAGE_THRESHOLD:
                pct_int = max(1, int(round(coverage_pct * 100)))
                caveats.append(
                    {
                        "code": "limited_coverage",
                        "count": pct_int,
                        "message": (
                            f"This layer's extent covers ~{pct_int}% of "
                            f"{region}. Confirm the question's area of "
                            f"interest falls inside the layer's "
                            f"coverage"
                            f"{self.TEXT['limited_coverage_hint_long']}."
                        ),
                    }
                )
                provenance.append(
                    f"**LIMITED COVERAGE:** this layer's extent "
                    f"covers ~{pct_int}% of {region}. Confirm the "
                    f"question's area of interest falls inside the "
                    f"layer's coverage"
                    f"{self.TEXT['limited_coverage_hint_short']}."
                )

        def _structured(rendered_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
            out: Dict[str, Any] = {
                "query": {
                    "item_id": item_id,
                    "service_url": service_url,
                    "where": where,
                    "out_fields": out_fields,
                    "limit": limit,
                    "geometry_type": geometry_type,
                },
                "summary": {
                    "returned": len(rendered_rows),
                    "total_count": total_count,
                    "truncated": bool(truncated or unknown_overflow),
                    "complete": bool(complete),
                },
                "rows": rendered_rows,
                "caveats": caveats,
            }
            # Coded fields keep their stored codes; ship the domain map
            # alongside so a consumer can decode without a second
            # round-trip. (Date fields are rendered into the rows
            # themselves -- see the row loop.)
            if coded_domains:
                out["coded_domains"] = {
                    field: {str(code): label for code, label in mapping.items()}
                    for field, mapping in coded_domains.items()
                }
            return out

        if not records:
            if provenance:
                return (
                    "\n".join(provenance + ["", "No records returned."]),
                    _structured([]),
                )
            return "No records returned.", _structured([])

        count_part = f"{len(records)}"
        if total_count is not None:
            count_part += f" of {total_count:,} total"
        lines = list(provenance)
        if provenance:
            lines.append("")
        if complete:
            # Say it in so many words: a model that is not told a result
            # is whole tends to go looking for the rest.
            lines.append(
                f"All {len(records):,} of {total_count:,} rows returned "
                f"-- complete, no paging needed."
            )
            if auto_expanded_from is not None:
                lines.append(
                    f"(This result has at most "
                    f"{self.SMALL_TABLE_ROW_THRESHOLD} rows, so the "
                    f"default limit of {auto_expanded_from} was lifted to "
                    f"return it whole.)"
                )
        else:
            lines.append(
                f"Returned {count_part} record(s) (limit: {limit})."
            )
        if total_count is not None:
            lines.append(
                f"TOTAL COUNT (records matching the WHERE clause): "
                f"{total_count:,}. "
                f"This is the answer to 'how many?' -- use it directly "
                f"instead of counting the records below."
            )
        # Polyline-grain warning: a single named entity (a trail, a
        # road, a transit route) is typically stored as MULTIPLE line
        # segments. Reporting "1,123 trails" when the layer holds
        # 1,123 segments is wrong -- there might be 200 unique named
        # trails. Surface this whenever the layer is polyline-typed
        # AND a count was requested, so the model frames its answer
        # honestly. If we know a likely name field, suggest the
        # follow-up call to count unique entities.
        if (
            geometry_type == "esriGeometryPolyline"
            and total_count is not None
        ):
            id_arg = (
                f"item_id='{item_id}'" if item_id else "item_id=<id>"
            )
            if name_field:
                follow_up = (
                    f"`get_distinct_values({id_arg}, "
                    f"field='{name_field}', limit=500)` and count "
                    f"the distinct values returned"
                )
            else:
                follow_up = (
                    f"`get_layer_schema({id_arg})` to find the "
                    f"name/identifier field, then "
                    f"`get_distinct_values` on it to count unique "
                    f"entities"
                )
            caveats.append(
                {
                    "code": "polyline_grain",
                    "count": total_count,
                    "message": (
                        f"The count is the number of LINE SEGMENTS "
                        f"(geometry features), NOT unique named "
                        f"entities. A single named trail, road or route "
                        f"is typically stored as multiple connected "
                        f"segments, so {total_count:,} records usually "
                        f"means fewer than {total_count:,} distinct "
                        f"named features. Say "
                        f"'{total_count:,} trail segments' rather than "
                        f"'{total_count:,} trails'."
                    ),
                }
            )
            lines.append(
                f"**GRAIN NOTE (polyline layer):** the count above "
                f"is the number of LINE SEGMENTS (geometry features), "
                f"NOT the number of unique named entities. A single "
                f"named trail, road, or route is typically stored as "
                f"multiple connected segments -- so '{total_count:,} "
                f"records' usually means fewer than {total_count:,} "
                f"distinct named features. When answering the user, "
                f"say e.g. '{total_count:,} trail segments' rather "
                f"than '{total_count:,} trails'. To count unique "
                f"named entities, call {follow_up}."
            )
        lines.append("")

        # A page where every non-identifier field is null is a grid of
        # empty cells -- pure payload with no information. Say so instead
        # of rendering it: the caller almost always asked for a field the
        # layer does not populate, and the empty grid hides that.
        _ID_FIELDS = {"OBJECTID", "OID", "FID", "__geometry__"}
        value_cols = [
            k
            for record in records
            for k in record
            if k not in _ID_FIELDS
        ]
        if value_cols and all(
            record.get(k) is None
            for record in records
            for k in set(value_cols)
        ):
            lines.append(
                f"All {len(set(value_cols))} requested non-identifier "
                f"field(s) are NULL on every one of the {len(records)} "
                f"returned record(s): "
                f"{', '.join(sorted(set(value_cols))[:8])}"
                f"{' ...' if len(set(value_cols)) > 8 else ''}. "
                f"The records exist but carry no values in these fields -- "
                f"check the field names with get_layer_schema, or the "
                f"layer may simply not populate them."
            )
            return "\n".join(lines), _structured(
                [
                    {k: v for k, v in r.items() if k != "__geometry__"}
                    for r in records
                ]
            )

        has_geometry = any(
            record.get("__geometry__") is not None for record in records
        )
        if (
            len(records) > self.COMPACT_FORMAT_THRESHOLD
            and not has_geometry
        ):
            # Large result sets: pipe-delimited table (header row + one
            # row per record) instead of per-record blocks, which cost
            # ~3x the bytes of the data they carry.
            columns: List[str] = []
            seen = set()
            for record in records:
                for key in record:
                    if key != "__geometry__" and key not in seen:
                        seen.add(key)
                        columns.append(key)
            lines.append(
                f"(Compact format: {len(records)} records, one "
                f"pipe-delimited row each; the first row is the header.)"
            )
            lines.append(" | ".join(columns))
            for record in records:
                lines.append(
                    " | ".join(
                        self._table_cell(
                            self._render_field_value(
                                k,
                                record.get(k),
                                date_fields,
                                coded_domains,
                            )
                        )
                        for k in columns
                    )
                )
            lines.append("")
        else:
            for i, record in enumerate(records, 1):
                lines.append(f"Record {i}:")
                geometry = record.get("__geometry__")
                for key, value in record.items():
                    if key == "__geometry__":
                        continue
                    value = self._render_field_value(
                        key, value, date_fields, coded_domains
                    )
                    lines.append(f"  {key}: {value}")
                if geometry is not None:
                    geom_str = json.dumps(geometry, separators=(",", ":"))
                    if len(geom_str) > self.GEOMETRY_STR_MAX:
                        geom_clip = geom_str[: self.GEOMETRY_STR_MAX]
                        geom_str = (
                            f"{geom_clip}... "
                            f"(truncated, {len(geom_str)} chars total; "
                            f"server-side simplified to "
                            f"~{self.GEOMETRY_SIMPLIFY_OFFSET_DEG} deg "
                            f"~ 5.5m)"
                        )
                    lines.append(
                        f"  geometry (GeoJSON, WGS84): {geom_str}"
                    )
                lines.append("")
        # The TRUNCATED banner and TOTAL COUNT line above already carry
        # the sampling caveat. Copilot (now GPT-5.1) and Claude both read
        # the full response top-down, so the older bottom-of-response
        # restatement that guarded against GPT-4o-style summarization is
        # no longer needed and has been dropped.
        rows: List[Dict[str, Any]] = []
        for record in records:
            row = {k: v for k, v in record.items() if k != "__geometry__"}
            # Dates render in the structured rows too: clients that
            # consume only structuredContent otherwise see raw epoch
            # milliseconds while the docs promise YYYY-MM-DD (the
            # date_format='epoch' path passes date_fields=None and is
            # unaffected). Coded fields stay raw here on purpose --
            # coded_domains ships alongside as the decode map.
            if date_fields:
                for k in date_fields:
                    if k in row:
                        row[k] = self._ms_to_iso_smart(row[k])
            geometry = record.get("__geometry__")
            if geometry is not None:
                # Full geometry, not the character-clipped rendering the
                # text uses -- truncating the machine-readable copy would
                # make it unusable for anything downstream.
                row["__geometry__"] = geometry
            rows.append(row)
        return "\n".join(lines), _structured(rows)

    # ── DataPlugin interface methods ──────────────────────────────────────

    async def search_datasets(
        self, query: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Search gallery and org layers for a topic."""
        gallery, layers = await asyncio.gather(
            self._search_gallery(query, limit),
            self._search_org_layers(
                query, self.LAYER_TYPES + self.DATA_TYPES, limit
            ),
        )
        return gallery + layers

    async def get_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """Get item details by ArcGIS item ID.

        Rejects items not owned by the configured org. ArcGIS portals
        will happily return any *public* item from any org, so without
        this check a caller could browse arbitrary ArcGIS Online content
        through this MCP and use item descriptions as a prompt-injection
        vector against the calling LLM.
        """
        dataset_id = self._validate_item_id(dataset_id)
        # Cached only AFTER the ownership check passes, so a foreign item
        # is re-fetched and re-rejected every time, never served from cache.
        return await self._get_meta_json(
            f"{self.plugin_config.portal_base_url}/content/items/{dataset_id}",
            validate=self._assert_owned_by_configured_org,
        )

    def _assert_owned_by_configured_org(self, item: Dict[str, Any]) -> None:
        """Fail-closed ownership check. Missing orgId is also a rejection.

        Accepts the configured org and every configured partner org.
        """
        allowed = self._allowed_org_ids
        item_org = (item.get("orgId") or "").lower()
        if not item_org or item_org not in allowed:
            raise ToolInputError(self._foreign_org_message(item))

    @classmethod
    def _explain_unresolved_error(
        cls, full: str, item_arg: str, where_clause: Optional[str]
    ) -> Optional[str]:
        """Hook: explain an ArcGIS error none of the generic cases in
        _rewrite_arcgis_error matched. Return the full replacement text,
        or None to pass the raw error through."""
        return None

    @classmethod
    def _rewrite_arcgis_error(
        cls,
        msg: str,
        details: List[str],
        resource_id: Optional[str] = None,
        has_out_fields: bool = False,
        has_where: bool = False,
        where_clause: Optional[str] = None,
    ) -> str:
        """Turn raw ArcGIS REST errors into actionable instructions.

        ArcGIS error messages assume a developer audience that knows
        the schema and the REST contract. A weaker model reading
        "Cannot perform query. Invalid query parameters." has no idea
        what to do next. We pattern-match the common shapes and append
        the concrete tool call the model should make to recover.
        """
        msg = msg or "Unknown error"
        detail_str = "; ".join(details) if details else ""
        full = f"{msg}" + (f" -- {detail_str}" if detail_str else "")
        item_arg = (
            f"item_id='{resource_id}'" if resource_id else "item_id=<id>"
        )
        # Case A: ArcGIS named the bad field (WHERE-clause case).
        m = re.search(
            r"[Ii]nvalid\s+field\s*:\s*([A-Za-z0-9_]+)", full
        )
        if m:
            bad = m.group(1)
            return (
                f"Field '{bad}' does not exist on this layer. "
                f"Field names are CASE-SENSITIVE. To recover: call "
                f"`get_layer_schema({item_arg})` to see valid field "
                f"names, then retry with the exact name shown there. "
                f"(Underlying error: {full})"
            )
        # Case B: generic "Invalid query parameters" -- usually
        # out_fields contains a name that doesn't exist. ArcGIS does
        # NOT echo the bad name back, so the model has to discover it.
        if "Invalid query parameters" in full:
            hint_parts = []
            if has_out_fields:
                hint_parts.append(
                    "out_fields may reference a field that does not "
                    "exist (ArcGIS does not name it in the error). "
                    "Try out_fields='*' to confirm, then narrow."
                )
            if has_where:
                hint_parts.append(
                    "the WHERE clause may reference a missing field "
                    "or use the wrong type (string values must be "
                    "single-quoted)."
                )
            hint_parts.append(
                f"Call `get_layer_schema({item_arg})` to see valid "
                f"field names -- they are CASE-SENSITIVE."
            )
            return f"{full}\n\nLikely cause: " + " ".join(hint_parts)
        explained = cls._explain_unresolved_error(
            full, item_arg, where_clause
        )
        return explained if explained else full

    @staticmethod
    def _no_data_hint(where_clause: str) -> str:
        """Hint to append after an empty result with a non-trivial WHERE.

        Any model can write ``Field='exact value'`` when what it wanted
        is ``Field LIKE '%substring%'``. ArcGIS returns zero rows
        silently, and without a nudge the model reports the data does
        not exist. Append a concrete recovery instruction so the model
        retries instead of giving up.
        """
        normalized = (where_clause or "").strip()
        if not normalized or normalized == "1=1":
            return ""
        return (
            "\n\n_If you expected matches:_\n"
            "- For TEXT fields, exact-match `=` is strict and "
            "case-sensitive. Try `Field LIKE '%substring%'` "
            "(% is the wildcard).\n"
            "- For NUMERIC/DATE fields, verify the value type "
            "matches the schema.\n"
            "- Field names are CASE-SENSITIVE -- call "
            "`get_layer_schema(item_id=<id>)` to confirm.\n"
            "- To verify the layer has data, retry with "
            "`where='1=1'`."
        )

    # Field-name priority for picking the user-facing identifier from
    # a feature's attributes. A model can latch onto the first
    # identifier-shaped value it sees in the response and report it back
    # as canonical -- if the lead line says "OBJECTID 778" the model may
    # say "parcel 778" even when Parcel_ID is right below. We promote one
    # of these field names into the lead position so the wrong identifier
    # never appears first, rather than relying on the model to sort it
    # out from a warning.
    NATURAL_ID_FIELD_PRIORITY = (
        # Parcel-style identifiers (most common request shape).
        "Parcel_ID", "PARCEL_ID", "ParcelID", "PARCELID",
        "Parcel_Num", "PARCEL_NUM", "ParcelNum", "PARCELNUM",
        "Parcel_Number", "ParcelNumber",
        "GIS_ParcelNum8Formatted", "GIS_ParcelNum11Formatted",
        "GIS_ParcelNum8", "GIS_ParcelNum11",
        # Polyline-entity name fields -- listed BEFORE generic `Name`
        # so a trails/roads layer with both `Trail_Name` and `Name`
        # picks the more specific one. These also drive the grain
        # warning's follow-up suggestion when query_data hits a
        # polyline layer.
        "Trail_Name", "TRAIL_NAME", "TrailName",
        "Road_Name", "ROAD_NAME", "RoadName",
        "Street_Name", "STREET_NAME", "StreetName",
        "Route_Name", "ROUTE_NAME", "RouteName",
        # Generic record-name fields (next-best fallback).
        "Name", "NAME", "FullName", "Full_Name",
        "Site_Name", "SiteName",
        "Site_Address", "Address", "ADDRESS",
        "Title", "TITLE",
        # Common code/identifier fields.
        "Permit_Number", "PermitNumber",
        "Project_Name", "ProjectName",
        "Plat_Number", "PlatNumber",
    )

    # Subset of NATURAL_ID_FIELD_PRIORITY that strongly indicates a
    # layer is at parcel grain (one row per legal parcel). Used to
    # detect when a classification arg is the wrong shape -- e.g., a
    # parcels-with-Zoning_District-attribute layer being used as the
    # classification for "parcels spanning multiple zones". The right
    # classification is a zoning-polygon layer where many polygons
    # share each zone code.
    PARCEL_INDICATOR_FIELDS = frozenset((
        "Parcel_ID", "PARCEL_ID", "ParcelID", "PARCELID",
        "Parcel_Num", "PARCEL_NUM", "ParcelNum", "PARCELNUM",
        "Parcel_Number", "ParcelNumber",
        "GIS_ParcelNum8", "GIS_ParcelNum11",
        "GIS_ParcelNum8Formatted", "GIS_ParcelNum11Formatted",
    ))

    @classmethod
    def _pick_natural_id(
        cls, attrs: Dict[str, Any]
    ) -> Optional[Tuple[str, Any]]:
        """Pick the most likely user-facing identifier from a feature's
        attributes. Returns (field_name, value) or None.

        Priority is: parcel-ish IDs > name-ish fields > misc codes.
        Skips None/empty values so the lead line never says
        "Parcel_ID None".
        """
        for field in cls.NATURAL_ID_FIELD_PRIORITY:
            if field in attrs:
                v = attrs[field]
                if v is not None and v != "":
                    return (field, v)
        return None

    @staticmethod
    def _not_queryable_message(
        item_id: str, item_type: str = ""
    ) -> str:
        type_note = (
            f" (item type: '{item_type}')" if item_type else ""
        )
        return (
            f"Item '{item_id}' is not a queryable Feature/Map "
            f"Service{type_note}. Web Maps, Apps, Dashboards, and "
            f"Story Maps are VIEWERS, not data -- they cannot be "
            f"queried for records. To recover: call "
            f"`find_gis_content(topic=<your topic>)` and pick from "
            f"the **QUERYABLE** section (Feature/Map Services), or "
            f"`get_item_details(item_id='{item_id}')` to inspect "
            f"this item's relationships."
        )

    # Cap on records when geometry is requested -- polygons can be
    # orders of magnitude larger than attribute rows, so we keep this
    # much tighter than the no-geometry cap of 1000.
    GEOMETRY_LIMIT_CAP = 50

    # Server-side simplification tolerance in the output SR's units.
    # We pin outSR=4326 when returnGeometry=true, so this is in decimal
    # degrees: 0.00005 deg ~ 5.5m at the equator. Fine for MCP-scale
    # reasoning about shape, keeps payloads manageable.
    GEOMETRY_SIMPLIFY_OFFSET_DEG = 0.00005

    async def query_data(
        self,
        resource_id: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        return_geometry: bool = False,
    ) -> List[Dict[str, Any]]:
        """Query records from a Feature Service by item ID.

        Two-hop resolution: looks up item to get service URL, then queries it.

        When return_geometry=True, the response format switches to GeoJSON
        (f=geojson), geometry is auto-simplified to
        ~GEOMETRY_SIMPLIFY_OFFSET_DEG, and `limit` is capped at
        GEOMETRY_LIMIT_CAP. Each returned record carries a `__geometry__`
        key holding the GeoJSON geometry object.
        """
        if limit < 1:
            raise ToolInputError(f"limit must be at least 1 (got {limit})")

        item = await self.get_dataset(resource_id)
        service_url = item.get("url", "")
        item_type = item.get("type", "")

        if not service_url:
            raise ToolInputError(
                self._not_queryable_message(resource_id, item_type)
            )
        if item_type and item_type not in self.QUERYABLE_TYPES:
            raise ToolInputError(
                self._not_queryable_message(resource_id, item_type)
            )

        where_clause = filters.get("where", "1=1") if filters else "1=1"
        where_clause = WhereValidator.validate(where_clause)
        raw_out_fields = filters.get("out_fields", "*") if filters else "*"
        out_fields = OutFieldsValidator.validate(raw_out_fields)
        order_by = OrderByValidator.validate(
            filters.get("order_by") or "" if filters else ""
        )

        service_url = self._ensure_layer_url(service_url)
        self._validate_service_url(service_url)
        query_url = f"{service_url}/query"

        max_records = (
            self.GEOMETRY_LIMIT_CAP if return_geometry else 1000
        )
        effective_limit = min(limit, max_records)
        params: Dict[str, Any] = {
            "where": where_clause,
            "outFields": out_fields,
            "resultRecordCount": effective_limit,
            "f": "geojson" if return_geometry else "json",
            "returnGeometry": "true" if return_geometry else "false",
        }
        if return_geometry:
            # Pin output SR so callers can't swap coordinate systems;
            # keeps the simplification offset in known units.
            params["outSR"] = "4326"
            params["maxAllowableOffset"] = str(
                self.GEOMETRY_SIMPLIFY_OFFSET_DEG
            )
        if order_by:
            params["orderByFields"] = order_by

        try:
            resp = await self._send("get", query_url, params=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise RuntimeError(
                f"Feature Service query error (HTTP {e.response.status_code}): "
                f"{e.response.text[:300]}"
            ) from e

        try:
            data = resp.json()
        except Exception as json_err:
            content_type = resp.headers.get("content-type", "")
            raise ValueError(
                f"Feature Service returned non-JSON response "
                f"(content-type: {content_type}). The item URL may not "
                f"point to a queryable ArcGIS Feature Service."
            ) from json_err

        error_in_body = data.get("error")
        if error_in_body:
            code = error_in_body.get("code", "unknown")
            msg = error_in_body.get("message", "Unknown error")
            details = error_in_body.get("details", [])
            raise RuntimeError(
                f"Feature Service query failed (code {code}): "
                + self._rewrite_arcgis_error(
                    msg,
                    details,
                    resource_id=resource_id,
                    has_out_fields=raw_out_fields not in ("*", ""),
                    has_where=where_clause not in ("1=1", ""),
                    where_clause=where_clause,
                )
            )

        features = data.get("features", [])
        if return_geometry:
            # f=geojson returns FeatureCollection with
            # {type,geometry,properties} features.
            return [
                {
                    **(f.get("properties") or {}),
                    "__geometry__": f.get("geometry"),
                }
                for f in features
            ]
        return [f.get("attributes", {}) for f in features]

    async def spatial_query_point(
        self,
        resource_id: str,
        lon: float,
        lat: float,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        info: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Find polygon features in a Feature Service that contain a point.

        Uses ArcGIS REST spatialRel=Intersects with a point geometry in
        WGS84 (EPSG:4326). Input SR is pinned server-side so callers
        cannot supply an arbitrary CRS. Returns attributes only; geometry
        is suppressed to keep payloads small.

        Pass a dict as ``info`` to learn whether the result was cut at
        ``limit`` (see ``_spatial_overflow``).
        """
        if limit < 1:
            raise ToolInputError(f"limit must be at least 1 (got {limit})")

        lon_f, lat_f = self._validate_lonlat(lon, lat)

        item = await self.get_dataset(resource_id)
        service_url = item.get("url", "")
        item_type = item.get("type", "")

        if not service_url:
            raise ToolInputError(
                self._not_queryable_message(resource_id, item_type)
            )
        if item_type and item_type not in self.QUERYABLE_TYPES:
            raise ToolInputError(
                self._not_queryable_message(resource_id, item_type)
            )

        raw_where = (filters or {}).get("where", "1=1")
        raw_out_fields = (filters or {}).get("out_fields", "*")
        where_clause = WhereValidator.validate(raw_where)
        out_fields = OutFieldsValidator.validate(raw_out_fields)

        service_url = self._ensure_layer_url(service_url)
        self._validate_service_url(service_url)

        # Pre-check: layer must be polygon-type for point-in-polygon
        # to make sense. Fail loudly rather than silently returning
        # whatever Intersects happens to hit on a points/lines layer.
        meta = await self._get_meta_json(service_url)
        geom_type = meta.get("geometryType", "")
        if geom_type not in (
            "esriGeometryPolygon",
            "esriGeometryMultiPatch",
        ):
            raise ToolInputError(
                f"spatial_query_point requires a polygon layer "
                f"(layer geometryType is {geom_type or 'unknown'!r})"
            )

        query_url = f"{service_url}/query"
        params = {
            "where": where_clause,
            "outFields": out_fields,
            "geometry": f"{lon_f},{lat_f}",
            "geometryType": "esriGeometryPoint",
            "inSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            "resultRecordCount": min(limit, 50),
            "returnGeometry": "false",
            "f": "json",
        }

        try:
            resp = await self._send("get", query_url, params=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise RuntimeError(
                f"Feature Service spatial query error "
                f"(HTTP {e.response.status_code}): {e.response.text[:300]}"
            ) from e

        try:
            data = resp.json()
        except Exception as json_err:
            content_type = resp.headers.get("content-type", "")
            raise ValueError(
                f"Feature Service returned non-JSON response "
                f"(content-type: {content_type})."
            ) from json_err

        error_in_body = data.get("error")
        if error_in_body:
            code = error_in_body.get("code", "unknown")
            msg = error_in_body.get("message", "Unknown error")
            details = error_in_body.get("details", [])
            raise RuntimeError(
                f"Feature Service spatial query failed (code {code}): "
                + self._rewrite_arcgis_error(
                    msg,
                    details,
                    resource_id=resource_id,
                    has_out_fields=raw_out_fields not in ("*", ""),
                    has_where=raw_where not in ("1=1", ""),
                    where_clause=raw_where,
                )
            )

        features = data.get("features") or []
        await self._spatial_overflow(
            "get",
            query_url,
            params,
            data,
            returned=len(features),
            requested=min(limit, 50),
            info=info,
        )
        return [f.get("attributes", {}) for f in features]

    # {tool name: {argument name: declared JSON type}}, built on first
    # use by _clean_arguments.
    _arg_types: Optional[Dict[str, Dict[str, Any]]] = None

    # Query parameters that shape the page, not the match set; the
    # count twin of a query drops them.
    _PAGE_ONLY_PARAMS = frozenset(
        {
            "outFields",
            "resultRecordCount",
            "resultOffset",
            "returnGeometry",
            "outSR",
            "maxAllowableOffset",
            "orderByFields",
            "f",
        }
    )

    async def _spatial_overflow(
        self,
        method: str,
        query_url: str,
        params: Dict[str, Any],
        data: Dict[str, Any],
        returned: int,
        requested: int,
        info: Optional[Dict[str, Any]],
    ) -> None:
        """Record in ``info`` whether a spatial page was cut short.

        A page that came back full, or that the server flagged with
        exceededTransferLimit, may be a sample. In that case (and only
        then, so the ordinary small answer costs no extra request) run
        the same query as returnCountOnly. ``info`` receives
        ``total_count`` when more match than were returned, else
        ``limit_reached`` when the count could not be had. Neither key
        is set when the page is the whole answer.
        """
        if info is None:
            return
        props = data.get("properties")
        exceeded = bool(
            data.get("exceededTransferLimit")
            or (
                isinstance(props, dict)
                and props.get("exceededTransferLimit")
            )
        )
        if returned == 0 or (not exceeded and returned < requested):
            return
        count_params = {
            k: v
            for k, v in params.items()
            if k not in self._PAGE_ONLY_PARAMS
        }
        count_params["returnCountOnly"] = "true"
        count_params["f"] = "json"
        count: Any = None
        try:
            if method == "post":
                resp = await self._send(
                    "post", query_url, data=count_params
                )
            else:
                resp = await self._send(
                    "get", query_url, params=count_params
                )
            resp.raise_for_status()
            body = resp.json()
            if isinstance(body, dict) and not body.get("error"):
                count = body.get("count")
        except Exception as exc:  # the rows are still worth returning
            logger.warning(
                "Spatial count twin failed for %s: %s",
                self._url_label(query_url),
                exc,
            )
        if isinstance(count, bool) or not isinstance(count, int):
            info["limit_reached"] = True
        elif count > returned:
            info["total_count"] = count
        elif exceeded:
            # The server says more exist but counts no more than we
            # hold: trust the flag over the arithmetic.
            info["limit_reached"] = True

    _SPATIAL_REL_MAP = {
        "intersects": "esriSpatialRelIntersects",
        "contains": "esriSpatialRelContains",
        "within": "esriSpatialRelWithin",
        "crosses": "esriSpatialRelCrosses",
        "touches": "esriSpatialRelTouches",
        "overlaps": "esriSpatialRelOverlaps",
        "envelope_intersects": "esriSpatialRelEnvelopeIntersects",
    }

    # Linear units accepted from the model (case-insensitive, with common
    # aliases) and how they map to (a) the ArcGIS REST `units` constant for
    # server-side buffered queries, and (b) meters for client-side distance
    # math in aggregate_by_polygon. Keep the two maps in lock-step.
    _LINEAR_UNIT_ALIASES = {
        "meters": "meters", "meter": "meters", "metre": "meters",
        "metres": "meters", "m": "meters",
        "kilometers": "kilometers", "kilometer": "kilometers",
        "kilometre": "kilometers", "kilometres": "kilometers",
        "km": "kilometers",
        "feet": "feet", "foot": "feet", "ft": "feet",
        "miles": "miles", "mile": "miles", "mi": "miles",
        "yards": "yards", "yard": "yards", "yd": "yards",
    }
    _ESRI_LINEAR_UNITS = {
        "meters": "esriSRUnit_Meter",
        "kilometers": "esriSRUnit_Kilometer",
        "feet": "esriSRUnit_Foot",
        "miles": "esriSRUnit_StatuteMile",
        "yards": "esriSRUnit_Yard",
    }
    _LINEAR_UNIT_TO_METERS = {
        "meters": 1.0,
        "kilometers": 1000.0,
        "feet": 0.3048,
        "miles": 1609.344,
        "yards": 0.9144,
    }

    @classmethod
    def _normalize_linear_unit(cls, units: Optional[str]) -> str:
        """Canonicalize a free-text linear unit (default meters)."""
        key = (units or "meters").strip().lower()
        canonical = cls._LINEAR_UNIT_ALIASES.get(key)
        if canonical is None:
            raise ToolInputError(
                f"units {units!r} is not a supported linear unit. Use "
                f"one of: meters, kilometers, feet, miles, yards."
            )
        return canonical

    async def spatial_query_polygon(
        self,
        resource_id: str,
        filter_geometry: Optional[Dict[str, Any]] = None,
        filter_item_id: Optional[str] = None,
        filter_where: str = "1=1",
        spatial_rel: str = "intersects",
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 50,
        return_geometry: bool = False,
        distance: Optional[float] = None,
        units: str = "meters",
        info: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Find features in a target layer that spatially relate to a polygon filter.

        Pass a dict as ``info`` to learn whether the result was cut at
        ``limit`` (see ``_spatial_overflow``).

        The filter polygon is supplied either inline as GeoJSON via
        ``filter_geometry`` (Polygon, MultiPolygon, or Feature), or by
        dereferencing polygon features in another layer via
        ``filter_item_id`` + ``filter_where``. The target layer may be
        polygon, polyline, or point.

        Unlike a centroid-based assignment, this returns every target
        feature whose geometry touches the filter polygon, so features
        that straddle the filter boundary are still included. Set
        ``return_geometry=True`` to get GeoJSON geometries back for
        precise client-side clipping.

        When ``distance`` is a positive number, the filter polygon is
        buffered by that distance (in ``units``) server-side before the
        spatial relation is evaluated -- this is how "within N miles/feet
        of <polygon>" proximity questions are answered (e.g. population
        within half a mile of a park).
        """
        if limit < 1:
            raise ToolInputError(f"limit must be at least 1 (got {limit})")

        buffer_distance: Optional[float] = None
        esri_units: Optional[str] = None
        if distance is not None and str(distance) != "":
            try:
                buffer_distance = float(distance)
            except (TypeError, ValueError) as exc:
                raise ToolInputError(
                    f"distance must be a number (got {distance!r})"
                ) from exc
            if not math.isfinite(buffer_distance) or buffer_distance < 0:
                raise ToolInputError(
                    f"distance must be a finite number >= 0 "
                    f"(got {distance!r})"
                )
            if buffer_distance == 0:
                buffer_distance = None  # no buffer; treat as exact
            else:
                esri_units = self._ESRI_LINEAR_UNITS[
                    self._normalize_linear_unit(units)
                ]

        if not filter_geometry and not filter_item_id:
            raise ToolInputError(
                "spatial_query_polygon requires either filter_geometry "
                "(inline GeoJSON) or filter_item_id + filter_where"
            )

        spatial_rel_esri = self._SPATIAL_REL_MAP.get(
            (spatial_rel or "intersects").lower()
        )
        if not spatial_rel_esri:
            raise ToolInputError(
                f"spatial_rel must be one of "
                f"{sorted(self._SPATIAL_REL_MAP)} (got {spatial_rel!r})"
            )

        if filter_geometry:
            esri_filter = self._geojson_to_esri_polygon(filter_geometry)
            self._check_lonlat_rings(esri_filter["rings"])
            # A MultiPolygon's parts may overlap or abut (two buffers,
            # adjacent parcels copied from an earlier result). Sent as
            # plain concatenated rings, the overlap reads as a hole and
            # targets inside it are dropped -- the same failure
            # _fetch_filter_polygon unions away for layer filters.
            inner = filter_geometry
            if inner.get("type") == "Feature":
                inner = inner.get("geometry") or {}
            if (
                inner.get("type") == "MultiPolygon"
                and len(inner.get("coordinates") or []) > 1
            ):
                unioned = self._union_esri_rings(esri_filter["rings"])
                if unioned:
                    esri_filter["rings"] = unioned
        else:
            esri_filter = await self._fetch_filter_polygon(
                filter_item_id, filter_where
            )

        item = await self.get_dataset(resource_id)
        target_url = item.get("url", "")
        item_type = item.get("type", "")
        if not target_url:
            raise ToolInputError(
                self._not_queryable_message(resource_id, item_type)
            )
        if item_type and item_type not in self.QUERYABLE_TYPES:
            raise ToolInputError(
                self._not_queryable_message(resource_id, item_type)
            )

        target_url = self._ensure_layer_url(target_url)
        self._validate_service_url(target_url)

        raw_where = (filters or {}).get("where", "1=1")
        raw_out_fields = (filters or {}).get("out_fields", "*")
        where_clause = WhereValidator.validate(raw_where)
        out_fields = OutFieldsValidator.validate(raw_out_fields)

        max_records = (
            self.GEOMETRY_LIMIT_CAP if return_geometry else 1000
        )
        effective_limit = min(limit, max_records)

        params: Dict[str, Any] = {
            "where": where_clause,
            "outFields": out_fields,
            "geometry": json.dumps(esri_filter, separators=(",", ":")),
            "geometryType": "esriGeometryPolygon",
            "inSR": "4326",
            "spatialRel": spatial_rel_esri,
            "resultRecordCount": str(effective_limit),
            "f": "geojson" if return_geometry else "json",
            "returnGeometry": "true" if return_geometry else "false",
        }
        # Server-side buffer: ArcGIS expands the filter geometry by
        # `distance` (in `units`) before applying spatialRel, so a plain
        # parks polygon answers "within half a mile of a park".
        if buffer_distance is not None:
            params["distance"] = str(buffer_distance)
            params["units"] = esri_units
        if return_geometry:
            params["outSR"] = "4326"
            params["maxAllowableOffset"] = str(
                self.GEOMETRY_SIMPLIFY_OFFSET_DEG
            )

        query_url = f"{target_url}/query"
        try:
            # POST because filter polygons routinely exceed URL length
            # limits -- spatial_query_point can get away with GET, this
            # one cannot.
            resp = await self._send("post", query_url, data=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise RuntimeError(
                f"Feature Service spatial query error "
                f"(HTTP {e.response.status_code}): {e.response.text[:300]}"
            ) from e

        try:
            data = resp.json()
        except Exception as json_err:
            content_type = resp.headers.get("content-type", "")
            raise ValueError(
                f"Feature Service returned non-JSON response "
                f"(content-type: {content_type})."
            ) from json_err

        error_in_body = data.get("error")
        if error_in_body:
            code = error_in_body.get("code", "unknown")
            msg = error_in_body.get("message", "Unknown error")
            details = error_in_body.get("details", [])
            raise RuntimeError(
                f"Feature Service spatial query failed (code {code}): "
                + self._rewrite_arcgis_error(
                    msg,
                    details,
                    resource_id=resource_id,
                    has_out_fields=raw_out_fields not in ("*", ""),
                    has_where=raw_where not in ("1=1", ""),
                    where_clause=raw_where,
                )
            )

        features = data.get("features") or []
        await self._spatial_overflow(
            "post",
            query_url,
            params,
            data,
            returned=len(features),
            requested=effective_limit,
            info=info,
        )
        if return_geometry:
            return [
                {
                    **(f.get("properties") or {}),
                    "__geometry__": f.get("geometry"),
                }
                for f in features
            ]
        return [f.get("attributes", {}) for f in features]

    # Caps on inbound filter polygons. ArcGIS will accept far larger geometries,
    # but huge inputs translate into huge POST bodies upstream and slow
    # spatial-query plans. Real Anchorage admin boundaries (council districts,
    # parks, plats) sit well under these limits. Raise with evidence.
    MAX_FILTER_RINGS = 1000
    MAX_FILTER_COORDS = 10000
    # Cap on how many features _fetch_filter_polygon will fetch and
    # union into one filter geometry. Above this, refuse LOUDLY: the
    # old behaviour (fetch the first 50, silently drop the rest) is
    # exactly the kind of quiet false negative this cap replaces.
    MAX_FILTER_FEATURES = 2000
    FILTER_FETCH_PAGE = 1000
    # Integer scaling for the degree-space pyclipper union of filter
    # rings: 1 clipper unit = 1e-7 degree (~1.1 cm of longitude at the
    # equator). Distinct from CLIP_SCALE, which scales EPSG:3338 meters.
    UNION_DEG_SCALE = 1e7

    @staticmethod
    def _check_lonlat_rings(rings: Any) -> None:
        """Refuse a caller's polygon that is not WGS84 lon/lat.

        The filter is sent to ArcGIS as EPSG:4326. Coordinates in
        another system (Web Mercator metres, State Plane feet) were
        accepted and matched nothing, so the caller got a confident
        "no features" for a polygon that was simply misread.
        """
        for ring in rings:
            if not isinstance(ring, list) or len(ring) < 4:
                raise ToolInputError(
                    "filter_geometry: every polygon ring needs at least "
                    "4 positions (the first repeated as the last)"
                )
            for pos in ring:
                ok = (
                    isinstance(pos, (list, tuple))
                    and len(pos) >= 2
                    and all(
                        isinstance(v, (int, float))
                        and not isinstance(v, bool)
                        and math.isfinite(v)
                        for v in pos[:2]
                    )
                )
                if not ok:
                    raise ToolInputError(
                        "filter_geometry: every position must be "
                        f"[longitude, latitude] numbers (got {pos!r})"
                    )
                if not (-180.0 <= pos[0] <= 180.0 and -90.0 <= pos[1] <= 90.0):
                    raise ToolInputError(
                        "filter_geometry must be WGS84 longitude/latitude "
                        "in degrees, longitude first (EPSG:4326). Got "
                        f"position {list(pos[:2])!r}, which is outside "
                        "[-180, 180] / [-90, 90] -- the polygon looks "
                        "projected (Web Mercator or State Plane) or has "
                        "latitude first. Reproject it, then retry."
                    )

    @staticmethod
    def _geojson_to_esri_polygon(geojson: Any) -> Dict[str, Any]:
        """Convert GeoJSON Polygon / MultiPolygon / Feature to Esri polygon JSON."""
        if not isinstance(geojson, dict):
            raise ToolInputError(
                f"filter_geometry must be a GeoJSON object "
                f"(got {type(geojson).__name__})"
            )
        gj_type = geojson.get("type", "")
        if gj_type == "Feature":
            return ArcGISHubPlugin._geojson_to_esri_polygon(
                geojson.get("geometry") or {}
            )
        if gj_type == "Polygon":
            rings = list(geojson.get("coordinates") or [])
        elif gj_type == "MultiPolygon":
            rings = []
            for poly in geojson.get("coordinates") or []:
                rings.extend(poly)
        else:
            raise ToolInputError(
                f"filter_geometry must be a GeoJSON Polygon, "
                f"MultiPolygon, or Feature wrapping one "
                f"(got type={gj_type!r})"
            )
        if not rings:
            raise ToolInputError("filter_geometry has no polygon rings")

        ring_count = len(rings)
        if ring_count > ArcGISHubPlugin.MAX_FILTER_RINGS:
            raise ToolInputError(
                f"filter_geometry has {ring_count} rings; "
                f"max is {ArcGISHubPlugin.MAX_FILTER_RINGS}. "
                f"Simplify the polygon or use filter_item_id with a "
                f"published boundary layer."
            )
        coord_count = sum(len(r) for r in rings if isinstance(r, list))
        if coord_count > ArcGISHubPlugin.MAX_FILTER_COORDS:
            raise ToolInputError(
                f"filter_geometry has {coord_count} coordinates; "
                f"max is {ArcGISHubPlugin.MAX_FILTER_COORDS}. "
                f"Simplify the polygon (e.g. mapshaper at 1% tolerance) "
                f"or use filter_item_id with a published boundary layer."
            )

        return {
            "rings": rings,
            "spatialReference": {"wkid": 4326},
        }

    @classmethod
    def _union_esri_rings(
        cls, rings: List[List[List[float]]]
    ) -> Optional[List[List[List[float]]]]:
        """Geometrically union possibly overlapping/adjacent rings.

        Concatenating rings from many features into one Esri polygon
        is NOT a union: under the even-odd fill rule applied to
        overlapping same-orientation rings, each added ring can flip
        already-covered area into a hole, so the filter region SHRINKS
        as features are added (observed: multi-parcel container
        filters silently dropping targets that sit squarely inside
        individual parcels). This resolves the rings into clean,
        non-overlapping rings with pyclipper before they go upstream.

        Ring orientation must be internally consistent (all-Esri or
        all-GeoJSON convention) so PFT_NONZERO adds overlapping
        exteriors instead of cancelling them; holes wound opposite
        their exteriors subtract correctly either way. Returns rings
        in Esri orientation (clockwise exteriors, counter-clockwise
        holes), or None when the union is empty or pyclipper rejects
        the input -- callers fall back to the raw rings.
        """
        scale = cls.UNION_DEG_SCALE
        paths = []
        for ring in rings or []:
            if not isinstance(ring, list):
                continue
            path = [
                (
                    int(round(pt[0] * scale)),
                    int(round(pt[1] * scale)),
                )
                for pt in ring
                if isinstance(pt, (list, tuple)) and len(pt) >= 2
            ]
            if len(path) > 1 and path[0] == path[-1]:
                path.pop()
            if len(path) >= 3:
                paths.append(path)
        if not paths:
            return None
        try:
            pc = pyclipper.Pyclipper()
            pc.AddPaths(paths, pyclipper.PT_SUBJECT, True)
            solution = pc.Execute(
                pyclipper.CT_UNION,
                pyclipper.PFT_NONZERO,
                pyclipper.PFT_NONZERO,
            )
        except pyclipper.ClipperException:
            return None
        out: List[List[List[float]]] = []
        for path in solution:
            if len(path) < 3:
                continue
            # Clipper emits CCW exteriors / CW holes (y-up); Esri
            # wants the opposite, so reverse every ring, then close it.
            ring = [[x / scale, y / scale] for x, y in reversed(path)]
            ring.append(list(ring[0]))
            out.append(ring)
        return out or None

    async def _fetch_filter_polygon(
        self, filter_item_id: str, filter_where: str
    ) -> Dict[str, Any]:
        """Resolve a polygon filter from feature(s) in another layer.

        Queries the filter layer with ``filter_where``, validates it is a
        polygon layer, and geometrically unions all matching features'
        rings into a single Esri polygon (a true pyclipper union, not
        ring concatenation -- see ``_union_esri_rings``). Refuses
        loudly when the WHERE matches more than ``MAX_FILTER_FEATURES``
        features rather than silently truncating. Typical use: pick one
        district or one park feature as the filter geometry for a
        target layer.
        """
        filter_item_id = self._validate_item_id(filter_item_id)
        validated_where = WhereValidator.validate(filter_where or "1=1")

        item = await self.get_dataset(filter_item_id)
        url = item.get("url", "")
        if not url:
            raise ToolInputError(
                f"filter_item_id {filter_item_id} has no service URL"
            )
        url = self._ensure_layer_url(url)
        self._validate_service_url(url)

        meta = await self._get_meta_json(url)
        geom_type = meta.get("geometryType", "")
        if geom_type not in (
            "esriGeometryPolygon",
            "esriGeometryMultiPatch",
        ):
            raise ToolInputError(
                f"filter_item_id must point at a polygon layer "
                f"(got geometryType={geom_type or 'unknown'!r})"
            )

        query_url = f"{url}/query"

        # Count first so an over-cap match refuses LOUDLY instead of
        # silently unioning a truncated subset of the filter features.
        count_resp = await self._send("get",
            query_url,
            params={
                "where": validated_where,
                "returnCountOnly": "true",
                "f": "json",
            },
        )
        count_resp.raise_for_status()
        count_data = count_resp.json()
        if "error" in count_data:
            raise RuntimeError(
                count_data["error"].get(
                    "message", str(count_data["error"])
                )
            )
        match_count = int(count_data.get("count") or 0)
        if match_count > self.MAX_FILTER_FEATURES:
            raise ToolInputError(
                f"filter_where {validated_where!r} matches "
                f"{match_count:,} features in filter layer "
                f"{filter_item_id}; max is "
                f"{self.MAX_FILTER_FEATURES:,} for a spatial filter. "
                f"Narrow the WHERE clause, or use "
                f"aggregate_by_polygon, which assigns source features "
                f"per polygon and has no such cap."
            )

        features: List[Dict[str, Any]] = []
        offset = 0
        while len(features) < match_count:
            params = {
                "where": validated_where,
                "outFields": "",
                "returnGeometry": "true",
                "outSR": "4326",
                "f": "json",
                "resultRecordCount": str(self.FILTER_FETCH_PAGE),
                "resultOffset": str(offset),
            }
            resp = await self._send("post", query_url, data=params)
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                raise RuntimeError(
                    data["error"].get("message", str(data["error"]))
                )
            page = data.get("features", [])
            if not page:
                break
            features.extend(page)
            offset += len(page)

        if not features:
            raise ToolInputError(
                f"filter_where {validated_where!r} matched no features "
                f"in filter layer {filter_item_id}"
            )

        all_rings: List[Any] = []
        for f in features:
            geom = f.get("geometry") or {}
            all_rings.extend(geom.get("rings") or [])

        if not all_rings:
            raise ToolInputError(
                f"filter features in {filter_item_id} have no "
                f"polygon rings"
            )

        # One feature's rings are already a coherent polygon -- pass
        # them through untouched (no integer-grid rounding). Several
        # features need a REAL union; see _union_esri_rings for why
        # plain concatenation loses area.
        if len(features) > 1:
            unioned = self._union_esri_rings(all_rings)
            if unioned is not None:
                all_rings = unioned

        return {
            "rings": all_rings,
            "spatialReference": {"wkid": 4326},
        }

    # ── Additional tool implementations ───────────────────────────────────

    async def _find_gis_content(
        self, args: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """Combined search: gallery + spatial layers."""
        topic = args.get("topic", "").strip()
        if not topic:
            raise ToolInputError(
                "topic is required -- pass the subject of the user's "
                "question as a 1-2 word topic (e.g. 'parks', 'trails', "
                "'flood', 'zoning'). Do not ask the user for clarification "
                "if the topic is obvious from their message."
            )
        limit = min(self._int_arg(args, "limit", 8), 50)

        gallery_results, layer_results = await asyncio.gather(
            self._search_gallery(topic, limit),
            self._search_org_layers(
                topic, self.LAYER_TYPES + self.DATA_TYPES, limit
            ),
        )

        city = self.plugin_config.city_name
        gallery_url = self.plugin_config.gallery_url
        records = [
            self._item_record(i, "gallery") for i in gallery_results
        ] + [self._item_record(i, "layers") for i in layer_results]

        if not gallery_results and not layer_results:
            retry = (
                "Retry with a broader, simpler keyword. Strip "
                "qualifiers like 'parcels', 'data', 'boundaries', "
                "'zones', 'areas' -- e.g. 'park parcels' -> 'parks', "
                "'flood zone boundaries' -> 'flood', 'school district "
                "areas' -> 'schools'. Use the most distinctive single "
                "word first. If still nothing, browse the full "
                f"gallery: {gallery_url}"
            )
            text = (
                f"No {city} GIS content found for topic '{topic}'.\n\n"
                f"NEXT STEP: retry with a broader, simpler keyword. "
                f"Strip qualifiers like 'parcels', 'data', 'boundaries', "
                f"'zones', 'areas' -- e.g. 'park parcels' -> 'parks', "
                f"'flood zone boundaries' -> 'flood', 'school district "
                f"areas' -> 'schools'. Use the most distinctive single "
                f"word first.\n\n"
                f"If still no results after retry, browse the full "
                f"gallery: {gallery_url}"
            )
            return text, self._item_search_result(
                records,
                text=topic,
                limit=limit,
                no_results_message=(
                    f"No {city} GIS content found for '{topic}'. {retry}"
                ),
            )

        text = f"## {city} GIS Content: '{topic}'\n\n"
        if gallery_results:
            text += (
                f"### {self.TEXT['gallery_section_title']} "
                f"({len(gallery_results)} found)\n\n"
                f"{self.TEXT['gallery_section_note']}"
            )
            for item in gallery_results:
                text += self._format_summary(item)
        if layer_results:
            text += self._format_layer_section(layer_results)
        text += (
            "\n---\n"
            "**NEXT STEPS** (pick based on the user's question):\n"
            "- COUNT records ('how many?'): "
            "`query_data(item_id, limit=1)` -- read the TOTAL COUNT "
            "line in the response.\n"
            "- LIST records: "
            "`query_data(item_id, where=..., limit=N)`.\n"
            "- DESCRIBE an item: `get_item_details(item_id)`.\n"
            "- DISCOVER fields before filtering: "
            "`get_layer_schema(item_id)`.\n"
            "**Pick from the QUERYABLE section above** -- those are "
            "Feature/Map Services that work with `query_data`. Items "
            "in the OTHER section are viewers, web maps, and "
            "downloadable files (not directly queryable).\n"
            f"_Full gallery: {gallery_url}_"
        )
        return text, self._item_search_result(
            records, text=topic, limit=limit
        )

    def _format_layer_section(
        self, layer_results: List[Dict[str, Any]]
    ) -> str:
        """Render the spatial-layers block, split queryable vs other.

        Esri's relevance ranking already puts canonical Feature
        Services first within their type, but interleaving them with
        Web Maps in a single list lets weaker models latch onto a
        non-queryable item. The split makes the queryable choices
        visually unmistakable.
        """
        queryable, other = [], []
        for item in layer_results:
            if item.get("type") in self.QUERYABLE_TYPES:
                queryable.append(item)
            else:
                other.append(item)

        text = (
            f"### Spatial Layers & Data "
            f"({len(layer_results)} found)\n\n"
        )
        if queryable:
            text += (
                f"#### QUERYABLE -- Feature/Map Services "
                f"({len(queryable)})\n"
                f"_Use these directly with `query_data`, "
                f"`get_layer_schema`, `spatial_query_*`._\n"
            )
            if len(queryable) >= 2:
                text += self.TEXT["ambiguity_warning"]
            else:
                text += "\n"
            for item in queryable:
                text += self._format_summary(item)
        if other:
            text += (
                f"#### OTHER -- Web Maps & Downloadable Data "
                f"({len(other)})\n"
                f"_Viewers and reference items. Not directly "
                f"queryable; use for context or to find the underlying "
                f"Feature Service via `get_item_details`._\n\n"
            )
            for item in other:
                text += self._format_summary(item)
        return text

    async def _browse_gallery(
        self, keyword: str, limit: int
    ) -> Tuple[str, Dict[str, Any]]:
        """Browse or search the curated gallery."""
        results = await self._search_gallery(keyword, limit)
        city = self.plugin_config.city_name
        gallery_url = self.plugin_config.gallery_url
        structured = self._item_search_result(
            [self._item_record(i) for i in results],
            text=keyword,
            limit=limit,
            no_results_message=(
                "No gallery items found"
                + (f" matching {keyword!r}" if keyword else "")
                + ". Use find_gis_content to also search the org's "
                "layers."
            ),
        )

        if not results:
            suffix = f" matching {repr(keyword)}" if keyword else ""
            return f"No gallery items found{suffix}.", structured

        header = (
            f"## {city} GIS Gallery -- '{keyword}' "
            f"({len(results)} items)\n\n"
            if keyword
            else f"## {city} GIS Gallery ({len(results)} items)\n\n"
        )
        text = header
        for item in results:
            text += self._format_summary(item)
        text += (
            "\n---\n"
            f"{self.TEXT['gallery_footer']}"
            f"_Full gallery: {gallery_url}_"
        )
        return text, structured

    async def _search_spatial_layers(
        self, query: str, layer_type: str, limit: int
    ) -> Tuple[str, Dict[str, Any]]:
        """Search raw spatial layers."""
        city = self.plugin_config.city_name
        if layer_type not in ("layers", "data", "all"):
            raise ToolInputError(
                "layer_type must be one of ['all', 'data', 'layers'] "
                f"(got {layer_type!r})"
            )
        if layer_type == "layers":
            item_types = self.LAYER_TYPES
        elif layer_type == "data":
            item_types = self.DATA_TYPES
        else:
            item_types = self.LAYER_TYPES + self.DATA_TYPES

        results = await self._search_org_layers(query, item_types, limit)
        structured = self._item_search_result(
            [self._item_record(i) for i in results],
            text=query,
            limit=limit,
            layer_type=layer_type,
            no_results_message=(
                f"No {city} spatial layers/data found matching "
                f"'{query}'. Retry with a broader keyword (strip "
                "'parcels', 'data', 'boundaries' -- e.g. 'park "
                "parcels' -> 'parks'), or call find_gis_content to "
                "also search the curated public gallery."
            ),
        )
        if not results:
            return (
                f"No {city} spatial layers/data found matching "
                f"'{query}'.\n\n"
                f"NEXT STEP: retry with a broader keyword (strip "
                f"'parcels', 'data', 'boundaries' -- e.g. 'park "
                f"parcels' -> 'parks'), or call `find_gis_content` "
                f"to also search the curated public gallery."
            ), structured

        text = (
            f"## {city} Spatial Layers: '{query}' "
            f"({len(results)} results)\n\n"
        )
        text += self._format_layer_section(results)
        text += (
            "\n---\n"
            "**NEXT STEPS:** `query_data(item_id, limit=1)` to count "
            "records (read the TOTAL COUNT line); "
            "`query_data(item_id, where=..., limit=N)` to list; "
            "`get_layer_schema(item_id)` to see field names; "
            "`get_item_details(item_id)` for the full description. "
            "Pick from the QUERYABLE section above for `query_data` "
            "calls.\n"
        )
        return text, structured

    async def _missing_layer_message(
        self, layer_url: str, error_text: str
    ) -> Optional[str]:
        """Explain a layer index the service does not have.

        Returns None when the failure is something else. Lists the
        layers and tables that do exist, read from the service root.
        """
        m = re.search(r"^(.*)/(\d+)$", layer_url)
        if not m or "not found" not in error_text.lower():
            return None
        root_url, index = m.group(1), m.group(2)
        available: List[str] = []
        try:
            root = await self._get_meta_json(root_url)
            for entry in (root.get("layers") or []) + (
                root.get("tables") or []
            ):
                if isinstance(entry, dict) and "id" in entry:
                    available.append(
                        f"{entry['id']} ({entry.get('name', '?')})"
                    )
        except Exception:
            available = []
        text = f"layer_index {index} does not exist on this service."
        if available:
            text += " Available layer_index values: " + ", ".join(
                available[:40]
            ) + "."
        return text

    async def _get_layer_schema(
        self, args: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """Fetch schema for a Feature/Map Service layer."""
        item_id = args.get("item_id", "").strip()
        service_url = args.get("service_url", "").strip()
        layer_index = self._int_arg(args, "layer_index", 0)
        keyword = args.get("keyword", "").strip().lower()
        item_title = service_url or item_id

        if item_id and not service_url:
            item_id = self._validate_item_id(item_id)
            item = await self.get_dataset(item_id)
            service_url = item.get("url", "")
            item_title = item.get("title", item_id)

        if not service_url:
            # A caller mistake, so a tool error -- it used to come back
            # as a successful result whose text happened to say "Error".
            raise ToolInputError("provide either item_id or service_url.")

        service_url = service_url.rstrip("/")
        if not re.search(r"/\d+$", service_url):
            service_url = f"{service_url}/{layer_index}"

        self._validate_service_url(service_url)
        try:
            data = await self._get_meta_json(service_url)
        except RuntimeError as e:
            missing = await self._missing_layer_message(service_url, str(e))
            if missing is None:
                raise
            raise ToolInputError(missing) from e

        fields = data.get("fields") or []
        layer_name = data.get("name", data.get("title", "Unknown Layer"))
        geometry_type = data.get("geometryType", "N/A")
        max_records = data.get("maxRecordCount", "Unknown")

        # Prefer the layer name over a raw URL for the heading
        if not item_id and layer_name != "Unknown Layer":
            item_title = layer_name

        if keyword:
            fields = [
                f
                for f in fields
                if keyword in f.get("name", "").lower()
                or keyword in (f.get("alias") or "").lower()
            ]

        max_records_int = (
            max_records
            if isinstance(max_records, int)
            and not isinstance(max_records, bool)
            else None
        )
        structured: Dict[str, Any] = {
            "layer": {
                "item_id": item_id or None,
                "title": str(item_title) if item_title else None,
                "name": str(layer_name),
                "geometry_type": data.get("geometryType") or None,
                "max_record_count": max_records_int,
                "service_url": service_url,
            },
            "keyword": keyword or None,
            "fields": [self._field_record(f) for f in fields],
            "caveats": [],
        }

        if not fields:
            text = (
                f"No fields matching '{keyword}' in layer '{layer_name}'."
                if keyword
                else f"No fields found in layer '{layer_name}'."
            )
            structured["caveats"].append(
                {
                    "code": (
                        "no_matching_fields" if keyword else "no_fields"
                    ),
                    "message": text
                    + (
                        " Retry without `keyword` to see every field."
                        if keyword
                        else ""
                    ),
                }
            )
            return text, structured

        text = f"## Schema: {item_title}\n"
        text += (
            f"**Layer:** {layer_name}  |  "
            f"**Geometry:** {geometry_type}  |  "
            f"**Max Records:** {max_records}\n"
        )
        text += f"**Service URL:** {service_url}\n\n"
        if keyword:
            text += f"_(Filtered to fields matching '{keyword}')_\n\n"
        text += f"### Fields ({len(fields)})\n\n"
        text += "| Field Name | Alias | Type |\n|---|---|---|\n"
        for f in fields:
            name = f.get("name", "")
            alias = f.get("alias", name)
            ftype = f.get("type", "").replace("esriFieldType", "")
            text += f"| `{name}` | {alias} | {ftype} |\n"

        domain_lines = []
        for f in fields:
            domain = f.get("domain")
            if domain and domain.get("type") == "codedValue":
                dname = domain.get("name", f.get("name", ""))
                codes = domain.get("codedValues", [])
                if codes:
                    vals = ", ".join(
                        f"{c['code']}={c['name']}" for c in codes[:15]
                    )
                    if len(codes) > 15:
                        vals += f" ... (+{len(codes) - 15} more)"
                    domain_lines.append(f"- **{dname}**: {vals}")

        if domain_lines:
            text += "\n### Coded Domains\n" + "\n".join(domain_lines)

        # Pick a real text-typed field from THIS layer for the
        # example, so the model sees a concrete, copy-pasteable call
        # instead of a generic placeholder. Falls back to "<field>"
        # if no string fields exist (rare for a public Feature
        # Service -- most have at least a name/title column).
        sample_text_field = next(
            (
                f.get("name")
                for f in fields
                if f.get("type") == "esriFieldTypeString"
                and f.get("name")
                and f.get("name").upper() != "OBJECTID"
            ),
            None,
        )
        sample_id_arg = (
            f"item_id='{item_id}'" if item_id else "item_id=<id>"
        )
        if sample_text_field:
            example_call = (
                f"`query_data({sample_id_arg}, "
                f"where=\"{sample_text_field} LIKE '%foo%'\", "
                f"limit=10)`"
            )
        else:
            example_call = (
                f"`query_data({sample_id_arg}, where=\"<Field>=<value>\", "
                f"limit=10)`"
            )

        text += (
            "\n\n---\n"
            "**NEXT STEPS:** use the field names above in `query_data` "
            "-- field names are CASE-SENSITIVE (use the exact `Field "
            "Name` column, not the alias). Quote string literals "
            "with single quotes. For text searches prefer `LIKE "
            "'%substring%'` over `=` (which requires the full exact "
            f"value).{self.TEXT['where_tip_extra']}\n\n"
            f"Example: {example_call}\n\n"
            "To just COUNT matches, set `limit=1` and read the "
            "TOTAL COUNT line in the response."
        )
        if keyword:
            structured["caveats"].append(
                {
                    "code": "fields_filtered",
                    "count": len(fields),
                    "message": (
                        f"Only fields matching '{keyword}' are listed; "
                        "this is not the layer's full schema."
                    ),
                }
            )
        return text, structured

    async def _search_layers_by_field(
        self, args: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """Find services containing a specific field name/alias."""
        field_keyword = args.get("field_keyword", "").strip().lower()
        if not field_keyword:
            raise ToolInputError("field_keyword is required")
        service_keyword = args.get("service_keyword", "").strip()
        limit = min(self._int_arg(args, "limit", 10), 20)

        type_filter = " OR ".join(f'type:"{t}"' for t in self.LAYER_TYPES)
        clauses = [
            f"orgid:{self.plugin_config.org_id}",
            f"({type_filter})",
        ]
        # Without a text filter, ArcGIS ranks the catalog by global popularity
        # and the top-`limit` pool skips less-trafficked services. Fall back to
        # the field_keyword so the sample is at least biased toward services
        # whose titles/descriptions mention the attribute of interest.
        effective_service_filter = service_keyword or field_keyword
        scoped_filter = self._user_query_clause(effective_service_filter)
        if scoped_filter:
            clauses.append(scoped_filter)
        candidates = await self._run_search(" AND ".join(clauses), limit)

        def _structured(
            matches: List[Dict[str, Any]],
            failed: Dict[str, int],
            caveats: List[Dict[str, Any]],
        ) -> Dict[str, Any]:
            return {
                "query": {
                    "field_keyword": field_keyword,
                    "service_keyword": service_keyword or None,
                    "limit": limit,
                },
                "summary": {
                    "services_inspected": len(candidates),
                    "layers_matched": len(matches),
                    "services_failed": failed["services"],
                    "layers_failed": failed["layers"],
                },
                "inspected": [
                    {
                        "item_id": str(c.get("id") or ""),
                        "title": str(c.get("title") or "Untitled"),
                    }
                    for c in candidates
                ],
                "matches": [
                    {
                        "item_id": str(m["item"].get("id") or ""),
                        "title": str(m["item"].get("title") or "Untitled"),
                        "layer_name": str(m.get("layer_name") or ""),
                        "layer_index": (
                            m.get("layer_index")
                            if isinstance(m.get("layer_index"), int)
                            else 0
                        ),
                        "portal_url": (
                            f"{self._portal_home}/home/item.html?id="
                            f"{m['item'].get('id', '')}"
                        ),
                        "matching_fields": [
                            self._field_record(f)
                            for f in m["matching_fields"]
                        ],
                    }
                    for m in matches
                ],
                "caveats": caveats,
            }

        if not candidates:
            msg = f"No services found matching '{effective_service_filter}'."
            return msg, _structured(
                [],
                {"services": 0, "layers": 0},
                [
                    {
                        "code": "no_services_found",
                        "message": (
                            msg + " Try a different service_keyword."
                        ),
                    }
                ],
            )

        # Bound concurrent ArcGIS calls. Without this, a 20-candidate search
        # can fire 20 service-root fetches plus 20*N layer-schema fetches in
        # parallel against the upstream portal -- a polite-burst that still
        # looks like a small DDoS to muniorg.maps.arcgis.com.
        inspect_sem = asyncio.Semaphore(5)
        # Services/layers whose metadata could not be read (timeout,
        # exhausted call budget, upstream error). Counted so a miss is
        # not reported as a confirmed absence.
        failed = {"services": 0, "layers": 0}

        async def check_service(
            item: Dict[str, Any],
        ) -> List[Dict[str, Any]]:
            url = (item.get("url") or "").rstrip("/")
            # A candidate that cannot be inspected is a service that was
            # NOT checked; counting it as inspected-and-clean reported
            # "no matching fields" for layers nobody looked at.
            if not url:
                failed["services"] += 1
                return []
            try:
                self._validate_service_url(url)
            except ValueError:
                failed["services"] += 1
                return []
            async with inspect_sem:
                return await _inspect_service(item, url)

        async def _inspect_service(
            item: Dict[str, Any], url: str
        ) -> List[Dict[str, Any]]:
            try:
                # Fetch service root to discover all layers
                resp = await self._send("get",
                    url, params={"f": "json"}, timeout=10.0
                )
                root = resp.json()
                if not isinstance(root, dict) or root.get("error"):
                    # HTTP 200 with an error body (token required, bad
                    # request): unread, not empty.
                    raise RuntimeError("service root returned an error")
                # An item URL that already ends in a layer index
                # (.../FeatureServer/0, usual for single-layer items)
                # IS the layer: its JSON has `fields` and no `layers`,
                # and asking for .../0/0 only yields an error body.
                url_is_layer = bool(re.search(r"/\d+$", url))
                layer_list = root.get("layers") or []
                if url_is_layer:
                    layer_list = [{"id": root.get("id", 0)}]
                elif not layer_list:
                    # Fallback: single-layer service, check /0
                    layer_list = [{"id": 0}]

                hits = []
                for layer_meta in layer_list:
                    layer_id = layer_meta.get("id", 0)
                    try:
                        if url_is_layer:
                            data = root
                        else:
                            lr = await self._send("get",
                                f"{url}/{layer_id}",
                                params={"f": "json"},
                                timeout=10.0,
                            )
                            data = lr.json()
                        if not isinstance(data, dict) or data.get("error"):
                            raise RuntimeError(
                                "layer metadata returned an error"
                            )
                        matching = [
                            f
                            for f in data.get("fields") or []
                            if field_keyword in f.get("name", "").lower()
                            or field_keyword
                            in (f.get("alias") or "").lower()
                        ]
                        if matching:
                            hits.append(
                                {
                                    "item": item,
                                    "layer_name": data.get("name", ""),
                                    "layer_index": layer_id,
                                    "matching_fields": matching,
                                }
                            )
                    except Exception:
                        failed["layers"] += 1
                        continue
                return hits
            except Exception:
                failed["services"] += 1
                return []

        results_raw = await asyncio.gather(
            *[check_service(item) for item in candidates]
        )
        matches = [m for hits in results_raw for m in hits]

        city = self.plugin_config.city_name
        partial_note = ""
        caveats: List[Dict[str, Any]] = []
        if failed["services"] or failed["layers"]:
            partial_note = (
                f"\n\n**Partial inspection:** {failed['services']} "
                f"service(s) and {failed['layers']} layer(s) could not "
                f"be read (timeout or upstream error), so matching "
                f"fields there were NOT checked. Retry, or narrow "
                f"`service_keyword` to inspect fewer services."
            )
            caveats.append(
                {
                    "code": "partial_inspection",
                    "count": failed["services"] + failed["layers"],
                    "message": partial_note.strip().replace("**", ""),
                }
            )

        if not matches:
            caveats.insert(
                0,
                {
                    "code": "no_matching_fields",
                    "message": (
                        f"None of the {len(candidates)} services "
                        f"inspected contain fields matching "
                        f"'{field_keyword}'. Try a different "
                        "service_keyword to broaden the search."
                    ),
                },
            )
            structured = _structured(matches, failed, caveats)
            inspected = "\n".join(
                f"- {item.get('title', 'Untitled')}" for item in candidates
            )
            filter_note = (
                f"pre-filtered by '{service_keyword}'"
                if service_keyword
                else f"auto-filtered by field_keyword '{field_keyword}'"
            )
            return (
                f"None of the {len(candidates)} {city} services inspected "
                f"contain fields matching '{field_keyword}' ({filter_note}).\n\n"
                f"Inspected services:\n{inspected}\n\n"
                f"Try a different `service_keyword` to broaden the search."
                f"{partial_note}"
            ), structured

        text = (
            f"## {city} Layers with '{field_keyword}' Fields\n\n"
            f"Found {len(matches)} layer(s) with matching fields "
            f"(checked {len(candidates)} services):\n\n"
        )
        for m in matches:
            item = m["item"]
            title = item.get("title", "Untitled")
            item_id = item.get("id", "")
            layer_idx = m.get("layer_index", 0)
            layer_suffix = (
                f" (layer {layer_idx})" if layer_idx != 0 else ""
            )
            text += f"### {title}{layer_suffix}\n"
            text += (
                f"**Layer:** {m['layer_name']}  |  **ID:** `{item_id}`\n"
            )
            text += "**Matching fields:**\n"
            for f in m["matching_fields"]:
                name = f.get("name", "")
                alias = f.get("alias", "")
                ftype = f.get("type", "").replace("esriFieldType", "")
                label = f"`{name}`" + (
                    f" ({alias})" if alias != name else ""
                )
                text += f"- {label} -- {ftype}\n"
            text += (
                f"**Portal:** "
                f"{self._portal_home}/home/item.html?id={item_id}\n\n"
            )
        text += (
            "_Use `get_layer_schema` with an item_id to see the "
            "complete field list._"
        )
        return text + partial_note, _structured(matches, failed, caveats)

    # ── Static helpers ────────────────────────────────────────────────────

    def _validate_service_url(self, url: str) -> str:
        """Reject any URL whose host is not on the allowlist.

        Prevents SSRF and tenant-scope creep via user-supplied service
        URLs or item URLs returned from portal search results.

        For ``*.arcgis.com`` (ArcGIS Online), the URL must either match
        this org's portal host (e.g. ``muniorg.maps.arcgis.com``) or
        include the configured ``org_id`` -- or a configured partner's
        org id -- as the first path segment
        (e.g. ``services.arcgis.com/<org_id>/...``,
        ``tiles7.arcgis.com/<org_id>/...``). This keeps the MCP from
        being used as an open proxy for arbitrary ArcGIS Online tenants.

        The org's own on-prem hosts (``ONPREM_HOST_SUFFIXES``) are
        accepted by suffix; partner on-prem hosts by exact match from
        ``partner_orgs[].hosts``.
        """
        if not url:
            raise ToolInputError("service URL cannot be empty")
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise ToolInputError(
                f"service URL must use http or https (got {parsed.scheme!r})"
            )
        host = (parsed.hostname or "").lower()
        if not host:
            raise ToolInputError("service URL must include a hostname")
        # The checks below read the path as written, but the HTTP client
        # resolves "." and ".." segments before sending, so
        # /<our-org>/../<other-org>/... would pass the org-prefix test
        # and then be fetched from the other org. A query or fragment
        # likewise hides the real path from the layer-index check.
        # None of these occur in a real service URL: refuse them.
        if parsed.query or parsed.fragment or "?" in url or "#" in url:
            raise ToolInputError(
                "service URL must not include a query string or "
                "fragment"
            )
        decoded_path = unquote(parsed.path)
        if "\\" in url or "\\" in decoded_path or any(
            seg in (".", "..") for seg in decoded_path.split("/")
        ):
            raise ToolInputError(
                "service URL must not contain '.' or '..' path "
                "segments or backslashes"
            )

        if any(
            host == suffix.lstrip(".") or host.endswith(suffix)
            for suffix in self.ONPREM_HOST_SUFFIXES
        ):
            return url
        if host in self._partner_hosts:
            return url

        if host == "arcgis.com" or host.endswith(".arcgis.com"):
            portal_host = self._portal_host
            if portal_host and host == portal_host:
                # Only service endpoints: the portal host also serves
                # /sharing/rest/content/items/<any id>, which would let
                # a caller read another organisation's item record.
                if "/rest/services/" not in parsed.path.lower():
                    raise ToolInputError(
                        "service URL on the portal host must be a "
                        "/rest/services/ endpoint"
                    )
                return url
            org_id = (self.plugin_config.org_id or "").lower() if self.plugin_config else ""
            path_lower = parsed.path.lower()
            if any(
                o and path_lower.startswith(f"/{o}/")
                for o in self._allowed_org_ids
            ):
                return url
            raise ToolInputError(
                self._foreign_tenant_message(host, parsed.path, org_id)
            )

        raise ToolInputError(
            f"service URL host {host!r} is not on the allowlist"
        )

    @property
    def _portal_host(self) -> str:
        """Hostname of the configured ArcGIS portal (lowercased)."""
        if not self.plugin_config or not self.plugin_config.portal_base_url:
            return ""
        return (urlparse(self.plugin_config.portal_base_url).hostname or "").lower()

    @classmethod
    def _validate_item_id(cls, item_id: str) -> str:
        """Require an ArcGIS item ID to be a 32-char hex string."""
        if not item_id or not cls.ITEM_ID_RE.match(item_id):
            raise ToolInputError(
                f"Invalid ArcGIS item id: {item_id!r} "
                f"(expected 32-character hex string)"
            )
        return item_id.lower()

    @staticmethod
    def _ensure_layer_url(service_url: str) -> str:
        """Append /0 if URL points at a FeatureServer/MapServer root."""
        stripped = service_url.rstrip("/")
        if re.search(r"/(FeatureServer|MapServer)$", stripped, re.IGNORECASE):
            return f"{stripped}/0"
        return stripped

    @staticmethod
    def _webmerc_to_wgs84(x: float, y: float) -> Tuple[float, float]:
        # Inline Web Mercator -> WGS84 to avoid a pyproj dependency for
        # one coordinate conversion. Earth radius per EPSG:3857 spec.
        import math
        lon = x / 6378137.0 * 180.0 / math.pi
        lat = (
            math.atan(math.exp(y / 6378137.0)) * 2.0 - math.pi / 2.0
        ) * 180.0 / math.pi
        return lon, lat

    @classmethod
    def _region_coverage_pct(cls, extent: Any) -> Optional[float]:
        """Share of the deployment's region a layer extent covers.

        (layer intersect region) / region-area, in [0, ~1+]; drives the
        LIMITED COVERAGE caveat. The base class has no region, so it
        returns None and the caveat stays silent. A deployment overrides
        this with its own bounding box(es) and projections.
        """
        return None

    @staticmethod
    def _validate_lonlat(lon: Any, lat: Any) -> tuple[float, float]:
        """Validate WGS84 coordinates. Note: lon first, then lat."""
        try:
            lon_f = float(lon)
            lat_f = float(lat)
        except (TypeError, ValueError) as e:
            raise ToolInputError(
                f"lon/lat must be numeric (got lon={lon!r}, lat={lat!r})"
            ) from e
        if not (-180.0 <= lon_f <= 180.0):
            raise ToolInputError(
                f"lon out of range [-180, 180]: {lon_f}"
            )
        if not (-90.0 <= lat_f <= 90.0):
            raise ToolInputError(
                f"lat out of range [-90, 90]: {lat_f}"
            )
        return lon_f, lat_f

    async def _get_record_count(
        self, service_url: str, where: str
    ) -> Optional[int]:
        """Fetch total record count for a query (best-effort)."""
        try:
            self._validate_service_url(service_url)
        except ValueError:
            return None
        query_url = f"{service_url}/query"
        try:
            resp = await self._send("get",
                query_url,
                params={
                    "where": where,
                    "returnCountOnly": "true",
                    "f": "json",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            count = data.get("count")
            return int(count) if count is not None else None
        except Exception:
            return None

    async def _safe_layer_meta(self, item_id: str) -> Dict[str, Any]:
        # Best-effort lookup of the layer quick-meta (date fields,
        # coded domains, geometry type, name field) for an item.
        # Used by paths (spatial_*) that don't otherwise need the layer
        # URL upstream -- resolve via get_dataset, then quick_meta. Any
        # failure returns {} so callers fall back to raw rendering
        # rather than blocking the user's spatial result.
        try:
            item = await self.get_dataset(item_id)
            service_url = self._ensure_layer_url(item.get("url", ""))
            if not service_url:
                return {}
            return await self._get_layer_quick_meta(service_url) or {}
        except Exception:
            return {}

    async def _get_layer_quick_meta(
        self, service_url: str
    ) -> Dict[str, Any]:
        """Fetch a small bundle of layer metadata used by the query
        formatter: date field names (for epoch->ISO conversion),
        coded-value domains (for decoding raw codes in output),
        and geometry type (for the polyline-grain warning on counts).
        Best-effort -- returns an empty dict on any error so callers
        can degrade gracefully.

        Returns ``{date_fields: set|None, coded_domains: dict|None,
        geometry_type: str|None, name_field: str|None}``.
        ``coded_domains`` maps field name -> {code: label} for fields
        whose schema declares an ``esriFieldTypeCodedValue`` domain,
        letting the formatter render ``ZONE: R1A
        (Single-Family Residential)`` instead of forcing the model to
        guess what ``R1A`` means. ``name_field`` is the first
        natural-ID-style field present, used to suggest a follow-up
        get_distinct_values call when warning about polyline grain.
        """
        try:
            self._validate_service_url(service_url)
        except ValueError:
            return {}
        try:
            data = await self._get_meta_json(service_url)
        except Exception:
            return {}
        fields = data.get("fields") or []
        date_fields = {
            f["name"]
            for f in fields
            if f.get("type") == "esriFieldTypeDate"
        } or None
        coded_domains: Dict[str, Dict[Any, str]] = {}
        for f in fields:
            domain = f.get("domain") or {}
            if domain.get("type") != "codedValue":
                continue
            fname = f.get("name")
            coded_values = domain.get("codedValues") or []
            mapping = {
                cv.get("code"): cv.get("name")
                for cv in coded_values
                if cv.get("code") is not None and cv.get("name")
            }
            if fname and mapping:
                coded_domains[fname] = mapping
        field_names = {f.get("name") for f in fields if f.get("name")}
        name_field = next(
            (
                f
                for f in self.NATURAL_ID_FIELD_PRIORITY
                if f in field_names
            ),
            None,
        )
        # Devil's-advocate signals -- pre-compute and stash so the
        # formatter can fire the staleness and coverage caveats without
        # a second round-trip. Both are best-effort: missing/malformed
        # values just suppress the corresponding caveat.
        editing = data.get("editingInfo") or {}
        last_edit_date = (
            editing.get("dataLastEditDate")
            or editing.get("lastEditDate")
        )
        if not isinstance(last_edit_date, (int, float)):
            last_edit_date = None
        coverage_pct = self._region_coverage_pct(data.get("extent"))

        return {
            "date_fields": date_fields,
            "coded_domains": coded_domains or None,
            "geometry_type": data.get("geometryType"),
            "name_field": name_field,
            "field_names": field_names or None,
            "last_edit_date": last_edit_date,
            "coverage_pct": coverage_pct,
        }

    # ── Aggregation helpers ───────────────────────────────────────────────

    # Cache TTL for aggregation layers (councils, districts, etc.) WHEN THE
    # LAYER REPORTS AN EDIT DATE. A cached set is then reused only while that
    # date is unchanged, so the TTL is a backstop and an edited layer is
    # picked up as soon as its (1h-cached) metadata shows the new date.
    AGG_CACHE_TTL_SECONDS = 86400
    # TTL when the layer reports NO edit date, so there is nothing to check
    # freshness against. Matches the metadata cache: an hour of staleness at
    # most. (Until opencontext-core 1.5.0 a plugin object lived for one
    # request and this cache never survived to be stale; since 1.5.0 one
    # object serves a warm container for hours.)
    AGG_CACHE_TTL_NO_EDIT_DATE_SECONDS = 3600

    # Cap on LRU entries, and defence against WHERE-clause-variant
    # cache-busting. The count alone does not bound memory: see below.
    AGG_CACHE_MAX_ENTRIES = 32
    # Cap on the cache's total size, measured as the polygons' compact JSON
    # length. One set of ~5,000 simple parcel polygons is ~1.1 MB of JSON
    # and ~9 MB of Python objects (measured), so 16 MB here is on the order
    # of 130 MB of process memory -- where 32 such entries, which the entry
    # cap alone allowed, would be ~290 MB, and detailed boundaries several
    # times that. A single set larger than the cap is returned uncached.
    AGG_CACHE_MAX_BYTES = 16 * 1024 * 1024

    # Safety cap on source features pulled for a single aggregation call.
    # A council-by-council rollup of a city-wide dataset is typically a few
    # hundred to a few thousand features; beyond this the analysis is
    # probably better served by a server-side stats endpoint. Raised from
    # 2000 to 5000 with evidence: the census-block layer
    # (CensusBlock_2020_MOA_Population) has 3,709 blocks, and a real
    # "population within 1/2 mile of a park" rollup truncated at 2000,
    # under-counting the total. 5000 covers it in one pass and matches both
    # the tool's advertised max and the Lambda memory already provisioned
    # for it (see config.yaml lambda_memory comment).
    AGG_SOURCE_LIMIT = 5000

    # ArcGIS maxRecordCount is usually 1000 or 2000. We page through with
    # resultOffset at this step until AGG_SOURCE_LIMIT is reached.
    AGG_PAGE_SIZE = 1000

    # Transient-failure retry for the spatial fetch path
    # (_paged_geojson_fetch, used by aggregate + coverage). Cold starts and
    # brief ArcGIS blips otherwise surface as an opaque "Tool execution
    # failed". Keep attempts few and backoff short -- the gateway ceiling is 29s.
    ARCGIS_MAX_ATTEMPTS = 3
    ARCGIS_RETRY_BACKOFF_S = 0.4

    # Portal item records and layer/service metadata (?f=json) change only
    # when a layer is republished, yet most tools fetch both before every
    # query. Cached per plugin instance (= per Lambda container). An hour
    # bounds how stale a republish can look; the entry cap bounds memory,
    # since the keys are caller-chosen item ids and URLs.
    META_CACHE_TTL_S = 3600.0
    META_CACHE_MAX_ENTRIES = 256

    # Caps for find_features_spanning_classifications. The source cap is
    # higher than AGG_SOURCE_LIMIT because the spanning analysis only
    # returns the qualifying subset (typically << source size); the
    # classification cap is higher than expected zoning/floodplain/council
    # polygon counts so a real layer never silently truncates.
    SPANNING_SOURCE_LIMIT = 5000
    SPANNING_CLASSIFICATION_LIMIT = 1000

    # Concurrency cap for per-classification spatial queries. The portal
    # tolerates polite bursts; a hard cap keeps the spanning tool from
    # looking like a small DDoS to muniorg.maps.arcgis.com when a
    # classification layer has hundreds of polygons.
    SPANNING_QUERY_CONCURRENCY = 10

    # coverage_by_polygon fetches overlay footprints in BATCHES of target
    # polygons (one spatial query per batch, not per target) then assigns
    # them client-side. A per-target fan-out (~235 queries) tripped ArcGIS
    # per-IP throttling; batching keeps it to ceil(targets / batch) queries.
    COVERAGE_TARGET_LIMIT = 800
    COVERAGE_TARGET_BATCH = 50          # target polygons per overlay query
    COVERAGE_OVERLAY_LIMIT = 5000       # overlay features fetched per batch
    # How many rows of the per-target breakdown to render (sorted by
    # coverage). The headline count is always exact; the table is a sample.
    COVERAGE_TABLE_ROWS = 50

    @staticmethod
    def _ring_contains_point(
        ring: List[List[float]], point: Tuple[float, float]
    ) -> bool:
        """Even-odd ray-cast point-in-ring test (2D, lon/lat).

        Expects ring as a list of [lon, lat] pairs (GeoJSON style, first
        and last point equal). Returns True if the point is strictly
        inside the ring; boundary behavior is not guaranteed (doesn't
        matter for aggregation -- a point exactly on a council boundary
        falls into exactly one bucket under first_match).
        """
        x, y = point
        inside = False
        n = len(ring)
        if n < 3:
            return False
        j = n - 1
        for i in range(n):
            xi, yi = ring[i][0], ring[i][1]
            xj, yj = ring[j][0], ring[j][1]
            # Does the horizontal ray at y cross edge (i, j)?
            intersect = ((yi > y) != (yj > y)) and (
                x < (xj - xi) * (y - yi) / ((yj - yi) or 1e-300) + xi
            )
            if intersect:
                inside = not inside
            j = i
        return inside

    @classmethod
    def _polygon_contains_point(
        cls,
        polygon: List[List[List[float]]],
        point: Tuple[float, float],
    ) -> bool:
        """Point-in-polygon for a GeoJSON Polygon coordinates array.

        Rings after the first are treated as holes: a point inside a hole
        is not inside the polygon.
        """
        if not polygon:
            return False
        if not cls._ring_contains_point(polygon[0], point):
            return False
        for hole in polygon[1:]:
            if cls._ring_contains_point(hole, point):
                return False
        return True

    @classmethod
    def _multipolygon_contains_point(
        cls,
        multipolygon: List[List[List[List[float]]]],
        point: Tuple[float, float],
    ) -> bool:
        return any(cls._polygon_contains_point(p, point) for p in multipolygon)

    @classmethod
    def _geometry_contains_point(
        cls, geometry: Dict[str, Any], point: Tuple[float, float]
    ) -> bool:
        gtype = (geometry or {}).get("type", "")
        coords = (geometry or {}).get("coordinates")
        if not coords:
            return False
        if gtype == "Polygon":
            return cls._polygon_contains_point(coords, point)
        if gtype == "MultiPolygon":
            return cls._multipolygon_contains_point(coords, point)
        return False

    # Meters per degree of latitude (spherical mean Earth radius). Degrees
    # of longitude are scaled by cos(latitude) so the planar approximation
    # stays metric at Anchorage's ~61 degN, where a degree of longitude is
    # only ~half a degree of latitude in ground distance.
    _M_PER_DEG_LAT = 111195.0

    @staticmethod
    def _lon_delta(lon: float, origin: float) -> float:
        """``lon - origin`` in degrees, the short way round the globe."""
        d = lon - origin
        if -180.0 <= d <= 180.0:
            return d
        return (d + 180.0) % 360.0 - 180.0

    @staticmethod
    def _bbox_excludes(
        bbox: Tuple[float, float, float, float],
        px: float,
        py: float,
        pad_lon: float = 0.0,
        pad_lat: float = 0.0,
    ) -> bool:
        """True when (px, py) cannot be within the padded bounding box.

        With padding (a buffer) the point is also tried one turn east
        and west, so a box that ends at the antimeridian still admits a
        point just across it.
        """
        if py < bbox[1] - pad_lat or py > bbox[3] + pad_lat:
            return True
        lo, hi = bbox[0] - pad_lon, bbox[2] + pad_lon
        if lo <= px <= hi:
            return False
        if pad_lon > 0.0:
            return not (lo <= px + 360.0 <= hi or lo <= px - 360.0 <= hi)
        return True

    @classmethod
    def _point_segment_distance_m(
        cls,
        point: Tuple[float, float],
        a: List[float],
        b: List[float],
        lat_scale: float,
    ) -> float:
        """Shortest distance (meters) from `point` to segment a-b.

        Works in a local equirectangular frame centered on `point`:
        longitudes are scaled by `lat_scale` = cos(lat) so x/y are both in
        meters. Accurate to well under a percent over the few-km spans this
        is used for.
        """
        px, py = point
        # Longitude differences are taken the short way round. ArcGIS
        # cuts geometry at +/-180, so a point at 179.9 and a shoreline
        # at -179.9 are 0.2 degrees apart, not 359.8: without this a
        # buffer never reached across the line (western Aleutians).
        ax = cls._lon_delta(a[0], px) * cls._M_PER_DEG_LAT * lat_scale
        ay = (a[1] - py) * cls._M_PER_DEG_LAT
        bx = cls._lon_delta(b[0], px) * cls._M_PER_DEG_LAT * lat_scale
        by = (b[1] - py) * cls._M_PER_DEG_LAT
        dx = bx - ax
        dy = by - ay
        seg_len_sq = dx * dx + dy * dy
        if seg_len_sq <= 0.0:
            return (ax * ax + ay * ay) ** 0.5
        # Project the origin (the point) onto the segment, clamped to [0, 1].
        t = -(ax * dx + ay * dy) / seg_len_sq
        t = max(0.0, min(1.0, t))
        cx = ax + t * dx
        cy = ay + t * dy
        return (cx * cx + cy * cy) ** 0.5

    @classmethod
    def _point_to_geometry_distance_m(
        cls, geometry: Dict[str, Any], point: Tuple[float, float]
    ) -> float:
        """Shortest distance (meters) from a point to a polygon boundary.

        Returns 0.0 when the point is inside the polygon (holes respected),
        otherwise the minimum distance to any ring edge. Used to test
        proximity ("within N of this polygon") in aggregate_by_polygon.
        """
        if cls._geometry_contains_point(geometry, point):
            return 0.0
        gtype = (geometry or {}).get("type", "")
        coords = (geometry or {}).get("coordinates") or []
        rings: List[List[List[float]]] = []
        if gtype == "Polygon":
            rings = list(coords)
        elif gtype == "MultiPolygon":
            for poly in coords:
                rings.extend(poly)
        else:
            return float("inf")
        lat_scale = math.cos(math.radians(point[1]))
        best = float("inf")
        for ring in rings:
            for i in range(len(ring) - 1):
                d = cls._point_segment_distance_m(
                    point, ring[i], ring[i + 1], lat_scale
                )
                if d < best:
                    best = d
                    if best == 0.0:
                        return 0.0
        return best

    @staticmethod
    def _geometry_bbox(
        geometry: Dict[str, Any],
    ) -> Optional[Tuple[float, float, float, float]]:
        """(min_lon, min_lat, max_lon, max_lat) for a Polygon/MultiPolygon.

        Used as a cheap candidate prefilter before the O(edges) precise
        distance test in aggregate_by_polygon -- skip a polygon entirely
        when the point lies outside its (buffer-padded) bounding box.
        """
        gtype = (geometry or {}).get("type", "")
        coords = (geometry or {}).get("coordinates") or []
        rings: List[List[List[float]]] = []
        if gtype == "Polygon":
            rings = list(coords)
        elif gtype == "MultiPolygon":
            for poly in coords:
                rings.extend(poly)
        else:
            return None
        min_x = min_y = float("inf")
        max_x = max_y = float("-inf")
        for ring in rings:
            for pt in ring:
                x, y = pt[0], pt[1]
                if x < min_x:
                    min_x = x
                if x > max_x:
                    max_x = x
                if y < min_y:
                    min_y = y
                if y > max_y:
                    max_y = y
        if min_x == float("inf"):
            return None
        return (min_x, min_y, max_x, max_y)

    @staticmethod
    def _ring_area(ring: List[List[float]]) -> float:
        """Shoelace area (signed). Positive for CCW rings."""
        area = 0.0
        n = len(ring)
        if n < 3:
            return 0.0
        for i in range(n):
            x1, y1 = ring[i][0], ring[i][1]
            x2, y2 = ring[(i + 1) % n][0], ring[(i + 1) % n][1]
            area += x1 * y2 - x2 * y1
        return area / 2.0

    @classmethod
    def _polygon_area(cls, polygon: List[List[List[float]]]) -> float:
        if not polygon:
            return 0.0
        a = abs(cls._ring_area(polygon[0]))
        for hole in polygon[1:]:
            a -= abs(cls._ring_area(hole))
        return a

    @classmethod
    def _geometry_area(cls, geometry: Dict[str, Any]) -> float:
        gtype = (geometry or {}).get("type", "")
        coords = (geometry or {}).get("coordinates") or []
        if gtype == "Polygon":
            return cls._polygon_area(coords)
        if gtype == "MultiPolygon":
            return sum(cls._polygon_area(p) for p in coords)
        return 0.0

    # Web Mercator (EPSG:3857/102100) inflates AREA by sec^2(latitude).
    # At Anchorage that is ~4.24x at Girdwood, ~4.31x in the bowl and
    # ~4.34x at Eagle River. MOA hosted layers publish `Shape__Area` in
    # that projection, so a raw Shape__Area read as real area is wrong by
    # roughly a factor of four.
    _WEB_MERCATOR_WKIDS = frozenset({3857, 102100, 102113})

    @classmethod
    def _layer_wkid(cls, meta: Dict[str, Any]) -> Optional[int]:
        """Spatial-reference WKID a layer's stored areas are computed in."""
        extent = (meta or {}).get("extent") or {}
        sr = extent.get("spatialReference") or {}
        wkid = sr.get("latestWkid") or sr.get("wkid")
        return wkid if isinstance(wkid, int) else None

    @classmethod
    def _true_area_m2(
        cls, stored_area: float, lat: float, wkid: Optional[int]
    ) -> Optional[float]:
        """Convert a stored planar area to TRUE square metres.

        Web Mercator's area scale factor is sec^2(latitude), so the true
        area is `stored * cos^2(lat)`. This is applied per feature at its
        own latitude, NOT as a flat divisor -- the factor varies ~2%
        across the municipality.

        Uses the stored area rather than recomputing from geometry on
        purpose: the fetched geometry is simplified
        (maxAllowableOffset ~5.5m), which costs ~10% of the area of a
        small parcel, while `Shape__Area` was computed upstream on the
        full-precision shape. Measured on parcel 00326133000, the
        corrected figure lands within 1.2% of the assessor's Lot_Size;
        recomputing from the simplified geometry came in 10.5% low.

        Returns None when the layer is not in a projection we can
        correct, so the caller can label the units instead of guessing.
        """
        if wkid in (4326, 4269):
            # Already geographic -- a stored "area" in square degrees is
            # not an area at all, so refuse rather than invent one.
            return None
        if wkid in cls.EQUAL_AREA_METRE_WKIDS:
            # Equal-area projection with metre units: the stored area
            # already IS true square metres.
            return stored_area
        if wkid not in cls._WEB_MERCATOR_WKIDS:
            return None
        return stored_area * (math.cos(math.radians(lat)) ** 2)

    M2_TO_SQFT = 10.7639

    @classmethod
    def _to_sqft(
        cls, stored_area: float, lat: float, wkid: Optional[int]
    ) -> Optional[float]:
        """Stored planar area -> true square feet, or None if uncorrectable."""
        m2 = cls._true_area_m2(stored_area, lat, wkid)
        return None if m2 is None else m2 * cls.M2_TO_SQFT

    # Share of targets at exactly 0% coverage above which the result is
    # more likely a centroid artefact than genuine vacancy.
    COVERAGE_ZERO_ARTIFACT_RATIO = 0.25

    # Mean Earth radius (m), IUGG. Used by the geodesic area below.
    _EARTH_RADIUS_M = 6371008.8

    @classmethod
    def _ring_area_m2(cls, ring: List[List[float]]) -> float:
        """Signed area of a lon/lat ring in TRUE square metres.

        Spherical-excess formula. Needed because the planar
        `_ring_area` above works in square DEGREES, which is not an
        area, and because the stored `Shape__Area` on MOA hosted layers
        is Web Mercator: at Anchorage's latitude that is inflated by
        ~4.26x (about 4.20 at Girdwood, 4.32 at Eagle River), so any
        figure derived from it is wrong by roughly a factor of four if
        read as real area.

        For parcel- and building-sized polygons the spherical
        approximation sits well inside a percent of the WGS84
        ellipsoidal value -- far tighter than the data itself.
        """
        n = len(ring)
        if n < 3:
            return 0.0
        total = 0.0
        for i in range(n):
            lon1, lat1 = math.radians(ring[i][0]), math.radians(ring[i][1])
            lon2, lat2 = (
                math.radians(ring[(i + 1) % n][0]),
                math.radians(ring[(i + 1) % n][1]),
            )
            total += (lon2 - lon1) * (2 + math.sin(lat1) + math.sin(lat2))
        return total * cls._EARTH_RADIUS_M * cls._EARTH_RADIUS_M / 2.0

    @classmethod
    def _polygon_area_m2(cls, polygon: List[List[List[float]]]) -> float:
        if not polygon:
            return 0.0
        a = abs(cls._ring_area_m2(polygon[0]))
        for hole in polygon[1:]:
            a -= abs(cls._ring_area_m2(hole))
        return max(a, 0.0)

    @classmethod
    def _geometry_area_m2(cls, geometry: Dict[str, Any]) -> float:
        """True area of a GeoJSON polygon in square metres."""
        gtype = (geometry or {}).get("type", "")
        coords = (geometry or {}).get("coordinates") or []
        if gtype == "Polygon":
            return cls._polygon_area_m2(coords)
        if gtype == "MultiPolygon":
            return sum(cls._polygon_area_m2(p) for p in coords)
        return 0.0

    @classmethod
    def _geometry_area_sqft(cls, geometry: Dict[str, Any]) -> float:
        return cls._geometry_area_m2(geometry) * cls.M2_TO_SQFT

    @staticmethod
    def _ring_centroid(ring: List[List[float]]) -> Tuple[float, float]:
        """Area-weighted centroid of a ring (first==last tolerated).

        Falls back to arithmetic mean of vertices for degenerate rings
        (colinear points, zero area).
        """
        n = len(ring)
        if n == 0:
            return (0.0, 0.0)
        # Drop duplicated closing vertex if present
        if n > 1 and ring[0] == ring[-1]:
            pts = ring[:-1]
        else:
            pts = ring
        m = len(pts)
        if m < 3:
            sx = sum(p[0] for p in pts) / m
            sy = sum(p[1] for p in pts) / m
            return (sx, sy)
        # Work relative to the first vertex. On raw lon/lat (about
        # -150, 61) the cross terms are ~9,000 while a building-sized
        # ring's area is ~1e-8, so the sums cancel and the result for a
        # 10 m square lands hundreds of metres away. Shifted to a local
        # origin the terms are the size of the ring itself.
        ox, oy = pts[0][0], pts[0][1]
        cx = 0.0
        cy = 0.0
        a = 0.0
        for i in range(m):
            x1, y1 = pts[i][0] - ox, pts[i][1] - oy
            x2 = pts[(i + 1) % m][0] - ox
            y2 = pts[(i + 1) % m][1] - oy
            cross = x1 * y2 - x2 * y1
            a += cross
            cx += (x1 + x2) * cross
            cy += (y1 + y2) * cross
        a *= 0.5
        if abs(a) < 1e-18:
            sx = sum(p[0] for p in pts) / m
            sy = sum(p[1] for p in pts) / m
            return (sx, sy)
        return (ox + cx / (6.0 * a), oy + cy / (6.0 * a))

    @classmethod
    def _geometry_centroid(
        cls, geometry: Dict[str, Any]
    ) -> Optional[Tuple[float, float]]:
        """Centroid of a GeoJSON geometry (Polygon/MultiPolygon/Point).

        For MultiPolygon, returns the area-weighted centroid of the
        constituent polygon centroids.
        """
        gtype = (geometry or {}).get("type", "")
        coords = (geometry or {}).get("coordinates")
        if coords is None:
            return None
        if gtype == "Point":
            return (float(coords[0]), float(coords[1]))
        if gtype == "Polygon":
            if not coords:
                return None
            return cls._ring_centroid(coords[0])
        if gtype == "MultiPolygon":
            if not coords:
                return None
            # Parts on both sides of the antimeridian (ArcGIS cuts
            # geometry at +/-180): average the part centroids with the
            # western ones moved a turn east, or islands at 179 and
            # -179 average to longitude 0, in the Atlantic.
            centroids = []
            for poly in coords:
                if not poly:
                    continue
                cx, cy = cls._ring_centroid(poly[0])
                centroids.append((cx, cy, cls._polygon_area(poly)))
            if not centroids:
                return None
            lons = [c[0] for c in centroids]
            shift = max(lons) - min(lons) > 180.0
            sum_x = 0.0
            sum_y = 0.0
            sum_a = 0.0
            for cx, cy, area in centroids:
                if shift and cx < 0.0:
                    cx += 360.0
                sum_x += cx * area
                sum_y += cy * area
                sum_a += area
            if sum_a <= 0:
                # Fall back to centroid of largest polygon
                largest = max(coords, key=lambda p: cls._polygon_area(p))
                return cls._ring_centroid(largest[0]) if largest else None
            x = sum_x / sum_a
            if x > 180.0:
                x -= 360.0
            return (x, sum_y / sum_a)
        return None

    @classmethod
    def _geometry_representative_point(
        cls, geometry: Dict[str, Any]
    ) -> Optional[Tuple[float, float]]:
        """Return a point guaranteed to be inside the geometry (best-effort).

        Uses the centroid when it's inside the geometry, otherwise sweeps
        a horizontal line at the centroid's y through the polygon and
        returns the midpoint of the widest interior segment. Final fallback
        is the first outer-ring vertex.
        """
        gtype = (geometry or {}).get("type", "")
        if gtype == "Point":
            coords = geometry.get("coordinates") or []
            return (float(coords[0]), float(coords[1])) if coords else None

        centroid = cls._geometry_centroid(geometry)
        if centroid is None:
            return None
        if cls._geometry_contains_point(geometry, centroid):
            return centroid

        # Horizontal sweep at y = centroid.y to find an interior segment.
        y = centroid[1]
        rings: List[List[List[float]]] = []
        coords = geometry.get("coordinates") or []
        if gtype == "Polygon":
            rings = list(coords)
        elif gtype == "MultiPolygon":
            for poly in coords:
                rings.extend(poly)

        xs: List[float] = []
        for ring in rings:
            n = len(ring)
            if n < 2:
                continue
            for i in range(n):
                x1, y1 = ring[i][0], ring[i][1]
                x2, y2 = ring[(i + 1) % n][0], ring[(i + 1) % n][1]
                if (y1 > y) == (y2 > y):
                    continue
                dy = y2 - y1
                if dy == 0:
                    continue
                t = (y - y1) / dy
                xs.append(x1 + t * (x2 - x1))
        xs.sort()
        # Take the widest interior span (odd pairs are inside under even-odd).
        best_mid = None
        best_width = -1.0
        for i in range(0, len(xs) - 1, 2):
            mid = (xs[i] + xs[i + 1]) / 2.0
            width = xs[i + 1] - xs[i]
            if width > best_width and cls._geometry_contains_point(
                geometry, (mid, y)
            ):
                best_width = width
                best_mid = (mid, y)
        if best_mid is not None:
            return best_mid

        # Last resort: first outer-ring vertex.
        if gtype == "Polygon" and coords and coords[0]:
            v = coords[0][0]
            return (float(v[0]), float(v[1]))
        if gtype == "MultiPolygon" and coords and coords[0] and coords[0][0]:
            v = coords[0][0][0]
            return (float(v[0]), float(v[1]))
        return centroid

    # ── Polyline reduction helpers ────────────────────────────────────────
    # Coordinates arrive in WGS84 (outSR=4326) so segment lengths are in
    # degrees. That's fine for finding a midpoint -- the result is exact in
    # Euclidean degree-space and still lies on the line. Anchorage spans ~3 deg
    # at lat 61 degN where 1 deg lon ~ 0.5 x 111 km, so the along-line position is
    # geodesically biased ~2x longward, but that bias is shared by the line
    # itself (same projection) so the chosen point lands on the right segment.
    @staticmethod
    def _segment_length(p1: List[float], p2: List[float]) -> float:
        dx = p2[0] - p1[0]
        dy = p2[1] - p1[1]
        return (dx * dx + dy * dy) ** 0.5

    @classmethod
    def _polyline_total_length(cls, coords: List[List[float]]) -> float:
        n = len(coords)
        if n < 2:
            return 0.0
        return sum(
            cls._segment_length(coords[i], coords[i + 1])
            for i in range(n - 1)
        )

    @classmethod
    def _polyline_point_at_length(
        cls, coords: List[List[float]], target: float
    ) -> Optional[Tuple[float, float]]:
        """Walk the polyline and return the point at `target` arc length."""
        n = len(coords)
        if n == 0:
            return None
        if n == 1 or target <= 0:
            return (float(coords[0][0]), float(coords[0][1]))
        accum = 0.0
        for i in range(n - 1):
            seg = cls._segment_length(coords[i], coords[i + 1])
            if accum + seg >= target:
                t = (target - accum) / seg if seg > 0 else 0.0
                x = coords[i][0] + t * (coords[i + 1][0] - coords[i][0])
                y = coords[i][1] + t * (coords[i + 1][1] - coords[i][1])
                return (float(x), float(y))
            accum += seg
        return (float(coords[-1][0]), float(coords[-1][1]))

    @classmethod
    def _polyline_midpoint(
        cls, coords: List[List[float]]
    ) -> Optional[Tuple[float, float]]:
        """Midpoint along a LineString by arc length (always lies on the line)."""
        total = cls._polyline_total_length(coords)
        return cls._polyline_point_at_length(coords, total / 2.0)

    @classmethod
    def _polyline_centroid(
        cls, coords: List[List[float]]
    ) -> Optional[Tuple[float, float]]:
        """Length-weighted centroid of a LineString.

        For a curved or U-shaped line the result can fall off the line.
        Use _polyline_midpoint when the point must lie on the line itself.
        """
        n = len(coords)
        if n == 0:
            return None
        if n == 1:
            return (float(coords[0][0]), float(coords[0][1]))
        sum_x = 0.0
        sum_y = 0.0
        sum_len = 0.0
        for i in range(n - 1):
            seg = cls._segment_length(coords[i], coords[i + 1])
            mid_x = (coords[i][0] + coords[i + 1][0]) / 2.0
            mid_y = (coords[i][1] + coords[i + 1][1]) / 2.0
            sum_x += mid_x * seg
            sum_y += mid_y * seg
            sum_len += seg
        if sum_len == 0:
            sx = sum(p[0] for p in coords) / n
            sy = sum(p[1] for p in coords) / n
            return (float(sx), float(sy))
        return (sum_x / sum_len, sum_y / sum_len)

    @classmethod
    def _multilinestring_midpoint(
        cls, lines: List[List[List[float]]]
    ) -> Optional[Tuple[float, float]]:
        """Midpoint along the concatenated arc length of all sub-lines."""
        if not lines:
            return None
        sub_lengths = [cls._polyline_total_length(line) for line in lines]
        total = sum(sub_lengths)
        if total == 0:
            for line in lines:
                if line:
                    return (float(line[0][0]), float(line[0][1]))
            return None
        target = total / 2.0
        accum = 0.0
        for line, seg_total in zip(lines, sub_lengths):
            if accum + seg_total >= target:
                return cls._polyline_point_at_length(line, target - accum)
            accum += seg_total
        for line in reversed(lines):
            if line:
                return (float(line[-1][0]), float(line[-1][1]))
        return None

    @classmethod
    def _multilinestring_centroid(
        cls, lines: List[List[List[float]]]
    ) -> Optional[Tuple[float, float]]:
        """Length-weighted centroid of a MultiLineString."""
        if not lines:
            return None
        parts = []
        for line in lines:
            seg_total = cls._polyline_total_length(line)
            if seg_total == 0:
                continue
            c = cls._polyline_centroid(line)
            if c is None:
                continue
            parts.append((c[0], c[1], seg_total))
        if not parts:
            for line in lines:
                if line:
                    return (float(line[0][0]), float(line[0][1]))
            return None
        # Same antimeridian handling as _geometry_centroid.
        lons = [c[0] for c in parts]
        shift = max(lons) - min(lons) > 180.0
        sum_x = 0.0
        sum_y = 0.0
        sum_len = 0.0
        for cx, cy, seg_total in parts:
            if shift and cx < 0.0:
                cx += 360.0
            sum_x += cx * seg_total
            sum_y += cy * seg_total
            sum_len += seg_total
        x = sum_x / sum_len
        if x > 180.0:
            x -= 360.0
        return (x, sum_y / sum_len)

    @classmethod
    def _feature_to_point(
        cls, geometry: Dict[str, Any], centroid_mode: str
    ) -> Optional[Tuple[float, float]]:
        """Reduce a feature's geometry to a single (lon, lat) point.

        Handles Point/MultiPoint, LineString/MultiLineString (e.g. road
        centerlines, trails, transit routes), and Polygon/MultiPolygon.
        For lines, `centroid_mode` has line-specific meaning:
          - 'centroid' = length-weighted centroid (cheap; can fall off the line)
          - 'representative_point' / 'auto' = midpoint along arc length
            (always lies on the line -- the right default for "which polygon
            does this road segment belong to" bucketing).
        """
        if not geometry:
            return None
        gtype = geometry.get("type", "")
        if gtype == "Point":
            coords = geometry.get("coordinates") or []
            return (float(coords[0]), float(coords[1])) if coords else None
        if gtype == "MultiPoint":
            coords = geometry.get("coordinates") or []
            return (float(coords[0][0]), float(coords[0][1])) if coords else None
        if gtype == "LineString":
            coords = geometry.get("coordinates") or []
            if not coords:
                return None
            if centroid_mode == "centroid":
                return cls._polyline_centroid(coords)
            return cls._polyline_midpoint(coords)
        if gtype == "MultiLineString":
            lines = geometry.get("coordinates") or []
            if not lines:
                return None
            if centroid_mode == "centroid":
                return cls._multilinestring_centroid(lines)
            return cls._multilinestring_midpoint(lines)
        if centroid_mode == "centroid":
            return cls._geometry_centroid(geometry)
        if centroid_mode == "representative_point":
            return cls._geometry_representative_point(geometry)
        # auto: centroid if inside, else representative_point
        centroid = cls._geometry_centroid(geometry)
        if centroid is not None and cls._geometry_contains_point(
            geometry, centroid
        ):
            return centroid
        return cls._geometry_representative_point(geometry)

    async def _resolve_layer_url(self, item_id: str) -> str:
        """Resolve an item ID to a validated /FeatureServer/N query URL."""
        item_id = self._validate_item_id(item_id)
        item = await self.get_dataset(item_id)
        url = item.get("url", "")
        item_type = item.get("type", "")
        if not url:
            raise ToolInputError(
                f"Item {item_id} has no queryable service URL"
            )
        if item_type and item_type not in self.QUERYABLE_TYPES:
            raise ToolInputError(
                f"Item type '{item_type}' is not queryable."
            )
        url = self._ensure_layer_url(url)
        self._validate_service_url(url)
        return url

    _OID_FALLBACK_NAMES = ("OBJECTID", "OID", "FID")

    @staticmethod
    def _oid_field_name(meta: Any) -> Optional[str]:
        """The object-id field a layer (or a query response) declares.

        It is usually OBJECTID, but not always: ObjectId, OBJECTID_1,
        FID and ESRI_OID are all in use, and code that assumed the
        usual name lost the join between a feature and its attributes.
        """
        if not isinstance(meta, dict):
            return None
        for key in ("objectIdField", "objectIdFieldName"):
            name = meta.get(key)
            if isinstance(name, str) and name:
                return name
        for f in meta.get("fields") or []:
            if (
                isinstance(f, dict)
                and f.get("type") == "esriFieldTypeOID"
                and f.get("name")
            ):
                return f["name"]
        return None

    async def _fetch_layer_meta(
        self, layer_url: str, require_fields: Tuple[str, ...] = ()
    ) -> Dict[str, Any]:
        """Layer ?f=json, through the metadata cache.

        ``require_fields`` are the caller's field names that it will
        reject as "not a field" when missing. If a cached copy lacks
        one, refetch once: a republish that added/renamed the field
        within META_CACHE_TTL_S must not make a correct name look
        wrong. A real typo just costs one extra request.
        """
        meta = await self._get_meta_json(layer_url)
        if require_fields:
            names = {f.get("name") for f in meta.get("fields") or []}
            if any(f and f not in names for f in require_fields):
                self._meta_cache.pop(layer_url, None)
                meta = await self._get_meta_json(layer_url)
        return meta

    async def _fetch_aggregation_polygons(
        self,
        aggregation_item_id: str,
        group_by_field: str,
        agg_where: str,
    ) -> List[Dict[str, Any]]:
        """Fetch polygons from an aggregation layer with their group value.

        Returns [{'group': value, 'geometry': {..GeoJSON..}, 'objectid': N}].
        Cached by (item_id, group_by_field, agg_where) for AGG_CACHE_TTL_SECONDS.
        """
        validated_where = WhereValidator.validate(agg_where or "1=1")
        cache_key = (
            self._validate_item_id(aggregation_item_id),
            group_by_field,
            validated_where,
        )
        now = time.time()
        # The layer's metadata is read BEFORE the cache is consulted (it is
        # itself cached for META_CACHE_TTL_S, so this is usually free): its
        # edit date is what says whether a cached set is still the layer.
        layer_url = await self._resolve_layer_url(aggregation_item_id)
        meta = await self._fetch_layer_meta(
            layer_url, require_fields=(group_by_field,)
        )
        editing = meta.get("editingInfo") or {}
        edit_stamp = editing.get("dataLastEditDate") or editing.get(
            "lastEditDate"
        )
        if not isinstance(edit_stamp, (int, float)):
            edit_stamp = None

        cached = self._agg_layer_cache.get(cache_key)
        if cached and cached[0] > now and cached[2] == edit_stamp:
            self._agg_layer_cache.move_to_end(cache_key)
            return cached[1]
        if cached:
            # Expired, or the layer was edited since -- drop and refetch.
            del self._agg_layer_cache[cache_key]

        geom_type = meta.get("geometryType", "")
        if geom_type not in (
            "esriGeometryPolygon",
            "esriGeometryMultiPatch",
        ):
            raise ToolInputError(
                f"aggregation_item_id must point at a polygon layer "
                f"(got geometryType={geom_type or 'unknown'!r})"
            )
        field_names = {f.get("name") for f in meta.get("fields", [])}
        if group_by_field not in field_names:
            raise ToolInputError(
                f"group_by_field {group_by_field!r} is not a field on "
                f"the aggregation layer. Available fields: "
                f"{sorted(field_names)[:12]}..."
            )

        polygons = await self._paged_geojson_fetch(
            layer_url,
            where=validated_where,
            out_fields=group_by_field,
            limit=self.AGG_SOURCE_LIMIT,
        )
        if len(polygons) >= self.AGG_SOURCE_LIMIT:
            # Bucketing into the first 5,000 would leave every source
            # feature in the rest reported as "unmatched", and the
            # partial set would sit in the cache for a day.
            raise ToolInputError(
                f"the aggregation layer has at least "
                f"{self.AGG_SOURCE_LIMIT:,} polygons matching agg_where "
                f"{agg_where!r}; aggregate_by_polygon handles fewer "
                f"than that. Narrow agg_where, or aggregate into a "
                f"coarser layer."
            )
        result = [
            {
                "group": (f.get("properties") or {}).get(group_by_field),
                "geometry": f.get("geometry"),
            }
            for f in polygons
            if f.get("geometry")
        ]
        try:
            approx_bytes = len(json.dumps(result, separators=(",", ":")))
        except (TypeError, ValueError):
            approx_bytes = self.AGG_CACHE_MAX_BYTES + 1  # unmeasurable: don't keep
        if approx_bytes > self.AGG_CACHE_MAX_BYTES:
            # Too large to keep at all: caching it would evict everything
            # else and still exceed the bound.
            return result
        ttl = (
            self.AGG_CACHE_TTL_SECONDS
            if edit_stamp is not None
            else min(
                self.AGG_CACHE_TTL_SECONDS,
                self.AGG_CACHE_TTL_NO_EDIT_DATE_SECONDS,
            )
        )
        self._agg_layer_cache[cache_key] = (
            now + ttl,
            result,
            edit_stamp,
            approx_bytes,
        )
        # LRU eviction to bound memory under cache-busting inputs: by entry
        # count, and by total size (the newest entry always fits, above).
        while len(self._agg_layer_cache) > self.AGG_CACHE_MAX_ENTRIES or (
            sum(entry[3] for entry in self._agg_layer_cache.values())
            > self.AGG_CACHE_MAX_BYTES
        ):
            self._agg_layer_cache.popitem(last=False)
        return result

    @staticmethod
    def _arcgis_error_text(err: Any) -> str:
        """Build a non-empty, actionable message from an ArcGIS error object.

        ArcGIS sometimes returns ``{"error": {"code": 500, "message": ""}}``
        on a transient blip -- never surface that as an empty string.
        """
        if not isinstance(err, dict):
            return str(err) or "unknown ArcGIS error"
        parts: List[str] = []
        code = err.get("code")
        if code is not None:
            parts.append(f"code {code}")
        msg = (err.get("message") or "").strip()
        if msg:
            parts.append(msg)
        details = [str(d) for d in (err.get("details") or []) if d]
        if details:
            parts.append("; ".join(details))
        return " -- ".join(parts) or (
            "ArcGIS returned an error with no message (usually a transient "
            "server blip -- retry the request)"
        )

    @classmethod
    def _is_transient_arcgis_error(cls, err: Any) -> bool:
        """Heuristic: 5xx-class codes and empty-message errors are transient."""
        if not isinstance(err, dict):
            return False
        code = err.get("code")
        msg = (err.get("message") or "").strip()
        if code in (500, 502, 503, 504):
            return True
        # Empty message (with or without a code) is the cold-start blip shape.
        return not msg

    # ── Upstream transport: budget, retry, metadata cache ──────────────

    def _request_timeout(self, cap: Optional[float] = None) -> Optional[float]:
        """Per-request timeout to pass, or None for the client default.

        ``cap`` is a caller's own per-request timeout (e.g. 10s for the
        layer-inventory fan-out). The result is the smaller of that (or
        the plugin timeout) and what is left of the call budget; None
        whenever nothing needs overriding, so the common path passes no
        extra argument at all.
        """
        deadline = _call_deadline.get()
        if deadline is None:
            return cap
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise self._budget_spent()
        limit = cap
        if limit is None:
            limit = getattr(self.plugin_config, "timeout", None)
        if limit is not None and remaining < limit:
            return remaining
        return cap

    @staticmethod
    def _budget_spent() -> UpstreamTimeoutError:
        return UpstreamTimeoutError(
            f"ArcGIS did not finish answering within this call's "
            f"{TOOL_CALL_BUDGET_S:g}s budget. The service may be slow "
            "right now; try again shortly, or narrow the request (a "
            "tighter WHERE clause, fewer fields, a smaller limit)."
        )

    @staticmethod
    def _check_budget(i: int = 0, every: int = 1) -> None:
        """Raise once the call budget is spent; for loops that do not await.

        The request timeouts only bound time spent waiting on ArcGIS.
        Matching thousands of features against polygons is pure CPU and
        never yields, so nothing else would stop it before the Lambda is
        killed. Pass the loop index and ``every`` to look at the clock
        only on every Nth iteration.
        """
        if every > 1 and i % every:
            return
        deadline = _call_deadline.get()
        if deadline is not None and time.monotonic() >= deadline:
            raise UpstreamTimeoutError(
                f"This request needed more computation than fits in a "
                f"call's {TOOL_CALL_BUDGET_S:g}s budget. Narrow it: a "
                "tighter WHERE clause on either layer, a smaller buffer, "
                "or fewer features."
            )

    @staticmethod
    def _url_label(url: str) -> str:
        """host/path of a URL, for error messages (never the query string)."""
        parsed = urlparse(url)
        return f"{parsed.netloc}{parsed.path}" or url

    def _may_retry(self, attempt: int) -> bool:
        if attempt >= self.ARCGIS_MAX_ATTEMPTS:
            return False
        deadline = _call_deadline.get()
        return (
            deadline is None
            or deadline - time.monotonic() >= RETRY_MIN_REMAINING_S
        )

    async def _backoff(self, attempt: int, url: str, why: str) -> None:
        logger.warning(
            "transient ArcGIS failure on %s (%s); retry %d",
            self._url_label(url),
            why,
            attempt,
        )
        await asyncio.sleep(self.ARCGIS_RETRY_BACKOFF_S * attempt)

    async def _send_once(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> httpx.Response:
        """One request inside the call budget.

        A timeout raises a readable UpstreamTimeoutError: an httpx timeout
        stringifies to '', which is how callers used to get a bare "Tool
        execution failed".

        The httpx timeout is per phase (connect, each read, write, pool),
        not for the whole request: a server that keeps sending a few
        bytes inside every read window never trips it. The call deadline
        is therefore enforced separately, as a wall-clock limit on the
        whole request.
        """
        effective = self._request_timeout(timeout)
        kwargs: Dict[str, Any] = {}
        if effective is not None:
            kwargs["timeout"] = effective
        deadline = _call_deadline.get()
        hard = None
        if deadline is not None:
            hard = max(deadline - time.monotonic(), 0.0)
        try:
            async with asyncio.timeout(hard) as scope:
                if method == "post":
                    return await self.client.post(url, data=data, **kwargs)
                return await self.client.get(url, params=params, **kwargs)
        except TimeoutError as e:
            if not scope.expired():
                raise
            raise self._budget_spent() from e
        except httpx.TimeoutException as e:
            seconds = effective or getattr(self.plugin_config, "timeout", 0)
            raise UpstreamTimeoutError(
                f"ArcGIS did not respond within {seconds:.0f}s "
                f"({type(e).__name__} on {self._url_label(url)}). The "
                "service may be slow right now; try again shortly, or "
                "narrow the request (a tighter WHERE clause, fewer fields, "
                "a smaller limit)."
            ) from e

    async def _send(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> httpx.Response:
        """GET/POST for every tool-path request, inside the call budget.

        Retries connection errors and HTTP 5xx while _may_retry allows;
        never retries a timeout or a 4xx. Returns the final response
        unchecked, so each caller keeps its own status/JSON handling.
        """
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = await self._send_once(
                    method, url, params=params, data=data, timeout=timeout
                )
            except UpstreamTimeoutError:
                raise
            except httpx.TransportError as e:
                if self._may_retry(attempt):
                    await self._backoff(attempt, url, repr(e))
                    continue
                raise RuntimeError(
                    f"Could not reach ArcGIS ({self._url_label(url)}) after "
                    f"{attempt} attempt(s): {type(e).__name__}"
                    f"{': ' + str(e) if str(e) else ''}"
                ) from e
            status = getattr(resp, "status_code", None)
            if (
                isinstance(status, int)
                and status >= 500
                and self._may_retry(attempt)
            ):
                await self._backoff(attempt, url, f"HTTP {status}")
                continue
            return resp

    async def _get_meta_json(
        self,
        url: str,
        validate: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        """GET ``url?f=json`` through a per-instance TTL/LRU cache.

        For portal item records and layer/service metadata, which change
        only when a layer is republished. ``validate`` (if given) runs on
        every fresh fetch before it is cached; failures and ArcGIS error
        bodies are never cached. Callers get a deep copy, so mutating the
        result cannot poison the cache.
        """
        now = time.monotonic()
        hit = self._meta_cache.get(url)
        if hit is not None:
            if hit[0] > now:
                self._meta_cache.move_to_end(url)
                return copy.deepcopy(hit[1])
            del self._meta_cache[url]
        resp = await self._send("get", url, params={"f": "json"})
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            # Not just `message`: for a missing layer ArcGIS sends an
            # empty message and puts the reason in `details`, which
            # reached the caller as "Tool execution failed".
            err = data["error"]
            msg = err.get("message") if isinstance(err, dict) else None
            raise RuntimeError(
                (msg or "").strip() or self._arcgis_error_text(err)
            )
        if validate is not None:
            validate(data)
        self._meta_cache[url] = (now + self.META_CACHE_TTL_S, data)
        while len(self._meta_cache) > self.META_CACHE_MAX_ENTRIES:
            self._meta_cache.popitem(last=False)
        return copy.deepcopy(data)

    async def _request_json_with_retry(
        self,
        url: str,
        *,
        method: str = "get",
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """HTTP request + JSON parse, retrying transient upstream failures.

        Retries httpx connection errors, HTTP 5xx, and transient ArcGIS
        error bodies (5xx-class or empty-message), while _may_retry allows.
        Never retries a timeout (see TOOL_CALL_BUDGET_S). Non-transient
        failures (4xx, real ArcGIS errors) raise immediately with a clear
        message, and so does the final transient one.
        """
        last_desc = "unknown error"
        attempt = 0
        while True:
            attempt += 1
            transient = False
            try:
                resp = await self._send_once(
                    method, url, params=params, data=data
                )
            except UpstreamTimeoutError:
                raise
            except httpx.TransportError as e:
                transient, last_desc = True, f"network error: {e!r}"
            else:
                status = resp.status_code
                if status >= 500:
                    transient, last_desc = True, f"upstream HTTP {status}"
                elif status >= 400:
                    raise RuntimeError(
                        f"Feature Service error (HTTP {status}): "
                        f"{resp.text[:200]}"
                    )
                else:
                    try:
                        payload = resp.json()
                    except Exception as e:
                        raise ValueError(
                            f"Feature Service returned non-JSON (content-type "
                            f"{resp.headers.get('content-type', '?')})"
                        ) from e
                    err = (
                        payload.get("error")
                        if isinstance(payload, dict)
                        else None
                    )
                    if not err:
                        return payload
                    if self._is_transient_arcgis_error(err):
                        transient = True
                        last_desc = self._arcgis_error_text(err)
                    else:
                        raise RuntimeError(
                            f"Feature Service query failed: "
                            f"{self._arcgis_error_text(err)}"
                        )
            if not transient or not self._may_retry(attempt):
                break
            await self._backoff(attempt, url, last_desc)
        raise RuntimeError(
            f"Feature Service request failed after {attempt} attempt(s): "
            f"{last_desc}"
        )

    async def _paged_geojson_fetch(
        self,
        layer_url: str,
        where: str,
        out_fields: str,
        limit: int,
        extra_params: Optional[Dict[str, Any]] = None,
        method: str = "get",
    ) -> List[Dict[str, Any]]:
        """Page through a layer and return raw GeoJSON features (geom + props).

        ``extra_params`` (e.g. a spatial geometry/spatialRel filter) is merged
        into each page request. Pass ``method='post'`` when the extra params
        carry a large geometry that would overflow a GET URL.
        """
        query_url = f"{layer_url}/query"
        out_fields = OutFieldsValidator.validate(out_fields or "*")
        features: List[Dict[str, Any]] = []
        offset = 0
        page_size = self.AGG_PAGE_SIZE
        last_first_id: Any = None
        while len(features) < limit:
            requested = min(page_size, limit - len(features))
            params = {
                "where": where,
                "outFields": out_fields,
                "returnGeometry": "true",
                "outSR": "4326",
                "f": "geojson",
                "resultRecordCount": str(requested),
                "resultOffset": str(offset),
                "maxAllowableOffset": str(self.GEOMETRY_SIMPLIFY_OFFSET_DEG),
            }
            if extra_params:
                params.update(extra_params)
            if method == "post":
                data = await self._request_json_with_retry(
                    query_url, method="post", data=params
                )
            else:
                data = await self._request_json_with_retry(
                    query_url, method="get", params=params
                )
            page = data.get("features") or []
            if not page:
                break
            # A server that ignores resultOffset returns page one again;
            # stop rather than collect the same features until `limit`.
            # Identify the page by its first feature's `id` when the
            # server sends one. It only does when the object-id field is
            # among outFields, so otherwise use the feature itself; a
            # guard keyed on `id` alone never fired for those fetches.
            first_id: Any = None
            if isinstance(page[0], dict):
                first_id = page[0].get("id")
                if first_id is None:
                    try:
                        first_id = json.dumps(
                            page[0], sort_keys=True, separators=(",", ":")
                        )
                    except (TypeError, ValueError):
                        first_id = None
            if offset and first_id is not None and first_id == last_first_id:
                logger.warning(
                    "Paging stalled on %s: resultOffset=%s returned the "
                    "previous page again",
                    self._url_label(query_url),
                    offset,
                )
                break
            last_first_id = first_id
            features.extend(page)
            offset += len(page)
            # A short page is the end ONLY if the server does not say
            # otherwise: a layer whose maxRecordCount is below the page
            # size returns short pages all the way through, flagged
            # with exceededTransferLimit. Stopping on length alone
            # fetched one page of such a layer and reported it as all.
            props = data.get("properties")
            exceeded = bool(
                data.get("exceededTransferLimit")
                or (
                    isinstance(props, dict)
                    and props.get("exceededTransferLimit")
                )
            )
            if len(page) < requested and not exceeded:
                break
        return features

    async def _aggregate_by_polygon(
        self, args: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """Aggregate source features into polygon buckets.

        Returns (rendered_markdown, structured_result). The structured
        half conforms to AGGREGATE_OUTPUT_SCHEMA, which is declared as
        the tool's outputSchema -- so any change to its shape is a
        breaking protocol change, not just an internal refactor."""
        source_item_id = self._validate_item_id(
            (args.get("source_item_id") or "").strip()
        )
        aggregation_item_id = self._validate_item_id(
            (args.get("aggregation_item_id") or "").strip()
        )
        group_by_field = (args.get("group_by_field") or "").strip()
        if not group_by_field:
            raise ToolInputError("group_by_field is required")
        raw_sum_fields = args.get("sum_fields") or []
        if isinstance(raw_sum_fields, str):
            raw_sum_fields = [
                s.strip() for s in raw_sum_fields.split(",") if s.strip()
            ]
        # Order-preserving dedupe: a repeated field would be added to
        # its own running sum twice per feature.
        sum_fields: List[str] = list(dict.fromkeys(raw_sum_fields))
        include_count = bool(args.get("count", True))
        # Validate both WHERE clauses up front so malformed/injection
        # attempts are rejected before any upstream I/O (otherwise a bad
        # source_where would still pay the aggregation-layer fetch cost).
        validated_source_where = WhereValidator.validate(
            args.get("source_where") or "1=1"
        )
        agg_where = WhereValidator.validate(args.get("agg_where") or "1=1")
        centroid_mode = (args.get("centroid_mode") or "auto").lower()
        if centroid_mode not in ("auto", "centroid", "representative_point"):
            raise ToolInputError(
                "centroid_mode must be one of: auto, centroid, "
                "representative_point"
            )
        overlap_policy = str(args.get("overlap_policy") or "").lower()
        if overlap_policy and overlap_policy not in (
            "first_match",
            "all_matches",
            "largest",
        ):
            raise ToolInputError(
                "overlap_policy must be one of: first_match, "
                "all_matches, largest"
            )
        # Optional proximity buffer: a source feature is bucketed into an
        # aggregation polygon when its point falls inside OR within
        # buffer_distance of that polygon. Answers "how many X within N of
        # each Y" (e.g. population points within half a mile of each park).
        raw_buffer = args.get("buffer_distance")
        buffer_m = 0.0
        buffer_units_canonical: Optional[str] = None
        if raw_buffer is not None and str(raw_buffer) != "":
            try:
                buffer_val = float(raw_buffer)
            except (TypeError, ValueError) as exc:
                raise ToolInputError(
                    f"buffer_distance must be a number "
                    f"(got {raw_buffer!r})"
                ) from exc
            if not math.isfinite(buffer_val) or buffer_val < 0:
                raise ToolInputError(
                    f"buffer_distance must be a finite number >= 0 "
                    f"(got {raw_buffer!r})"
                )
            if buffer_val > 0:
                buffer_units_canonical = self._normalize_linear_unit(
                    args.get("buffer_units")
                )
                buffer_m = (
                    buffer_val
                    * self._LINEAR_UNIT_TO_METERS[buffer_units_canonical]
                )
        if not overlap_policy:
            # "Within N of each polygon" is a per-polygon question: a
            # feature in range of two polygons belongs in both rows.
            # first_match would hand it to whichever polygon the server
            # happened to list first -- not the nearest, and the other
            # polygon's row would silently lose it. Without a buffer
            # the polygons normally tile, so first_match is exact and
            # keeps the rows summable.
            overlap_policy = "all_matches" if buffer_m > 0 else "first_match"
        max_source = min(
            self._int_arg(args, "max_source_features", self.AGG_SOURCE_LIMIT),
            self.AGG_SOURCE_LIMIT,
        )

        # Fetch aggregation polygons first so we can validate the group field
        # name before paying for a source-layer fetch.
        agg_polygons = await self._fetch_aggregation_polygons(
            aggregation_item_id, group_by_field, agg_where
        )
        if not agg_polygons:
            raise ToolInputError(
                f"agg_where {agg_where!r} matched no polygons on "
                f"the aggregation layer"
            )

        # Fetch source features. Validate sum_fields exist.
        source_url = await self._resolve_layer_url(source_item_id)
        source_meta = await self._fetch_layer_meta(
            source_url, require_fields=tuple(sum_fields)
        )
        source_fields = {f.get("name") for f in source_meta.get("fields", [])}
        numeric_types = {
            "esriFieldTypeInteger",
            "esriFieldTypeSmallInteger",
            "esriFieldTypeDouble",
            "esriFieldTypeSingle",
            "esriFieldTypeOID",
        }
        numeric_fields = {
            f.get("name")
            for f in source_meta.get("fields", [])
            if f.get("type") in numeric_types
        }
        for f in sum_fields:
            if f not in source_fields:
                raise ToolInputError(
                    f"sum_fields entry {f!r} is not a field on the "
                    f"source layer"
                )
            if f not in numeric_fields:
                raise ToolInputError(
                    f"sum_fields entry {f!r} is not a numeric field "
                    f"(cannot be summed)"
                )
        source_geom_type = source_meta.get("geometryType", "")

        # Request only the fields we need + geometry.
        out_fields = ",".join(sum_fields) if sum_fields else "OBJECTID"
        source_features = await self._paged_geojson_fetch(
            source_url,
            where=validated_source_where,
            out_fields=out_fields,
            limit=max_source,
        )

        # Reduce each source feature to a point.
        source_points: List[Tuple[Tuple[float, float], Dict[str, Any]]] = []
        no_geometry_count = 0
        for i, feat in enumerate(source_features):
            self._check_budget(i, 200)
            props = feat.get("properties") or {}
            geom = feat.get("geometry") or {}
            point = self._feature_to_point(geom, centroid_mode)
            if point is None:
                no_geometry_count += 1
                continue
            source_points.append((point, props))

        # Pre-compute a bounding box per polygon (cheap) for the candidate
        # prefilter below, plus areas when 'largest' needs them. Without the
        # prefilter, matching is O(source x polygons x edges) -- on the full
        # census-block layer (~3,700 blocks) against ~180 park polygons that
        # blew past the 29s API Gateway timeout. The bbox check prunes all
        # but the few nearby polygons before the precise distance test.
        for p in agg_polygons:
            p["_bbox"] = self._geometry_bbox(p["geometry"])
        if overlap_policy == "largest":
            for p in agg_polygons:
                p["_area"] = self._geometry_area(p["geometry"])

        first_only = overlap_policy == "first_match"
        m_per_deg = self._M_PER_DEG_LAT
        buckets: Dict[Any, Dict[str, Any]] = defaultdict(
            lambda: {"count": 0, "sums": {f: 0.0 for f in sum_fields}}
        )
        unmatched_count = 0
        multi_bucket_count = 0
        for i, (point, props) in enumerate(source_points):
            self._check_budget(i, 50)
            px, py = point
            # Buffer padding in degrees, consistent with the cos(lat)
            # scaling _point_to_geometry_distance_m uses, so the prefilter
            # is a safe superset (never excludes a true match).
            if buffer_m > 0:
                lat_scale = math.cos(math.radians(py))
                pad_lat = buffer_m / m_per_deg
                pad_lon = (
                    buffer_m / (m_per_deg * lat_scale)
                    if lat_scale > 1e-9
                    else 360.0
                )
            else:
                pad_lat = pad_lon = 0.0
            matches = []
            for p in agg_polygons:
                bb = p["_bbox"]
                if bb is not None and self._bbox_excludes(
                    bb, px, py, pad_lon, pad_lat
                ):
                    continue
                if buffer_m > 0:
                    hit = (
                        self._point_to_geometry_distance_m(p["geometry"], point)
                        <= buffer_m
                    )
                else:
                    hit = self._geometry_contains_point(p["geometry"], point)
                if hit:
                    matches.append(p)
                    if first_only:
                        break
            if not matches:
                unmatched_count += 1
                continue
            if overlap_policy == "largest":
                matches = [max(matches, key=lambda p: p.get("_area", 0.0))]
            # One feature counts once per GROUP: two polygons carrying
            # the same group value must not add it to that bucket twice.
            groups = list(dict.fromkeys(p["group"] for p in matches))
            if len(groups) > 1:
                multi_bucket_count += 1
            for group in groups:
                b = buckets[group]
                b["count"] += 1
                for fld in sum_fields:
                    v = props.get(fld)
                    if v is None:
                        continue
                    try:
                        num = float(v)
                    except (TypeError, ValueError):
                        continue
                    if math.isfinite(num):
                        b["sums"][fld] += num

        # Format: sorted by count desc for readability.
        bucket_list = sorted(
            buckets.items(),
            key=lambda kv: kv[1]["count"],
            reverse=True,
        )

        city = self.plugin_config.city_name
        buffer_label = (
            f"{raw_buffer} {buffer_units_canonical}"
            if buffer_m > 0
            else "none"
        )

        # Structured twin of the rendering below. Assembled here so the
        # empty-result path and the normal path cannot emit different
        # shapes -- a declared outputSchema has to hold on every branch.
        agg_query: Dict[str, Any] = {
            "source_item_id": source_item_id,
            "aggregation_item_id": aggregation_item_id,
            "group_by_field": group_by_field,
            "source_geometry_type": source_geom_type,
            "centroid_mode": centroid_mode,
            "overlap_policy": overlap_policy,
            "sum_fields": list(sum_fields),
            "count": include_count,
            "buffer": (
                {
                    "distance": float(raw_buffer),
                    "units": buffer_units_canonical,
                    "meters": buffer_m,
                }
                if buffer_m > 0
                else None
            ),
        }
        agg_summary: Dict[str, Any] = {
            "source_features": len(source_points),
            "buckets": len(bucket_list),
            "unmatched": unmatched_count,
        }
        agg_caveats: List[Dict[str, Any]] = []
        lines = [
            f"## Aggregation: {source_item_id} -> {aggregation_item_id}",
            f"**City:** {city}  |  **Group field:** `{group_by_field}`",
            f"**Source geometry:** {source_geom_type}  |  "
            f"**Centroid mode:** {centroid_mode}  |  "
            f"**Overlap policy:** {overlap_policy}  |  "
            f"**Proximity buffer:** {buffer_label}",
            f"**Source features:** {len(source_points):,}  |  "
            f"**Buckets:** {len(bucket_list)}  |  "
            f"**Unmatched:** {unmatched_count:,}",
            "",
        ]
        if not bucket_list:
            lines.append(
                "_No source features fell inside any aggregation polygon._"
            )
            self._append_aggregate_caveats(
                agg_caveats,
                small_bucket_seen=False,
                unmatched_count=unmatched_count,
                buffer_m=buffer_m,
                buffer_label=buffer_label,
                fetched=len(source_features),
                max_source=max_source,
                agg_where=agg_where,
                overlap_policy=overlap_policy,
                multi_bucket_count=multi_bucket_count,
                no_geometry_count=no_geometry_count,
                source_geom_type=source_geom_type,
            )
            for caveat in agg_caveats:
                lines += ["", f"_{caveat['message']}_"]
            return "\n".join(lines), {
                "query": agg_query,
                "summary": agg_summary,
                "rows": [],
                "caveats": agg_caveats,
            }

        header_cols = ["Group"]
        if include_count:
            header_cols.append("Count")
        header_cols.extend(sum_fields)
        lines.append("| " + " | ".join(header_cols) + " |")
        lines.append("|" + "---|" * len(header_cols))
        small_bucket_seen = False
        for group, bucket in bucket_list:
            row = [str(group)]
            if include_count:
                cnt = bucket["count"]
                if cnt < self.SMALL_SAMPLE_THRESHOLD:
                    small_bucket_seen = True
                    row.append(f"{cnt:,} (small sample)")
                else:
                    row.append(f"{cnt:,}")
            for fld in sum_fields:
                val = bucket["sums"][fld]
                if isinstance(val, float) and val.is_integer():
                    row.append(f"{int(val):,}")
                else:
                    row.append(f"{val:,}")
            lines.append("| " + " | ".join(row) + " |")
        self._append_aggregate_caveats(
            agg_caveats,
            small_bucket_seen=small_bucket_seen,
            unmatched_count=unmatched_count,
            buffer_m=buffer_m,
            buffer_label=buffer_label,
            fetched=len(source_features),
            max_source=max_source,
            agg_where=agg_where,
            overlap_policy=overlap_policy,
            multi_bucket_count=multi_bucket_count,
            no_geometry_count=no_geometry_count,
            source_geom_type=source_geom_type,
        )
        # The italic notes and the structured caveats are rendered from the
        # same list, so the two can never disagree about what qualified a
        # result.
        for caveat in agg_caveats:
            lines += ["", f"_{caveat['message']}_"]

        rows: List[Dict[str, Any]] = []
        for group, bucket in bucket_list:
            row: Dict[str, Any] = {
                "group": group,
                "sums": dict(bucket["sums"]),
            }
            if include_count:
                row["count"] = bucket["count"]
                row["small_sample"] = (
                    bucket["count"] < self.SMALL_SAMPLE_THRESHOLD
                )
            rows.append(row)

        return "\n".join(lines), {
            "query": agg_query,
            "summary": agg_summary,
            "rows": rows,
            "caveats": agg_caveats,
        }

    def _append_aggregate_caveats(
        self,
        caveats: List[Dict[str, Any]],
        *,
        small_bucket_seen: bool,
        unmatched_count: int,
        buffer_m: float,
        buffer_label: str,
        fetched: int,
        max_source: int,
        agg_where: str = "1=1",
        overlap_policy: str = "first_match",
        multi_bucket_count: int = 0,
        no_geometry_count: int = 0,
        source_geom_type: str = "",
    ) -> None:
        """Append the qualifications that apply to an aggregation result.

        Messages are plain text, not markdown: they are consumed verbatim
        by structured clients and only wrapped in italics for rendering.
        """
        if multi_bucket_count:
            caveats.append(
                {
                    "code": "counted_in_multiple_buckets",
                    "count": multi_bucket_count,
                    "message": (
                        f"{multi_bucket_count:,} source feature(s) "
                        f"matched more than one group and are counted "
                        f"in each (overlap_policy=all_matches). Each "
                        f"row is right on its own, but the rows "
                        f"overlap: do NOT add them up for an overall "
                        f"total."
                    ),
                }
            )
        if buffer_m > 0 and overlap_policy == "first_match":
            caveats.append(
                {
                    "code": "first_match_with_buffer",
                    "message": (
                        f"With a buffer and overlap_policy=first_match, "
                        f"a source feature within {buffer_label} of "
                        f"several polygons is counted for ONE of them "
                        f"only -- the first the server listed, not the "
                        f"nearest. The overall total is right; "
                        f"per-row figures are not 'everything within "
                        f"{buffer_label} of this polygon'. Use "
                        f"overlap_policy=all_matches for that."
                    ),
                }
            )
        if no_geometry_count:
            caveats.append(
                {
                    "code": "source_features_without_geometry",
                    "count": no_geometry_count,
                    "message": (
                        f"{no_geometry_count:,} fetched source "
                        f"feature(s) have no usable geometry and are "
                        f"in no bucket, nor in the source-feature or "
                        f"unmatched counts."
                    ),
                }
            )
        if source_geom_type and "Point" not in source_geom_type:
            what = (
                "line" if "Polyline" in source_geom_type else "polygon"
            )
            caveats.append(
                {
                    "code": "whole_feature_assignment",
                    "message": (
                        f"Each source {what} is assigned WHOLE to the "
                        f"bucket holding its "
                        f"{'midpoint' if what == 'line' else 'interior point'}"
                        f"; a {what} that straddles a boundary is not "
                        f"split, so its full count and sums go to one "
                        f"side."
                    ),
                }
            )
        if small_bucket_seen:
            caveats.append(
                {
                    "code": "small_sample_buckets",
                    "limit": self.SMALL_SAMPLE_THRESHOLD,
                    "message": (
                        f"Buckets tagged (small sample) hold fewer than "
                        f"{self.SMALL_SAMPLE_THRESHOLD} source features. "
                        f"Percentages and shares computed on these buckets "
                        f"are weak -- name the count, not the percent, when "
                        f"summarizing."
                    ),
                }
            )
        if unmatched_count:
            outside_what = (
                f"farther than {buffer_label} from every aggregation polygon"
                if buffer_m > 0
                else "outside every aggregation polygon"
            )
            # With a narrowed agg_where, most sources falling outside
            # the selected buckets is EXPECTED, not a data-quality
            # signal -- the old wording sent models chasing phantom
            # stray-coordinate problems.
            if agg_where != "1=1":
                diagnosis = (
                    f"Expected when bucketing into a subset: agg_where "
                    f"{agg_where!r} narrows the aggregation polygons, "
                    f"and sources outside that subset count here."
                )
            else:
                diagnosis = self.TEXT["unmatched_diagnosis"]
            caveats.append(
                {
                    "code": "unmatched_source_features",
                    "count": unmatched_count,
                    "message": (
                        f"{unmatched_count:,} source feature(s) fell "
                        f"{outside_what}. {diagnosis}"
                    ),
                }
            )
        if fetched >= max_source:
            caveats.append(
                {
                    "code": "source_cap_reached",
                    "limit": max_source,
                    "message": (
                        f"Source fetch hit the {max_source:,}-feature cap. "
                        f"Narrow source_where to get a complete picture."
                    ),
                }
            )

    # Esri numeric field types eligible to hold an area value.
    _NUMERIC_FIELD_TYPES = frozenset({
        "esriFieldTypeInteger",
        "esriFieldTypeSmallInteger",
        "esriFieldTypeBigInteger",
        "esriFieldTypeDouble",
        "esriFieldTypeSingle",
        "esriFieldTypeOID",
    })

    def _coverage_batches(
        self, parcels: List[Dict[str, Any]]
    ) -> List[Tuple[List[Dict[str, Any]], List[Any]]]:
        """Group targets into overlay queries that fit the filter caps.

        Returns (targets, MultiPolygon coordinates) per query. A fixed
        50 targets per query ran into MAX_FILTER_COORDS / _RINGS when
        the targets were detailed (shorelines, large tracts), and the
        call then failed with "filter_geometry has N coordinates" --
        about an argument the caller never passed. Batches now close
        when the next target would cross a cap. A single target over a
        cap on its own is queried by its bounding box instead: a
        superset is harmless here, because overlay features are
        assigned to targets afterwards, by containment.
        """
        max_coords = self.MAX_FILTER_COORDS
        max_rings = self.MAX_FILTER_RINGS
        batches: List[Tuple[List[Dict[str, Any]], List[Any]]] = []
        cur: List[Dict[str, Any]] = []
        cur_polys: List[Any] = []
        cur_coords = cur_rings = 0

        def flush() -> None:
            nonlocal cur, cur_polys, cur_coords, cur_rings
            if cur:
                batches.append((cur, cur_polys))
            cur, cur_polys, cur_coords, cur_rings = [], [], 0, 0

        for p in parcels:
            g = p["geom"]
            if g.get("type") == "Polygon":
                polys = [g.get("coordinates") or []]
            elif g.get("type") == "MultiPolygon":
                polys = list(g.get("coordinates") or [])
            else:
                polys = []
            rings = sum(len(poly) for poly in polys)
            coords = sum(len(r) for poly in polys for r in poly)
            if coords > max_coords or rings > max_rings:
                bb = p["bbox"]
                if bb is None:
                    continue
                flush()
                box = [
                    [bb[0], bb[1]], [bb[2], bb[1]], [bb[2], bb[3]],
                    [bb[0], bb[3]], [bb[0], bb[1]],
                ]
                batches.append(([p], [[box]]))
                continue
            if cur and (
                len(cur) >= self.COVERAGE_TARGET_BATCH
                or cur_coords + coords > max_coords
                or cur_rings + rings > max_rings
            ):
                flush()
            cur.append(p)
            cur_polys.extend(polys)
            cur_coords += coords
            cur_rings += rings
        flush()
        return batches

    async def _coverage_by_polygon(
        self, args: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """Measure overlay coverage of each target polygon.

        Returns (rendered_markdown, structured_result). The structured
        half conforms to COVERAGE_OUTPUT_SCHEMA, declared as the
        tool's outputSchema -- changing its shape is a breaking
        protocol change, not an internal refactor."""
        target_item_id = self._validate_item_id(
            (args.get("target_item_id") or "").strip()
        )
        overlay_item_id = self._validate_item_id(
            (args.get("overlay_item_id") or "").strip()
        )
        target_id_field = (args.get("target_id_field") or "").strip()
        if not target_id_field:
            raise ToolInputError(
                "target_id_field is required -- a field that labels each "
                "target polygon (e.g. 'Parcel_ID')."
            )
        target_area_field = (
            args.get("target_area_field") or "Shape__Area"
        ).strip()
        overlay_area_field = (
            args.get("overlay_area_field") or "Shape__Area"
        ).strip()
        target_where = WhereValidator.validate(args.get("target_where") or "1=1")
        overlay_where = WhereValidator.validate(
            args.get("overlay_where") or "1=1"
        )

        def _opt_pct(name: str) -> Optional[float]:
            raw = args.get(name)
            if raw is None or str(raw) == "":
                return None
            try:
                v = float(raw)
            except (TypeError, ValueError) as exc:
                raise ToolInputError(f"{name} must be a number (got {raw!r})") from exc
            if not math.isfinite(v) or v < 0:
                raise ToolInputError(
                    f"{name} must be a finite number >= 0 (got {raw!r})"
                )
            return v

        max_cov = _opt_pct("max_coverage_pct")
        min_cov = _opt_pct("min_coverage_pct")
        if min_cov is not None and max_cov is not None and min_cov >= max_cov:
            raise ToolInputError(
                "min_coverage_pct must be less than max_coverage_pct"
            )
        max_targets = min(
            self._int_arg(args, "max_targets", self.COVERAGE_TARGET_LIMIT),
            self.COVERAGE_TARGET_LIMIT,
        )

        target_url = await self._resolve_layer_url(target_item_id)
        overlay_url = await self._resolve_layer_url(overlay_item_id)
        target_meta = await self._fetch_layer_meta(
            target_url,
            require_fields=(target_id_field, target_area_field),
        )
        overlay_meta = await self._fetch_layer_meta(
            overlay_url, require_fields=(overlay_area_field,)
        )
        poly_types = ("esriGeometryPolygon", "esriGeometryMultiPatch")
        if target_meta.get("geometryType") not in poly_types:
            raise ToolInputError(
                f"target_item_id must be a polygon layer (got "
                f"geometryType={target_meta.get('geometryType')!r})"
            )
        if overlay_meta.get("geometryType") not in poly_types:
            raise ToolInputError(
                f"overlay_item_id must be a polygon layer (got "
                f"geometryType={overlay_meta.get('geometryType')!r})"
            )

        # Projection the stored Shape__Area values live in. MOA hosted
        # layers are Web Mercator, whose area is inflated by sec^2(lat);
        # see _true_area_m2.
        target_wkid = self._layer_wkid(target_meta)
        overlay_wkid = self._layer_wkid(overlay_meta)

        target_fields = {
            f.get("name"): f for f in target_meta.get("fields", [])
        }
        overlay_fields = {
            f.get("name"): f for f in overlay_meta.get("fields", [])
        }
        if target_id_field not in target_fields:
            raise ToolInputError(
                f"target_id_field {target_id_field!r} is not a field on the "
                f"target layer. Available: {sorted(target_fields)[:12]}..."
            )
        for label, fld, fields in (
            ("target_area_field", target_area_field, target_fields),
            ("overlay_area_field", overlay_area_field, overlay_fields),
        ):
            if fld not in fields:
                raise ToolInputError(
                    f"{label} {fld!r} is not a field on the layer. Most "
                    f"hosted layers expose 'Shape__Area'; otherwise pass an "
                    f"explicit numeric area field. Available: "
                    f"{sorted(fields)[:12]}..."
                )
            if fields[fld].get("type") not in self._NUMERIC_FIELD_TYPES:
                raise ToolInputError(
                    f"{label} {fld!r} is not numeric, so it can't be an area."
                )

        targets = await self._paged_geojson_fetch(
            target_url,
            where=target_where,
            out_fields=f"{target_id_field},{target_area_field}",
            limit=max_targets,
        )
        if not targets:
            raise ToolInputError(
                f"target_where {target_where!r} matched no polygons on the "
                f"target layer"
            )
        truncated = len(targets) >= max_targets

        # Build the per-target geometry/area/bbox set for client-side
        # assignment. Skip targets with no geometry or non-positive area.
        parcels: List[Dict[str, Any]] = []
        skipped = 0
        for feat in targets:
            props = feat.get("properties") or {}
            geom = feat.get("geometry")
            tid = props.get(target_id_field)
            try:
                area = float(props.get(target_area_field))
            except (TypeError, ValueError):
                area = 0.0
            if not geom or area <= 0:
                skipped += 1
                continue
            bbox = self._geometry_bbox(geom)
            parcels.append({
                "id": tid, "area": area, "geom": geom,
                "bbox": bbox, "covered": 0.0,
                # Latitude of the target, used to undo Web Mercator's
                # area inflation at THIS feature rather than applying one
                # flat factor across the municipality.
                "lat": ((bbox[1] + bbox[3]) / 2.0) if bbox else 61.2,
            })

        # Fetch overlay footprints that intersect the targets, ONE spatial
        # query per BATCH of target polygons (not per target -- that fan-out
        # tripped ArcGIS per-IP throttling). Each batch unions its parcels'
        # rings into a single filter geometry, POSTed so the large body fits.
        overlay_truncated = False
        seen_overlay: set = set()
        overlay_pts: List[Tuple[Tuple[float, float], float]] = []
        # An overlay feature that touches targets in two batches comes
        # back from both queries and must be counted once. ArcGIS puts an
        # `id` on a GeoJSON feature ONLY when the object-id field is in
        # outFields, so it is requested here; without it every feature's
        # id was None, the de-duplication below never ran, and a building
        # on a batch boundary was credited once per batch (a parcel read
        # 38% covered in a 400-target call and 19% on its own).
        overlay_oid_field = self._oid_field_name(overlay_meta)
        overlay_out_fields = overlay_area_field
        if overlay_oid_field and overlay_oid_field != overlay_area_field:
            overlay_out_fields = f"{overlay_area_field},{overlay_oid_field}"
        for batch, multi_coords in self._coverage_batches(parcels):
            esri = self._geojson_to_esri_polygon(
                {"type": "MultiPolygon", "coordinates": multi_coords}
            )
            # True union, not ring concatenation: adjacent parcels'
            # concatenated rings flip shared area into even-odd holes
            # and silently drop overlay features (see
            # _union_esri_rings).
            if len(batch) > 1:
                unioned = self._union_esri_rings(
                    esri.get("rings") or []
                )
                if unioned is not None:
                    esri = {
                        "rings": unioned,
                        "spatialReference": {"wkid": 4326},
                    }
            feats = await self._paged_geojson_fetch(
                overlay_url,
                where=overlay_where,
                out_fields=overlay_out_fields,
                limit=self.COVERAGE_OVERLAY_LIMIT,
                method="post",
                extra_params={
                    "geometry": json.dumps(esri, separators=(",", ":")),
                    "geometryType": "esriGeometryPolygon",
                    "inSR": "4326",
                    "spatialRel": "esriSpatialRelIntersects",
                },
            )
            if len(feats) >= self.COVERAGE_OVERLAY_LIMIT:
                overlay_truncated = True
            for i, f in enumerate(feats):
                self._check_budget(i, 200)
                props = f.get("properties") or {}
                pt = self._feature_to_point(f.get("geometry") or {}, "auto")
                if pt is None:
                    continue
                try:
                    a = float(props.get(overlay_area_field))
                except (TypeError, ValueError):
                    continue
                # The feature's identity: its id, else its object-id
                # attribute, else -- for a layer that offers neither --
                # its representative point and area, which two distinct
                # footprints do not share.
                oid = f.get("id")
                if oid is None and overlay_oid_field:
                    oid = props.get(overlay_oid_field)
                key: Any = (
                    ("oid", oid)
                    if oid is not None
                    else ("geom", round(pt[0], 8), round(pt[1], 8), a)
                )
                if key in seen_overlay:
                    continue
                seen_overlay.add(key)
                overlay_pts.append((pt, a))

        # Assign each overlay feature to EVERY target whose polygon
        # contains its centroid (bbox prefilter), summing overlay area.
        # Targets that tile the ground (lots, tracts) can only contain a
        # given centroid once, so each footprint still lands on one of
        # them. Targets that are stacked on each other -- the units of a
        # condominium sharing one polygon, a lease record over its
        # parcel -- each get it: the loop used to stop at the first
        # match, which left every other record in the stack reading 0%,
        # i.e. as vacant land.
        stacked_ids: set = set()
        for i, (pt, a) in enumerate(overlay_pts):
            self._check_budget(i, 200)
            px, py = pt
            holders = []
            for p in parcels:
                bb = p["bbox"]
                if bb is not None and (
                    px < bb[0] or px > bb[2] or py < bb[1] or py > bb[3]
                ):
                    continue
                if self._geometry_contains_point(p["geom"], pt):
                    p["covered"] += a
                    holders.append(p)
            if len(holders) > 1:
                stacked_ids.update(id(p) for p in holders)
        stacked_targets = len(stacked_ids)

        # The stored-area ratio is only a percentage when both layers
        # store area in the SAME projection: then the distortion cancels.
        # With a Web Mercator target and an equal-area overlay (or the
        # reverse) the raw ratio is off by sec^2(latitude) -- 50% read
        # as 11.6% at Anchorage -- so the ratio is taken from the true
        # areas instead, when both can be had.
        mixed_projections = target_wkid != overlay_wkid
        mixed_uncorrected = False
        scored = []
        for p in parcels:
            covered_sqft = self._to_sqft(
                p["covered"], p["lat"], overlay_wkid
            )
            area_sqft = self._to_sqft(p["area"], p["lat"], target_wkid)
            coverage = 100.0 * p["covered"] / p["area"]
            if mixed_projections:
                if covered_sqft is not None and area_sqft:
                    coverage = 100.0 * covered_sqft / area_sqft
                else:
                    mixed_uncorrected = True
            scored.append(
                {
                    "id": p["id"],
                    "coverage": coverage,
                    "covered": p["covered"],
                    "area": p["area"],
                    "covered_sqft": covered_sqft,
                    "area_sqft": area_sqft,
                }
            )
        zero_cov = sum(1 for r in scored if r["coverage"] == 0.0)

        def in_band(c: float) -> bool:
            if min_cov is not None and c < min_cov:
                return False
            if max_cov is not None and c >= max_cov:
                return False
            return True

        matched = sorted(
            (r for r in scored if in_band(r["coverage"])),
            key=lambda r: r["coverage"],
        )

        if max_cov is not None and min_cov is not None:
            band = f"{min_cov:g}% <= coverage < {max_cov:g}%"
        elif max_cov is not None:
            band = f"coverage < {max_cov:g}%"
        elif min_cov is not None:
            band = f"coverage >= {min_cov:g}%"
        else:
            band = "all (no coverage filter)"

        city = self.plugin_config.city_name
        lines = [
            f"## Coverage: {overlay_area_field} of {overlay_item_id} "
            f"within {target_item_id}",
            f"**City:** {city}  |  **Target filter:** `{target_where}`  |  "
            f"**ID field:** `{target_id_field}`",
            f"**Coverage band:** {band}",
            f"**Targets measured:** {len(scored):,}  |  "
            f"**In band:** {len(matched):,}  |  "
            f"**Out of band:** {len(scored) - len(matched):,}",
            "",
            f"**{len(matched):,} of {len(scored):,} targets fall in the "
            f"band ({band}).**",
            "",
        ]

        if matched:
            shown = matched[: self.COVERAGE_TABLE_ROWS]
            corrected = shown[0].get("area_sqft") is not None
            unit = "sqft" if corrected else "Web-Mercator m2"
            lines.append(
                f"| {target_id_field} | Coverage % | Covered ({unit}) | "
                f"Target ({unit}) |"
            )
            lines.append("|---|---|---|---|")
            for r in shown:
                cov = r["covered_sqft"] if corrected else r["covered"]
                tgt = r["area_sqft"] if corrected else r["area"]
                cov = r["covered"] if cov is None else cov
                tgt = r["area"] if tgt is None else tgt
                lines.append(
                    f"| {r['id']} | {r['coverage']:.1f}% | "
                    f"{cov:,.0f} | {tgt:,.0f} |"
                )
            if len(matched) > len(shown):
                lines.append("")
                lines.append(
                    f"_Showing the {len(shown)} lowest-coverage of "
                    f"{len(matched):,} in-band targets (sorted ascending). "
                    f"The headline count above is exact._"
                )

        # Caveats. Structured objects rather than prose strings: the
        # rendering below and the `caveats` array of the structured result
        # are produced from this one list, so they cannot drift. Messages
        # are plain text -- structured clients consume them verbatim.
        caveats: List[Dict[str, Any]] = [
            {
                "code": "centroid_assignment",
                "message": (
                    "Each overlay feature is assigned to the target whose "
                    "polygon contains its centroid, and its full area is "
                    "counted (not geometrically clipped), so coverage can "
                    "exceed 100% on dense lots. Good for screening, not "
                    "survey-grade."
                ),
            },
        ]
        if mixed_uncorrected:
            caveats.append(
                {
                    "code": "mixed_projections",
                    "message": (
                        f"The two layers store area in different "
                        f"projections (target wkid {target_wkid}, "
                        f"overlay wkid {overlay_wkid}) and at least one "
                        f"cannot be converted to true area, so the "
                        f"coverage percentages compare unlike units "
                        f"and are NOT reliable. Use layers in the same "
                        f"projection."
                    ),
                }
            )
        _areas_corrected = bool(scored) and scored[0].get("area_sqft") is not None
        if _areas_corrected and mixed_projections and not mixed_uncorrected:
            caveats.append(
                {
                    "code": "area_units_corrected",
                    "message": (
                        f"Areas are reported in true square feet. The "
                        f"two layers store {target_area_field} / "
                        f"{overlay_area_field} in different projections "
                        f"(target wkid {target_wkid}, overlay wkid "
                        f"{overlay_wkid}), so each figure is converted "
                        f"to true area at its own feature's latitude "
                        f"and the coverage percentage is computed from "
                        f"the converted areas."
                    ),
                }
            )
        elif _areas_corrected and not mixed_projections:
            caveats.append(
                {
                    "code": "area_units_corrected",
                    "message": (
                        f"Areas are reported in true square feet. The "
                        f"layers store {target_area_field} / "
                        f"{overlay_area_field} in Web Mercator, which "
                        f"inflates area by sec^2(latitude) ("
                        f"{self.TEXT['mercator_inflation']}); "
                        f"each figure is corrected at its own "
                        f"feature's latitude. The coverage PERCENTAGE is "
                        f"unaffected either way, because the distortion "
                        f"cancels in a ratio of two same-projection areas."
                    ),
                }
            )
        else:
            caveats.append(
                {
                    "code": "area_units_uncorrected",
                    "message": (
                        f"Raw areas are the layers' stored "
                        f"{target_area_field} / {overlay_area_field} in the "
                        f"layer's own projection -- NOT square feet or "
                        f"square metres, and not comparable to assessor "
                        f"figures. The coverage percentage is still valid: "
                        f"the projection cancels in the ratio."
                    ),
                }
            )
        if zero_cov:
            caveats.append(
                {
                    "code": "zero_coverage_targets",
                    "count": zero_cov,
                    "message": (
                        f"{zero_cov:,} target(s) returned 0% coverage (no "
                        f"overlay feature centroid falls inside) -- vacant "
                        f"land, or outside the overlay layer's extent. "
                        f"Decide whether 0% belongs in your answer."
                    ),
                }
            )
        if overlay_truncated:
            caveats.append(
                {
                    "code": "overlay_cap_reached",
                    "limit": self.COVERAGE_OVERLAY_LIMIT,
                    "message": (
                        f"An overlay batch hit the "
                        f"{self.COVERAGE_OVERLAY_LIMIT:,}-feature fetch cap; "
                        f"coverage for some targets may be undercounted. "
                        f"Narrow target_where."
                    ),
                }
            )
        if stacked_targets:
            caveats.append(
                {
                    "code": "stacked_targets",
                    "count": stacked_targets,
                    "message": (
                        f"{stacked_targets:,} target(s) lie on top of "
                        f"one another (condominium units sharing a "
                        f"polygon, lease or economic records over a "
                        f"parcel). Each is credited with the overlay "
                        f"features on its ground, so the same building "
                        f"counts toward several of them: do not add "
                        f"their covered areas together."
                    ),
                }
            )
        if skipped:
            caveats.append(
                {
                    "code": "targets_skipped",
                    "count": skipped,
                    "message": (
                        f"{skipped:,} target(s) were skipped (missing "
                        f"geometry/area or an upstream query error) and are "
                        f"not in the counts."
                    ),
                }
            )
        tmeta = await self._safe_layer_meta(target_item_id)
        cov_pct = tmeta.get("coverage_pct")
        if cov_pct is not None and cov_pct < self.COVERAGE_THRESHOLD:
            caveats.append(
                {
                    "code": "partial_layer_extent",
                    "message": (
                        f"The target layer's extent covers ~"
                        f"{cov_pct * 100:.0f}% of {city}; targets outside "
                        f"that extent are absent entirely."
                    ),
                }
            )
        # Task 2: centroid assignment dumps a shared building polygon onto
        # whichever lot holds its centroid, so in attached / zero-lot-line
        # housing the neighbours all read 0%. A high 0% share is the
        # tool's own tell that this happened.
        if scored:
            zero_ratio = zero_cov / len(scored)
            if zero_ratio > self.COVERAGE_ZERO_ARTIFACT_RATIO:
                caveats.append(
                    {
                        "code": "possible_centroid_artifact",
                        "count": zero_cov,
                        "message": (
                            f"{zero_cov:,} of {len(scored):,} targets "
                            f"({zero_ratio:.0%}) returned 0% coverage. In "
                            f"developed areas this usually means attached / "
                            f"zero-lot-line housing, where centroid "
                            f"assignment dumps a shared building polygon "
                            f"onto one lot and reads its neighbours as "
                            f"empty. "
                            f"{self.TEXT['coverage_verify_hint']}"
                        ),
                    }
                )

        # Task 3: MOA layers carry several polygons per Parcel_ID
        # (multipart parcels, plus stacked Lease and Economic records), so
        # a count of targets is a count of POLYGONS, not parcels.
        distinct_ids = len({r["id"] for r in scored if r["id"] is not None})
        if distinct_ids and distinct_ids < len(scored):
            caveats.append(
                {
                    "code": "targets_are_polygons_not_parcels",
                    "count": len(scored),
                    "message": (
                        f"{len(scored):,} target polygons cover only "
                        f"{distinct_ids:,} distinct {target_id_field} "
                        f"values -- counts here are POLYGONS, not parcels. "
                        f"{self.TEXT['stacked_records_note']}"
                    ),
                }
            )

        if truncated:
            caveats.append(
                {
                    "code": "target_cap_reached",
                    "limit": max_targets,
                    "message": (
                        f"Hit the {max_targets:,}-target cap. Narrow "
                        f"target_where for a complete picture."
                    ),
                }
            )
        lines.append("")
        lines.extend(f"_{c['message']}_" for c in caveats)

        return "\n".join(lines), {
            "query": {
                "target_item_id": target_item_id,
                "overlay_item_id": overlay_item_id,
                "target_id_field": target_id_field,
                "target_area_field": target_area_field,
                "overlay_area_field": overlay_area_field,
                "target_where": target_where,
                "band": {
                    "min_coverage_pct": min_cov,
                    "max_coverage_pct": max_cov,
                },
            },
            "summary": {
                "targets_measured": len(scored),
                "in_band": len(matched),
                "out_of_band": len(scored) - len(matched),
                "zero_coverage": zero_cov,
                "skipped": skipped,
                "distinct_target_ids": distinct_ids,
                "areas_corrected_to_sqft": _areas_corrected,
            },
            # EVERY in-band target, not just the COVERAGE_TABLE_ROWS shown
            # in the table. Truncating the machine-readable channel would
            # defeat its purpose, and the full set is far under the Lambda
            # response limit.
            "rows": [
                {
                    "id": r["id"],
                    "coverage_pct": r["coverage"],
                    "covered_area": r["covered"],
                    "target_area": r["area"],
                    "covered_sqft": r["covered_sqft"],
                    "target_sqft": r["area_sqft"],
                }
                for r in matched
            ],
            "caveats": caveats,
        }

    async def _filter_by_polygon(
        self, args: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """Return source features falling inside container polygons.

        Returns (text, structured); the structured half conforms to
        QUERY_RESULT_OUTPUT_SCHEMA like the other record tools."""
        source_item_id = self._validate_item_id(
            (args.get("source_item_id") or "").strip()
        )
        container_item_id = self._validate_item_id(
            (args.get("container_item_id") or "").strip()
        )
        container_where = (args.get("container_where") or "").strip()
        if not container_where:
            raise ToolInputError(
                "container_where is required -- it identifies which "
                "polygon(s) in the container layer to filter against"
            )
        validated_container_where = WhereValidator.validate(container_where)
        # Validate source_where up front too -- rejection shouldn't wait
        # for the container-layer lookup to complete.
        source_where = WhereValidator.validate(
            args.get("source_where") or "1=1"
        )
        out_fields = args.get("out_fields", "*")
        return_geometry = bool(args.get("return_geometry", False))
        requested_limit = self._int_arg(args, "limit", 100)
        effective_limit = (
            min(requested_limit, self.GEOMETRY_LIMIT_CAP)
            if return_geometry
            else min(requested_limit, 1000)
        )

        # Resolve the container polygon(s) up-front so we can report a
        # friendly 0-match error instead of silently returning no records.
        container_url = await self._resolve_layer_url(container_item_id)
        container_meta = await self._fetch_layer_meta(container_url)
        geom_type = container_meta.get("geometryType", "")
        if geom_type not in (
            "esriGeometryPolygon",
            "esriGeometryMultiPatch",
        ):
            raise ToolInputError(
                f"container_item_id must point at a polygon layer "
                f"(got geometryType={geom_type or 'unknown'!r})"
            )

        count_resp = await self._send("get",
            f"{container_url}/query",
            params={
                "where": validated_container_where,
                "returnCountOnly": "true",
                "f": "json",
            },
        )
        count_resp.raise_for_status()
        count_data = count_resp.json()
        if "error" in count_data:
            raise RuntimeError(
                count_data["error"].get(
                    "message", str(count_data["error"])
                )
            )
        matched_polygons = int(count_data.get("count") or 0)
        if matched_polygons == 0:
            return (
                f"Nothing was filtered: container_where "
                f"{container_where!r} matched 0 "
                f"polygons on container layer `{container_item_id}`. "
                f"Did you misspell a name? Try "
                f"`search_spatial_layers` or `query_data` to browse "
                f"valid values for the container field."
            ), {
                "query": {
                    "item_id": source_item_id,
                    "service_url": None,
                    "where": source_where,
                    "out_fields": out_fields,
                    "limit": effective_limit,
                    "geometry_type": None,
                    "container_item_id": container_item_id,
                    "container_where": container_where,
                    "container_polygons_matched": 0,
                },
                "summary": {
                    "returned": 0,
                    "total_count": 0,
                    "truncated": False,
                },
                "rows": [],
                # The explanation must be a structured caveat, not just
                # the markdown half: structured-output clients render
                # only structuredContent, and a bare empty rows list
                # here reads as "no features inside", which is wrong --
                # the CONTAINER lookup failed.
                "caveats": [
                    {
                        "code": "container_no_match",
                        "count": 0,
                        "message": (
                            f"container_where {container_where!r} "
                            f"matched 0 polygons on container layer "
                            f"{container_item_id} -- the container "
                            f"lookup failed, so nothing was filtered. "
                            f"Check the value with get_distinct_values "
                            f"on the container field."
                        ),
                    }
                ],
            }

        # Delegate the actual spatial query to spatial_query_polygon's
        # filter-item pathway so we get server-side intersection and
        # consistent result formatting with the rest of the plugin.
        overflow: Dict[str, Any] = {}
        records, source_meta = await asyncio.gather(
            self.spatial_query_polygon(
                source_item_id,
                filter_geometry=None,
                filter_item_id=container_item_id,
                filter_where=validated_container_where,
                spatial_rel="intersects",
                filters={
                    "where": source_where,
                    "out_fields": out_fields,
                },
                limit=effective_limit,
                return_geometry=return_geometry,
                info=overflow,
            ),
            self._safe_layer_meta(source_item_id),
        )

        city = self.plugin_config.city_name
        header = (
            f"## Filter: {source_item_id} inside "
            f"{container_item_id} where {container_where!r}\n"
            f"**City:** {city}  |  "
            f"**Container polygons matched:** {matched_polygons:,}\n\n"
        )
        if not records:
            return (
                header
                + f"No features in `{source_item_id}` fall inside the "
                f"selected polygon(s)."
            ), {
                "query": {
                    "item_id": source_item_id,
                    "service_url": None,
                    "where": source_where,
                    "out_fields": out_fields,
                    "limit": effective_limit,
                    "geometry_type": None,
                    "container_item_id": container_item_id,
                    "container_where": container_where,
                    "container_polygons_matched": matched_polygons,
                },
                "summary": {
                    "returned": 0,
                    "total_count": 0,
                    "truncated": False,
                },
                "rows": [],
                "caveats": [],
            }
        body, structured = self._format_query_results(
            records,
            effective_limit,
            total_count=overflow.get("total_count"),
            date_fields=source_meta.get("date_fields"),
            coded_domains=source_meta.get("coded_domains"),
            limit_reached=bool(overflow.get("limit_reached")),
        )
        # The shared formatter does not know about the container layer;
        # add that context so the structured result explains what was
        # actually filtered.
        structured["query"].update(
            {
                "item_id": source_item_id,
                "where": source_where,
                "out_fields": out_fields,
                "container_item_id": container_item_id,
                "container_where": container_where,
                "container_polygons_matched": matched_polygons,
            }
        )
        return header + body, structured

    async def _get_distinct_values(
        self, args: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """Return distinct values of a field -- for confirming exact
        identifier/code formats before constructing a WHERE clause.

        Returns (text, structured); the structured half conforms to
        DISTINCT_VALUES_OUTPUT_SCHEMA."""
        item_id = self._validate_item_id(
            (args.get("item_id") or "").strip()
        )
        field = (args.get("field") or "").strip()
        if not field:
            raise ToolInputError("field is required")
        like = (args.get("like") or "").strip()
        where = (args.get("where") or "1=1").strip()
        limit = min(self._int_arg(args, "limit", 50), 500)

        layer_url = await self._resolve_layer_url(item_id)
        meta = await self._fetch_layer_meta(
            layer_url, require_fields=(field,)
        )
        field_names = {f.get("name") for f in meta.get("fields", [])}
        if field not in field_names:
            raise ToolInputError(
                f"field {field!r} is not a field on this layer. "
                f"Call `get_layer_schema(item_id='{item_id}')` to "
                f"see valid names -- they are CASE-SENSITIVE. "
                f"Available: {sorted(field_names)[:12]}..."
            )

        # Splice the LIKE clause into the user's WHERE before validation
        # so any injected SQL gets rejected by WhereValidator together
        # with the rest. Single-quote escape is local to LIKE-substring
        # construction; not a substitute for WhereValidator.
        if like:
            safe_like = like.replace("'", "''")
            like_clause = f"{field} LIKE '%{safe_like}%'"
            where = (
                f"({where}) AND ({like_clause})"
                if where != "1=1"
                else like_clause
            )
        where = WhereValidator.validate(where)

        # returnDistinctValues only works when returnGeometry=false on
        # most hosted Feature Services -- geometry coords break the
        # de-dup hash. We also dedupe client-side as a backstop in case
        # the upstream returns duplicates anyway (older portals do).
        params = {
            "f": "json",
            "where": where,
            "outFields": field,
            "returnDistinctValues": "true",
            "returnGeometry": "false",
            "orderByFields": field,
            "resultRecordCount": str(limit),
        }
        resp = await self._send("get",
            f"{layer_url}/query", params=params
        )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            err = data["error"]
            raise RuntimeError(
                "Distinct-values query failed: "
                + self._rewrite_arcgis_error(
                    err.get("message", "Unknown error"),
                    err.get("details", []),
                    resource_id=item_id,
                    has_where=True,
                )
            )

        seen: set = set()
        values: List[Any] = []
        raw_rows = data.get("features") or []
        # Judge the cap on the rows the server sent, before NULLs and
        # repeats are dropped: a full page holding one NULL yields
        # limit-1 values and used to read as complete.
        hit_cap = len(raw_rows) >= limit or bool(
            data.get("exceededTransferLimit")
        )
        for f in raw_rows:
            v = (f.get("attributes") or {}).get(field)
            if v is None or v in seen:
                continue
            seen.add(v)
            values.append(v)

        def _structured(
            vals: List[Any], capped: bool
        ) -> Dict[str, Any]:
            caveats: List[Dict[str, Any]] = []
            if capped:
                caveats.append(
                    {
                        "code": "values_cap_reached",
                        "limit": limit,
                        "count": len(vals),
                        "message": (
                            f"Hit the {limit}-value cap, so this list may "
                            f"be incomplete. Raise limit or narrow with "
                            f"`like` for the full set."
                        ),
                    }
                )
            return {
                "query": {
                    "item_id": item_id,
                    "field": field,
                    "where": where,
                    "like": like or None,
                    "limit": limit,
                },
                "summary": {
                    "returned": len(vals),
                    "truncated": capped,
                },
                # Raw stored values, in the layer's own types. These are
                # what a WHERE clause must match -- matching is
                # case-sensitive, so they are NOT normalized here.
                "values": vals,
                "caveats": caveats,
            }

        if not values:
            suffix = f" containing '{like}'" if like else ""
            return (
                f"No distinct values for field `{field}`{suffix} on "
                f"item `{item_id}` (where: {where!r}). "
                + (
                    "Try a different `like` substring, or omit `like` "
                    "to see all values."
                    if like
                    else "The layer may have no records, or every "
                    "value may be NULL."
                )
            ), _structured([], False)

        capped_note = " (truncated to limit)" if hit_cap else ""
        lines = [
            f"## Distinct `{field}` values "
            f"({len(values)}{capped_note}, limit={limit})"
        ]
        if like:
            lines.append(
                f"_Filtered to values containing '{like}' "
                f"(case-sensitive)._"
            )
        lines.append("")
        for v in values:
            lines.append(f"- `{v}`")
        lines += [
            "",
            "---",
            "**NEXT STEP:** these are the EXACT values the layer "
            "stores -- copy them verbatim into a WHERE clause. Field "
            "and value matching is CASE-SENSITIVE.",
            f"Example: `query_data(item_id='{item_id}', "
            f"where=\"{field}='<paste a value above>'\", limit=1)` "
            "to count records with that value.",
        ]
        return "\n".join(lines), _structured(values, bool(capped_note))

    @staticmethod
    def _scalar(value: Any) -> Any:
        """A raw attribute value narrowed to a JSON scalar."""
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    async def _find_features_spanning_classifications(
        self, args: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """Find source features whose footprint touches >= min_distinct
        distinct values of a classification field in another polygon
        layer. The "split-zoned parcel" pattern, generalised."""
        source_item_id = self._validate_item_id(
            (args.get("source_item_id") or "").strip()
        )
        classification_item_id = self._validate_item_id(
            (args.get("classification_item_id") or "").strip()
        )
        classification_field = (
            args.get("classification_field") or ""
        ).strip()
        if not classification_field:
            raise ToolInputError("classification_field is required")

        # Enforce min_distinct >= 2 -- anything less is "any feature
        # touching a classification", which is just spatial_query_polygon.
        min_distinct = max(2, self._int_arg(args, "min_distinct", 2))
        source_where = WhereValidator.validate(
            args.get("source_where") or "1=1"
        )
        classification_where = WhereValidator.validate(
            args.get("classification_where") or "1=1"
        )
        out_fields = OutFieldsValidator.validate(
            args.get("out_fields") or "*"
        )
        limit = min(self._int_arg(args, "limit", 100), 1000)
        max_source = min(
            self._int_arg(
                args, "max_source_features", self.SPANNING_SOURCE_LIMIT
            ),
            self.SPANNING_SOURCE_LIMIT,
        )

        # Self-intersection makes no sense -- every feature touches
        # itself, every feature qualifies, the answer is trivially
        # "all of them". A weak model occasionally picks the same
        # layer for both args when reasoning is muddled; refuse fast
        # so we don't waste a spatial loop and confuse the user.
        if source_item_id == classification_item_id:
            raise ToolInputError(
                "source_item_id and classification_item_id are the "
                "same layer. Self-intersection is meaningless -- "
                "every feature trivially touches itself. For a "
                "'parcels spanning multiple zones' question, source "
                f"{self.TEXT['spanning_example']}"
                "`find_gis_content(topic='parcels')` and "
                "`find_gis_content(topic='zoning')` to discover the "
                "right item IDs for each side."
            )

        source_url = await self._resolve_layer_url(source_item_id)
        classification_url = await self._resolve_layer_url(
            classification_item_id
        )
        cls_meta = await self._fetch_layer_meta(
            classification_url, require_fields=(classification_field,)
        )
        cls_geom = cls_meta.get("geometryType", "")
        if cls_geom not in (
            "esriGeometryPolygon",
            "esriGeometryMultiPatch",
        ):
            raise ToolInputError(
                f"classification_item_id must point at a polygon "
                f"layer (got geometryType={cls_geom or 'unknown'!r}). "
                f"Spanning analysis needs polygons to define the "
                f"distinct regions a source feature might cross."
            )
        cls_field_names = {
            f.get("name") for f in cls_meta.get("fields", [])
        }
        if classification_field not in cls_field_names:
            raise ToolInputError(
                f"classification_field {classification_field!r} is "
                f"not a field on the classification layer. Call "
                f"`get_layer_schema(item_id="
                f"'{classification_item_id}')` to see valid names -- "
                f"they are CASE-SENSITIVE. Available: "
                f"{sorted(cls_field_names)[:12]}..."
            )

        # Wrong-grain classification detection. A classification layer
        # that has parcel-identifier fields is itself a per-parcel
        # layer (one polygon per legal parcel) -- its `Zoning_District`
        # or similar field is an attribute on each parcel, NOT a
        # categorisation across few polygons. Spatial-intersecting
        # parcels against parcels mostly produces "0 qualifying"
        # because of the 1,000-polygon classification cap. The right
        # classification is a dedicated zoning-polygon layer. If the
        # caller deliberately wants to span on another parcel-grain
        # field, they can pass that field explicitly -- only refuse
        # when classification_field is NOT itself a parcel-id
        # (otherwise we'd block the legit "find parcels with multiple
        # parcel IDs" sanity-check use case).
        cls_is_parcel_grain = bool(
            self.PARCEL_INDICATOR_FIELDS & cls_field_names
        )
        if (
            cls_is_parcel_grain
            and classification_field not in self.PARCEL_INDICATOR_FIELDS
        ):
            parcel_fields_present = sorted(
                self.PARCEL_INDICATOR_FIELDS & cls_field_names
            )
            raise ToolInputError(
                f"classification_item_id `{classification_item_id}` "
                f"looks like a per-parcel layer -- it has parcel "
                f"identifier field(s) "
                f"{parcel_fields_present}. That means each polygon is "
                f"one parcel, and `{classification_field}` is an "
                f"attribute stored on each parcel record (parcel "
                f"grain), NOT a categorisation across a small number "
                f"of zone polygons (zone grain).\n\n"
                f"Spanning analysis needs the classification layer to "
                f"have FEW polygons (zones, districts, regions) where "
                f"many polygons share each distinct value. Otherwise "
                f"the upstream cap of "
                f"{self.SPANNING_CLASSIFICATION_LIMIT} polygons "
                f"forces a random sample and the spatial join misses "
                f"most matches.\n\n"
                f"For zoning specifically, use the dedicated zoning "
                f"layer -- call `find_gis_content(topic='zoning')` "
                f"and pick a Feature Service whose name is "
                f"`Zoning_Hosted` or similar (NOT a parcels or "
                f"property-information layer). Confirm with "
                f"`get_layer_schema` that the layer has a "
                f"`GeneralizedZone` or `ZONING_DESIGNATIONS` field "
                f"and only ~1,000 polygons total."
            )

        # Source layer must have a user-facing identifier field, OR
        # the caller must have explicitly chosen out_fields (which
        # signals "I know what fields this layer has").
        # Without this check, a model that picks an aggregate layer
        # (zoning districts, council areas) as source produces output
        # that lists OBJECTIDs as "parcel numbers" -- the exact bug
        # the user keeps hitting.
        src_meta = await self._fetch_layer_meta(source_url)
        oid_field = self._oid_field_name(src_meta)
        oid_names = set(self._OID_FALLBACK_NAMES)
        if oid_field:
            oid_names.add(oid_field)
        if out_fields == "*":
            src_field_names = {
                f.get("name") for f in src_meta.get("fields", [])
            }
            has_natural = any(
                f in src_field_names
                for f in self.NATURAL_ID_FIELD_PRIORITY
            )
            if not has_natural:
                visible_fields = [
                    f for f in sorted(src_field_names)
                    if f
                    and not f.startswith("Shape__")
                    and f not in oid_names
                ][:12]
                raise ToolInputError(
                    f"source_item_id `{source_item_id}` has no "
                    f"user-facing identifier field (no Parcel_ID, "
                    f"Name, Address, Title, etc.). Without one, "
                    f"results would only have internal OBJECTIDs -- "
                    f"unsafe to report as parcel numbers / "
                    f"addresses / names.\n\n"
                    f"This layer's non-shape fields: "
                    f"{visible_fields or '(none)'}.\n\n"
                    f"This is most likely a wrong layer choice -- "
                    f"aggregate / boundary layers (zoning districts, "
                    f"council areas, regions) typically have only "
                    f"OBJECTID + Shape fields and are meant to be "
                    f"the CLASSIFICATION, not the source. For "
                    f"'parcels spanning multiple X' use "
                    f"`find_gis_content(topic='parcels')` and pick "
                    f"a layer with `Parcel_ID` "
                    f"{self.TEXT['parcel_layer_examples']}from the "
                    f"QUERYABLE section. To override and proceed "
                    f"anyway, pass `out_fields` explicitly."
                )

        # Pre-flight: refuse if the source layer has more features
        # matching source_where than the cap. Doing this up front saves
        # the per-classification spatial-query round-trips that would
        # otherwise be wasted.
        source_count = await self._get_record_count(
            source_url, source_where
        )
        if source_count is None:
            # _get_record_count returns None on ANY failure, including a
            # source_where the layer rejects. Reading that as 0 answered
            # "nothing matches" for what was really an error.
            raise RuntimeError(
                f"could not count the source features matching "
                f"source_where {source_where!r}. Check the field names "
                f"with get_layer_schema, or retry if the service was "
                f"busy."
            )
        if source_count > max_source:
            raise ToolInputError(
                f"source layer has {source_count:,} features matching "
                f"source_where, exceeding the cap of "
                f"{max_source:,}. Narrow `source_where` to fit under "
                f"the cap (e.g. limit by neighborhood, zone, or "
                f"region). The cap exists to bound per-call compute "
                f"on the upstream portal."
            )
        if source_count == 0:
            return (
                f"source layer has 0 features matching source_where "
                f"({source_where!r}). Nothing to spatially join "
                f"against."
            ), {
                "query": {
                    "source_item_id": source_item_id,
                    "classification_item_id": classification_item_id,
                    "classification_field": classification_field,
                    "source_where": source_where,
                    "min_distinct": min_distinct,
                    "limit": limit,
                },
                "summary": {
                    "source_features": 0,
                    "qualifying": 0,
                    "returned": 0,
                },
                "rows": [],
                "caveats": [
                    {
                        "code": "no_source_features",
                        "count": 0,
                        "message": (
                            f"The source layer has 0 features matching "
                            f"source_where ({source_where!r}); there was "
                            f"nothing to spatially join against."
                        ),
                    }
                ],
            }

        cls_polys = await self._paged_geojson_fetch(
            classification_url,
            where=classification_where,
            out_fields=classification_field,
            limit=self.SPANNING_CLASSIFICATION_LIMIT,
        )
        if not cls_polys:
            raise ToolInputError(
                f"classification_where ({classification_where!r}) "
                f"matched no polygons in the classification layer. "
                f"Verify with `query_data(item_id="
                f"'{classification_item_id}', limit=1)`."
            )
        # Detect cap-hit. The fetch tops out at SPANNING_CLASSIFICATION
        # _LIMIT -- anything >= 95% of cap probably means there are more
        # polygons than we sampled, and the spatial join is operating
        # on a partial view. Surface this so the model knows results
        # are not exhaustive.
        cls_cap_hit = (
            len(cls_polys) >= 0.95 * self.SPANNING_CLASSIFICATION_LIMIT
        )

        # Drop polygons whose classification value is NULL -- they would
        # falsely contribute "no value" as a distinct classification.
        valid_cls: List[Tuple[Any, Dict[str, Any]]] = []
        for p in cls_polys:
            val = (p.get("properties") or {}).get(classification_field)
            geom = p.get("geometry")
            if val is None or not geom:
                continue
            valid_cls.append((val, geom))
        if not valid_cls:
            raise ToolInputError(
                f"all classification polygons matching "
                f"classification_where have NULL "
                f"`{classification_field}` values or missing "
                f"geometry. Pick a different field or narrow "
                f"classification_where."
            )

        # Group polygons by their classification value so we send ONE
        # spatial query per distinct value instead of one per polygon.
        # For a typical zoning layer with 1,000 polygons but ~50 zones,
        # this collapses 1,000 upstream queries into 50 -- the original
        # implementation hit the Esri 6,000-req/min quota easily and
        # the silent failure path then masked the rate-limit error as
        # "attributes unavailable". One query per value is also more
        # honest semantically: we want distinct VALUES per source
        # feature, not distinct polygons.
        by_value: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for value, geom in valid_cls:
            by_value[value].append(geom)

        sem = asyncio.Semaphore(self.SPANNING_QUERY_CONCURRENCY)
        src_to_values: Dict[Any, set] = defaultdict(set)
        skipped_polys = 0
        rate_limited = False
        timed_out = False

        async def query_geometry(
            value: Any, esri_geom: Dict[str, Any]
        ) -> None:
            nonlocal skipped_polys, rate_limited, timed_out
            params = {
                "where": source_where,
                "geometry": json.dumps(
                    esri_geom, separators=(",", ":")
                ),
                "geometryType": "esriGeometryPolygon",
                "spatialRel": "esriSpatialRelIntersects",
                "inSR": "4326",
                "returnIdsOnly": "true",
                "f": "json",
            }
            async with sem:
                try:
                    resp = await self._send("post",
                        f"{source_url}/query", data=params
                    )
                    resp.raise_for_status()
                    data = resp.json()
                except UpstreamTimeoutError:
                    timed_out = True
                    skipped_polys += 1
                    return
                except Exception:
                    skipped_polys += 1
                    return
            if "error" in data:
                err_msg = (
                    data["error"].get("message", "") or ""
                ).lower()
                # Esri uses both phrasings depending on the tier.
                if (
                    "too many requests" in err_msg
                    or "quota exceeded" in err_msg
                ):
                    rate_limited = True
                skipped_polys += 1
                return
            for oid in data.get("objectIds") or []:
                src_to_values[oid].add(value)

        async def query_one_value(
            value: Any, geoms: List[Dict[str, Any]]
        ) -> None:
            """Combine all polygons for this value into one query
            when feasible; fall back to per-polygon if the combined
            geometry exceeds the filter caps."""
            nonlocal skipped_polys
            all_rings: List[Any] = []
            used = 0
            skipped_in_value = 0
            for geom in geoms:
                try:
                    esri = self._geojson_to_esri_polygon(geom)
                except ValueError:
                    skipped_in_value += 1
                    continue
                all_rings.extend(esri.get("rings") or [])
                used += 1
            if not all_rings:
                skipped_polys += skipped_in_value
                return
            # Concatenated rings from several polygons are not a
            # union -- overlapping/adjacent rings flip covered area
            # into holes (see _union_esri_rings). Dissolving also
            # shrinks the payload, so combined queries fit the caps
            # more often.
            if used > 1:
                unioned = self._union_esri_rings(all_rings)
                if unioned is not None:
                    all_rings = unioned
            coord_count = sum(
                len(r) for r in all_rings if isinstance(r, list)
            )
            if (
                len(all_rings) <= self.MAX_FILTER_RINGS
                and coord_count <= self.MAX_FILTER_COORDS
            ):
                # Combined fits -- one query for the whole value.
                combined = {
                    "rings": all_rings,
                    "spatialReference": {"wkid": 4326},
                }
                skipped_polys += skipped_in_value
                await query_geometry(value, combined)
                return
            # Combined too big -- fall back to per-polygon for this
            # value. Rare, but real for sprawling zone categories.
            skipped_polys += skipped_in_value
            for geom in geoms:
                try:
                    esri = self._geojson_to_esri_polygon(geom)
                except ValueError:
                    skipped_polys += 1
                    continue
                await query_geometry(value, esri)

        await asyncio.gather(
            *[
                query_one_value(v, gs)
                for v, gs in by_value.items()
            ]
        )

        qualifying = {
            oid: vals
            for oid, vals in src_to_values.items()
            if len(vals) >= min_distinct
        }

        # Histogram covers ALL source features that touched any
        # classification, not just qualifiers -- gives the model context
        # about the distribution before highlighting the cutoff.
        histogram = Counter(
            len(vals) for vals in src_to_values.values()
        )

        city = self.plugin_config.city_name
        lines = [
            f"## Source features spanning multiple "
            f"`{classification_field}` values",
            f"**City:** {city}",
            f"**Source:** {source_item_id} "
            f"({source_count:,} features matching source_where)",
            f"**Classification:** {classification_item_id}, field "
            f"`{classification_field}` "
            f"({len(valid_cls):,} polygon(s) with non-null value)",
            f"**Threshold:** features touching "
            f">= {min_distinct} distinct values qualify",
            f"**Qualifying:** {len(qualifying):,} feature(s)",
        ]
        # Structured twin of the "Skipped" line, so structuredContent-only
        # clients also learn the qualifying count may be an undercount.
        skip_caveats: List[Dict[str, Any]] = []
        if cls_cap_hit:
            skip_caveats.append(
                {
                    "code": "classification_cap_reached",
                    "limit": self.SPANNING_CLASSIFICATION_LIMIT,
                    "message": (
                        f"The classification layer fetch hit the "
                        f"{self.SPANNING_CLASSIFICATION_LIMIT:,}-polygon "
                        f"cap, so results are based on a partial "
                        f"sample and the qualifying count may be an "
                        f"undercount. Narrow classification_where, or "
                        f"use a layer with fewer, larger polygons."
                    ),
                }
            )
        if skipped_polys:
            skip_caveats.append(
                {
                    "code": "classification_polygons_skipped",
                    "count": skipped_polys,
                    "message": (
                        f"{skipped_polys} classification polygon(s) "
                        f"could not be queried ("
                        + (
                            "upstream rate limit"
                            if rate_limited
                            else f"ran out of the {TOOL_CALL_BUDGET_S:g}s"
                            " per-call time budget"
                            if timed_out
                            else "geometry too complex or upstream error"
                        )
                        + "), so qualifying counts may be an "
                        "undercount. Retry, or narrow "
                        "classification_where / source_where."
                    ),
                }
            )
            note = (
                "Coverage is partial; narrow "
                "`classification_where` to inspect them separately."
            )
            if rate_limited:
                note = (
                    "**Upstream rate-limited some queries** -- "
                    "results below are partial. Wait 60s and retry, "
                    "or narrow `classification_where` to reduce the "
                    "per-call query count."
                )
            elif timed_out:
                note = (
                    "**Some queries ran out of the "
                    f"{TOOL_CALL_BUDGET_S:g}s per-call time budget** -- "
                    "results below are partial (an undercount). Retry, "
                    "or narrow `classification_where` / `source_where`."
                )
            lines.append(
                f"**Skipped:** {skipped_polys} classification "
                f"polygon(s) -- too complex for spatial query payload "
                f"or upstream error. {note}"
            )
        if cls_cap_hit:
            lines.append(
                f"**Cap reached:** classification layer fetch "
                f"hit the {self.SPANNING_CLASSIFICATION_LIMIT:,}-"
                f"polygon cap -- results are based on a partial "
                f"sample. If qualifying count looks unexpectedly "
                f"low, the classification layer is probably at the "
                f"wrong grain (e.g., per-parcel instead of per-zone) "
                f"or has more polygons than the cap allows. Narrow "
                f"`classification_where` to a region or use a layer "
                f"with fewer, larger polygons."
            )
        lines.append("")

        if histogram:
            lines.append(
                "### Distribution of source features by distinct "
                "classification touch-count"
            )
            for count in sorted(histogram.keys()):
                n = histogram[count]
                marker = (
                    "  <- qualifying"
                    if count >= min_distinct
                    else ""
                )
                lines.append(
                    f"- touches {count} distinct value(s): "
                    f"{n:,} feature(s){marker}"
                )
            lines.append("")

        if not qualifying:
            lines.append(
                f"_No source features touch >= {min_distinct} "
                f"distinct `{classification_field}` values. "
                f"Try lowering `min_distinct`, broadening "
                f"`source_where`, or confirming the classification "
                f"layer has the expected boundary detail._"
            )
            return "\n".join(lines), {
                "query": {
                    "source_item_id": source_item_id,
                    "classification_item_id": classification_item_id,
                    "classification_field": classification_field,
                    "source_where": source_where,
                    "min_distinct": min_distinct,
                    "limit": limit,
                },
                "summary": {
                    "source_features": source_count,
                    "qualifying": 0,
                    "returned": 0,
                },
                "rows": [],
                "caveats": [
                    {
                        "code": "none_qualifying",
                        "count": 0,
                        "limit": min_distinct,
                        "message": (
                            f"No source feature touches >= "
                            f"{min_distinct} distinct "
                            f"{classification_field} values. Lower "
                            f"min_distinct, broaden source_where, or "
                            f"confirm the classification layer has the "
                            f"expected boundary detail."
                        ),
                    }
                ]
                + skip_caveats,
            }

        # Fetch attributes for the qualifying subset, capped at limit.
        # Sort by descending distinct-value count so the most-split
        # features show first -- they tend to be the most interesting.
        qualifying_oids = sorted(
            qualifying.keys(),
            key=lambda o: (-len(qualifying[o]), o),
        )[:limit]

        # POST (not GET) for the attribute fetch -- with limit=100, the
        # objectIds list pushes the URL past portal length limits when
        # combined with where + outFields, and the upstream silently
        # truncated to zero features. POST sends the same params in
        # the body and is unaffected.
        # The object-id field must come back even when the caller
        # narrowed out_fields: it is the only thing that ties a returned
        # row to its value set. Without it nothing joined, and every
        # row was then re-fetched one request at a time.
        fetch_fields = out_fields
        if (
            oid_field
            and fetch_fields != "*"
            and oid_field not in fetch_fields.split(",")
        ):
            fetch_fields = f"{fetch_fields},{oid_field}"
        attrs_resp = await self._send("post",
            f"{source_url}/query",
            data={
                "where": source_where,
                "objectIds": ",".join(map(str, qualifying_oids)),
                "outFields": fetch_fields,
                "returnGeometry": "false",
                "f": "json",
            },
        )
        attrs_resp.raise_for_status()
        attrs_data = attrs_resp.json()
        attrs_error: Optional[str] = None
        if "error" in attrs_data:
            err = attrs_data["error"]
            attrs_error = self._rewrite_arcgis_error(
                err.get("message", "Unknown error"),
                err.get("details", []),
                resource_id=source_item_id,
                has_out_fields=out_fields != "*",
                has_where=source_where != "1=1",
            )

        # Index by object id for the join with the value sets, under
        # the name the response or the layer declares, then the usual
        # names. `is not None`, not truthiness: object id 0 is real
        # (shapefile-backed layers number from 0).
        join_names = [
            n
            for n in dict.fromkeys(
                (
                    self._oid_field_name(attrs_data),
                    oid_field,
                    *self._OID_FALLBACK_NAMES,
                )
            )
            if n
        ]
        oid_names.update(join_names)
        features_by_oid: Dict[Any, Dict[str, Any]] = {}
        if not attrs_error:
            for f in attrs_data.get("features", []):
                attrs = f.get("attributes") or {}
                oid = next(
                    (
                        attrs[n]
                        for n in join_names
                        if attrs.get(n) is not None
                    ),
                    None,
                )
                if oid is not None:
                    features_by_oid[oid] = attrs

        # Per-OID fallback: if the bulk fetch failed (rate limit, URL
        # length, transient upstream error) we MUST get attributes
        # before rendering. Weak models report OBJECTIDs as parcel
        # numbers when shown raw, no matter how loud the warning. Per-
        # OID queries are cheap (1 record each) and survive rate
        # limiting that kills the bulk path. Bounded concurrency to
        # stay polite to the upstream.
        fallback_used = False
        fallback_recovered = 0
        if not features_by_oid and qualifying_oids:
            fallback_used = True
            fallback_sem = asyncio.Semaphore(3)

            async def fetch_one_oid(oid: Any) -> None:
                # AND with source_where so the fallback honors the
                # same predicate as the bulk path.
                oid_col = oid_field or "OBJECTID"
                if source_where and source_where != "1=1":
                    where_one = f"({source_where}) AND {oid_col}={oid}"
                else:
                    where_one = f"{oid_col}={oid}"
                where_one = WhereValidator.validate(where_one)
                async with fallback_sem:
                    try:
                        r = await self._send("get",
                            f"{source_url}/query",
                            params={
                                "where": where_one,
                                "outFields": out_fields,
                                "returnGeometry": "false",
                                "f": "json",
                            },
                        )
                        r.raise_for_status()
                        d = r.json()
                    except Exception:
                        return
                    if "error" in d:
                        return
                    feats = d.get("features") or []
                    if feats:
                        attrs = feats[0].get("attributes") or {}
                        if attrs:
                            features_by_oid[oid] = attrs

            await asyncio.gather(
                *[fetch_one_oid(o) for o in qualifying_oids]
            )
            fallback_recovered = len(features_by_oid)

        showing = len(qualifying_oids)
        total = len(qualifying)
        lines.append(
            f"### Qualifying features (showing {showing:,} of "
            f"{total:,}, sorted by distinct-value count desc)"
        )
        if showing < total:
            lines.append(
                f"_Truncated to limit={limit}. Increase `limit` to "
                f"see more._"
            )
        if fallback_used and fallback_recovered:
            lines.append(
                f"_Bulk attribute fetch failed (likely upstream "
                f"rate limit); recovered {fallback_recovered:,} of "
                f"{showing:,} via per-OID fallback._"
            )

        # Detect the natural-ID field across the loaded attributes so
        # we can promote it to the lead position of each entry. Pick
        # the first feature's natural ID -- schemas are consistent
        # within a layer, so any one feature's pick applies to all.
        natural_id_field: Optional[str] = None
        for attrs in features_by_oid.values():
            pick = self._pick_natural_id(attrs)
            if pick:
                natural_id_field = pick[0]
                break
        if natural_id_field:
            lines.append("")
            lines.append(
                f"> **The user-facing identifier in this layer is "
                f"`{natural_id_field}`** -- that is the value to "
                f"REPORT TO THE USER for each feature below. "
                f"`OBJECTID` is an internal layer row ID; do NOT "
                f"report it as a parcel number, address, or name."
            )
        lines.append("")

        # If even the per-OID fallback couldn't load attributes, we
        # must NOT surface raw OBJECTIDs -- a model can report them as
        # parcel numbers no matter how loud the warning sits next to
        # them, so we don't hand over the raw IDs at all. Return a clean
        # refusal with concrete recovery steps for the model to give the
        # user, instead of dangerous raw data.
        if not features_by_oid and qualifying_oids:
            reason = (
                attrs_error
                or "the upstream returned 0 records for the OIDs "
                "sent, AND the per-OID fallback also failed. Most "
                "common cause is a sustained rate-limit from the "
                "Esri portal."
            )
            lines.append(
                "> **CANNOT RETURN PARCEL NUMBERS RIGHT NOW.** "
                f"The spanning analysis identified {total:,} "
                "qualifying features, but the upstream layer would "
                "not return their attributes (the user-facing "
                "identifiers).\n"
                f">\n"
                f"> Reason: {reason}\n"
                f">\n"
                f"> **DO NOT report internal OBJECTIDs to the user "
                f"as parcel numbers** -- they are not parcel numbers, "
                f"addresses, or any identifier the user would "
                f"recognize. Tell the user the upstream is "
                f"temporarily rate-limiting bulk lookups and "
                f"suggest one of: (1) retry in 60 seconds, (2) "
                f"narrow the request to a smaller area via "
                f"`source_where`, or (3) ask for fewer results "
                f"(`limit=5`)."
            )
            return "\n".join(lines), {
                "query": {
                    "source_item_id": source_item_id,
                    "classification_item_id": classification_item_id,
                    "classification_field": classification_field,
                    "source_where": source_where,
                    "min_distinct": min_distinct,
                    "limit": limit,
                },
                "summary": {
                    "source_features": source_count,
                    "qualifying": len(qualifying),
                    "returned": 0,
                },
                # Deliberately empty: the qualifying OBJECTIDs are known
                # but their real attributes are not, and OBJECTIDs are
                # internal row numbers. Emitting them as rows invites a
                # caller to report them as parcel numbers.
                "rows": [],
                "caveats": [
                    {
                        "code": "attribute_lookup_failed",
                        "count": len(qualifying),
                        "message": (
                            f"Found {len(qualifying)} qualifying "
                            f"feature(s) but could not fetch their "
                            f"attributes ({reason}). Internal OBJECTIDs "
                            f"are withheld: they are not parcel numbers "
                            f"or any identifier a user would recognize. "
                            f"Retry in 60s, narrow source_where, or ask "
                            f"for fewer results."
                        ),
                    }
                ],
            }

        for oid in qualifying_oids:
            attrs = features_by_oid.get(oid)
            vals = sorted(str(v) for v in qualifying[oid])
            if attrs is None:
                # Per-row miss in an otherwise-successful response.
                # Still surface it honestly rather than silently.
                lines.append(
                    f"**OBJECTID {oid}** _(attributes not returned "
                    f"by upstream -- internal row ID only, NOT a "
                    f"user-facing parcel number)_ -- touches "
                    f"{len(vals)} value(s): "
                    + ", ".join(f"`{v}`" for v in vals)
                )
                lines.append("")
                continue
            # Lead with the user-facing identifier when one is
            # detectable; demote OBJECTID to a parenthetical so the
            # model never reports it as the answer.
            natural = self._pick_natural_id(attrs)
            if natural:
                nat_field, nat_value = natural
                lines.append(
                    f"**`{nat_value}`** ({nat_field}; internal "
                    f"OBJECTID {oid}) -- touches {len(vals)} "
                    f"value(s): "
                    + ", ".join(f"`{v}`" for v in vals)
                )
            else:
                lines.append(
                    f"**OBJECTID {oid}** -- touches {len(vals)} "
                    f"value(s): "
                    + ", ".join(f"`{v}`" for v in vals)
                )
            for k, v in attrs.items():
                if k in oid_names:
                    continue
                lines.append(f"  {k}: {v}")
            lines.append("")

        lines += [
            "---",
            "_Values shown use the layer's native format -- "
            "case-sensitive. Pass them verbatim if you filter on "
            "them next._",
        ]

        span_caveats: List[Dict[str, Any]] = list(skip_caveats)
        if len(qualifying) > showing:
            span_caveats.append(
                {
                    "code": "results_capped",
                    "count": len(qualifying),
                    "limit": limit,
                    "message": (
                        f"{len(qualifying):,} features qualify but only "
                        f"{showing} are listed (limit={limit}), sorted "
                        f"by descending distinct-value count."
                    ),
                }
            )
        if fallback_used:
            span_caveats.append(
                {
                    "code": "attribute_fallback_used",
                    "count": fallback_recovered,
                    "message": (
                        f"The bulk attribute fetch failed; attributes "
                        f"were recovered one-by-one for "
                        f"{fallback_recovered} of {showing} feature(s)."
                    ),
                }
            )

        rows: List[Dict[str, Any]] = []
        for oid in qualifying_oids:
            attrs = features_by_oid.get(oid)
            rows.append(
                {
                    "objectid": oid,
                    "distinct_count": len(qualifying[oid]),
                    # Native format, case-sensitive: these are what a
                    # follow-up WHERE clause must match verbatim.
                    "values": sorted(str(v) for v in qualifying[oid]),
                    "attributes": (
                        {
                            k: v
                            for k, v in attrs.items()
                            if k not in oid_names
                        }
                        if attrs
                        else None
                    ),
                }
            )

        return "\n".join(lines), {
            "query": {
                "source_item_id": source_item_id,
                "classification_item_id": classification_item_id,
                "classification_field": classification_field,
                "source_where": source_where,
                "min_distinct": min_distinct,
                "limit": limit,
            },
            "summary": {
                "source_features": source_count,
                "qualifying": len(qualifying),
                "returned": len(rows),
            },
            "rows": rows,
            "caveats": span_caveats,
        }

    @staticmethod
    def _int_arg(
        args: Dict[str, Any],
        name: str,
        default: Any = None,
        minimum: Optional[int] = None,
    ) -> int:
        """Coerce a caller-supplied argument to int, or explain why not.

        A bare int() raises "invalid literal for int() with base 10:
        'abc'" -- which reads as a server fault in the logs and tells the
        caller nothing about which argument was wrong.

        An explicit null means "use the default". Every integer argument
        of these tools is a count or a cap, so the floor defaults to 1
        (0 for ``layer_index``): a zero or negative limit used to reach
        the portal and come back as a false "nothing found".
        """
        raw = args.get(name)
        if raw is None:
            raw = default
        try:
            if isinstance(raw, bool):
                raise TypeError("bool is not an integer argument")
            value = int(raw)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ToolInputError(
                f"{name} must be an integer (got {raw!r})"
            ) from exc
        floor = minimum
        if floor is None:
            floor = 0 if name == "layer_index" else 1
        if value < floor:
            raise ToolInputError(
                f"{name} must be at least {floor} (got {value})"
            )
        return value

    @staticmethod
    def _float_arg(
        args: Dict[str, Any], name: str, default: Any = None
    ) -> float:
        """Coerce a caller-supplied argument to float, or explain why not.

        NaN and infinity are refused: they pass every ``< 0`` guard,
        and a NaN in a result is not valid JSON.
        """
        raw = args.get(name)
        if raw is None:
            raw = default
        try:
            value = float(raw)
        except (TypeError, ValueError) as exc:
            raise ToolInputError(
                f"{name} must be a number (got {raw!r})"
            ) from exc
        if not math.isfinite(value):
            raise ToolInputError(
                f"{name} must be a finite number (got {raw!r})"
            )
        return value

    # ── Tool definitions ──────────────────────────────────────────────────

    # ── Structured output ───────────────────────────────────────────
    #
    # Declaring an `outputSchema` is a commitment, not a hint: the MCP
    # spec says servers MUST return structured results that conform to
    # it, and clients may validate and reject. Every optional field and
    # edge case below is therefore deliberate -- see the schema tests.
    #
    # Emitted inline in both schemas rather than via $ref, so each is
    # self-contained and no client has to resolve references.
    _CAVEATS_SCHEMA = {
        "type": "array",
        "description": (
            "Machine-readable qualifications on the result. Present so a "
            "caller can branch on `code` instead of parsing the prose "
            "rendering. An empty array means the result is unqualified."
        ),
        "items": {
            "type": "object",
            "required": ["code", "message"],
            "properties": {
                "code": {
                    "type": "string",
                    "description": "Stable identifier for the caveat.",
                },
                "message": {
                    "type": "string",
                    "description": "The human-readable form of this caveat.",
                },
                "count": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Affected feature/target count, if any.",
                },
                "limit": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "The cap that was reached, if any.",
                },
                "candidates": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": (
                        "Disambiguation records (e.g. the parcel "
                        "matches behind a parcel_ambiguous caveat), "
                        "so the caller can pick one without parsing "
                        "the message."
                    ),
                },
            },
        },
    }

    # Human-readable display names. The wire `name` is prefixed
    # (`anchorage_gis__spatial_query_polygon`) because it must be a stable,
    # collision-free identifier; that string reads poorly in a client's tool
    # picker. Clients resolve display names as title -> annotations.title ->
    # name, so these are what a user actually sees. Keyed by the UNPREFIXED
    # tool name, matching the ToolDefinition entries below.
    # Shared by the four record-listing tools -- query_data,
    # spatial_query_point, spatial_query_polygon and filter_by_polygon --
    # because they all render through _format_query_results.
    QUERY_RESULT_OUTPUT_SCHEMA = {
        "type": "object",
        "required": ["query", "summary", "rows", "caveats"],
        "additionalProperties": False,
        "properties": {
            "query": {
                "type": "object",
                "description": (
                    "What was asked. Tools that add their own context "
                    "(filter_by_polygon's container layer) put it here, "
                    "so extra properties are allowed."
                ),
                "properties": {
                    "item_id": {"type": ["string", "null"]},
                    "service_url": {"type": ["string", "null"]},
                    "where": {"type": ["string", "null"]},
                    "out_fields": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 0},
                    "geometry_type": {"type": ["string", "null"]},
                },
            },
            "summary": {
                "type": "object",
                "required": ["returned", "total_count", "truncated"],
                "properties": {
                    "returned": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Records in `rows`.",
                    },
                    "total_count": {
                        "type": ["integer", "null"],
                        "minimum": 0,
                        "description": (
                            "Records matching the query -- the "
                            "answer to 'how many?'. The spatial tools "
                            "report it only when the result was cut "
                            "at `limit`; null there with `truncated` "
                            "false means `returned` is the whole set."
                        ),
                    },
                    "truncated": {
                        "type": "boolean",
                        "description": (
                            "True when more records match than were "
                            "returned (total_count exceeds returned, or "
                            "a limit_reached caveat when the total is "
                            "unknown), so `rows` is a sample and must "
                            "not be used for counts or percentages."
                        ),
                    },
                    "complete": {
                        "type": "boolean",
                        "description": (
                            "True when `rows` holds EVERY matching "
                            "record (returned == total_count): no "
                            "paging or follow-up call is needed."
                        ),
                    },
                },
            },
            "rows": {
                "type": "array",
                "description": (
                    "Raw field values in the layer's own types: dates "
                    "are epoch milliseconds and coded fields carry their "
                    "stored codes, NOT the decoded labels the prose "
                    "shows. Use `coded_domains` to decode. Any geometry "
                    "is under `__geometry__` as full GeoJSON -- not the "
                    "character-clipped form the text renders."
                ),
                "items": {"type": "object"},
            },
            "coded_domains": {
                "type": "object",
                "description": (
                    "field -> {stored code: label}, present only for "
                    "layers that declare coded-value domains."
                ),
                "additionalProperties": {
                    "type": "object",
                    "additionalProperties": {"type": "string"},
                },
            },
            "caveats": _CAVEATS_SCHEMA,
        },
    }

    DISTINCT_VALUES_OUTPUT_SCHEMA = {
        "type": "object",
        "required": ["query", "summary", "values", "caveats"],
        "additionalProperties": False,
        "properties": {
            "query": {
                "type": "object",
                "required": ["item_id", "field", "limit"],
                "properties": {
                    "item_id": {"type": "string"},
                    "field": {"type": "string"},
                    "where": {"type": ["string", "null"]},
                    "like": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 0},
                },
            },
            "summary": {
                "type": "object",
                "required": ["returned", "truncated"],
                "properties": {
                    "returned": {"type": "integer", "minimum": 0},
                    "truncated": {"type": "boolean"},
                },
            },
            "values": {
                "type": "array",
                "description": (
                    "The EXACT values the layer stores, in their native "
                    "types, deliberately not normalized -- a WHERE "
                    "clause must match them verbatim and matching is "
                    "case-sensitive."
                ),
                "items": {
                    "type": ["string", "number", "boolean", "null"]
                },
            },
            "caveats": _CAVEATS_SCHEMA,
        },
    }

    SPANNING_OUTPUT_SCHEMA = {
        "type": "object",
        "required": ["query", "summary", "rows", "caveats"],
        "additionalProperties": False,
        "properties": {
            "query": {
                "type": "object",
                "required": [
                    "source_item_id",
                    "classification_item_id",
                    "classification_field",
                    "min_distinct",
                    "limit",
                ],
                "properties": {
                    "source_item_id": {"type": "string"},
                    "classification_item_id": {"type": "string"},
                    "classification_field": {"type": "string"},
                    "source_where": {"type": ["string", "null"]},
                    "min_distinct": {"type": "integer", "minimum": 0},
                    "limit": {"type": "integer", "minimum": 0},
                },
            },
            "summary": {
                "type": "object",
                "required": ["source_features", "qualifying", "returned"],
                "properties": {
                    "source_features": {"type": "integer", "minimum": 0},
                    "qualifying": {
                        "type": "integer",
                        "minimum": 0,
                        "description": (
                            "Features meeting min_distinct. May exceed "
                            "`returned`, which is capped by limit."
                        ),
                    },
                    "returned": {"type": "integer", "minimum": 0},
                },
            },
            "rows": {
                "type": "array",
                "description": (
                    "Sorted by descending distinct_count. Empty when "
                    "attributes could not be fetched -- OBJECTIDs are "
                    "withheld rather than emitted, because they are "
                    "internal row numbers a caller could mistake for "
                    "parcel identifiers."
                ),
                "items": {
                    "type": "object",
                    "required": ["objectid", "distinct_count", "values"],
                    "properties": {
                        "objectid": {"type": ["integer", "string"]},
                        "distinct_count": {
                            "type": "integer",
                            "minimum": 0,
                        },
                        "values": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "attributes": {
                            "type": ["object", "null"],
                            "description": (
                                "null when this row's attributes could "
                                "not be recovered."
                            ),
                        },
                    },
                },
            },
            "caveats": _CAVEATS_SCHEMA,
        },
    }

    AGGREGATE_OUTPUT_SCHEMA = {
        "type": "object",
        "required": ["query", "summary", "rows", "caveats"],
        "additionalProperties": False,
        "properties": {
            "query": {
                "type": "object",
                "description": "The resolved parameters this run used.",
                "required": [
                    "source_item_id",
                    "aggregation_item_id",
                    "group_by_field",
                    "centroid_mode",
                    "overlap_policy",
                    "sum_fields",
                    "count",
                    "buffer",
                ],
                "properties": {
                    "source_item_id": {"type": "string"},
                    "aggregation_item_id": {"type": "string"},
                    "group_by_field": {"type": "string"},
                    "source_geometry_type": {"type": "string"},
                    "centroid_mode": {
                        "enum": ["auto", "centroid", "representative_point"]
                    },
                    "overlap_policy": {
                        "enum": ["first_match", "all_matches", "largest"]
                    },
                    "sum_fields": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "count": {"type": "boolean"},
                    "buffer": {
                        "type": ["object", "null"],
                        "description": (
                            "null when no proximity buffer was applied."
                        ),
                        "required": ["distance", "units", "meters"],
                        "properties": {
                            "distance": {"type": "number", "minimum": 0},
                            "units": {"type": "string"},
                            "meters": {"type": "number", "minimum": 0},
                        },
                    },
                },
            },
            "summary": {
                "type": "object",
                "required": ["source_features", "buckets", "unmatched"],
                "properties": {
                    "source_features": {"type": "integer", "minimum": 0},
                    "buckets": {"type": "integer", "minimum": 0},
                    "unmatched": {"type": "integer", "minimum": 0},
                },
            },
            "rows": {
                "type": "array",
                "description": (
                    "One entry per bucket, sorted by count descending -- "
                    "the same order as the rendered table. Empty when no "
                    "source feature matched any aggregation polygon."
                ),
                "items": {
                    "type": "object",
                    "required": ["group", "sums"],
                    "properties": {
                        "group": {
                            "type": ["string", "number", "boolean", "null"],
                            "description": (
                                "The group field's raw value. NOT always a "
                                "string -- it is whatever the aggregation "
                                "layer stores."
                            ),
                        },
                        "count": {
                            "type": "integer",
                            "minimum": 0,
                            "description": (
                                "Omitted when the caller passed count=false."
                            ),
                        },
                        "small_sample": {
                            "type": "boolean",
                            "description": (
                                "True when count is below the small-sample "
                                "threshold; percentages over this bucket "
                                "are weak."
                            ),
                        },
                        "sums": {
                            "type": "object",
                            "description": (
                                "Summed value per requested sum_field. "
                                "Empty when none were requested. Nested "
                                "because the field names are caller-chosen."
                            ),
                            "additionalProperties": {"type": "number"},
                        },
                    },
                },
            },
            "caveats": _CAVEATS_SCHEMA,
        },
    }

    COVERAGE_OUTPUT_SCHEMA = {
        "type": "object",
        "required": ["query", "summary", "rows", "caveats"],
        "additionalProperties": False,
        "properties": {
            "query": {
                "type": "object",
                "required": [
                    "target_item_id",
                    "overlay_item_id",
                    "target_id_field",
                    "target_area_field",
                    "overlay_area_field",
                    "band",
                ],
                "properties": {
                    "target_item_id": {"type": "string"},
                    "overlay_item_id": {"type": "string"},
                    "target_id_field": {"type": "string"},
                    "target_area_field": {"type": "string"},
                    "overlay_area_field": {"type": "string"},
                    "target_where": {"type": "string"},
                    "band": {
                        "type": "object",
                        "description": (
                            "Either bound may be null; both null means no "
                            "coverage filter was applied."
                        ),
                        "required": [
                            "min_coverage_pct",
                            "max_coverage_pct",
                        ],
                        "properties": {
                            "min_coverage_pct": {
                                "type": ["number", "null"],
                                "minimum": 0,
                            },
                            "max_coverage_pct": {
                                "type": ["number", "null"],
                                "minimum": 0,
                            },
                        },
                    },
                },
            },
            "summary": {
                "type": "object",
                "required": [
                    "targets_measured",
                    "in_band",
                    "out_of_band",
                    "zero_coverage",
                    "skipped",
                ],
                "properties": {
                    "targets_measured": {"type": "integer", "minimum": 0},
                    "in_band": {"type": "integer", "minimum": 0},
                    "out_of_band": {"type": "integer", "minimum": 0},
                    "zero_coverage": {
                        "type": "integer",
                        "minimum": 0,
                        "description": (
                            "Targets at exactly 0%. A high share usually "
                            "means centroid-assignment artefacts in "
                            "attached housing, not genuine vacancy."
                        ),
                    },
                    "skipped": {"type": "integer", "minimum": 0},
                    "distinct_target_ids": {
                        "type": "integer",
                        "minimum": 0,
                        "description": (
                            "Distinct target_id_field values. Lower than "
                            "targets_measured means rows are polygons, not "
                            "unique parcels."
                        ),
                    },
                    "areas_corrected_to_sqft": {
                        "type": "boolean",
                        "description": (
                            "Whether *_sqft could be computed; false means "
                            "only the raw projected areas are available."
                        ),
                    },
                },
            },
            "rows": {
                "type": "array",
                "description": (
                    "EVERY in-band target, ascending by coverage. The "
                    "rendered table shows only the lowest-coverage "
                    "handful; this carries the full set."
                ),
                "items": {
                    "type": "object",
                    "required": [
                        "id",
                        "coverage_pct",
                        "covered_area",
                        "target_area",
                    ],
                    "properties": {
                        "id": {"type": ["string", "number", "null"]},
                        "coverage_pct": {
                            "type": "number",
                            "minimum": 0,
                            "description": (
                                "Deliberately has no maximum: overlay areas "
                                "are counted whole rather than clipped to "
                                "the target, so dense lots legitimately "
                                "exceed 100."
                            ),
                        },
                        "covered_area": {
                            "type": "number",
                            "minimum": 0,
                            "description": (
                                "RAW stored area in the layer's own "
                                "projection -- Web Mercator on ArcGIS Online "
                                "hosted layers, i.e. NOT square feet or metres. "
                                "Prefer covered_sqft."
                            ),
                        },
                        "target_area": {
                            "type": "number",
                            "exclusiveMinimum": 0,
                            "description": (
                                "Raw stored area; see covered_area. Prefer "
                                "target_sqft."
                            ),
                        },
                        "covered_sqft": {
                            "type": ["number", "null"],
                            "minimum": 0,
                            "description": (
                                "True square feet, corrected for the "
                                "projection at this feature's latitude. "
                                "null when the layer's projection is not "
                                "one we can correct."
                            ),
                        },
                        "target_sqft": {
                            "type": ["number", "null"],
                            "minimum": 0,
                            "description": "True square feet; see covered_sqft.",
                        },
                    },
                },
            },
            "caveats": _CAVEATS_SCHEMA,
        },
    }

    TOOL_TITLES = {
        "find_gis_content": "Find GIS Content",
        "browse_gallery": "Browse Map Gallery",
        "search_spatial_layers": "Search Spatial Layers",
        "get_item_details": "Get Item Details",
        "get_layer_schema": "Get Layer Schema",
        "get_distinct_values": "List Distinct Field Values",
        "search_layers_by_field": "Search Layers by Field",
        "query_data": "Query Layer Data",
        "spatial_query_point": "Query by Point",
        "spatial_query_polygon": "Query by Area or Buffer",
        "aggregate_by_polygon": "Aggregate by Area",
        "coverage_by_polygon": "Coverage by Area",
        "filter_by_polygon": "Filter by Area",
        "find_features_spanning_classifications": (
            "Find Features Spanning Classifications"
        ),
    }

    # ── Discovery / metadata tools ──────────────────────────────────
    #
    # These used to return prose only. Claude Code and claude.ai show a
    # tool's structuredContent INSTEAD of its text once one is present,
    # so anything a model needs from these tools -- above all whether
    # an item is queryable, and the multiple-layers ambiguity warning --
    # has to live in the structured half, not just the markdown.

    # A raw attribute value: layers store strings, numbers, booleans
    # and nulls, and a schema that pinned one type would be violated by
    # the next layer.
    _SCALAR_TYPES = ["string", "number", "boolean", "null"]

    _ITEM_RECORD_SCHEMA = {
        "type": "object",
        "required": ["item_id", "title", "type", "queryable", "portal_url"],
        "properties": {
            "item_id": {"type": "string"},
            "title": {"type": "string"},
            "type": {
                "type": "string",
                "description": "Portal item type, e.g. 'Feature Service'.",
            },
            "queryable": {
                "type": "boolean",
                "description": (
                    "True for Feature/Map Services, Feature Layers and "
                    "Tables -- the items query_data, get_layer_schema "
                    "and the spatial_* tools accept. False for web "
                    "maps, apps, dashboards and downloadable files, "
                    "which must not be passed to query_data."
                ),
            },
            "snippet": {"type": ["string", "null"]},
            "tags": {"type": "array", "items": {"type": "string"}},
            "url": {
                "type": ["string", "null"],
                "description": (
                    "The service URL for a layer, or the app URL for "
                    "a viewer; null when the item has none."
                ),
            },
            "portal_url": {
                "type": "string",
                "description": "The item's page on the ArcGIS portal.",
            },
            "source": {
                "type": "string",
                "enum": ["gallery", "layers"],
                "description": (
                    "find_gis_content only: which search found it -- "
                    "the curated public gallery or the org's layers."
                ),
            },
        },
    }

    # Shared by find_gis_content, browse_gallery and
    # search_spatial_layers: all three are portal item searches.
    ITEM_SEARCH_OUTPUT_SCHEMA = {
        "type": "object",
        "required": ["query", "summary", "items", "caveats"],
        "additionalProperties": False,
        "properties": {
            "query": {
                "type": "object",
                "properties": {
                    "text": {"type": ["string", "null"]},
                    "layer_type": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 0},
                },
            },
            "summary": {
                "type": "object",
                "required": ["returned", "queryable"],
                "properties": {
                    "returned": {"type": "integer", "minimum": 0},
                    "queryable": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Items with queryable=true.",
                    },
                },
            },
            "items": {"type": "array", "items": _ITEM_RECORD_SCHEMA},
            "caveats": _CAVEATS_SCHEMA,
        },
    }

    ITEM_DETAILS_OUTPUT_SCHEMA = {
        "type": "object",
        "required": ["item", "caveats"],
        "additionalProperties": False,
        "properties": {
            "item": {
                "type": "object",
                "required": [
                    "item_id", "title", "type", "queryable", "portal_url",
                ],
                "properties": {
                    **_ITEM_RECORD_SCHEMA["properties"],
                    "owner": {"type": ["string", "null"]},
                    "access": {"type": ["string", "null"]},
                    "description": {
                        "type": ["string", "null"],
                        "description": "Often HTML, as the portal stores it.",
                    },
                    "categories": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "num_views": {"type": ["integer", "null"]},
                    "created": {
                        "type": ["string", "null"],
                        "description": "UTC date, YYYY-MM-DD.",
                    },
                    "modified": {
                        "type": ["string", "null"],
                        "description": "UTC date, YYYY-MM-DD.",
                    },
                    "extent": {
                        "type": ["object", "null"],
                        "description": "WGS84 bounding box, degrees.",
                        "properties": {
                            "west": {"type": "number"},
                            "south": {"type": "number"},
                            "east": {"type": "number"},
                            "north": {"type": "number"},
                        },
                    },
                },
            },
            "caveats": _CAVEATS_SCHEMA,
        },
    }

    _FIELD_RECORD_SCHEMA = {
        "type": "object",
        "required": ["name", "alias", "type"],
        "properties": {
            "name": {
                "type": "string",
                "description": (
                    "The CASE-SENSITIVE name to use in WHERE clauses "
                    "and out_fields -- not the alias."
                ),
            },
            "alias": {"type": "string"},
            "type": {
                "type": "string",
                "description": (
                    "Esri field type without the esriFieldType prefix, "
                    "e.g. 'String', 'Integer', 'Date'."
                ),
            },
            "coded_values": {
                "type": "array",
                "description": (
                    "Present only for coded-value domain fields: the "
                    "stored `code` (what a WHERE clause compares "
                    "against) and its display `name`."
                ),
                "items": {
                    "type": "object",
                    "required": ["code", "name"],
                    "properties": {
                        "code": {"type": _SCALAR_TYPES},
                        "name": {"type": ["string", "null"]},
                    },
                },
            },
        },
    }

    LAYER_SCHEMA_OUTPUT_SCHEMA = {
        "type": "object",
        "required": ["layer", "fields", "caveats"],
        "additionalProperties": False,
        "properties": {
            "layer": {
                "type": "object",
                "required": ["name", "service_url"],
                "properties": {
                    "item_id": {"type": ["string", "null"]},
                    "title": {"type": ["string", "null"]},
                    "name": {"type": "string"},
                    "geometry_type": {
                        "type": ["string", "null"],
                        "description": "null for a non-spatial table.",
                    },
                    "max_record_count": {"type": ["integer", "null"]},
                    "service_url": {"type": "string"},
                },
            },
            "keyword": {
                "type": ["string", "null"],
                "description": (
                    "The keyword `fields` was filtered by, if any -- "
                    "a filtered list is not the whole schema."
                ),
            },
            "fields": {"type": "array", "items": _FIELD_RECORD_SCHEMA},
            "caveats": _CAVEATS_SCHEMA,
        },
    }

    FIELD_SEARCH_OUTPUT_SCHEMA = {
        "type": "object",
        "required": ["query", "summary", "matches", "caveats"],
        "additionalProperties": False,
        "properties": {
            "query": {
                "type": "object",
                "properties": {
                    "field_keyword": {"type": "string"},
                    "service_keyword": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 0},
                },
            },
            "summary": {
                "type": "object",
                "required": [
                    "services_inspected",
                    "layers_matched",
                    "services_failed",
                    "layers_failed",
                ],
                "properties": {
                    "services_inspected": {"type": "integer", "minimum": 0},
                    "layers_matched": {"type": "integer", "minimum": 0},
                    "services_failed": {
                        "type": "integer",
                        "minimum": 0,
                        "description": (
                            "Services whose metadata could not be read, "
                            "so they were NOT checked. Non-zero means "
                            "an empty `matches` is not a confirmed "
                            "absence."
                        ),
                    },
                    "layers_failed": {"type": "integer", "minimum": 0},
                },
            },
            "inspected": {
                "type": "array",
                "description": "Every candidate service that was checked.",
                "items": {
                    "type": "object",
                    "required": ["item_id", "title"],
                    "properties": {
                        "item_id": {"type": "string"},
                        "title": {"type": "string"},
                    },
                },
            },
            "matches": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": [
                        "item_id",
                        "title",
                        "layer_name",
                        "layer_index",
                        "matching_fields",
                    ],
                    "properties": {
                        "item_id": {"type": "string"},
                        "title": {"type": "string"},
                        "layer_name": {"type": "string"},
                        "layer_index": {"type": "integer"},
                        "portal_url": {"type": "string"},
                        "matching_fields": {
                            "type": "array",
                            "items": _FIELD_RECORD_SCHEMA,
                        },
                    },
                },
            },
            "caveats": _CAVEATS_SCHEMA,
        },
    }

    # Every tool declares an outputSchema and returns structured_content
    # that conforms to it on every success path; the pairing is tested
    # (tests/test_structured_output.py), so adding a tool here without
    # also returning structured_content fails the suite.
    TOOL_OUTPUT_SCHEMAS = {
        "find_gis_content": ITEM_SEARCH_OUTPUT_SCHEMA,
        "browse_gallery": ITEM_SEARCH_OUTPUT_SCHEMA,
        "search_spatial_layers": ITEM_SEARCH_OUTPUT_SCHEMA,
        "get_item_details": ITEM_DETAILS_OUTPUT_SCHEMA,
        "get_layer_schema": LAYER_SCHEMA_OUTPUT_SCHEMA,
        "search_layers_by_field": FIELD_SEARCH_OUTPUT_SCHEMA,
        "aggregate_by_polygon": AGGREGATE_OUTPUT_SCHEMA,
        "coverage_by_polygon": COVERAGE_OUTPUT_SCHEMA,
        # All four render through _format_query_results, so they share
        # one schema.
        "query_data": QUERY_RESULT_OUTPUT_SCHEMA,
        "spatial_query_point": QUERY_RESULT_OUTPUT_SCHEMA,
        "spatial_query_polygon": QUERY_RESULT_OUTPUT_SCHEMA,
        "filter_by_polygon": QUERY_RESULT_OUTPUT_SCHEMA,
        "get_distinct_values": DISTINCT_VALUES_OUTPUT_SCHEMA,
        "find_features_spanning_classifications": SPANNING_OUTPUT_SCHEMA,
    }

    def get_tools(self) -> List[ToolDefinition]:
        tools = self._tool_definitions()
        # Every tool in this plugin is read-only and reaches out to external
        # ArcGIS services, so advertise the MCP safety hints uniformly. This
        # lets clients (e.g. M365 Copilot) treat them as safe and skip
        # per-call confirmation prompts.
        #
        # Deliberately NOT set: `idempotentHint`. The MCP schema documents it
        # as "meaningful only when readOnlyHint == false", so on a read-only
        # tool it would carry no information.
        for tool in tools:
            if tool.annotations is None:
                tool.annotations = {"readOnlyHint": True, "openWorldHint": True}
            if tool.title is None:
                tool.title = self.TOOL_TITLES.get(tool.name)
            if tool.output_schema is None:
                tool.output_schema = self.TOOL_OUTPUT_SCHEMAS.get(tool.name)
        return tools

    def _tool_definitions(self) -> List[ToolDefinition]:
        """The tools, before titles / annotations / output schemas are
        attached by get_tools. A subclass extends the list here."""
        city = (
            self.plugin_config.city_name if self.plugin_config else "Unknown"
        )
        tools = [
            ToolDefinition(
                name="find_gis_content",
                description=(
                    f"START HERE for any {city} GIS question. Searches "
                    f"the GIS portal for maps, apps, and datasets on a "
                    f"topic (e.g. 'flood zones', 'trails', 'parks'). "
                    f"Returns items each with an `id`; follow up with "
                    f"`query_data(item_id, limit=1)` for counts, "
                    f"`query_data(item_id, where=..., limit=N)` for "
                    f"listings, or `get_layer_schema(item_id)` to see "
                    f"real field names before writing a WHERE clause."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": (
                                "Topic to search for. REQUIRED -- extract "
                                "from the user's question. Use the "
                                "simplest 1-2 word form: 'parks' (not "
                                "'park parcels'), 'flood' (not 'flood "
                                "zone boundaries'), 'trails', 'zoning', "
                                "'schools'. If a multi-word topic returns "
                                "nothing, retry with the most distinctive "
                                "single word."
                            ),
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Max results per source (default 8).",
                            "default": 8,
                        },
                    },
                    "required": ["topic"],
                },
            ),
            ToolDefinition(
                name="browse_gallery",
                description=(
                    f"Browse or search {city}'s curated public GIS "
                    f"gallery -- interactive maps, dashboards, apps, "
                    f"and StoryMaps. Optionally filter by keyword.\n\n"
                    f"NOTE: gallery items are VIEWERS (Web Maps, "
                    f"Dashboards, Apps), NOT queryable data. Use this "
                    f"tool only when the user wants to *visit a "
                    f"viewer* -- e.g. 'show me the parks app', "
                    f"'where's the flood zone map?'. For 'how many?' "
                    f"or 'list X' questions you need queryable data: "
                    f"call `find_gis_content(topic=...)` instead and "
                    f"pick a Feature Service from the QUERYABLE "
                    f"section."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "keyword": {
                            "type": "string",
                            "description": "Optional keyword to filter gallery items.",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Max results (default 20).",
                            "default": 20,
                        },
                    },
                },
            ),
            ToolDefinition(
                name="search_spatial_layers",
                description=(
                    f"Search {city}'s ArcGIS Online for raw spatial layers -- "
                    f"Feature Services, Map Services, tile layers, Web Maps, "
                    f"and downloadable data (GeoJSON, Shapefile, CSV). Use "
                    f"when the user wants underlying GIS data rather than a "
                    f"pre-built viewer. Prefer `find_gis_content` for "
                    f"general questions -- it also searches the public "
                    f"gallery. Use this tool when you specifically need a "
                    f"queryable layer to pass to `query_data`. Use simple "
                    f"1-2 word keywords (e.g. 'parks' not 'park parcels')."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Keyword search query.",
                        },
                        "layer_type": {
                            "type": "string",
                            "enum": ["layers", "data", "all"],
                            "description": (
                                "'layers' = Feature/Map/Image Services and "
                                "tile layers; 'data' = Web Maps, GeoJSON, "
                                "Shapefile, CSV; 'all' = both."
                            ),
                            "default": "all",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Max results (default 10, max 50).",
                            "default": 10,
                        },
                    },
                    "required": ["query"],
                },
            ),
            ToolDefinition(
                name="get_item_details",
                description=(
                    f"Get full details for a specific {city} GIS item by its "
                    f"ArcGIS item ID: title, full description, tags, owner, "
                    f"dates, service URL, and spatial extent."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "item_id": {
                            "type": "string",
                            "description": (
                                "ArcGIS Online item ID "
                                "(32-character hex string)."
                            ),
                        },
                    },
                    "required": ["item_id"],
                },
            ),
            ToolDefinition(
                name="get_layer_schema",
                description=(
                    "Fetch the schema (field names, types, aliases, coded "
                    "domains) for a Feature Service or Map Service layer. "
                    "Use to discover what attributes a dataset contains. "
                    "Accepts an ArcGIS item ID or a direct service URL."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "item_id": {
                            "type": "string",
                            "description": (
                                "ArcGIS Online item ID. "
                                "Use this or service_url."
                            ),
                        },
                        "service_url": {
                            "type": "string",
                            "description": (
                                "Direct ArcGIS REST service URL ending in "
                                "/FeatureServer or /FeatureServer/0."
                            ),
                        },
                        "layer_index": {
                            "type": "integer",
                            "description": (
                                "Layer index within the service (default 0)."
                            ),
                            "default": 0,
                        },
                        "keyword": {
                            "type": "string",
                            "description": (
                                "Only show fields whose name/alias "
                                "contains this term."
                            ),
                        },
                    },
                },
            ),
            ToolDefinition(
                name="get_distinct_values",
                description=(
                    f"List the distinct values in a {city} Feature "
                    f"Service field -- to confirm EXACT format of "
                    f"identifiers/codes before writing a WHERE clause. "
                    f"Catches variants like 'R2M' vs 'R-2M', "
                    f"'1-213-29' vs '0012132900', 'Fairview' vs "
                    f"'FAIRVIEW'. Optional `like` substring-narrows. "
                    f"Chain: get_layer_schema -> get_distinct_values "
                    f"-> query_data with the verified value."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "item_id": {
                            "type": "string",
                            "description": (
                                "ArcGIS item ID of a queryable "
                                "Feature/Map Service."
                            ),
                        },
                        "field": {
                            "type": "string",
                            "description": (
                                "Field name to get distinct values "
                                "for. CASE-SENSITIVE -- use the exact "
                                "name from `get_layer_schema`."
                            ),
                        },
                        "like": {
                            "type": "string",
                            "description": (
                                "Optional substring; only values "
                                "containing this string are returned. "
                                "E.g. like='2M' surfaces 'R-2M', "
                                "'R-2M (PUD)'."
                            ),
                        },
                        "where": {
                            "type": "string",
                            "description": (
                                "Optional SQL WHERE to narrow which "
                                "records contribute distinct values."
                            ),
                            "default": "1=1",
                        },
                        "limit": {
                            "type": "integer",
                            "description": (
                                "Max distinct values to return "
                                "(default 50, max 500)."
                            ),
                            "default": 50,
                        },
                    },
                    "required": ["item_id", "field"],
                },
            ),
            ToolDefinition(
                name="search_layers_by_field",
                description=(
                    f"Find {city} Feature Services that contain a specific "
                    f"field name or alias. Use to discover which datasets "
                    f"have a particular attribute -- e.g. 'flood', 'permit', "
                    f"'zone'."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "field_keyword": {
                            "type": "string",
                            "description": (
                                "Keyword to match in field names/aliases."
                            ),
                        },
                        "service_keyword": {
                            "type": "string",
                            "description": (
                                "Optional: pre-filter services by title "
                                "keyword. If omitted, `field_keyword` is "
                                "used as the catalog filter so the inspected "
                                "sample is biased toward relevant services "
                                "(the ArcGIS catalog is popularity-ranked, "
                                "so an unfiltered search often misses "
                                "less-trafficked services)."
                            ),
                        },
                        "limit": {
                            "type": "integer",
                            "description": (
                                "Max services to inspect "
                                "(default 10, max 20)."
                            ),
                            "default": 10,
                        },
                    },
                    "required": ["field_keyword"],
                },
            ),
            ToolDefinition(
                name="query_data",
                description=(
                    f"Query records from a {city} ArcGIS Feature "
                    f"Service. Field names are CASE-SENSITIVE -- use "
                    f"`get_layer_schema(item_id)` first; typos are "
                    f"caught with a 'did you mean' suggestion. "
                    f"COUNTING ('how many X?'): set `limit=1` and "
                    f"read the 'TOTAL COUNT' line. LISTING: set "
                    f"`limit` to how many records you want and "
                    f"optional `where` to filter (default 1=1 = all). "
                    f"Omit `limit` to read a small table whole: up to "
                    f"{self.SMALL_TABLE_ROW_THRESHOLD} matching rows "
                    f"come back complete in ONE call -- never page by "
                    f"OBJECTID ranges. Item must be a Feature/Map "
                    f"Service (not a Web Map or app)."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "item_id": {
                            "type": "string",
                            "description": "ArcGIS Online item ID.",
                        },
                        "where": {
                            "type": "string",
                            "description": "SQL WHERE clause for filtering.",
                            "default": "1=1",
                        },
                        "out_fields": {
                            "type": "string",
                            "description": (
                                "Comma-separated field names to return."
                            ),
                            "default": "*",
                        },
                        "limit": {
                            "type": "integer",
                            "description": (
                                "Maximum number of records. Omitted: 25, "
                                "or EVERY row when at most "
                                f"{self.SMALL_TABLE_ROW_THRESHOLD} match "
                                "(the response then says 'complete'). "
                                "Set it explicitly to cap the rows (1 "
                                "for a count); up to 1000 per call."
                            ),
                            # No "default" key on purpose: the effective
                            # default is conditional (25, or the whole
                            # small table), and a client that pre-fills
                            # schema defaults would send limit=25 and
                            # switch the small-table rule off.
                            "minimum": 1,
                            "maximum": 1000,
                        },
                        "order_by": {
                            "type": "string",
                            "description": (
                                "Field name to sort by, optionally followed "
                                "by ASC or DESC (e.g. 'DateOfAdoption DESC')."
                            ),
                        },
                        "date_format": {
                            "type": "string",
                            "enum": ["date", "epoch"],
                            "description": (
                                "'date' (default) returns date fields as "
                                "YYYY-MM-DD (datetimes as ISO 8601); "
                                "'epoch' keeps raw millisecond "
                                "timestamps for data pipeline use."
                            ),
                            "default": "date",
                        },
                        "return_geometry": {
                            "type": "boolean",
                            "description": (
                                "If true, each record includes a GeoJSON "
                                "geometry in WGS84 (EPSG:4326), "
                                "server-side simplified to ~5m. Default "
                                "false. When true, limit is capped at 50 "
                                "to guard against polygon payload bloat. "
                                "For point-in-polygon lookups, prefer "
                                "spatial_query_point."
                            ),
                            "default": False,
                        },
                    },
                    "required": ["item_id"],
                },
            ),
            ToolDefinition(
                name="spatial_query_point",
                description=(
                    f"Point-in-polygon lookup on a {city} polygon "
                    f"Feature Service. Given a lon/lat (WGS84), "
                    f"returns the attributes of every polygon "
                    f"containing the point ('which park / zone / "
                    f"flood zone is here?'). Call `get_layer_schema` "
                    f"first to confirm geometryType is "
                    f"esriGeometryPolygon and to see field names for "
                    f"the optional `where` / `out_fields`. Field "
                    f"names are CASE-SENSITIVE. Returns attributes "
                    f"only; no geometry."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "item_id": {
                            "type": "string",
                            "description": (
                                "ArcGIS Online item ID of a polygon "
                                "Feature Service."
                            ),
                        },
                        "lon": {
                            "type": "number",
                            "description": (
                                "Longitude in WGS84 decimal degrees "
                                "(-180 to 180). Note: lon comes first."
                            ),
                        },
                        "lat": {
                            "type": "number",
                            "description": (
                                "Latitude in WGS84 decimal degrees "
                                "(-90 to 90)."
                            ),
                        },
                        "where": {
                            "type": "string",
                            "description": (
                                "Optional SQL WHERE clause to further "
                                "filter candidate features."
                            ),
                            "default": "1=1",
                        },
                        "out_fields": {
                            "type": "string",
                            "description": (
                                "Comma-separated field names to return."
                            ),
                            "default": "*",
                        },
                        "limit": {
                            "type": "integer",
                            "description": (
                                "Max features to return (default 10, "
                                "max 50). Point-in-polygon usually "
                                "returns 0-3 features."
                            ),
                            "default": 10,
                            "minimum": 1,
                            "maximum": 50,
                        },
                    },
                    "required": ["item_id", "lon", "lat"],
                },
            ),
            ToolDefinition(
                name="spatial_query_polygon",
                description=(
                    f"Server-side spatial intersection on a {city} "
                    f"Feature Service. Polygon filter is either inline "
                    f"GeoJSON (`filter_geometry`) or polygon feature(s) "
                    f"in another layer (`filter_item_id` + "
                    f"`filter_where`). Returns target features whose "
                    f"geometry intersects (or other `spatial_rel`) the "
                    f"filter. Target layer can be polygon, polyline, "
                    f"or point -- use this over centroid-based "
                    f"assignments when features can straddle "
                    f"boundaries. PROXIMITY ('within N miles/feet of "
                    f"X'): set `distance` + `units` to buffer the filter "
                    f"polygon server-side -- e.g. people within half a "
                    f"mile of a park = target a population layer, filter "
                    f"on the parks layer, distance=0.5, units='miles'. "
                    f"Call `get_layer_schema` on BOTH "
                    f"layers (target and filter) before writing "
                    f"`where`/`filter_where` -- they have different "
                    f"schemas. Set `return_geometry=true` for GeoJSON "
                    f"output if you need to clip client-side."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "item_id": {
                            "type": "string",
                            "description": (
                                "ArcGIS Online item ID of the target "
                                "layer (the layer to select features "
                                "from)."
                            ),
                        },
                        "filter_item_id": {
                            "type": "string",
                            "description": (
                                "Item ID of a polygon layer whose "
                                "feature(s) will be used as the filter "
                                "geometry. Combine with filter_where to "
                                "pick specific features. Use this when "
                                "the filter polygon already exists as a "
                                "published layer -- avoids passing large "
                                "geometries through the model."
                            ),
                        },
                        "filter_where": {
                            "type": "string",
                            "description": (
                                "SQL WHERE clause applied to "
                                "filter_item_id to select which polygon "
                                "feature(s) to use as the filter. E.g. "
                                "\"DistrictName = 'District 5'\". All "
                                "matching features' rings are unioned."
                            ),
                            "default": "1=1",
                        },
                        "filter_geometry": {
                            "type": "object",
                            "description": (
                                "Inline GeoJSON Polygon, MultiPolygon, "
                                "or Feature wrapping one. Use this "
                                "instead of filter_item_id when you "
                                "already have the polygon in hand. "
                                "Coordinates must be WGS84 (EPSG:4326)."
                            ),
                        },
                        "spatial_rel": {
                            "type": "string",
                            "enum": [
                                "intersects",
                                "contains",
                                "within",
                                "crosses",
                                "touches",
                                "overlaps",
                                "envelope_intersects",
                            ],
                            "description": (
                                "ArcGIS spatial relation: 'intersects' "
                                "(any overlap, default), 'contains' "
                                "(filter contains target), 'within' "
                                "(target within filter), 'crosses', "
                                "'touches', 'overlaps', "
                                "'envelope_intersects'."
                            ),
                            "default": "intersects",
                        },
                        "where": {
                            "type": "string",
                            "description": (
                                "Optional SQL WHERE clause to further "
                                "filter target features by attribute."
                            ),
                            "default": "1=1",
                        },
                        "distance": {
                            "type": "number",
                            "description": (
                                "Proximity buffer: expand the filter "
                                "polygon by this distance (in `units`) "
                                "before testing the spatial relation. Use "
                                "for 'within N of' questions, e.g. "
                                "distance=0.5 units='miles' for 'within "
                                "half a mile of'. Omit or 0 for an exact "
                                "(unbuffered) overlap."
                            ),
                        },
                        "units": {
                            "type": "string",
                            "enum": [
                                "meters",
                                "kilometers",
                                "feet",
                                "miles",
                                "yards",
                            ],
                            "description": (
                                "Linear unit for `distance` (default "
                                "meters). Ignored when distance is "
                                "omitted."
                            ),
                            "default": "meters",
                        },
                        "out_fields": {
                            "type": "string",
                            "description": (
                                "Comma-separated field names to return."
                            ),
                            "default": "*",
                        },
                        "limit": {
                            "type": "integer",
                            "description": (
                                "Max features to return (default 25, "
                                "capped at 1000; capped at 50 when "
                                "return_geometry=true)."
                            ),
                            "default": 25,
                            "minimum": 1,
                            "maximum": 1000,
                        },
                        "return_geometry": {
                            "type": "boolean",
                            "description": (
                                "If true, each record includes a "
                                "GeoJSON geometry in WGS84, "
                                "server-side simplified to ~5m. "
                                "Useful for precise client-side "
                                "clipping. Limit is capped at 50."
                            ),
                            "default": False,
                        },
                    },
                    "required": ["item_id"],
                },
            ),
            ToolDefinition(
                name="aggregate_by_polygon",
                description=(
                    f"Bucket records from one {city} layer into "
                    f"polygons from another and return counts + sums "
                    f"per bucket. Answers 'how many X per community "
                    f"council', 'total tons by district', etc. "
                    f"Source can be points/polylines/polygons; "
                    f"aggregation must be polygons. Returns count + "
                    f"summed numeric fields per bucket, plus an "
                    f"unmatched count. Prefer this over a loop of "
                    f"spatial_query_point calls. PROXIMITY: set "
                    f"`buffer_distance` + `buffer_units` to bucket source "
                    f"features that fall WITHIN that distance of a polygon "
                    f"(not just inside it) -- e.g. population points "
                    f"within half a mile of each park: source=population, "
                    f"aggregation=parks, buffer_distance=0.5, "
                    f"buffer_units='miles'."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "source_item_id": {
                            "type": "string",
                            "description": (
                                "ArcGIS item ID of the layer whose "
                                "records you want to bucket (points, "
                                "polylines, or polygons)."
                            ),
                        },
                        "aggregation_item_id": {
                            "type": "string",
                            "description": (
                                "ArcGIS item ID of the polygon layer "
                                "to bucket into (e.g. community "
                                "councils, assembly districts)."
                            ),
                        },
                        "group_by_field": {
                            "type": "string",
                            "description": (
                                "Field on the aggregation layer whose "
                                "value labels each bucket (e.g. "
                                "'COUNCIL', 'DISTRICT_NAME')."
                            ),
                        },
                        "sum_fields": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": (
                                "Numeric field names from the source "
                                "layer to sum per bucket. Omit for "
                                "count-only."
                            ),
                        },
                        "count": {
                            "type": "boolean",
                            "description": (
                                "Include a feature count per bucket "
                                "(default true)."
                            ),
                            "default": True,
                        },
                        "source_where": {
                            "type": "string",
                            "description": (
                                "SQL WHERE clause applied to the source "
                                "layer before aggregating."
                            ),
                            "default": "1=1",
                        },
                        "agg_where": {
                            "type": "string",
                            "description": (
                                "SQL WHERE clause to narrow the "
                                "aggregation polygons (e.g. exclude "
                                "retired districts)."
                            ),
                            "default": "1=1",
                        },
                        "centroid_mode": {
                            "type": "string",
                            "enum": [
                                "auto",
                                "centroid",
                                "representative_point",
                            ],
                            "description": (
                                "How to reduce source features to a "
                                "single point for the join. For "
                                "polygons: 'centroid' is cheap, "
                                "'representative_point' stays inside "
                                "L-shaped/donut shapes, 'auto' (default) "
                                "uses centroid unless it falls outside. "
                                "For polylines (road centerlines, "
                                "trails, transit routes): 'auto' and "
                                "'representative_point' use the line "
                                "midpoint, interpolated at 50% along the "
                                "line's arc length -- always on the line "
                                "itself. 'centroid' uses the "
                                "length-weighted centroid of the line "
                                "(NOT a bounding-box centroid); on a "
                                "curved or U-shaped line that point can "
                                "fall off the line, so prefer 'auto' for "
                                "polyline sources. Note: a road segment "
                                "straddling two districts is assigned to "
                                "whichever district contains its "
                                "midpoint, not both -- use "
                                "spatial_query_polygon if you need "
                                "intersects-based assignment."
                            ),
                            "default": "auto",
                        },
                        "overlap_policy": {
                            "type": "string",
                            "enum": [
                                "first_match",
                                "all_matches",
                                "largest",
                            ],
                            "description": (
                                "How to handle a source feature that "
                                "matches several aggregation polygons. "
                                "'first_match' counts it once, for the "
                                "first polygon listed (rows add up to "
                                "the total); 'all_matches' counts it "
                                "for every polygon it matches (each "
                                "row is complete, rows overlap); "
                                "'largest' picks the biggest polygon. "
                                "Default: 'first_match' without a "
                                "buffer, 'all_matches' with "
                                "buffer_distance, since 'within N of "
                                "each polygon' is a per-polygon "
                                "question."
                            ),
                            "default": "first_match",
                        },
                        "buffer_distance": {
                            "type": "number",
                            "description": (
                                "Proximity buffer: bucket a source feature "
                                "into a polygon when its point is inside OR "
                                "within this distance (in `buffer_units`) "
                                "of the polygon. Use for 'within N of' "
                                "questions, e.g. buffer_distance=0.5 "
                                "buffer_units='miles'. Omit or 0 for "
                                "strict containment."
                            ),
                        },
                        "buffer_units": {
                            "type": "string",
                            "enum": [
                                "meters",
                                "kilometers",
                                "feet",
                                "miles",
                                "yards",
                            ],
                            "description": (
                                "Linear unit for `buffer_distance` "
                                "(default meters). Ignored when "
                                "buffer_distance is omitted."
                            ),
                            "default": "meters",
                        },
                        "max_source_features": {
                            "type": "integer",
                            "description": (
                                "Safety cap on source features pulled "
                                "(default 5000, max 5000). Narrow "
                                "source_where if you hit the cap."
                            ),
                            "default": 5000,
                        },
                    },
                    "required": [
                        "source_item_id",
                        "aggregation_item_id",
                        "group_by_field",
                    ],
                },
            ),
            ToolDefinition(
                name="coverage_by_polygon",
                description=(
                    f"SCREENING-GRADE. Compute, for each {city} target "
                    f"polygon, the share of "
                    f"its area covered by overlapping features from an "
                    f"overlay polygon layer, then count/list the targets in a "
                    f"coverage band. This is the LOT-COVERAGE / "
                    f"impervious-surface / buildable-land pattern -- e.g. "
                    f"'religious parcels with under 40% building coverage', "
                    f"'lots more than 80% paved'. Works WITHOUT a shared key: "
                    f"the overlay is joined to each target spatially (so use "
                    f"this when footprints/overlay have no parcel id). For "
                    f"each target it sums the area of overlay features whose "
                    f"centroid falls inside and divides by the target's own "
                    f"area. Returns 'N of M targets in band' plus the "
                    f"lowest-coverage targets. Centroid-assigned, unclipped "
                    f"(screening-grade)."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "target_item_id": {
                            "type": "string",
                            "description": (
                                "ArcGIS item ID of the polygon layer being "
                                "measured (e.g. parcels). Coverage is "
                                "reported per target polygon."
                            ),
                        },
                        "overlay_item_id": {
                            "type": "string",
                            "description": (
                                "ArcGIS item ID of the polygon layer whose "
                                "area counts as coverage (e.g. building "
                                "footprints)."
                            ),
                        },
                        "target_id_field": {
                            "type": "string",
                            "description": (
                                "Field on the target layer that labels each "
                                "target in the output (e.g. 'Parcel_ID'). "
                                "Call get_layer_schema to find it."
                            ),
                        },
                        "target_where": {
                            "type": "string",
                            "description": (
                                "SQL WHERE to select target polygons, e.g. "
                                "\"Land_Use='Religious'\". Narrow this -- the "
                                "tool runs one spatial query per target and "
                                "caps the count."
                            ),
                            "default": "1=1",
                        },
                        "overlay_where": {
                            "type": "string",
                            "description": (
                                "Optional SQL WHERE applied to the overlay "
                                "layer before summing (e.g. exclude sheds)."
                            ),
                            "default": "1=1",
                        },
                        "max_coverage_pct": {
                            "type": "number",
                            "description": (
                                "Keep only targets with coverage STRICTLY "
                                "BELOW this percent (e.g. 40 for 'under 40% "
                                "covered'). Omit for no upper bound."
                            ),
                        },
                        "min_coverage_pct": {
                            "type": "number",
                            "description": (
                                "Keep only targets with coverage AT OR ABOVE "
                                "this percent. Omit for no lower bound. "
                                "Combine with max for a band."
                            ),
                        },
                        "target_area_field": {
                            "type": "string",
                            "description": (
                                "Numeric area field on the target layer "
                                "(denominator). Defaults to 'Shape__Area'. "
                                f"{self.TEXT['shape_area_note']}"
                                "tool corrects it and reports true "
                                "target_sqft; the raw value is passed "
                                "through as target_area."
                            ),
                            "default": "Shape__Area",
                        },
                        "overlay_area_field": {
                            "type": "string",
                            "description": (
                                "Numeric area field on the overlay layer "
                                "(numerator). Defaults to 'Shape__Area', "
                                "which is Web Mercator on "
                                f"{self.TEXT['hosted_layers']} "
                                "-- see target_area_field. Corrected true "
                                "figures are reported as covered_sqft."),
                            "default": "Shape__Area",
                        },
                        "max_targets": {
                            "type": "integer",
                            "description": (
                                "Cap on target polygons measured (default "
                                "800, max 800). Narrow target_where if you "
                                "hit it."
                            ),
                            "default": 800,
                        },
                    },
                    "required": [
                        "target_item_id",
                        "overlay_item_id",
                        "target_id_field",
                    ],
                },
            ),
            ToolDefinition(
                name="filter_by_polygon",
                description=(
                    f"Return the subset of records from one {city} layer "
                    f"that fall inside a named polygon (or polygons) "
                    f"from another layer. Use this for questions like "
                    f"'what are the reports in Fairview', 'show me the "
                    f"cleanups in Midtown'. The polygon is identified by "
                    f"SQL, not coordinates -- never ask the user for "
                    f"lat/lon when a polygon name will do. If "
                    f"container_where matches 0 polygons, returns a "
                    f"clear error (not a silently empty result). "
                    f"Multi-polygon containers (e.g. "
                    f"COUNCIL IN ('A','B','C')) are unioned."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "source_item_id": {
                            "type": "string",
                            "description": (
                                "ArcGIS item ID of the layer to filter."
                            ),
                        },
                        "container_item_id": {
                            "type": "string",
                            "description": (
                                "ArcGIS item ID of the polygon layer "
                                "that provides the named container "
                                "(e.g. a community-councils layer)."
                            ),
                        },
                        "container_where": {
                            "type": "string",
                            "description": (
                                "SQL to pick the container polygon(s), "
                                "e.g. \"COUNCIL='Fairview'\" or "
                                "\"COUNCIL IN ('Midtown','Fairview')\". "
                                "Must match >=1 polygon; 0 matches is "
                                "reported as an error."
                            ),
                        },
                        "source_where": {
                            "type": "string",
                            "description": (
                                "Optional SQL WHERE clause applied to "
                                "source records as well."
                            ),
                            "default": "1=1",
                        },
                        "out_fields": {
                            "type": "string",
                            "description": (
                                "Comma-separated field names to return."
                            ),
                            "default": "*",
                        },
                        "return_geometry": {
                            "type": "boolean",
                            "description": (
                                "If true, each record includes GeoJSON "
                                "geometry (WGS84, simplified). Limit "
                                "caps at 50 when true."
                            ),
                            "default": False,
                        },
                        "limit": {
                            "type": "integer",
                            "description": (
                                "Max features to return (default 100, "
                                "max 1000; 50 when return_geometry=true)."
                            ),
                            "default": 100,
                        },
                    },
                    "required": [
                        "source_item_id",
                        "container_item_id",
                        "container_where",
                    ],
                },
            ),
            ToolDefinition(
                name="find_features_spanning_classifications",
                description=(
                    f"Find {city} features that cross multiple values "
                    f"of a classification field -- the 'split-zoned "
                    f"parcels', 'parcels on a flood-zone boundary', "
                    f"'roads crossing district lines' pattern. "
                    f"`source_item_id` is the layer of THINGS to "
                    f"return (parcels, roads); "
                    f"`classification_item_id` is a polygon layer of "
                    f"REGIONS (zoning, flood). Use DIFFERENT layers "
                    f"for each. Returns a touch-count histogram + the "
                    f"qualifying features in one call -- no chaining "
                    f"needed. Pre-flight with `get_layer_schema` and "
                    f"`get_distinct_values` on the classification "
                    f"layer to confirm the field. Caps: source <= "
                    f"5000 matches, classification <= 1000 polygons."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "source_item_id": {
                            "type": "string",
                            "description": (
                                "ArcGIS item ID of the layer whose "
                                "features you want to test for "
                                "spanning (e.g. parcels, addresses, "
                                "roads). Can be polygon, polyline, "
                                "or point."
                            ),
                        },
                        "classification_item_id": {
                            "type": "string",
                            "description": (
                                "ArcGIS item ID of the polygon layer "
                                "whose distinct values define the "
                                "boundaries (e.g. zoning, flood "
                                "zones, community councils). MUST "
                                "be a polygon layer."
                            ),
                        },
                        "classification_field": {
                            "type": "string",
                            "description": (
                                "Field on the classification layer "
                                "whose distinct values matter "
                                "(e.g. 'ZONE_CODE', "
                                "'FLOOD_ZONE_TYPE', 'COUNCIL'). "
                                "CASE-SENSITIVE."
                            ),
                        },
                        "min_distinct": {
                            "type": "integer",
                            "description": (
                                "Minimum number of distinct "
                                "classification values a source "
                                "feature must touch to qualify. "
                                "Default 2 (= 'spans a boundary'). "
                                "Use 3+ for 'spans multiple "
                                "boundaries'."
                            ),
                            "default": 2,
                            "minimum": 2,
                        },
                        "source_where": {
                            "type": "string",
                            "description": (
                                "SQL WHERE on the source layer to "
                                "narrow features inspected. Required "
                                "to be tight enough that <= 5,000 "
                                "features match."
                            ),
                            "default": "1=1",
                        },
                        "classification_where": {
                            "type": "string",
                            "description": (
                                "SQL WHERE on the classification "
                                "layer to narrow which polygons "
                                "contribute (e.g. exclude retired "
                                "districts)."
                            ),
                            "default": "1=1",
                        },
                        "out_fields": {
                            "type": "string",
                            "description": (
                                "Comma-separated source-layer field "
                                "names to return for qualifying "
                                "features."
                            ),
                            "default": "*",
                        },
                        "limit": {
                            "type": "integer",
                            "description": (
                                "Max qualifying features to list in "
                                "the response (default 100, max "
                                "1000). The histogram covers ALL "
                                "qualifying features regardless."
                            ),
                            "default": 100,
                        },
                        "max_source_features": {
                            "type": "integer",
                            "description": (
                                "Safety cap on source features "
                                "matching source_where (default "
                                "5000, max 5000). Tool refuses with "
                                "a clear error if exceeded."
                            ),
                            "default": 5000,
                        },
                    },
                    "required": [
                        "source_item_id",
                        "classification_item_id",
                        "classification_field",
                    ],
                },
            ),
        ]
        return tools

    # ── Tool dispatch ─────────────────────────────────────────────────────

    async def execute_tool(
        self, tool_name: str, arguments: Dict[str, Any]
    ) -> ToolResult:
        token = _call_deadline.set(time.monotonic() + TOOL_CALL_BUDGET_S)
        try:
            try:
                arguments = self._clean_arguments(tool_name, arguments)
            except ToolInputError as e:
                logger.warning(
                    f"Invalid arguments for tool {tool_name}: {e}",
                    extra={
                        "tool_name": tool_name,
                        "error_type": type(e).__name__,
                    },
                )
                return ToolResult(
                    content=[], success=False, error_message=str(e)
                )
            return await self._execute_tool(tool_name, arguments)
        finally:
            _call_deadline.reset(token)

    def _clean_arguments(
        self, tool_name: str, arguments: Any
    ) -> Dict[str, Any]:
        """Normalise caller arguments before any tool reads them.

        * An explicit JSON null means "not supplied": some clients send
          null for every optional they leave unset, and the tools read
          arguments with ``.get(name, default)``, which a present null
          defeats.
        * A parameter the tool declares as a string must be a string.
          A list of strings is joined with commas, because models
          routinely send ``out_fields`` or ``sum_fields`` either way.
          Anything else is the caller's mistake, said plainly, rather
          than an AttributeError from deep inside the tool.
        """
        if arguments is None:
            return {}
        if not isinstance(arguments, dict):
            raise ToolInputError("tool arguments must be an object")
        if self._arg_types is None:
            self._arg_types = {
                t.name: {
                    name: spec.get("type")
                    for name, spec in (
                        (t.input_schema or {}).get("properties") or {}
                    ).items()
                    if isinstance(spec, dict)
                }
                for t in self.get_tools()
            }
        declared = self._arg_types.get(tool_name, {})
        cleaned: Dict[str, Any] = {}
        for name, value in arguments.items():
            if value is None:
                continue
            if declared.get(name) == "string" and not isinstance(value, str):
                if isinstance(value, (list, tuple)) and all(
                    isinstance(v, str) for v in value
                ):
                    value = ",".join(value)
                else:
                    raise ToolInputError(
                        f"{name} must be a string "
                        f"(got {type(value).__name__})"
                    )
            cleaned[name] = value
        return cleaned

    async def _execute_tool(
        self, tool_name: str, arguments: Dict[str, Any]
    ) -> ToolResult:
        # Only tools that declare an outputSchema set this; it stays None
        # for the rest, and MCPServer omits `structuredContent` entirely
        # when it is None.
        structured: Optional[Dict[str, Any]] = None
        try:
            if tool_name == "find_gis_content":
                text, structured = await self._find_gis_content(arguments)

            elif tool_name == "browse_gallery":
                text, structured = await self._browse_gallery(
                    arguments.get("keyword", "").strip(),
                    min(self._int_arg(arguments, "limit", 20), 100),
                )

            elif tool_name == "search_spatial_layers":
                query = arguments.get("query", "").strip()
                if not query:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message="query is required",
                    )
                text, structured = await self._search_spatial_layers(
                    query,
                    arguments.get("layer_type", "all"),
                    min(self._int_arg(arguments, "limit", 10), 50),
                )

            elif tool_name == "get_item_details":
                item_id = arguments.get("item_id", "").strip()
                if not item_id:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message="item_id is required",
                    )
                item = await self.get_dataset(item_id)
                text = self._format_details(item)
                structured = self._item_details_record(item)

            elif tool_name == "get_layer_schema":
                text, structured = await self._get_layer_schema(arguments)

            elif tool_name == "get_distinct_values":
                text, structured = await self._get_distinct_values(arguments)

            elif tool_name == "search_layers_by_field":
                text, structured = await self._search_layers_by_field(
                    arguments
                )

            elif tool_name == "query_data":
                item_id = arguments.get("item_id", "").strip()
                if not item_id:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message="item_id is required",
                    )
                where = arguments.get("where", "1=1")
                out_fields = arguments.get("out_fields", "*")
                order_by = arguments.get("order_by", "")
                date_format = arguments.get("date_format", "date")
                if date_format not in ("date", "epoch"):
                    raise ToolInputError(
                        "date_format must be 'date' or 'epoch' "
                        f"(got {date_format!r})"
                    )
                return_geometry = bool(
                    arguments.get("return_geometry", False)
                )
                # An omitted limit opts into the small-table rule below;
                # an explicit one is honoured. (An explicit null is still
                # rejected by _int_arg, as before.)
                limit_given = "limit" in arguments
                requested_limit = self._int_arg(arguments, "limit", 25)
                effective_limit = (
                    min(requested_limit, self.GEOMETRY_LIMIT_CAP)
                    if return_geometry
                    else requested_limit
                )
                # Small-table rule: fetch up to the threshold in the SAME
                # parallel round-trip as the count, so a small table can
                # come back whole without a second request. Rows past the
                # default are trimmed again below when the table turns
                # out to be large. Geometry keeps its own 50-row cap.
                fetch_limit = effective_limit
                if not limit_given and not return_geometry:
                    fetch_limit = max(
                        effective_limit, self.SMALL_TABLE_ROW_THRESHOLD
                    )
                filters = {
                    "where": where,
                    "out_fields": out_fields,
                    "order_by": order_by,
                }

                # Resolve service URL once for parallel queries
                item = await self.get_dataset(item_id)
                service_url = self._ensure_layer_url(
                    item.get("url", "")
                )
                validated_where = WhereValidator.validate(where)

                # Fetch the layer quick-meta up front so we can
                # validate the WHERE clause against real field names
                # *before* incurring the query -- a typo'd field name
                # otherwise produces a cryptic ArcGIS error and a
                # round-trip wasted. Best-effort: if the meta fetch
                # fails (returns {}) we skip schema validation and
                # let the query speak for itself.
                quick_meta = await self._get_layer_quick_meta(service_url) or {}
                try:
                    WhereValidator.validate_against_schema(
                        validated_where, quick_meta.get("field_names")
                    )
                except ValueError as e:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message=str(e),
                    )

                # Now run the (more expensive) query and count in
                # parallel for latency.
                parallel_tasks = [
                    self.query_data(
                        item_id,
                        filters,
                        fetch_limit,
                        return_geometry=return_geometry,
                    ),
                    self._get_record_count(service_url, validated_where),
                ]

                results = await asyncio.gather(*parallel_tasks)
                records = results[0]
                total_count = results[1]
                auto_expanded_from: Optional[int] = None
                if fetch_limit > effective_limit:
                    if (
                        total_count is not None
                        and total_count <= self.SMALL_TABLE_ROW_THRESHOLD
                        and len(records) > effective_limit
                    ):
                        # Small table: keep every row, and report the
                        # limit actually used.
                        auto_expanded_from = effective_limit
                        effective_limit = fetch_limit
                    else:
                        # Large (or uncounted) result: the default
                        # behaviour, exactly as before the over-fetch.
                        records = records[:effective_limit]
                # NOTE: f=geojson does NOT render dates as ISO strings
                # on MOA hosted services (verified live: epoch ms in
                # GeoJSON properties), so the conversion applies to the
                # return_geometry path too. _ms_to_iso_smart passes
                # non-numeric values through, so a server that did
                # return ISO strings would be unharmed.
                date_fields = (
                    quick_meta.get("date_fields")
                    if date_format != "epoch"
                    else None
                )

                text, structured = self._format_query_results(
                    records,
                    effective_limit,
                    total_count,
                    date_fields,
                    geometry_type=quick_meta.get("geometry_type"),
                    name_field=quick_meta.get("name_field"),
                    item_id=item_id,
                    service_url=service_url,
                    where=validated_where,
                    out_fields=out_fields,
                    coded_domains=(
                        quick_meta.get("coded_domains")
                        if not return_geometry
                        else None
                    ),
                    last_edit_date=quick_meta.get("last_edit_date"),
                    coverage_pct=quick_meta.get("coverage_pct"),
                    auto_expanded_from=auto_expanded_from,
                )
                if not records:
                    text += self._no_data_hint(where)
                text = self._apply_result_banner(item_id, text, structured)

            elif tool_name == "spatial_query_point":
                item_id = arguments.get("item_id", "").strip()
                if not item_id:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message="item_id is required",
                    )
                if "lon" not in arguments or "lat" not in arguments:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message="lon and lat are required",
                    )
                limit = min(self._int_arg(arguments, "limit", 10), 50)
                overflow: Dict[str, Any] = {}
                records, layer_meta = await asyncio.gather(
                    self.spatial_query_point(
                        item_id,
                        lon=arguments["lon"],
                        lat=arguments["lat"],
                        filters={
                            "where": arguments.get("where", "1=1"),
                            "out_fields": arguments.get("out_fields", "*"),
                        },
                        limit=limit,
                        info=overflow,
                    ),
                    self._safe_layer_meta(item_id),
                )
                if not records:
                    text = (
                        f"No features in item `{item_id}` contain point "
                        f"(lon={arguments['lon']}, lat={arguments['lat']})."
                    )
                    # This branch skips _format_query_results, so the
                    # structured half has to be built here. The tool
                    # declares an outputSchema, and the spec makes that
                    # binding on EVERY path -- returning nothing on the
                    # empty result would silently break conformance.
                    structured = self._empty_query_result(
                        item_id=item_id,
                        where=arguments.get("where"),
                        out_fields=arguments.get("out_fields"),
                        limit=limit,
                    )
                else:
                    # total_count stays None when every match is in
                    # `records` (no "of N total" line for the ordinary
                    # answer); spatial_query_point supplies a count
                    # only when the page was cut at `limit`.
                    # layer_meta is best-effort; raw rendering falls
                    # through on any failure.
                    text, structured = self._format_query_results(
                        records,
                        limit,
                        total_count=overflow.get("total_count"),
                        date_fields=layer_meta.get("date_fields"),
                        coded_domains=layer_meta.get("coded_domains"),
                        last_edit_date=layer_meta.get("last_edit_date"),
                        coverage_pct=layer_meta.get("coverage_pct"),
                        limit_reached=bool(
                            overflow.get("limit_reached")
                        ),
                    )
                    structured["query"].update(
                        {
                            "item_id": item_id,
                            "where": arguments.get("where"),
                            "out_fields": arguments.get("out_fields"),
                        }
                    )
                text = self._apply_result_banner(item_id, text, structured)

            elif tool_name == "spatial_query_polygon":
                item_id = arguments.get("item_id", "").strip()
                if not item_id:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message="item_id is required",
                    )
                filter_geometry = arguments.get("filter_geometry")
                filter_item_id = (
                    arguments.get("filter_item_id") or ""
                ).strip() or None
                if not filter_geometry and not filter_item_id:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message=(
                            "spatial_query_polygon requires either "
                            "filter_geometry (inline GeoJSON) or "
                            "filter_item_id"
                        ),
                    )
                return_geometry = bool(
                    arguments.get("return_geometry", False)
                )
                requested_limit = self._int_arg(arguments, "limit", 25)
                effective_limit = (
                    min(requested_limit, self.GEOMETRY_LIMIT_CAP)
                    if return_geometry
                    else min(requested_limit, 1000)
                )
                overflow = {}
                records, layer_meta = await asyncio.gather(
                    self.spatial_query_polygon(
                        item_id,
                        filter_geometry=filter_geometry,
                        filter_item_id=filter_item_id,
                        filter_where=arguments.get("filter_where", "1=1"),
                        spatial_rel=arguments.get("spatial_rel", "intersects"),
                        filters={
                            "where": arguments.get("where", "1=1"),
                            "out_fields": arguments.get("out_fields", "*"),
                        },
                        limit=effective_limit,
                        return_geometry=return_geometry,
                        distance=arguments.get("distance"),
                        units=arguments.get("units", "meters"),
                        info=overflow,
                    ),
                    self._safe_layer_meta(item_id),
                )
                # Dates convert in BOTH branches: f=geojson does NOT
                # render dates as ISO strings on MOA hosted services
                # (verified live -- epoch ms in GeoJSON properties).
                # Coded-domain decoding still skips the GeoJSON branch,
                # where downstream tooling expects the original codes.
                date_fields = layer_meta.get("date_fields")
                coded_domains = (
                    None
                    if return_geometry
                    else layer_meta.get("coded_domains")
                )
                if not records:
                    text = (
                        f"No features in item `{item_id}` match the "
                        f"filter polygon."
                    )
                    # Same as spatial_query_point above: the no-match
                    # path bypasses the formatter, so build the
                    # conforming empty result explicitly.
                    structured = self._empty_query_result(
                        item_id=item_id,
                        where=arguments.get("where"),
                        out_fields=arguments.get("out_fields"),
                        limit=effective_limit,
                    )
                else:
                    text, structured = self._format_query_results(
                        records,
                        effective_limit,
                        total_count=overflow.get("total_count"),
                        date_fields=date_fields,
                        coded_domains=coded_domains,
                        last_edit_date=layer_meta.get("last_edit_date"),
                        coverage_pct=layer_meta.get("coverage_pct"),
                        limit_reached=bool(
                            overflow.get("limit_reached")
                        ),
                    )
                    structured["query"].update(
                        {
                            "item_id": item_id,
                            "where": arguments.get("where"),
                            "out_fields": arguments.get("out_fields"),
                        }
                    )
                text = self._apply_result_banner(item_id, text, structured)

            elif tool_name == "aggregate_by_polygon":
                text, structured = await self._aggregate_by_polygon(arguments)

            elif tool_name == "coverage_by_polygon":
                text, structured = await self._coverage_by_polygon(arguments)

            elif tool_name == "filter_by_polygon":
                text, structured = await self._filter_by_polygon(arguments)
                text = self._apply_result_banner(
                    str(arguments.get("source_item_id") or "").strip(),
                    text,
                    structured,
                )

            elif tool_name == "find_features_spanning_classifications":
                (
                    text,
                    structured,
                ) = await self._find_features_spanning_classifications(
                    arguments
                )

            else:
                extra = await self._execute_extra_tool(tool_name, arguments)
                if extra is None:
                    return ToolResult(
                        content=[],
                        success=False,
                        error_message=f"Unknown tool: {tool_name}",
                    )
                text, structured = extra

            return ToolResult(
                content=[
                    {"type": "text", "text": self._with_retrieved_footer(text)}
                ],
                success=True,
                structured_content=structured,
            )

        except ToolInputError as e:
            # The caller asked for something invalid. That is not a
            # server fault, so no traceback -- a stack trace here is a
            # false claim that this server broke, and it buries the
            # real ones. The message still reaches the caller verbatim.
            logger.warning(
                f"Invalid arguments for tool {tool_name}: {e}",
                extra={"tool_name": tool_name, "error_type": type(e).__name__},
            )
            return ToolResult(
                content=[],
                success=False,
                error_message=str(e) if str(e) else "Invalid tool arguments",
            )

        except Exception as e:
            logger.error(
                f"Error executing tool {tool_name}: {e}", exc_info=True
            )
            return ToolResult(
                content=[],
                success=False,
                # An httpx timeout or connect error stringifies to '', so
                # name the type rather than return a bare "failed".
                error_message=str(e)
                or f"Tool execution failed ({type(e).__name__})",
            )

    # ── Health check ──────────────────────────────────────────────────────

    async def health_check(self) -> bool:
        try:
            resp = await self.client.get(
                f"{self.plugin_config.portal_base_url}/search",
                params={
                    "q": f"orgid:{self.plugin_config.org_id}",
                    "f": "json",
                    "num": "1",
                },
            )
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
