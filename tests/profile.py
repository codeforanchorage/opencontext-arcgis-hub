"""The plugin the suite runs against.

`ArcGISHubPlugin` is a base class: deployments subclass it. The tests use
the smallest possible subclass, so what they exercise is the base's own
behaviour and default wording. The fixtures use real Municipality of
Anchorage ids and hosts (the code was extracted from that deployment), so
the profile accepts its on-prem host suffix; nothing else is customised.

A deployment's own wording, coverage box and extra tools are tested in
that deployment's repository.
"""

from arcgis_hub.config_schema import ArcGISHubPluginConfig
from arcgis_hub.plugin import ArcGISHubPlugin


class ExampleConfig(ArcGISHubPluginConfig):
    """Single org, single gallery group: the base config, unchanged."""


class ExamplePlugin(ArcGISHubPlugin):
    plugin_name = "example_hub"
    DISPLAY_NAME = "Example Hub"
    ONPREM_HOST_SUFFIXES = (".muni.org",)
