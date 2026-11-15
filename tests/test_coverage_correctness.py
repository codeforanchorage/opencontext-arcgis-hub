"""Correctness of coverage_by_polygon's area figures and diagnostics.

The percentage was always right -- the distortion cancels in a ratio of
two same-projection areas. The RAW areas were not: MOA hosted layers
publish Shape__Area in Web Mercator, which inflates area by sec^2(lat),
about 4.3x at Anchorage. Read as square feet they were wrong by roughly
a factor of four, and nothing in the output said so.
"""

import math

import pytest

from tests.profile import ExampleConfig, ExamplePlugin


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


class TestWebMercatorAreaCorrection:
    def test_matches_assessor_lot_size_on_a_real_parcel(self):
        """Ground truth: parcel 00326133000 stores Shape__Area = 1010
        (Web Mercator m2); the assessor's Lot_Size is 2,493 sqft."""
        sqft = ExamplePlugin._to_sqft(1010.0, 61.2016, 3857)
        assert sqft is not None
        assert abs(sqft - 2493) / 2493 < 0.05, f"{sqft:,.0f} sqft vs 2,493"

    def test_correction_is_per_feature_not_a_flat_divisor(self):
        """The factor varies ~2% across the municipality, so applying one
        constant would be wrong at the edges."""
        girdwood = ExamplePlugin._to_sqft(1000.0, 60.95, 3857)
        eagle_river = ExamplePlugin._to_sqft(1000.0, 61.32, 3857)
        assert girdwood > eagle_river  # less distortion further south
        assert abs(girdwood - eagle_river) / girdwood > 0.01

    @pytest.mark.parametrize("wkid", [3857, 102100, 102113])
    def test_recognises_web_mercator_variants(self, wkid):
        assert ExamplePlugin._to_sqft(1000.0, 61.2, wkid) is not None

    @pytest.mark.parametrize("wkid", [4326, 4269, 26935, None])
    def test_refuses_to_guess_for_other_projections(self, wkid):
        """Returning a number we cannot justify would be the original bug
        in a new costume."""
        assert ExamplePlugin._to_sqft(1000.0, 61.2, wkid) is None

    def test_percentage_is_unaffected_by_the_projection(self):
        """Why this was silent: the ratio was always correct."""
        lat = 61.2
        raw_ratio = 250.0 / 1000.0
        true_ratio = (
            ExamplePlugin._to_sqft(250.0, lat, 3857)
            / ExamplePlugin._to_sqft(1000.0, lat, 3857)
        )
        assert abs(raw_ratio - true_ratio) < 1e-9

    def test_layer_wkid_reads_latest_then_wkid(self):
        f = ExamplePlugin._layer_wkid
        assert f({"extent": {"spatialReference": {"latestWkid": 3857}}}) == 3857
        assert f({"extent": {"spatialReference": {"wkid": 102100}}}) == 102100
        assert f({}) is None


class TestGeodesicArea:
    def test_matches_the_analytic_area_of_a_box(self):
        """R^2 * dlon * (sin(lat2) - sin(lat1)) for a lon/lat box."""
        lon1, lat1, lon2, lat2 = -149.90, 61.20, -149.89, 61.21
        box = {
            "type": "Polygon",
            "coordinates": [[[lon1, lat1], [lon1, lat2], [lon2, lat2],
                             [lon2, lat1], [lon1, lat1]]],
        }
        R = ExamplePlugin._EARTH_RADIUS_M
        expected = (
            R * R
            * math.radians(lon2 - lon1)
            * (math.sin(math.radians(lat2)) - math.sin(math.radians(lat1)))
        )
        got = ExamplePlugin._geometry_area_m2(box)
        assert abs(got - abs(expected)) / abs(expected) < 0.001

    def test_subtracts_holes(self):
        outer = [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]
        hole = [[0.4, 0.4], [0.4, 0.6], [0.6, 0.6], [0.6, 0.4], [0.4, 0.4]]
        solid = ExamplePlugin._geometry_area_m2(
            {"type": "Polygon", "coordinates": [outer]})
        holed = ExamplePlugin._geometry_area_m2(
            {"type": "Polygon", "coordinates": [outer, hole]})
        assert holed < solid
        assert abs((solid - holed) / solid - 0.04) < 0.005  # hole is 4%


class TestDiagnosticThresholds:
    def test_zero_artifact_threshold_matches_the_measured_tiles(self):
        """Eastridge SW1433 measured 128/195 (66%) false zeros; Hillside
        SW2436 measured 0/79. The threshold must split those."""
        t = ExamplePlugin.COVERAGE_ZERO_ARTIFACT_RATIO
        assert (128 / 195) > t, "Eastridge must trigger the warning"
        assert (0 / 79) <= t, "Hillside must not"


class TestAllNullShortCircuit:
    def test_all_null_page_is_summarised_not_rendered(self, plugin):
        records = [{"OBJECTID": i, "Owner": None, "Land_Use": None}
                   for i in range(30)]
        text, structured = plugin._format_query_results(records, limit=50)
        assert "are NULL on every one of the 30" in text
        assert "Record 1:" not in text
        assert "get_layer_schema" in text
        # the data is still in the structured half
        assert len(structured["rows"]) == 30

    def test_page_with_any_value_renders_normally(self, plugin):
        records = [{"OBJECTID": 1, "Owner": None},
                   {"OBJECTID": 2, "Owner": "SMITH"}]
        text, _ = plugin._format_query_results(records, limit=50)
        assert "are NULL on every one" not in text
        assert "SMITH" in text
