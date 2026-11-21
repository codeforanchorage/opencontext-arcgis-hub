"""The WHERE / schema validator every query tool passes its filter through.

Moved here from the deployments' tests of the generic arcgis plugin, which
carried the same validator before it became part of this package.
"""

import pytest

from core.interfaces import ToolInputError
from arcgis_hub.where_validator import OutFieldsValidator, WhereValidator


class TestWhereValidator:
    def test_where_validator_blocks_delete(self):
        with pytest.raises(ValueError, match="DELETE"):
            WhereValidator.validate("DELETE FROM x")

    def test_where_validator_allows_valid(self):
        result = WhereValidator.validate("status = 'Active'")
        assert result == "status = 'Active'"

    def test_where_validator_empty(self):
        result = WhereValidator.validate("")
        assert result == "1=1"

    def test_where_validator_does_not_flag_deleted_at(self):
        result = WhereValidator.validate("deleted_at IS NULL")
        assert result == "deleted_at IS NULL"

    # Quote-awareness: forbidden tokens inside quoted string literals
    # are data, not SQL (real street names include "Credit Union Dr")
    # and must pass through unmodified.
    @pytest.mark.parametrize(
        "where",
        [
            "STREET_NAME LIKE '%UNION%'",
            "Owner_Name LIKE '%O''BRIEN%'",
            "Category = 'SHED; GARAGE'",
            "Legal_Description LIKE '%LOT 4 -- REAR%'",
        ],
    )
    def test_forbidden_tokens_inside_literals_allowed(self, where):
        assert WhereValidator.validate(where) == where

    @pytest.mark.parametrize(
        "where",
        [
            "1=1 UNION SELECT 1",
            "1=1; DROP TABLE x",
            "1=1) UNION SELECT * FROM sysobjects --",
            "Owner_Name = 'x' UNION SELECT 1",  # injection AFTER a literal
        ],
    )
    def test_injection_outside_literals_still_rejected(self, where):
        with pytest.raises(ValueError, match="Forbidden"):
            WhereValidator.validate(where)

    def test_unbalanced_quote_rejected(self):
        with pytest.raises(ValueError, match="Unbalanced quote"):
            WhereValidator.validate("Category = 'ABC")


class TestWhereValidatorAgainstSchema:
    def test_default_where_skipped(self):
        WhereValidator.validate_against_schema("1=1", {"STATUS"})
        WhereValidator.validate_against_schema("", {"STATUS"})

    def test_empty_allowed_fields_skips(self):
        WhereValidator.validate_against_schema("FOO='x'", None)
        WhereValidator.validate_against_schema("FOO='x'", set())

    def test_valid_field_passes(self):
        WhereValidator.validate_against_schema(
            "STATUS='Active'", {"STATUS", "NAME"}
        )

    @pytest.mark.parametrize(
        "where",
        [
            "CAST(WellDepth AS INTEGER) > 500",
            "CAST(WellDepth AS FLOAT) >= 0.5 AND NAME <> ''",
            "cast(WellDepth as int) > 0",
        ],
    )
    def test_cast_type_names_are_not_fields(self, where):
        """CAST(field AS INTEGER) is the only way to compare a text-typed
        numeric field to a number on ArcGIS Server; the type name must
        not be mistaken for a (missing) field reference."""
        WhereValidator.validate_against_schema(where, {"WellDepth", "NAME"})

    def test_cast_still_checks_the_real_field(self):
        with pytest.raises(ValueError, match="WellDeep"):
            WhereValidator.validate_against_schema(
                "CAST(WellDeep AS INTEGER) > 0", {"WellDepth"}
            )

    def test_typo_field_raises_with_suggestion(self):
        with pytest.raises(ValueError, match="STATUUS"):
            WhereValidator.validate_against_schema(
                "STATUUS='Active'", {"STATUS", "NAME"}
            )

    def test_typo_field_includes_closest_match(self):
        try:
            WhereValidator.validate_against_schema(
                "STATUUS='Active'", {"STATUS", "NAME"}
            )
        except ValueError as e:
            assert "STATUS" in str(e)
            assert "did you mean" in str(e).lower()

    def test_unknown_field_no_close_match(self):
        with pytest.raises(ValueError, match="XYZQR"):
            WhereValidator.validate_against_schema(
                "XYZQR='foo'", {"STATUS", "NAME"}
            )

    def test_sql_keywords_not_treated_as_fields(self):
        # IS, NULL, AND, OR, IN, LIKE — all SQL keywords, not fields.
        WhereValidator.validate_against_schema(
            "STATUS IS NULL AND NAME LIKE 'A%'", {"STATUS", "NAME"}
        )

    def test_case_sensitive_field_match(self):
        # ArcGIS field names are case-sensitive. STATUS != status.
        with pytest.raises(ValueError, match="status"):
            WhereValidator.validate_against_schema(
                "status='Active'", {"STATUS"}
            )

    def test_unknown_field_error_ends_with_the_schema_tool(self):
        with pytest.raises(ToolInputError) as raised:
            WhereValidator.validate_against_schema("FOO='x'", {"STATUS"})
        assert str(raised.value).endswith(
            "Call get_layer_schema to see all available field names."
        )

    def test_a_server_without_that_tool_passes_its_own_hint(self):
        """A deployment that uses the validators but is not an
        ArcGISHubPlugin (anchorage-parcels-mcp) has no get_layer_schema."""
        hint = "Use query_parcels with out_fields='*' to see every field."
        with pytest.raises(ToolInputError) as raised:
            WhereValidator.validate_against_schema(
                "STATU='x'", {"STATUS"}, schema_hint=hint
            )
        message = str(raised.value)
        assert message.endswith(hint)
        assert "get_layer_schema" not in message
        # The did-you-mean is the validator's own and is kept.
        assert "did you mean 'STATUS'" in message

    def test_string_values_not_treated_as_fields(self):
        # The literal 'Park' must not be flagged as an unknown field.
        WhereValidator.validate_against_schema(
            "NAME='Park'", {"NAME"}
        )

    def test_function_call_field_inside(self):
        # UPPER is a keyword; STATUS inside is the real field.
        WhereValidator.validate_against_schema(
            "UPPER(STATUS)='ACTIVE'", {"STATUS"}
        )


class TestFieldNamesThatLookLikeAttacks:
    """Blocklist tokens matched as bare substrings refused real fields."""

    @pytest.mark.parametrize(
        "where",
        [
            "INSP_DATE > DATE '2024-01-01'",
            "EXP_DATE IS NOT NULL",
            "Disp_Code = 'A'",
            "Box0x = 1",
            "EXTRACT(YEAR FROM INSP_DATE) = 2024",
            "extract( month from INSP_DATE ) = 6",
        ],
    )
    def test_accepted(self, where):
        assert WhereValidator.validate(where) == where

    @pytest.mark.parametrize(
        "where",
        [
            "1=1 OR xp_cmdshell('dir') = 1",
            "sp_executesql = 1",
            "OBJECTID = 0x1F",
            "NAME = (SELECT 1 FROM t)",
            "OBJECTID IN (1) FROM t",
            "EXTRACT(YEAR FROM d) = 1 FROM t",
        ],
    )
    def test_still_refused(self, where):
        with pytest.raises(ToolInputError):
            WhereValidator.validate(where)

    @pytest.mark.parametrize("bad", [5, ["a"], {"a": 1}])
    def test_non_string_is_a_caller_error(self, bad):
        with pytest.raises(ToolInputError, match="must be a string"):
            WhereValidator.validate(bad)

    @pytest.mark.parametrize(
        "where",
        [
            "EXTRACT(YEAR FROM INSP_DATE) = 2024",
            "INSP_DATE > CURRENT_TIMESTAMP - INTERVAL '7' DAY",
            "MOD(OBJECTID, 2) = 0",
            "CHAR_LENGTH(NAME) > 3",
        ],
    )
    def test_standard_functions_are_not_read_as_fields(self, where):
        WhereValidator.validate_against_schema(
            WhereValidator.validate(where),
            ["INSP_DATE", "OBJECTID", "NAME"],
        )

    def test_out_fields_accepts_a_list(self):
        assert OutFieldsValidator.validate(["A", "B"]) == "A,B"
        with pytest.raises(ToolInputError):
            OutFieldsValidator.validate(5)
