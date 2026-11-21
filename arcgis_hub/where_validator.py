"""Input validators for ArcGIS Feature Service queries.

Hardens SQL WHERE clauses, out_fields lists, and order_by expressions
against injection before they are forwarded to an ArcGIS REST endpoint.
"""

import difflib
import re
from typing import Iterable, Optional
from core.interfaces import ToolInputError


class WhereValidator:
    """Validates WHERE clause strings for Feature Service queries."""

    FORBIDDEN_KEYWORDS = [
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "TRUNCATE",
        "ALTER",
        "CREATE",
        "EXEC",
        "EXECUTE",
        "UNION",
        "SELECT",
        "FROM",
        "JOIN",
        "INTO",
        "EXISTS",
        "MERGE",
        "GRANT",
        "REVOKE",
        "DECLARE",
        "SLEEP",
        "WAITFOR",
        "BENCHMARK",
    ]

    FORBIDDEN_SUBSTRINGS = [
        ";",
        "--",
        "/*",
        "*/",
        "@@",
    ]

    # Tokens that are only suspicious at the START of a word: the
    # SQL Server procedure prefixes and a hex literal. Matched as bare
    # substrings they refused ordinary field names -- INSP_DATE,
    # EXP_DATE and Disp_Code all contain "sp_" or "xp_".
    FORBIDDEN_PREFIXES = [
        (re.compile(r"\bxp_", re.IGNORECASE), "xp_"),
        (re.compile(r"\bsp_", re.IGNORECASE), "sp_"),
        (re.compile(r"\b0x[0-9a-f]", re.IGNORECASE), "0x"),
    ]

    # EXTRACT(<unit> FROM <field>) is standard ArcGIS SQL and the only
    # legitimate FROM in a WHERE clause; it is blanked before the
    # keyword scan so the scan still refuses every other FROM.
    _EXTRACT_FROM_RE = re.compile(
        r"\bEXTRACT\s*\(\s*"
        r"(YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)\s+FROM\b",
        re.IGNORECASE,
    )

    MAX_LENGTH = 2000

    @staticmethod
    def _mask_string_literals(where: str) -> str:
        """Blank out the contents of '...' literals so the forbidden-token
        scan only sees SQL code, never quoted data values (a legitimate
        address like '%UNION%' or owner 'SMITH; JONES' must not trip it).

        Handles the SQL-standard '' escape for a literal quote. Raises on
        an unbalanced quote -- itself a good reason to reject the clause.
        """
        out = []
        i, n = 0, len(where)
        in_str = False
        while i < n:
            c = where[i]
            if not in_str:
                out.append(c)
                if c == "'":
                    in_str = True
                i += 1
            elif c == "'":
                if i + 1 < n and where[i + 1] == "'":
                    i += 2  # escaped '' inside the literal
                    continue
                in_str = False
                out.append(c)
                i += 1
            else:
                i += 1  # mask literal content
        if in_str:
            raise ToolInputError("Unbalanced quote in WHERE clause")
        return "".join(out)

    @classmethod
    def validate(cls, where: str) -> str:
        """Validate and sanitize a WHERE clause string.

        Args:
            where: SQL WHERE clause string

        Returns:
            The original WHERE clause if valid, or "1=1" if empty/None

        Raises:
            ValueError: If the clause contains forbidden SQL keywords or
                suspicious substrings (stacked queries, comments, etc.)
                outside of quoted string literals, or an unbalanced
                quote.
        """
        if where is None or where == "":
            return "1=1"
        if not isinstance(where, str):
            raise ToolInputError(
                f"WHERE clause must be a string "
                f"(got {type(where).__name__})"
            )

        where = where.strip()
        if not where:
            return "1=1"

        if len(where) > cls.MAX_LENGTH:
            raise ToolInputError(
                f"WHERE clause exceeds max length ({cls.MAX_LENGTH} chars)"
            )

        # Scan a copy with string-literal contents masked out: quoted
        # values are data, not SQL, and must never trip the blocklist.
        masked = cls._mask_string_literals(where)

        lowered = masked.lower()
        for bad in cls.FORBIDDEN_SUBSTRINGS:
            if bad.lower() in lowered:
                raise ToolInputError(
                    f"Forbidden substring {bad!r} detected in WHERE clause"
                )
        for pattern, label in cls.FORBIDDEN_PREFIXES:
            if pattern.search(masked):
                raise ToolInputError(
                    f"Forbidden substring {label!r} detected in WHERE "
                    f"clause"
                )

        masked_upper = cls._EXTRACT_FROM_RE.sub(
            "EXTRACT(", masked
        ).upper()
        for keyword in cls.FORBIDDEN_KEYWORDS:
            if re.search(rf"\b{keyword}\b", masked_upper):
                raise ToolInputError(
                    f"Forbidden keyword '{keyword}' detected in WHERE clause"
                )

        return where

    # Reserved SQL/Esri tokens that look like identifiers but aren't
    # field references. Anything outside this set that survives literal
    # stripping is treated as a candidate field name and checked against
    # the layer schema in ``validate_against_schema``.
    SQL_RESERVED = frozenset(
        {
            "AND", "OR", "NOT", "BETWEEN", "IN", "LIKE", "ESCAPE", "IS",
            "NULL", "TRUE", "FALSE",
            "DATE", "TIMESTAMP", "TIME", "INTERVAL", "FROM",
            "CURRENT_DATE", "CURRENT_TIMESTAMP", "CURRENT_TIME",
            "MOD", "POWER", "CHAR_LENGTH", "CONCAT",
            "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
            "CASE", "WHEN", "THEN", "ELSE", "END",
            "UPPER", "LOWER", "TRIM", "LTRIM", "RTRIM",
            "LENGTH", "LEN", "SUBSTRING", "SUBSTR",
            "CHARINDEX", "POSITION", "COALESCE", "NULLIF",
            "CAST", "AS", "EXTRACT", "TO_DATE", "TO_TIMESTAMP",
            # Type names inside CAST(field AS <type>); ArcGIS standardized
            # SQL accepts these. Without them "INTEGER" is read as a
            # field reference and CAST() -- the only way to compare a
            # text-typed numeric field to a number -- is rejected.
            "INTEGER", "INT", "SMALLINT", "BIGINT", "FLOAT", "DOUBLE",
            "REAL", "NUMERIC", "DECIMAL", "VARCHAR", "CHAR", "STRING",
            "TEXT", "BOOLEAN", "DATETIME",
            "ABS", "ROUND", "CEIL", "CEILING", "FLOOR",
            "MIN", "MAX", "SUM", "AVG", "COUNT", "STDDEV",
            "ANY", "ALL", "SOME", "DISTINCT",
        }
    )

    _STRING_LITERAL_RE = re.compile(r"'(?:[^']|'')*'")
    _NUM_LITERAL_RE = re.compile(r"\b\d+(?:\.\d+)?\b")
    _IDENT_RE = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")

    # Closing sentence of an unknown-field error: where the caller can
    # see the real field names. The default names the tool every
    # ArcGISHubPlugin deployment has. A server without that tool passes
    # its own ``schema_hint``; a caller following advice to call a tool
    # that does not exist there gets "Unknown tool" back.
    DEFAULT_SCHEMA_HINT = (
        "Call get_layer_schema to see all available field names."
    )

    @classmethod
    def validate_against_schema(
        cls,
        where: str,
        allowed_fields: Optional[Iterable[str]],
        schema_hint: Optional[str] = None,
    ) -> None:
        """Verify every identifier in ``where`` is a real layer field.

        Catches typo'd field names -- a hallucination magnet for LLMs --
        before they hit ArcGIS, which would otherwise return a cryptic
        ``Unable to perform query`` error. On unknown identifiers we
        raise with a ``difflib`` suggestion so the caller (often a
        model) can self-correct.

        Args:
            where: A WHERE clause that has already passed
                ``WhereValidator.validate``.
            allowed_fields: Field names from the layer schema. Field
                names in ArcGIS are case-sensitive. Pass ``None`` /
                empty to skip the check (e.g. when the schema fetch
                failed -- graceful degradation).
            schema_hint: Sentence that ends the error, telling the
                caller how to see the field names on THIS server.
                Defaults to ``DEFAULT_SCHEMA_HINT``.

        Raises:
            ValueError: When the WHERE references an identifier that
                isn't in ``allowed_fields`` and isn't a SQL keyword or
                function from ``SQL_RESERVED``.
        """
        if not where:
            return
        stripped_where = where.strip()
        if not stripped_where or stripped_where == "1=1":
            return
        if not allowed_fields:
            return

        allowed_set = {f for f in allowed_fields if f}
        if not allowed_set:
            return

        # Drop string literals first so values like 'Park' don't get
        # mis-tokenized as field names. Then strip numeric literals.
        no_strings = cls._STRING_LITERAL_RE.sub("", where)
        no_numbers = cls._NUM_LITERAL_RE.sub("", no_strings)
        candidates = set(cls._IDENT_RE.findall(no_numbers))
        candidates = {
            c for c in candidates if c.upper() not in cls.SQL_RESERVED
        }
        unknown = sorted(c for c in candidates if c not in allowed_set)
        if not unknown:
            return

        sorted_allowed = sorted(allowed_set)
        parts = []
        for u in unknown:
            suggestions = difflib.get_close_matches(
                u, sorted_allowed, n=1, cutoff=0.6
            )
            if suggestions:
                parts.append(
                    f"Field {u!r} not found in this layer -- did you "
                    f"mean {suggestions[0]!r}? (Field names are "
                    f"case-sensitive.)"
                )
            else:
                parts.append(
                    f"Field {u!r} not found in this layer."
                )
        parts.append(schema_hint or cls.DEFAULT_SCHEMA_HINT)
        raise ToolInputError(" ".join(parts))


class OutFieldsValidator:
    """Validates an ArcGIS ``outFields`` parameter.

    Accepts ``*`` or a comma-separated list of bare field identifiers.
    Rejects anything with whitespace, operators, or non-identifier chars
    to keep this surface from becoming an injection sink.
    """

    _IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    MAX_FIELDS = 100

    @classmethod
    def validate(cls, out_fields: str) -> str:
        if out_fields is None:
            return "*"
        if isinstance(out_fields, (list, tuple)) and all(
            isinstance(f, str) for f in out_fields
        ):
            out_fields = ",".join(out_fields)
        if not isinstance(out_fields, str):
            raise ToolInputError(
                f"out_fields must be a comma-separated string "
                f"(got {type(out_fields).__name__})"
            )
        value = out_fields.strip()
        if not value or value == "*":
            return "*"

        parts = [p.strip() for p in value.split(",")]
        if len(parts) > cls.MAX_FIELDS:
            raise ToolInputError(
                f"out_fields exceeds max of {cls.MAX_FIELDS} fields"
            )
        for part in parts:
            if not cls._IDENT.match(part):
                raise ToolInputError(
                    f"Invalid field name in out_fields: {part!r}"
                )
        return ",".join(parts)


class OrderByValidator:
    """Validates an ArcGIS ``orderByFields`` parameter.

    Accepts one or more comma-separated ``<field>[ ASC|DESC]`` entries.
    """

    _ENTRY = re.compile(
        r"^[A-Za-z_][A-Za-z0-9_]*(\s+(ASC|DESC))?$",
        re.IGNORECASE,
    )
    MAX_FIELDS = 10

    @classmethod
    def validate(cls, order_by: str) -> str:
        if not order_by:
            return ""
        if not isinstance(order_by, str):
            raise ToolInputError(
                f"order_by must be a string "
                f"(got {type(order_by).__name__})"
            )
        value = order_by.strip()
        if not value:
            return ""

        parts = [p.strip() for p in value.split(",")]
        if len(parts) > cls.MAX_FIELDS:
            raise ToolInputError(
                f"order_by exceeds max of {cls.MAX_FIELDS} fields"
            )
        for part in parts:
            if not cls._ENTRY.match(part):
                raise ToolInputError(f"Invalid order_by entry: {part!r}")
        return ",".join(parts)
