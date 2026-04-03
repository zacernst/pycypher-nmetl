"""Input validation for multi-query composition.

Validates the list of ``(query_id, cypher_string)`` pairs before they
enter the composition pipeline.  Checks performed:

- **Query ID uniqueness** — duplicate IDs are rejected
- **Non-empty content** — empty or whitespace-only Cypher strings are rejected
- **Non-empty query IDs** — blank query IDs are rejected
- **Parseability** — each Cypher string must be parseable by :class:`ASTConverter`

All errors are collected (not fail-fast) for complete diagnostics.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from shared.logger import LOGGER


@dataclass
class InputValidationResult:
    """Result of validating multi-query inputs.

    Attributes:
        is_valid: ``True`` if no validation errors were found.
        errors: List of human-readable error descriptions.

    """

    is_valid: bool = True
    errors: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        """Human-readable summary of validation result."""
        if self.is_valid:
            return "Valid"
        lines = [f"Invalid ({len(self.errors)} error(s)):"]
        for err in self.errors:
            lines.append(f"  - {err}")
        return "\n".join(lines)


class InputValidator:
    """Validates multi-query inputs before composition.

    Collects all errors rather than failing on the first, providing
    a complete diagnostic picture.
    """

    def validate(
        self,
        queries: list[tuple[str, str]],
    ) -> InputValidationResult:
        """Validate a list of (query_id, cypher_string) pairs.

        Args:
            queries: The query pairs to validate.

        Returns:
            An :class:`InputValidationResult` with ``is_valid`` and ``errors``.

        """
        errors: list[str] = []

        if not queries:
            return InputValidationResult(is_valid=True, errors=[])

        self._check_query_ids(queries, errors)
        self._check_content(queries, errors)
        self._check_parseability(queries, errors)

        if errors:
            LOGGER.warning(
                "input validation failed: %d error(s) for %d queries",
                len(errors),
                len(queries),
            )
            for err in errors:
                LOGGER.debug("  validation error: %s", err)
        else:
            LOGGER.debug(
                "input validation passed for %d queries", len(queries)
            )

        return InputValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
        )

    def _check_query_ids(
        self,
        queries: list[tuple[str, str]],
        errors: list[str],
    ) -> None:
        """Check for duplicate and empty query IDs."""
        seen: set[str] = set()
        for query_id, _ in queries:
            if not query_id or not query_id.strip():
                errors.append(f"Query ID is empty or blank: {query_id!r}")
            elif query_id in seen:
                errors.append(f"Duplicate query ID: {query_id!r}")
            seen.add(query_id)

    def _check_content(
        self,
        queries: list[tuple[str, str]],
        errors: list[str],
    ) -> None:
        """Check for empty or whitespace-only Cypher strings."""
        for query_id, cypher in queries:
            if not cypher or not cypher.strip():
                errors.append(
                    f"Query '{query_id}' has empty or whitespace-only "
                    f"Cypher content",
                )

    def _check_parseability(
        self,
        queries: list[tuple[str, str]],
        errors: list[str],
    ) -> None:
        """Check that each non-empty Cypher string is parseable."""
        from lark.exceptions import LarkError

        from pycypher.ast_models import ASTConverter
        from pycypher.exceptions import ASTConversionError

        for query_id, cypher in queries:
            if not cypher or not cypher.strip():
                continue  # Already reported by _check_content
            try:
                ASTConverter.from_cypher(cypher)
            except (ASTConversionError, LarkError) as exc:
                LOGGER.warning(
                    "query '%s' failed to parse: %s",
                    query_id,
                    exc,
                )
                errors.append(f"Query '{query_id}' failed to parse: {exc}")
