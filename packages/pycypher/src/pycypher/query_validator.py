"""Combined query validation for multi-query composition.

Validates combined Cypher query ASTs for structural integrity:

- **Clause ordering** — RETURN must be the final clause if present
- **RETURN uniqueness** — at most one RETURN clause allowed
- **Structural consistency** — clauses follow valid Cypher sequencing rules

All validation errors are collected (not fail-fast) so the user receives
a complete diagnostic picture.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from shared.logger import LOGGER

from pycypher.ast_models import (
    Create,
    Match,
    Merge,
    Query,
    Return,
    With,
)


@dataclass
class ValidationResult:
    """Result of validating a combined Cypher query AST.

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


class CombinedQueryValidator:
    """Validates combined Cypher query ASTs for structural integrity.

    Collects all validation errors rather than failing on the first,
    providing a complete diagnostic picture.
    """

    def validate(self, query: Query) -> ValidationResult:
        """Validate a combined Query AST.

        Args:
            query: The :class:`~pycypher.ast_models.Query` to validate.

        Returns:
            A :class:`ValidationResult` with ``is_valid`` and ``errors``.

        """
        errors: list[str] = []

        if not query.clauses:
            return ValidationResult(is_valid=True, errors=[])

        self._check_return_count(query, errors)
        self._check_return_position(query, errors)
        self._check_clause_ordering(query, errors)

        if errors:
            LOGGER.warning(
                "combined query validation failed: %d error(s), %d clauses",
                len(errors),
                len(query.clauses),
            )
            for err in errors:
                LOGGER.debug("  validation error: %s", err)
        else:
            LOGGER.debug(
                "combined query validation passed: %d clauses",
                len(query.clauses),
            )

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
        )

    def _check_return_count(
        self,
        query: Query,
        errors: list[str],
    ) -> None:
        """Ensure at most one RETURN clause exists."""
        return_count = sum(1 for c in query.clauses if isinstance(c, Return))
        if return_count > 1:
            errors.append(
                f"Multiple RETURN clauses found ({return_count}). "
                f"A combined query must have at most one RETURN clause.",
            )

    def _check_return_position(
        self,
        query: Query,
        errors: list[str],
    ) -> None:
        """Ensure RETURN is the last clause if present."""
        for i, clause in enumerate(query.clauses):
            if isinstance(clause, Return) and i != len(query.clauses) - 1:
                errors.append(
                    f"RETURN clause at position {i} is not the final "
                    f"clause. RETURN must be the last clause in a "
                    f"combined query.",
                )

    def _check_clause_ordering(
        self,
        query: Query,
        errors: list[str],
    ) -> None:
        """Check that read clauses don't appear before write context.

        Specifically: a bare RETURN before any MATCH/CREATE is suspicious
        and likely a construction error.
        """
        # Track whether we've seen a data-producing clause
        _data_clause_types = (Match, Create, Merge, With)
        seen_data_clause = False

        for clause in query.clauses:
            if isinstance(clause, _data_clause_types):
                seen_data_clause = True
            elif isinstance(clause, Return) and not seen_data_clause:
                errors.append(
                    "RETURN clause appears before any MATCH, CREATE, "
                    "MERGE, or WITH clause. This is likely a construction "
                    "error in the combined query.",
                )
