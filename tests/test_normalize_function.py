"""TDD tests for the normalize() scalar function.

These tests are written *before* the implementation (TDD red phase).

Run with:
    uv run pytest tests/test_normalize_function.py -v
"""

from __future__ import annotations

import unicodedata

import pandas as pd
import pytest
from pycypher.scalar_functions import ScalarFunctionRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reg() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


def _s(*values: object) -> pd.Series:
    return pd.Series(list(values), dtype=object)


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestNormalizeRegistered:
    def test_normalize_is_registered(self) -> None:
        """normalize must be in the scalar function registry."""
        assert _reg().has_function("normalize"), (
            "Expected 'normalize' to be registered in ScalarFunctionRegistry"
        )


# ---------------------------------------------------------------------------
# Default form (NFC)
# ---------------------------------------------------------------------------


class TestNormalizeDefaultNFC:
    def test_already_nfc_is_idempotent(self) -> None:
        """normalize('café') returns the same string when input is already NFC."""
        cafe_nfc = unicodedata.normalize("NFC", "café")
        result = _reg().execute("normalize", [_s(cafe_nfc)])
        assert result.iloc[0] == cafe_nfc

    def test_decomposed_input_composed_to_nfc(self) -> None:
        """normalize('cafe\\u0301') returns the NFC single-codepoint form."""
        decomposed = "cafe\u0301"  # e + combining accent
        expected = unicodedata.normalize("NFC", decomposed)
        result = _reg().execute("normalize", [_s(decomposed)])
        assert result.iloc[0] == expected
        # NFC form is shorter (precomposed é)
        assert len(result.iloc[0]) < len(decomposed)

    def test_plain_ascii_unchanged(self) -> None:
        """ASCII strings are unchanged under NFC normalization."""
        result = _reg().execute("normalize", [_s("hello")])
        assert result.iloc[0] == "hello"


# ---------------------------------------------------------------------------
# Explicit NFC
# ---------------------------------------------------------------------------


class TestNormalizeExplicitNFC:
    def test_explicit_nfc_same_as_default(self) -> None:
        """normalize(s, 'NFC') equals normalize(s) with no form argument."""
        s = "cafe\u0301"
        r_default = _reg().execute("normalize", [_s(s)])
        r_explicit = _reg().execute("normalize", [_s(s), _s("NFC")])
        assert r_default.iloc[0] == r_explicit.iloc[0]


# ---------------------------------------------------------------------------
# NFD
# ---------------------------------------------------------------------------


class TestNormalizeNFD:
    def test_nfd_decomposes_precomposed_char(self) -> None:
        """normalize('café', 'NFD') decomposes é into base + combining accent."""
        precomposed = unicodedata.normalize("NFC", "café")  # single é
        result = _reg().execute("normalize", [_s(precomposed), _s("NFD")])
        decomposed = unicodedata.normalize("NFD", precomposed)
        assert result.iloc[0] == decomposed
        # NFD is longer (base + combining char)
        assert len(result.iloc[0]) > len(precomposed)


# ---------------------------------------------------------------------------
# NFKC / NFKD
# ---------------------------------------------------------------------------


class TestNormalizeNFKC:
    def test_nfkc_expands_ligature(self) -> None:
        """normalize('ﬃ', 'NFKC') expands the fi-ligature to 'ffi'."""
        result = _reg().execute("normalize", [_s("\ufb03"), _s("NFKC")])
        assert result.iloc[0] == "ffi"


class TestNormalizeNFKD:
    def test_nfkd_expands_ligature(self) -> None:
        """normalize('ﬃ', 'NFKD') also expands the fi-ligature to 'ffi'."""
        result = _reg().execute("normalize", [_s("\ufb03"), _s("NFKD")])
        assert result.iloc[0] == "ffi"


# ---------------------------------------------------------------------------
# NFKCCaseFold
# ---------------------------------------------------------------------------


class TestNormalizeNFKCCaseFold:
    def test_casefold_lowercases(self) -> None:
        """normalize('CAFÉ', 'NFKCCaseFold') produces a lowercased NFC form."""
        result = _reg().execute("normalize", [_s("CAFÉ"), _s("NFKCCaseFold")])
        expected = unicodedata.normalize("NFKC", "CAFÉ").casefold()
        assert result.iloc[0] == expected

    def test_casefold_german_sharp_s(self) -> None:
        """normalize('ß', 'NFKCCaseFold') expands German sharp s to 'ss'."""
        result = _reg().execute("normalize", [_s("ß"), _s("NFKCCaseFold")])
        # casefold() maps ß → ss
        assert result.iloc[0] == "ss"

    def test_casefold_case_insensitive_form_name(self) -> None:
        """normalize(s, 'nfkccasefold') works regardless of form name casing."""
        result = _reg().execute("normalize", [_s("HELLO"), _s("nfkccasefold")])
        assert result.iloc[0] == "hello"


# ---------------------------------------------------------------------------
# Null propagation
# ---------------------------------------------------------------------------


class TestNormalizeNullPropagation:
    def test_null_input_returns_null(self) -> None:
        """normalize(null) returns null."""
        result = _reg().execute("normalize", [_s(None)])
        assert pd.isna(result.iloc[0])

    def test_null_input_with_explicit_form_returns_null(self) -> None:
        """normalize(null, 'NFC') returns null."""
        result = _reg().execute("normalize", [_s(None), _s("NFC")])
        assert pd.isna(result.iloc[0])

    def test_mixed_null_and_non_null_rows(self) -> None:
        """normalize propagates null correctly across a multi-row series."""
        result = _reg().execute(
            "normalize", [_s("cafe\u0301", None, "\ufb03")]
        )
        assert result.iloc[0] == unicodedata.normalize("NFC", "cafe\u0301")
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == unicodedata.normalize("NFC", "\ufb03")


# ---------------------------------------------------------------------------
# Error: invalid form
# ---------------------------------------------------------------------------


class TestNormalizeInvalidForm:
    def test_invalid_form_raises(self) -> None:
        """normalize(s, 'INVALID') raises ValueError or RuntimeError."""
        with pytest.raises((ValueError, RuntimeError)):
            _reg().execute("normalize", [_s("hello"), _s("INVALID")])


# ---------------------------------------------------------------------------
# Column-level test
# ---------------------------------------------------------------------------


class TestNormalizeColumnInput:
    def test_all_rows_normalised(self) -> None:
        """normalize works correctly over a full column."""
        inputs = ["cafe\u0301", "ﬃre", "résumé"]
        expected = [unicodedata.normalize("NFC", v) for v in inputs]
        result = _reg().execute("normalize", [_s(*inputs)])
        assert list(result) == expected


# ---------------------------------------------------------------------------
# Integration: invoked from Cypher execute_query
# ---------------------------------------------------------------------------


class TestNormalizeCypherIntegration:
    def test_normalize_in_return(self) -> None:
        """RETURN normalize('cafe\\u0301') AS n returns the NFC form."""
        from pycypher.relational_models import (
            Context,
            EntityMapping,
            RelationshipMapping,
        )
        from pycypher.star import Star

        star = Star(
            context=Context(
                entity_mapping=EntityMapping(mapping={}),
                relationship_mapping=RelationshipMapping(mapping={}),
            )
        )
        result = star.execute_query("RETURN normalize('cafe\u0301') AS n")
        assert len(result) == 1
        assert result["n"].iloc[0] == unicodedata.normalize(
            "NFC", "cafe\u0301"
        )

    def test_normalize_with_form_in_return(self) -> None:
        """RETURN normalize('ﬃ', 'NFKC') AS n returns 'ffi'."""
        from pycypher.relational_models import (
            Context,
            EntityMapping,
            RelationshipMapping,
        )
        from pycypher.star import Star

        star = Star(
            context=Context(
                entity_mapping=EntityMapping(mapping={}),
                relationship_mapping=RelationshipMapping(mapping={}),
            )
        )
        result = star.execute_query("RETURN normalize('\ufb03', 'NFKC') AS n")
        assert len(result) == 1
        assert result["n"].iloc[0] == "ffi"
