"""TDD tests for Documentation Loop 258: Add missing docstrings to data_sources.py Format methods.

Critical documentation gaps in packages/pycypher/src/pycypher/ingestion/data_sources.py:
- CsvFormat.name and CsvFormat.view_sql methods lack docstrings
- ParquetFormat.name and ParquetFormat.view_sql methods lack docstrings
- JsonFormat.name and JsonFormat.view_sql methods lack docstrings

These are public methods that should have comprehensive docstrings per D102 (ruff).
This affects developer experience and API documentation generation.
"""

from pycypher.ingestion.data_sources import (
    CsvFormat,
    JsonFormat,
    ParquetFormat,
)

# Current state tests removed after successful documentation improvements.
# The Format methods now have comprehensive docstrings as verified by the tests below.


class TestDocumentationState:
    """Tests for Format methods with comprehensive docstrings.

    These tests verify that all Format method implementations have proper
    docstrings that meet D102 requirements and follow Google-style format.
    """

    def test_fixed_csvformat_name_has_comprehensive_docstring(self):
        """Test that CsvFormat.name has a comprehensive docstring after fix."""
        name_method = CsvFormat.name.fget

        # Should have a docstring
        assert name_method.__doc__ is not None, (
            "CsvFormat.name should have docstring after fix"
        )

        # Should be reasonably comprehensive
        docstring = name_method.__doc__.strip()
        assert len(docstring) >= 20, (
            "CsvFormat.name docstring should be reasonably comprehensive"
        )

        # Should mention it returns "csv"
        assert "csv" in docstring.lower(), (
            "CsvFormat.name docstring should mention it returns 'csv'"
        )

    def test_fixed_csvformat_view_sql_has_comprehensive_docstring(self):
        """Test that CsvFormat.view_sql has a comprehensive docstring after fix."""
        view_sql_method = CsvFormat.view_sql

        # Should have a docstring
        assert view_sql_method.__doc__ is not None, (
            "CsvFormat.view_sql should have docstring after fix"
        )

        # Should be reasonably comprehensive
        docstring = view_sql_method.__doc__.strip()
        assert len(docstring) >= 50, (
            "CsvFormat.view_sql docstring should be reasonably comprehensive"
        )

        # Should mention read_csv_auto and key concepts
        assert "read_csv_auto" in docstring, (
            "CsvFormat.view_sql docstring should mention read_csv_auto"
        )
        assert (
            "delimiter" in docstring.lower() or "header" in docstring.lower()
        ), "CsvFormat.view_sql docstring should mention CSV-specific options"

    def test_fixed_parquetformat_name_has_comprehensive_docstring(self):
        """Test that ParquetFormat.name has a comprehensive docstring after fix."""
        name_method = ParquetFormat.name.fget

        # Should have a docstring
        assert name_method.__doc__ is not None, (
            "ParquetFormat.name should have docstring after fix"
        )

        # Should mention it returns "parquet"
        docstring = name_method.__doc__.strip()
        assert "parquet" in docstring.lower(), (
            "ParquetFormat.name docstring should mention it returns 'parquet'"
        )

    def test_fixed_parquetformat_view_sql_has_comprehensive_docstring(self):
        """Test that ParquetFormat.view_sql has a comprehensive docstring after fix."""
        view_sql_method = ParquetFormat.view_sql

        # Should have a docstring
        assert view_sql_method.__doc__ is not None, (
            "ParquetFormat.view_sql should have docstring after fix"
        )

        # Should mention read_parquet
        docstring = view_sql_method.__doc__.strip()
        assert "read_parquet" in docstring, (
            "ParquetFormat.view_sql docstring should mention read_parquet"
        )

    def test_fixed_jsonformat_name_has_comprehensive_docstring(self):
        """Test that JsonFormat.name has a comprehensive docstring after fix."""
        name_method = JsonFormat.name.fget

        # Should have a docstring
        assert name_method.__doc__ is not None, (
            "JsonFormat.name should have docstring after fix"
        )

        # Should mention it returns "json"
        docstring = name_method.__doc__.strip()
        assert "json" in docstring.lower(), (
            "JsonFormat.name docstring should mention it returns 'json'"
        )

    def test_fixed_jsonformat_view_sql_has_comprehensive_docstring(self):
        """Test that JsonFormat.view_sql has a comprehensive docstring after fix."""
        view_sql_method = JsonFormat.view_sql

        # Should have a docstring
        assert view_sql_method.__doc__ is not None, (
            "JsonFormat.view_sql should have docstring after fix"
        )

        # Should mention read_json_auto and records format
        docstring = view_sql_method.__doc__.strip()
        assert "read_json_auto" in docstring, (
            "JsonFormat.view_sql docstring should mention read_json_auto"
        )
        assert "records" in docstring.lower(), (
            "JsonFormat.view_sql docstring should mention records attribute"
        )

    def test_no_d102_violations_after_documentation_fix(self):
        """Test that D102 (missing docstring) violations are eliminated for Format methods."""
        # All public methods should have docstrings
        format_classes = [CsvFormat, ParquetFormat, JsonFormat]

        for format_class in format_classes:
            # Check name property
            name_method = format_class.name.fget
            assert name_method.__doc__ is not None, (
                f"{format_class.__name__}.name should have docstring after fix"
            )

            # Check view_sql method
            view_sql_method = format_class.view_sql
            assert view_sql_method.__doc__ is not None, (
                f"{format_class.__name__}.view_sql should have docstring after fix"
            )

    def test_docstrings_follow_google_style_format(self):
        """Test that added docstrings follow Google-style format expected by Sphinx."""
        format_classes = [CsvFormat, ParquetFormat, JsonFormat]

        for format_class in format_classes:
            # Check view_sql method docstring format (more complex method)
            view_sql_method = format_class.view_sql
            docstring = view_sql_method.__doc__

            if docstring:
                # Should have proper structure for methods with parameters
                # At minimum should have Args and Returns sections for view_sql
                docstring_lower = docstring.lower()
                assert (
                    "args:" in docstring_lower
                    or "parameters:" in docstring_lower
                ), (
                    f"{format_class.__name__}.view_sql should document its parameters"
                )
                assert (
                    "returns:" in docstring_lower
                    or "return:" in docstring_lower
                ), (
                    f"{format_class.__name__}.view_sql should document its return value"
                )

    def test_docstrings_provide_actionable_information(self):
        """Test that docstrings provide actionable information for developers."""
        # Test a few key methods to ensure they provide useful information

        # CsvFormat.view_sql should explain CSV-specific behavior
        csv_docstring = CsvFormat.view_sql.__doc__
        if csv_docstring:
            # Should mention at least one CSV-specific concept
            docstring_lower = csv_docstring.lower()
            csv_concepts = ["delimiter", "header", "null_padding", "csv"]
            assert any(
                concept in docstring_lower for concept in csv_concepts
            ), (
                "CsvFormat.view_sql docstring should explain CSV-specific behavior"
            )

        # JsonFormat.view_sql should explain JSON-specific behavior
        json_docstring = JsonFormat.view_sql.__doc__
        if json_docstring:
            # Should mention JSON or records concept
            docstring_lower = json_docstring.lower()
            json_concepts = ["json", "records", "newline_delimited", "ndjson"]
            assert any(
                concept in docstring_lower for concept in json_concepts
            ), (
                "JsonFormat.view_sql docstring should explain JSON-specific behavior"
            )
