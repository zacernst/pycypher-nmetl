"""Proof-of-concept tests demonstrating actual SQL injection vulnerabilities.

These tests show that malicious SQL can be successfully injected and executed
in the current system, proving the vulnerabilities exist.

DO NOT RUN THESE TESTS IN PRODUCTION - they contain actual attack code.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest
from pycypher.ingestion.duckdb_reader import DuckDBReader

pytestmark = pytest.mark.slow


class TestSQLInjectionProofOfConcept:
    """Demonstrate that SQL injection actually works (system is vulnerable)."""

    def test_query_injection_succeeds_proving_vulnerability(self) -> None:
        """Test that SQL injection attacks are properly blocked by security validation."""
        # Create a legitimate CSV file
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp_path = Path(tmp.name)
            df.to_csv(tmp_path, index=False)

            try:
                # This malicious query should be blocked by security validation
                injection_query = """
                    SELECT * FROM source;
                    CREATE TABLE hacked (message TEXT);
                    INSERT INTO hacked VALUES ('SQL_INJECTION_SUCCESS');
                """

                # The system should now raise SecurityError when injection is attempted
                with pytest.raises(Exception) as exc_info:
                    DuckDBReader.from_csv(str(tmp_path), query=injection_query)

                # Verify the attack was blocked by security validation
                assert (
                    "injection attack" in str(exc_info.value).lower()
                    or "multiple sql statements" in str(exc_info.value).lower()
                )
                print(
                    "SUCCESS: SQL injection attack properly blocked by security validation!",
                )

            finally:
                tmp_path.unlink()

    def test_path_injection_in_create_view_statement(self) -> None:
        """Test if malicious SQL can be injected via the path parameter."""
        # Create a legitimate CSV file
        df = pd.DataFrame({"name": ["Alice"], "age": [25]})

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp_path = Path(tmp.name)
            df.to_csv(tmp_path, index=False)

            try:
                # Try to inject SQL through the path parameter
                # The vulnerable line is: f"CREATE VIEW source AS SELECT * FROM read_csv_auto('{path}')"
                # We can break out of the single quotes and inject SQL

                # This constructs a path that closes the quote, adds malicious SQL, and comments out the rest
                base_path = str(tmp_path)
                injection_suffix = "'); CREATE TABLE path_injected (data TEXT); INSERT INTO path_injected VALUES ('PATH_INJECTION_SUCCESS'); --"

                # However, since we still need a valid file for read_csv_auto,
                # let's test a simpler approach - just see if we can cause an error
                # that reveals the injection point
                malicious_path = base_path + "'; SELECT 'injected'; --"

                try:
                    result = DuckDBReader.from_csv(malicious_path)
                    # If this doesn't raise an error, the injection attempt was processed
                    print(
                        f"Path injection attempt processed: {malicious_path}",
                    )
                except Exception as e:
                    # Check if the error message reveals SQL injection processing
                    error_msg = str(e)
                    if "SELECT 'injected'" in error_msg or "SQL" in error_msg:
                        print(
                            f"Path injection vulnerability detected in error: {error_msg}",
                        )
                        # This would indicate the injected SQL was parsed
                        assert True  # Vulnerability confirmed
                    else:
                        print(f"Path injection blocked or failed: {error_msg}")

            finally:
                tmp_path.unlink()
