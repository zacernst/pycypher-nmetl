import pytest
import sys
from io import StringIO
from unittest.mock import patch, MagicMock
from pycypher.grammar_parser import main, GrammarParser

class TestParserCLI:
    """Test the command-line interface of the parser."""

    def test_main_parse_print(self):
        """Test main() with simple parsing and printing tree."""
        query = "MATCH (n) RETURN n"
        with patch('sys.argv', ['parser.py', query]), \
             patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            main()
            output = mock_stdout.getvalue()
            assert "match_clause" in output
            assert "return_clause" in output

    def test_main_ast_json(self):
        """Test main() with --ast and --json."""
        query = "MATCH (n) RETURN n"
        with patch('sys.argv', ['parser.py', '--ast', '--json', query]), \
             patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            main()
            output = mock_stdout.getvalue()
            assert '"type": "Query"' in output
            # Check for MatchClause or QueryStatement since actual output structure is complex
            assert '"type": "QueryStatement"' in output
            assert "Match" in output # Matches "MatchClause" or similar

    def test_main_validate_success(self):
        """Test main() with --validate for valid query."""
        query = "MATCH (n) RETURN n"
        with patch('sys.argv', ['parser.py', '--validate', query]), \
             patch('sys.stdout', new_callable=StringIO) as mock_stdout, \
             patch('sys.exit') as mock_exit:
            main()
            output = mock_stdout.getvalue()
            assert "Valid" in output
            mock_exit.assert_called_with(0)

    def test_main_validate_failure(self):
        """Test main() with --validate for invalid query."""
        query = "INVALID QUERY SYNTAX"
        with patch('sys.argv', ['parser.py', '--validate', query]), \
             patch('sys.stdout', new_callable=StringIO) as mock_stdout, \
             patch('sys.stderr', new_callable=StringIO) as mock_stderr, \
             patch('sys.exit') as mock_exit:
            main()
            output = mock_stdout.getvalue()
            assert "Invalid" in output
            mock_exit.assert_called_with(1)

    def test_main_file_input(self, tmp_path):
        """Test main() reading from file."""
        query_file = tmp_path / "query.cypher"
        query_file.write_text("MATCH (n) RETURN n")
        
        with patch('sys.argv', ['parser.py', '--file', str(query_file)]), \
             patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            main()
            output = mock_stdout.getvalue()
            assert "match_clause" in output

    def test_main_error_handling(self):
        """Test main() exception handling."""
        with patch('sys.argv', ['parser.py', "BAD SYNTAX"]), \
             patch('sys.stderr', new_callable=StringIO) as mock_stderr, \
             patch('sys.exit') as mock_exit:
            main()
            output = mock_stderr.getvalue()
            assert "Error:" in output
            mock_exit.assert_called_with(1)
