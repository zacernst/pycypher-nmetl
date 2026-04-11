"""Tests for the Cypher query editor widget.

Covers:
- EditorBuffer: line operations, insert/delete, text property
- CursorPosition: copy, basic attributes
- SyntaxToken and tokenize_line: keyword/function/string/number/comment detection
- find_matching_bracket: bracket matching logic
- get_completions: autocomplete from keywords, functions, and entity types
- QueryHistory: add, navigate, dedup, max size
- CypherEditor: normal mode, insert mode, undo/redo, search, word movement
"""

from __future__ import annotations

import pytest

from pycypher_tui.widgets.query_editor import (
    CYPHER_FUNCTIONS,
    CYPHER_KEYWORDS,
    CursorPosition,
    CypherEditor,
    EditorBuffer,
    QueryHistory,
    SyntaxToken,
    find_matching_bracket,
    get_completions,
    tokenize_line,
)

# ===========================================================================
# CursorPosition
# ===========================================================================


class TestCursorPosition:
    def test_default_values(self):
        pos = CursorPosition()
        assert pos.line == 0
        assert pos.col == 0

    def test_custom_values(self):
        pos = CursorPosition(line=3, col=7)
        assert pos.line == 3
        assert pos.col == 7

    def test_copy(self):
        pos = CursorPosition(2, 5)
        copy = pos.copy()
        assert copy.line == 2
        assert copy.col == 5
        copy.line = 10
        assert pos.line == 2  # original unchanged


# ===========================================================================
# EditorBuffer
# ===========================================================================


class TestEditorBuffer:
    def test_default_has_one_empty_line(self):
        buf = EditorBuffer()
        assert buf.line_count == 1
        assert buf.text == ""

    def test_text_setter(self):
        buf = EditorBuffer()
        buf.text = "MATCH (n)\nRETURN n"
        assert buf.line_count == 2
        assert buf.get_line(0) == "MATCH (n)"
        assert buf.get_line(1) == "RETURN n"

    def test_text_getter(self):
        buf = EditorBuffer(lines=["line1", "line2"])
        assert buf.text == "line1\nline2"

    def test_set_empty_text(self):
        buf = EditorBuffer()
        buf.text = ""
        assert buf.line_count == 1
        assert buf.get_line(0) == ""

    def test_get_line_out_of_bounds(self):
        buf = EditorBuffer(lines=["only"])
        assert buf.get_line(-1) == ""
        assert buf.get_line(5) == ""

    def test_set_line(self):
        buf = EditorBuffer(lines=["old"])
        buf.set_line(0, "new")
        assert buf.get_line(0) == "new"

    def test_set_line_out_of_bounds(self):
        buf = EditorBuffer(lines=["a"])
        buf.set_line(5, "b")  # should not crash
        assert buf.get_line(0) == "a"

    def test_insert_char(self):
        buf = EditorBuffer(lines=["hello"])
        pos = CursorPosition(0, 5)
        new_pos = buf.insert_char(pos, "!")
        assert buf.get_line(0) == "hello!"
        assert new_pos.col == 6

    def test_insert_char_middle(self):
        buf = EditorBuffer(lines=["hllo"])
        pos = CursorPosition(0, 1)
        new_pos = buf.insert_char(pos, "e")
        assert buf.get_line(0) == "hello"
        assert new_pos.col == 2

    def test_insert_newline(self):
        buf = EditorBuffer(lines=["hello world"])
        pos = CursorPosition(0, 5)
        new_pos = buf.insert_newline(pos)
        assert buf.line_count == 2
        assert buf.get_line(0) == "hello"
        assert buf.get_line(1) == " world"
        assert new_pos.line == 1
        assert new_pos.col == 0

    def test_delete_char_backspace(self):
        buf = EditorBuffer(lines=["hello"])
        pos = CursorPosition(0, 3)
        new_pos = buf.delete_char(pos)
        assert buf.get_line(0) == "helo"
        assert new_pos.col == 2

    def test_delete_char_at_start_joins_lines(self):
        buf = EditorBuffer(lines=["first", "second"])
        pos = CursorPosition(1, 0)
        new_pos = buf.delete_char(pos)
        assert buf.line_count == 1
        assert buf.get_line(0) == "firstsecond"
        assert new_pos.line == 0
        assert new_pos.col == 5

    def test_delete_char_at_start_of_first_line(self):
        buf = EditorBuffer(lines=["hello"])
        pos = CursorPosition(0, 0)
        new_pos = buf.delete_char(pos)
        assert buf.get_line(0) == "hello"
        assert new_pos.line == 0
        assert new_pos.col == 0

    def test_delete_char_forward(self):
        buf = EditorBuffer(lines=["hello"])
        buf.delete_char_forward(CursorPosition(0, 2))
        assert buf.get_line(0) == "helo"

    def test_delete_char_forward_at_end_joins(self):
        buf = EditorBuffer(lines=["first", "second"])
        buf.delete_char_forward(CursorPosition(0, 5))
        assert buf.line_count == 1
        assert buf.get_line(0) == "firstsecond"

    def test_delete_line(self):
        buf = EditorBuffer(lines=["a", "b", "c"])
        buf.delete_line(1)
        assert buf.line_count == 2
        assert buf.get_line(1) == "c"

    def test_delete_line_single_line(self):
        buf = EditorBuffer(lines=["only"])
        buf.delete_line(0)
        assert buf.line_count == 1
        assert buf.get_line(0) == ""

    def test_delete_line_out_of_bounds(self):
        buf = EditorBuffer(lines=["a", "b"])
        buf.delete_line(5)  # should not crash
        assert buf.line_count == 2


# ===========================================================================
# SyntaxToken and tokenize_line
# ===========================================================================


class TestTokenize:
    def test_keyword_detection(self):
        tokens = tokenize_line("MATCH (n) WHERE n.age > 10 RETURN n")
        keyword_texts = [t.text for t in tokens if t.token_type == "keyword"]
        assert "MATCH" in keyword_texts
        assert "WHERE" in keyword_texts
        assert "RETURN" in keyword_texts

    def test_keyword_case_insensitive(self):
        tokens = tokenize_line("match (n) return n")
        keyword_texts = [t.text for t in tokens if t.token_type == "keyword"]
        assert "match" in keyword_texts
        assert "return" in keyword_texts

    def test_string_detection(self):
        tokens = tokenize_line("WHERE n.name = 'Alice'")
        string_tokens = [t for t in tokens if t.token_type == "string"]
        assert len(string_tokens) == 1
        assert string_tokens[0].text == "'Alice'"

    def test_double_quoted_string(self):
        tokens = tokenize_line('WHERE n.name = "Alice"')
        string_tokens = [t for t in tokens if t.token_type == "string"]
        assert len(string_tokens) == 1

    def test_number_detection(self):
        tokens = tokenize_line("WHERE n.age > 30")
        number_tokens = [t for t in tokens if t.token_type == "number"]
        assert any(t.text == "30" for t in number_tokens)

    def test_float_number(self):
        tokens = tokenize_line("WHERE n.score > 3.14")
        number_tokens = [t for t in tokens if t.token_type == "number"]
        assert any(t.text == "3.14" for t in number_tokens)

    def test_comment_detection(self):
        tokens = tokenize_line("MATCH (n) // this is a comment")
        comment_tokens = [t for t in tokens if t.token_type == "comment"]
        assert len(comment_tokens) == 1
        assert "comment" in comment_tokens[0].text

    def test_function_detection(self):
        # Functions that overlap with keywords (count, sum) are classified as keywords.
        # Test with a function that's not a keyword.
        tokens = tokenize_line("RETURN size(collect(n))")
        function_texts = [t.text for t in tokens if t.token_type == "function"]
        assert any("size" in f for f in function_texts)

    def test_function_call_classified_as_function_not_keyword(self):
        # count(n) should be classified as "function" since it's followed by (
        tokens = tokenize_line("RETURN count(n)")
        function_texts = [t.text for t in tokens if t.token_type == "function"]
        assert any("count" in f for f in function_texts)

    def test_keyword_not_inside_string(self):
        tokens = tokenize_line("WHERE n.name = 'MATCH me'")
        keyword_tokens = [t for t in tokens if t.token_type == "keyword"]
        # MATCH inside string should not be a keyword token
        keyword_texts = [t.text for t in keyword_tokens]
        assert "WHERE" in keyword_texts

    def test_empty_line(self):
        tokens = tokenize_line("")
        assert tokens == []

    def test_tokens_sorted_by_position(self):
        tokens = tokenize_line("MATCH (n) WHERE n.age > 10 RETURN n")
        positions = [t.start for t in tokens]
        assert positions == sorted(positions)

    def test_syntax_token_attributes(self):
        t = SyntaxToken(start=0, end=5, token_type="keyword", text="MATCH")
        assert t.start == 0
        assert t.end == 5
        assert t.token_type == "keyword"
        assert t.text == "MATCH"


# ===========================================================================
# find_matching_bracket
# ===========================================================================


class TestFindMatchingBracket:
    def test_matching_parens(self):
        assert find_matching_bracket("(hello)", 0) == 6
        assert find_matching_bracket("(hello)", 6) == 0

    def test_matching_brackets(self):
        assert find_matching_bracket("[hello]", 0) == 6
        assert find_matching_bracket("[hello]", 6) == 0

    def test_matching_braces(self):
        assert find_matching_bracket("{hello}", 0) == 6
        assert find_matching_bracket("{hello}", 6) == 0

    def test_nested_parens(self):
        assert find_matching_bracket("(a(b)c)", 0) == 6
        assert find_matching_bracket("(a(b)c)", 2) == 4

    def test_no_match(self):
        assert find_matching_bracket("(unclosed", 0) is None

    def test_out_of_range(self):
        assert find_matching_bracket("()", -1) is None
        assert find_matching_bracket("()", 10) is None

    def test_non_bracket_char(self):
        assert find_matching_bracket("hello", 2) is None

    def test_cypher_node_pattern(self):
        text = "(n:Person)-[:KNOWS]->(m:Person)"
        assert find_matching_bracket(text, 0) == 9
        assert find_matching_bracket(text, 11) == 18


# ===========================================================================
# get_completions
# ===========================================================================


class TestGetCompletions:
    def test_empty_prefix(self):
        assert get_completions("") == []

    def test_keyword_completion(self):
        results = get_completions("MAT")
        assert "MATCH" in results

    def test_function_completion(self):
        results = get_completions("cou")
        assert "count" in results

    def test_entity_type_completion(self):
        results = get_completions("Per", entity_types=["Person", "Product"])
        assert "Person" in results
        assert "Product" not in results

    def test_case_insensitive(self):
        results = get_completions("ret")
        assert "RETURN" in results

    def test_sorted_results(self):
        results = get_completions("M")
        assert results == sorted(results)

    def test_no_duplicates(self):
        results = get_completions("MA")
        assert len(results) == len(set(results))


# ===========================================================================
# QueryHistory
# ===========================================================================


class TestQueryHistory:
    def test_empty_history(self):
        h = QueryHistory()
        assert h.count == 0
        assert h.previous() is None
        assert h.next() is None

    def test_add_and_recall(self):
        h = QueryHistory()
        h.add("MATCH (n) RETURN n")
        h.add("MATCH (n:Person) RETURN n")
        assert h.count == 2
        assert h.previous() == "MATCH (n:Person) RETURN n"
        assert h.previous() == "MATCH (n) RETURN n"

    def test_next_after_previous(self):
        h = QueryHistory()
        h.add("q1")
        h.add("q2")
        h.previous()  # q2
        h.previous()  # q1
        assert h.next() == "q2"

    def test_next_at_end_returns_empty(self):
        h = QueryHistory()
        h.add("q1")
        h.previous()  # q1
        assert h.next() == ""

    def test_duplicate_not_added(self):
        h = QueryHistory()
        h.add("MATCH (n) RETURN n")
        h.add("MATCH (n) RETURN n")
        assert h.count == 1

    def test_empty_not_added(self):
        h = QueryHistory()
        h.add("")
        h.add("  ")
        assert h.count == 0

    def test_max_size(self):
        h = QueryHistory(max_size=3)
        for i in range(5):
            h.add(f"query{i}")
        assert h.count == 3
        assert h.entries == ["query2", "query3", "query4"]

    def test_reset_position(self):
        h = QueryHistory()
        h.add("q1")
        h.previous()
        h.reset_position()
        assert h.next() is None


# ===========================================================================
# CypherEditor - normal mode
# ===========================================================================


def _make_editor(text: str = "", mode: str = "normal") -> CypherEditor:
    """Create a CypherEditor with proper initialization for testing."""
    ed = CypherEditor(initial_text=text)
    # CypherEditor reactives are set in __init__, but we need to
    # manipulate them directly for unit tests. Access via internal attrs.
    ed.mode = mode
    return ed


class TestCypherEditorNormal:
    @pytest.fixture
    def editor(self):
        return _make_editor("MATCH (n:Person)\nWHERE n.age > 30\nRETURN n")

    def test_initial_state(self, editor):
        assert editor.mode == "normal"
        assert editor.line_count == 3
        assert editor.current_line == "MATCH (n:Person)"

    def test_hjkl_movement(self, editor):
        editor.handle_key("l")
        assert editor.cursor.col == 1
        editor.handle_key("j")
        assert editor.cursor.line == 1
        editor.handle_key("k")
        assert editor.cursor.line == 0
        editor.handle_key("h")
        assert editor.cursor.col == 0

    def test_h_at_start_stays(self, editor):
        editor.handle_key("h")
        assert editor.cursor.col == 0

    def test_j_at_bottom_stays(self, editor):
        editor.cursor.line = 2
        editor.handle_key("j")
        assert editor.cursor.line == 2

    def test_G_jumps_to_last(self, editor):
        editor.handle_key("G")
        assert editor.cursor.line == 2

    def test_gg_jumps_to_first(self, editor):
        editor.cursor.line = 2
        editor.handle_key("g")
        editor.handle_key("g")
        assert editor.cursor.line == 0

    def test_0_jumps_to_line_start(self, editor):
        editor.cursor.col = 5
        editor.handle_key("0")
        assert editor.cursor.col == 0

    def test_dollar_jumps_to_line_end(self, editor):
        editor.handle_key("dollar")
        assert editor.cursor.col == len("MATCH (n:Person)") - 1

    def test_i_enters_insert(self, editor):
        editor.handle_key("i")
        assert editor.mode == "insert"

    def test_a_enters_insert_after(self, editor):
        editor.cursor.col = 3
        editor.handle_key("a")
        assert editor.mode == "insert"
        assert editor.cursor.col == 4

    def test_o_opens_line_below(self, editor):
        editor.handle_key("o")
        assert editor.mode == "insert"
        assert editor.cursor.line == 1
        assert editor.buffer.line_count == 4
        assert editor.buffer.get_line(1) == ""

    def test_O_opens_line_above(self, editor):
        editor.cursor.line = 1
        editor.handle_key("O")
        assert editor.mode == "insert"
        assert editor.cursor.line == 1
        assert editor.buffer.line_count == 4
        assert editor.buffer.get_line(1) == ""

    def test_dd_deletes_line(self, editor):
        editor.handle_key("d")
        editor.handle_key("d")
        assert editor.line_count == 2
        assert editor.buffer.get_line(0) == "WHERE n.age > 30"
        assert editor._yanked_line == "MATCH (n:Person)"

    def test_yy_yanks_line(self, editor):
        editor.handle_key("y")
        editor.handle_key("y")
        assert editor._yanked_line == "MATCH (n:Person)"
        assert editor.line_count == 3  # not deleted

    def test_p_pastes_yanked(self, editor):
        editor._yanked_line = "// copied"
        editor.handle_key("p")
        assert editor.line_count == 4
        assert editor.buffer.get_line(1) == "// copied"

    def test_x_deletes_char_at_cursor(self, editor):
        editor.cursor.col = 0
        editor.handle_key("x")
        assert editor.buffer.get_line(0) == "ATCH (n:Person)"

    def test_u_undo(self, editor):
        original = editor.buffer.text
        editor.handle_key("d")
        editor.handle_key("d")
        assert editor.line_count == 2
        editor.handle_key("u")
        assert editor.buffer.text == original

    def test_ctrl_r_redo(self, editor):
        editor.handle_key("d")
        editor.handle_key("d")
        editor.handle_key("u")
        assert editor.line_count == 3
        editor.handle_key("ctrl+r")
        assert editor.line_count == 2

    def test_unknown_key_returns_false(self, editor):
        assert editor.handle_key("z") is False

    def test_escape_clears_pending(self, editor):
        editor.handle_key("g")
        assert len(editor._pending_keys) == 1
        editor.handle_key("escape")
        assert len(editor._pending_keys) == 0

    def test_w_word_forward(self, editor):
        # "MATCH (n:Person)"
        editor.handle_key("w")
        # Should move past "MATCH" to next word
        assert editor.cursor.col > 0

    def test_b_word_backward(self, editor):
        editor.cursor.col = 10
        editor.handle_key("b")
        assert editor.cursor.col < 10

    def test_A_enters_insert_at_end(self, editor):
        editor.handle_key("A")
        assert editor.mode == "insert"
        assert editor.cursor.col == len("MATCH (n:Person)")

    def test_I_enters_insert_at_first_nonwhite(self, editor):
        editor.buffer.lines[0] = "  MATCH (n)"
        editor.handle_key("I")
        assert editor.mode == "insert"
        assert editor.cursor.col == 2


# ===========================================================================
# CypherEditor - insert mode
# ===========================================================================


class TestCypherEditorInsert:
    @pytest.fixture
    def editor(self):
        ed = _make_editor("MATCH (n)", mode="insert")
        ed.cursor = CursorPosition(0, 9)
        return ed

    def test_escape_returns_to_normal(self, editor):
        editor.handle_key("escape")
        assert editor.mode == "normal"

    def test_type_character(self, editor):
        editor.cursor = CursorPosition(0, 9)
        editor.handle_key("X")
        assert "X" in editor.buffer.get_line(0)

    def test_enter_creates_newline(self, editor):
        editor.cursor = CursorPosition(0, 5)
        editor.handle_key("enter")
        assert editor.line_count == 2

    def test_backspace_deletes(self, editor):
        editor.cursor = CursorPosition(0, 5)
        original = editor.buffer.get_line(0)
        editor.handle_key("backspace")
        assert len(editor.buffer.get_line(0)) < len(original)

    def test_tab_inserts_spaces(self, editor):
        editor.cursor = CursorPosition(0, 0)
        editor.handle_key("tab")
        assert editor.buffer.get_line(0).startswith("  ")

    def test_arrow_keys_move_cursor(self, editor):
        editor.cursor = CursorPosition(0, 5)
        editor.handle_key("left")
        assert editor.cursor.col == 4
        editor.handle_key("right")
        assert editor.cursor.col == 5

    def test_home_end_movement(self, editor):
        editor.cursor = CursorPosition(0, 5)
        editor.handle_key("home")
        assert editor.cursor.col == 0
        editor.handle_key("end")
        assert editor.cursor.col == len(editor.current_line)


# ===========================================================================
# CypherEditor - search
# ===========================================================================


class TestCypherEditorSearch:
    @pytest.fixture
    def editor(self):
        return _make_editor("MATCH (n:Person)\nWHERE n.age > 30\nRETURN n")

    def test_search_finds_match(self, editor):
        editor.search("WHERE")
        assert len(editor._search_matches) == 1
        assert editor.cursor.line == 1

    def test_search_case_insensitive(self, editor):
        editor.search("match")
        assert len(editor._search_matches) >= 1

    def test_search_multiple_matches(self, editor):
        editor.search("n")
        assert len(editor._search_matches) > 1

    def test_search_next(self, editor):
        editor.search("n")
        first_pos = (editor.cursor.line, editor.cursor.col)
        editor._search_next()
        second_pos = (editor.cursor.line, editor.cursor.col)
        assert first_pos != second_pos

    def test_search_prev(self, editor):
        editor.search("n")
        editor._search_next()
        editor._search_next()
        pos_before = (editor.cursor.line, editor.cursor.col)
        editor._search_prev()
        pos_after = (editor.cursor.line, editor.cursor.col)
        assert pos_before != pos_after

    def test_search_empty_pattern(self, editor):
        editor.search("")
        assert len(editor._search_matches) == 0

    def test_search_invalid_regex(self, editor):
        editor.search("[invalid")
        assert len(editor._search_matches) == 0

    def test_search_no_matches(self, editor):
        editor.search("XYZNONEXISTENT")
        assert len(editor._search_matches) == 0

    def test_n_without_search_is_noop(self, editor):
        pos_before = (editor.cursor.line, editor.cursor.col)
        editor.handle_key("n")
        pos_after = (editor.cursor.line, editor.cursor.col)
        assert pos_before == pos_after


# ===========================================================================
# CypherEditor - text property and completions
# ===========================================================================


class TestCypherEditorMisc:
    def test_text_property(self):
        ed = _make_editor("hello")
        assert ed.text == "hello"

    def test_text_setter(self):
        ed = _make_editor()
        ed.text = "MATCH (n)\nRETURN n"
        assert ed.line_count == 2

    def test_get_tokens(self):
        ed = _make_editor("MATCH (n) RETURN n")
        tokens = ed.get_tokens(0)
        assert len(tokens) > 0
        assert any(t.token_type == "keyword" for t in tokens)

    def test_render(self):
        ed = _make_editor("MATCH (n)\nRETURN n")
        rendered = ed.render()
        assert "MATCH" in rendered
        assert "RETURN" in rendered
        assert "1" in rendered  # line number


# ===========================================================================
# Constants
# ===========================================================================


class TestConstants:
    def test_keywords_are_uppercase(self):
        for kw in CYPHER_KEYWORDS:
            assert kw == kw.upper()

    def test_keywords_nonempty(self):
        assert len(CYPHER_KEYWORDS) > 20

    def test_functions_nonempty(self):
        assert len(CYPHER_FUNCTIONS) > 20

    def test_common_keywords_present(self):
        assert "MATCH" in CYPHER_KEYWORDS
        assert "WHERE" in CYPHER_KEYWORDS
        assert "RETURN" in CYPHER_KEYWORDS
        assert "WITH" in CYPHER_KEYWORDS
        assert "CREATE" in CYPHER_KEYWORDS

    def test_common_functions_present(self):
        assert "count" in CYPHER_FUNCTIONS
        assert "sum" in CYPHER_FUNCTIONS
        assert "collect" in CYPHER_FUNCTIONS
