"""Tests for advanced VIM features: registers, search/replace."""

from __future__ import annotations

import pytest

from pycypher_tui.modes.base import KeyResult, ModeType
from pycypher_tui.modes.manager import ModeManager
from pycypher_tui.modes.registers import Register, RegisterFile
from pycypher_tui.modes.search_replace import (
    ReplacementResult,
    SearchReplaceCommand,
    execute_substitute,
    parse_substitute_command,
)


# ═══════════════════════════════════════════════════════════════════════════
# Register system tests
# ═══════════════════════════════════════════════════════════════════════════


class TestRegister:
    """Tests for the Register dataclass."""

    def test_default_register(self):
        r = Register()
        assert r.content == ""
        assert r.is_linewise is False

    def test_register_with_content(self):
        r = Register(content="hello", is_linewise=True)
        assert r.content == "hello"
        assert r.is_linewise is True


class TestRegisterFile:
    """Tests for the RegisterFile named register storage."""

    def test_get_empty_register(self):
        rf = RegisterFile()
        r = rf.get("a")
        assert r.content == ""

    def test_set_and_get(self):
        rf = RegisterFile()
        rf.set("a", "hello")
        assert rf.get("a").content == "hello"

    def test_set_linewise(self):
        rf = RegisterFile()
        rf.set("b", "line\n", linewise=True)
        assert rf.get("b").is_linewise is True

    def test_case_insensitive(self):
        rf = RegisterFile()
        rf.set("A", "upper")
        assert rf.get("a").content == "upper"

    def test_invalid_register_name_ignored(self):
        rf = RegisterFile()
        rf.set("!", "invalid")
        assert rf.get("!").content == ""

    def test_append_to_register(self):
        rf = RegisterFile()
        rf.set("a", "hello")
        rf.append("a", " world")
        assert rf.get("a").content == "hello world"

    def test_append_to_empty(self):
        rf = RegisterFile()
        rf.append("a", "first")
        assert rf.get("a").content == "first"

    def test_append_only_named_registers(self):
        rf = RegisterFile()
        rf.set("0", "num")
        rf.append("0", " extra")
        # 0 is not in NAMED, so append is a no-op
        assert rf.get("0").content == "num"

    def test_yank_sets_default_and_yank_register(self):
        rf = RegisterFile()
        rf.yank("yanked text")
        assert rf.get('"').content == "yanked text"
        assert rf.get("0").content == "yanked text"

    def test_yank_with_named_register(self):
        rf = RegisterFile()
        rf.yank("text", register="a")
        assert rf.get("a").content == "text"
        assert rf.get('"').content == "text"
        assert rf.get("0").content == "text"

    def test_paste_default(self):
        rf = RegisterFile()
        rf.yank("pasted")
        assert rf.paste() == "pasted"

    def test_paste_named(self):
        rf = RegisterFile()
        rf.set("z", "from z")
        assert rf.paste("z") == "from z"

    def test_paste_empty_register(self):
        rf = RegisterFile()
        assert rf.paste("x") == ""

    def test_clear_register(self):
        rf = RegisterFile()
        rf.set("a", "content")
        rf.clear("a")
        assert rf.get("a").content == ""

    def test_clear_all(self):
        rf = RegisterFile()
        rf.set("a", "one")
        rf.set("b", "two")
        rf.clear_all()
        assert rf.get("a").content == ""
        assert rf.get("b").content == ""

    def test_list_nonempty(self):
        rf = RegisterFile()
        rf.set("a", "one")
        rf.set("b", "")
        rf.set("c", "three")
        result = rf.list_nonempty()
        assert "a" in result
        assert "b" not in result
        assert "c" in result

    def test_is_valid_name(self):
        assert RegisterFile.is_valid_name("a")
        assert RegisterFile.is_valid_name("Z")
        assert RegisterFile.is_valid_name("0")
        assert RegisterFile.is_valid_name('"')
        assert RegisterFile.is_valid_name("+")
        assert not RegisterFile.is_valid_name("!")
        assert not RegisterFile.is_valid_name("@")


# ═══════════════════════════════════════════════════════════════════════════
# Search/Replace tests
# ═══════════════════════════════════════════════════════════════════════════


class TestParseSubstituteCommand:
    """Tests for parsing :s commands."""

    def test_basic_substitute(self):
        cmd = parse_substitute_command("s/foo/bar/")
        assert cmd is not None
        assert cmd.pattern == "foo"
        assert cmd.replacement == "bar"
        assert not cmd.global_flag
        assert not cmd.whole_buffer

    def test_global_flag(self):
        cmd = parse_substitute_command("s/foo/bar/g")
        assert cmd.global_flag is True

    def test_whole_buffer(self):
        cmd = parse_substitute_command("%s/foo/bar/g")
        assert cmd.whole_buffer is True
        assert cmd.global_flag is True

    def test_confirm_flag(self):
        cmd = parse_substitute_command("s/foo/bar/gc")
        assert cmd.confirm_flag is True
        assert cmd.global_flag is True

    def test_case_insensitive(self):
        cmd = parse_substitute_command("s/foo/bar/gi")
        assert cmd.case_insensitive is True

    def test_alternative_separator(self):
        cmd = parse_substitute_command("s#foo#bar#g")
        assert cmd is not None
        assert cmd.pattern == "foo"
        assert cmd.replacement == "bar"

    def test_empty_replacement(self):
        cmd = parse_substitute_command("s/foo//g")
        assert cmd is not None
        assert cmd.replacement == ""

    def test_empty_pattern_returns_none(self):
        cmd = parse_substitute_command("s//bar/")
        assert cmd is None

    def test_not_a_substitute(self):
        assert parse_substitute_command("w") is None
        assert parse_substitute_command("q") is None

    def test_too_short(self):
        assert parse_substitute_command("s") is None

    def test_alphanumeric_separator_rejected(self):
        assert parse_substitute_command("safoobarg") is None

    def test_no_replacement_part(self):
        assert parse_substitute_command("s/foo") is None

    def test_escaped_separator(self):
        cmd = parse_substitute_command(r"s/a\/b/c/")
        assert cmd is not None
        assert cmd.pattern == "a/b"

    def test_regex_pattern(self):
        cmd = parse_substitute_command(r"s/\d+/NUM/g")
        assert cmd is not None
        assert cmd.pattern == r"\d+"

    def test_no_trailing_separator(self):
        cmd = parse_substitute_command("s/foo/bar")
        assert cmd is not None
        assert cmd.pattern == "foo"
        assert cmd.replacement == "bar"


class TestExecuteSubstitute:
    """Tests for executing search/replace."""

    def test_replace_first(self):
        cmd = SearchReplaceCommand(pattern="foo", replacement="bar")
        result = execute_substitute(["foo foo foo"], cmd, current_line=0)
        assert result.new_text == "bar foo foo"
        assert result.count == 1

    def test_replace_global(self):
        cmd = SearchReplaceCommand(pattern="foo", replacement="bar", global_flag=True)
        result = execute_substitute(["foo foo foo"], cmd, current_line=0)
        assert result.new_text == "bar bar bar"
        assert result.count == 3

    def test_replace_whole_buffer(self):
        cmd = SearchReplaceCommand(
            pattern="x", replacement="y", global_flag=True, whole_buffer=True
        )
        lines = ["x 1", "x 2", "x 3"]
        result = execute_substitute(lines, cmd)
        assert result.new_text == "y 1\ny 2\ny 3"
        assert result.count == 3

    def test_current_line_only(self):
        cmd = SearchReplaceCommand(pattern="x", replacement="y", global_flag=True)
        lines = ["x 1", "x 2", "x 3"]
        result = execute_substitute(lines, cmd, current_line=1)
        assert result.new_text == "x 1\ny 2\nx 3"
        assert result.count == 1

    def test_no_match(self):
        cmd = SearchReplaceCommand(pattern="xyz", replacement="abc")
        result = execute_substitute(["hello world"], cmd)
        assert result.count == 0
        assert result.new_text == "hello world"

    def test_regex_replacement(self):
        cmd = SearchReplaceCommand(
            pattern=r"\d+", replacement="NUM", global_flag=True
        )
        result = execute_substitute(["item 42 costs 100"], cmd)
        assert result.new_text == "item NUM costs NUM"
        assert result.count == 2

    def test_invalid_regex(self):
        cmd = SearchReplaceCommand(pattern="[invalid", replacement="x")
        result = execute_substitute(["test"], cmd)
        assert result.success is False
        assert result.error is not None
        assert "Invalid regex" in result.error

    def test_case_insensitive(self):
        cmd = SearchReplaceCommand(
            pattern="hello", replacement="hi", case_insensitive=True
        )
        result = execute_substitute(["Hello HELLO hello"], cmd)
        assert result.new_text == "hi HELLO hello"
        assert result.count == 1

    def test_case_insensitive_global(self):
        cmd = SearchReplaceCommand(
            pattern="hello",
            replacement="hi",
            case_insensitive=True,
            global_flag=True,
        )
        result = execute_substitute(["Hello HELLO hello"], cmd)
        assert result.new_text == "hi hi hi"
        assert result.count == 3

    def test_line_out_of_range(self):
        cmd = SearchReplaceCommand(pattern="x", replacement="y")
        result = execute_substitute(["test"], cmd, current_line=5)
        assert result.success is False

    def test_empty_lines(self):
        cmd = SearchReplaceCommand(
            pattern="x", replacement="y", whole_buffer=True
        )
        result = execute_substitute([], cmd)
        assert result.count == 0
        assert result.new_text == ""

    def test_delete_pattern(self):
        cmd = SearchReplaceCommand(
            pattern=" extra", replacement="", global_flag=True
        )
        result = execute_substitute(["hello extra extra world"], cmd)
        assert result.new_text == "hello world"


# ═══════════════════════════════════════════════════════════════════════════
# Normal mode advanced key handling tests
# ═══════════════════════════════════════════════════════════════════════════


class TestNormalModeWordMotions:
    """Test w/b/e motions in normal mode."""

    def _make_manager(self):
        return ModeManager()

    def test_w_motion(self):
        mgr = self._make_manager()
        result = mgr.handle_key("w")
        assert result.handled is True
        assert result.command == "motion:word_forward"

    def test_b_motion(self):
        mgr = self._make_manager()
        result = mgr.handle_key("b")
        assert result.handled is True
        assert result.command == "motion:word_backward"

    def test_e_motion(self):
        mgr = self._make_manager()
        result = mgr.handle_key("e")
        assert result.handled is True
        assert result.command == "motion:word_end"


class TestNormalModeCharFind:
    """Test f/t/F/T char find in normal mode."""

    def _make_manager(self):
        return ModeManager()

    def test_f_char(self):
        mgr = self._make_manager()
        r1 = mgr.handle_key("f")
        assert r1.handled is True
        assert r1.pending is True
        r2 = mgr.handle_key("x")
        assert r2.handled is True
        assert r2.command == "motion:find_char:x"

    def test_t_char(self):
        mgr = self._make_manager()
        mgr.handle_key("t")
        r = mgr.handle_key("a")
        assert r.command == "motion:till_char:a"

    def test_F_char(self):
        mgr = self._make_manager()
        mgr.handle_key("F")
        r = mgr.handle_key("z")
        assert r.command == "motion:find_char_back:z"

    def test_T_char(self):
        mgr = self._make_manager()
        mgr.handle_key("T")
        r = mgr.handle_key("m")
        assert r.command == "motion:till_char_back:m"

    def test_f_escape_cancels(self):
        mgr = self._make_manager()
        mgr.handle_key("f")
        r = mgr.handle_key("escape")
        assert r.handled is True
        # Should be back to normal, no pending
        r2 = mgr.handle_key("j")
        assert r2.command == "navigate:down"


class TestNormalModeMacro:
    """Test q{reg} and @{reg} in normal mode."""

    def _make_manager(self):
        return ModeManager()

    def test_q_register(self):
        mgr = self._make_manager()
        r1 = mgr.handle_key("q")
        assert r1.pending is True
        r2 = mgr.handle_key("a")
        assert r2.command == "macro:toggle_record:a"

    def test_q_invalid_register(self):
        mgr = self._make_manager()
        mgr.handle_key("q")
        r = mgr.handle_key("0")  # not alpha
        assert r.handled is False

    def test_at_register(self):
        mgr = self._make_manager()
        r1 = mgr.handle_key("at")
        assert r1.pending is True
        r2 = mgr.handle_key("b")
        assert r2.command == "macro:play:b"

    def test_at_at_replay_last(self):
        mgr = self._make_manager()
        mgr.handle_key("at")
        r = mgr.handle_key("at")
        assert r.command == "macro:replay_last"


class TestNormalModeChangeInside:
    """Test ci{char} and ca{char} in normal mode."""

    def _make_manager(self):
        return ModeManager()

    def test_ci_paren(self):
        mgr = self._make_manager()
        mgr.handle_key("c")
        mgr.handle_key("i")
        r = mgr.handle_key("(")
        assert r.command == "textobj:change_i:("
        assert r.transition_to == ModeType.INSERT

    def test_ca_bracket(self):
        mgr = self._make_manager()
        mgr.handle_key("c")
        mgr.handle_key("a")
        r = mgr.handle_key("[")
        assert r.command == "textobj:change_a:["
        assert r.transition_to == ModeType.INSERT

    def test_ci_quote(self):
        mgr = self._make_manager()
        mgr.handle_key("c")
        mgr.handle_key("i")
        r = mgr.handle_key('"')
        assert r.command == 'textobj:change_i:"'

    def test_cc_changes_line(self):
        mgr = self._make_manager()
        mgr.handle_key("c")
        r = mgr.handle_key("c")
        assert r.command == "edit:change_line"
        assert r.transition_to == ModeType.INSERT

    def test_c_escape_cancels(self):
        mgr = self._make_manager()
        mgr.handle_key("c")
        r = mgr.handle_key("escape")
        assert r.handled is True
        # No pending state
        r2 = mgr.handle_key("j")
        assert r2.command == "navigate:down"


class TestNormalModeRegisterSelect:
    """Test "{reg} register selection."""

    def _make_manager(self):
        return ModeManager()

    def test_quote_register(self):
        mgr = self._make_manager()
        mgr.handle_key("quotation_mark")
        r = mgr.handle_key("a")
        assert r.command == "register:select:a"


class TestNormalModeExistingKeys:
    """Verify existing keybindings still work after additions."""

    def _make_manager(self):
        return ModeManager()

    def test_hjkl_navigation(self):
        mgr = self._make_manager()
        assert mgr.handle_key("h").command == "navigate:left"
        assert mgr.handle_key("j").command == "navigate:down"
        assert mgr.handle_key("k").command == "navigate:up"
        assert mgr.handle_key("l").command == "navigate:right"

    def test_gg_first(self):
        mgr = self._make_manager()
        mgr.handle_key("g")
        r = mgr.handle_key("g")
        assert r.command == "navigate:first"

    def test_G_last(self):
        mgr = self._make_manager()
        r = mgr.handle_key("G")
        assert r.command == "navigate:last"

    def test_dd_delete_line(self):
        mgr = self._make_manager()
        mgr.handle_key("d")
        r = mgr.handle_key("d")
        assert r.command == "edit:delete_line"

    def test_i_insert(self):
        mgr = self._make_manager()
        r = mgr.handle_key("i")
        assert r.transition_to == ModeType.INSERT

    def test_colon_command(self):
        mgr = self._make_manager()
        r = mgr.handle_key("colon")
        assert r.transition_to == ModeType.COMMAND

    def test_u_undo(self):
        mgr = self._make_manager()
        r = mgr.handle_key("u")
        assert r.command == "edit:undo"

    def test_y_yank(self):
        mgr = self._make_manager()
        r = mgr.handle_key("y")
        assert r.command == "clipboard:yank"

    def test_p_paste(self):
        mgr = self._make_manager()
        r = mgr.handle_key("p")
        assert r.command == "clipboard:paste"
