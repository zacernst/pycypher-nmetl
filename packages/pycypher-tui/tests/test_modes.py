"""Tests for the VIM modal system."""

import pytest

from pycypher_tui.modes.base import BaseMode, KeyResult, ModeType
from pycypher_tui.modes.manager import ModeManager
from pycypher_tui.modes.normal import NormalMode
from pycypher_tui.modes.insert import InsertMode
from pycypher_tui.modes.visual import VisualMode
from pycypher_tui.modes.command import CommandMode, CommandHistory


class TestModeType:
    def test_all_modes_defined(self):
        """All four mode types exist as distinct enum members."""
        expected = {"NORMAL", "INSERT", "VISUAL", "COMMAND"}
        actual = {m.name for m in ModeType}
        assert expected.issubset(actual)

    def test_modes_are_distinct(self):
        modes = [
            ModeType.NORMAL,
            ModeType.INSERT,
            ModeType.VISUAL,
            ModeType.COMMAND,
        ]
        assert len(set(modes)) == 4


class TestKeyResult:
    def test_default_values(self):
        r = KeyResult()
        assert r.handled is False
        assert r.transition_to is None
        assert r.command is None
        assert r.text_input is None
        assert r.pending is False

    def test_handled_result(self):
        r = KeyResult(handled=True)
        assert r.handled is True

    def test_transition_result(self):
        r = KeyResult(
            handled=True, transition_to=ModeType.INSERT
        )
        assert r.transition_to == ModeType.INSERT

    def test_command_result(self):
        r = KeyResult(handled=True, command="navigate:down")
        assert r.command == "navigate:down"


class TestModeManager:
    def test_initial_mode_is_normal(self):
        mgr = ModeManager()
        assert mgr.current_type == ModeType.NORMAL

    def test_display_name_normal(self):
        mgr = ModeManager()
        assert mgr.display_name == "NORMAL"

    def test_transition_to_insert(self):
        mgr = ModeManager()
        mgr.transition_to(ModeType.INSERT)
        assert mgr.current_type == ModeType.INSERT
        assert mgr.display_name == "INSERT"

    def test_transition_to_same_mode_is_noop(self):
        mgr = ModeManager()
        transitions = []
        mgr.add_listener(
            lambda old, new: transitions.append((old, new))
        )
        mgr.transition_to(ModeType.NORMAL)
        assert len(transitions) == 0

    def test_transition_listener_called(self):
        mgr = ModeManager()
        transitions = []
        mgr.add_listener(
            lambda old, new: transitions.append((old, new))
        )
        mgr.transition_to(ModeType.INSERT)
        assert transitions == [
            (ModeType.NORMAL, ModeType.INSERT)
        ]

    def test_handle_key_routes_to_current_mode(self):
        mgr = ModeManager()
        result = mgr.handle_key("j")
        assert result.handled is True
        assert result.command == "navigate:down"

    def test_handle_key_executes_transition(self):
        mgr = ModeManager()
        result = mgr.handle_key("i")
        assert result.handled is True
        assert mgr.current_type == ModeType.INSERT

    def test_get_mode_returns_correct_instance(self):
        mgr = ModeManager()
        normal = mgr.get_mode(ModeType.NORMAL)
        assert isinstance(normal, NormalMode)
        insert = mgr.get_mode(ModeType.INSERT)
        assert isinstance(insert, InsertMode)

    def test_full_mode_cycle(self):
        """Test normal -> insert -> normal cycle."""
        mgr = ModeManager()
        assert mgr.current_type == ModeType.NORMAL

        mgr.handle_key("i")
        assert mgr.current_type == ModeType.INSERT

        mgr.handle_key("escape")
        assert mgr.current_type == ModeType.NORMAL

    def test_normal_to_command_cycle(self):
        """Test normal -> command -> normal cycle."""
        mgr = ModeManager()
        mgr.handle_key("colon")
        assert mgr.current_type == ModeType.COMMAND

        mgr.handle_key("escape")
        assert mgr.current_type == ModeType.NORMAL

    def test_normal_to_visual_cycle(self):
        """Test normal -> visual -> normal cycle."""
        mgr = ModeManager()
        mgr.handle_key("v")
        assert mgr.current_type == ModeType.VISUAL

        mgr.handle_key("escape")
        assert mgr.current_type == ModeType.NORMAL


class TestNormalMode:
    @pytest.fixture
    def mgr(self):
        return ModeManager()

    @pytest.fixture
    def normal(self, mgr):
        return mgr.get_mode(ModeType.NORMAL)

    def test_hjkl_navigation(self, normal):
        for key, direction in [
            ("h", "left"),
            ("j", "down"),
            ("k", "up"),
            ("l", "right"),
        ]:
            result = normal.handle_key(key)
            assert result.handled is True
            assert result.command == f"navigate:{direction}"

    def test_arrow_keys_navigation(self, normal):
        for key, direction in [
            ("left", "left"),
            ("down", "down"),
            ("up", "up"),
            ("right", "right"),
        ]:
            result = normal.handle_key(key)
            assert result.handled is True
            assert result.command == f"navigate:{direction}"

    def test_gg_jumps_to_first(self, normal):
        r1 = normal.handle_key("g")
        assert r1.pending is True
        r2 = normal.handle_key("g")
        assert r2.command == "navigate:first"

    def test_G_jumps_to_last(self, normal):
        result = normal.handle_key("G")
        assert result.command == "navigate:last"

    def test_i_enters_insert(self, normal):
        result = normal.handle_key("i")
        assert result.transition_to == ModeType.INSERT

    def test_a_enters_insert_after(self, normal):
        result = normal.handle_key("a")
        assert result.transition_to == ModeType.INSERT
        assert result.command == "cursor:after"

    def test_o_enters_insert_newline(self, normal):
        result = normal.handle_key("o")
        assert result.transition_to == ModeType.INSERT
        assert result.command == "line:new_below"

    def test_colon_enters_command(self, normal):
        result = normal.handle_key("colon")
        assert result.transition_to == ModeType.COMMAND

    def test_slash_enters_search(self, normal):
        result = normal.handle_key("slash")
        assert result.transition_to == ModeType.COMMAND
        assert result.command == "command:search"

    def test_v_enters_visual(self, normal):
        result = normal.handle_key("v")
        assert result.transition_to == ModeType.VISUAL

    def test_yank(self, normal):
        result = normal.handle_key("y")
        assert result.command == "clipboard:yank"

    def test_paste(self, normal):
        result = normal.handle_key("p")
        assert result.command == "clipboard:paste"

    def test_dd_deletes_line(self, normal):
        r1 = normal.handle_key("d")
        assert r1.pending is True
        r2 = normal.handle_key("d")
        assert r2.command == "edit:delete_line"

    def test_undo(self, normal):
        result = normal.handle_key("u")
        assert result.command == "edit:undo"

    def test_redo(self, normal):
        result = normal.handle_key("ctrl+r")
        assert result.command == "edit:redo"

    def test_enter_confirms(self, normal):
        result = normal.handle_key("enter")
        assert result.command == "action:confirm"

    def test_escape_clears_pending(self, normal):
        normal.handle_key("g")
        result = normal.handle_key("escape")
        assert result.handled is True
        # After escape, g should start a new sequence
        r2 = normal.handle_key("g")
        assert r2.pending is True

    def test_unknown_key_not_handled(self, normal):
        result = normal.handle_key("z")
        assert result.handled is False

    def test_invalid_pending_sequence_not_handled(self, normal):
        normal.handle_key("g")
        result = normal.handle_key("x")
        assert result.handled is False

    def test_mode_properties(self, normal):
        assert normal.mode_type == ModeType.NORMAL
        assert normal.display_name == "NORMAL"
        assert normal.style_color == "#7aa2f7"


class TestInsertMode:
    @pytest.fixture
    def mgr(self):
        return ModeManager()

    @pytest.fixture
    def insert(self, mgr):
        return mgr.get_mode(ModeType.INSERT)

    def test_escape_returns_to_normal(self, insert):
        result = insert.handle_key("escape")
        assert result.transition_to == ModeType.NORMAL

    def test_printable_char_is_text_input(self, insert):
        result = insert.handle_key("a")
        assert result.handled is True
        assert result.text_input == "a"

    def test_enter_produces_newline(self, insert):
        result = insert.handle_key("enter")
        assert result.text_input == "\n"

    def test_tab_produces_tab(self, insert):
        result = insert.handle_key("tab")
        assert result.text_input == "\t"

    def test_backspace_command(self, insert):
        result = insert.handle_key("backspace")
        assert result.command == "edit:backspace"

    def test_delete_command(self, insert):
        result = insert.handle_key("delete")
        assert result.command == "edit:delete"

    def test_mode_properties(self, insert):
        assert insert.mode_type == ModeType.INSERT
        assert insert.display_name == "INSERT"
        assert insert.style_color == "#9ece6a"


class TestVisualMode:
    @pytest.fixture
    def mgr(self):
        return ModeManager()

    @pytest.fixture
    def visual(self, mgr):
        return mgr.get_mode(ModeType.VISUAL)

    def test_escape_cancels_selection(self, visual):
        result = visual.handle_key("escape")
        assert result.transition_to == ModeType.NORMAL
        assert result.command == "selection:clear"

    def test_v_toggles_back(self, visual):
        result = visual.handle_key("v")
        assert result.transition_to == ModeType.NORMAL

    def test_hjkl_extends_selection(self, visual):
        for key, direction in [
            ("h", "left"),
            ("j", "down"),
            ("k", "up"),
            ("l", "right"),
        ]:
            result = visual.handle_key(key)
            assert result.handled is True
            assert (
                result.command == f"selection:extend_{direction}"
            )

    def test_yank_selection(self, visual):
        result = visual.handle_key("y")
        assert result.transition_to == ModeType.NORMAL
        assert result.command == "selection:yank"

    def test_delete_selection(self, visual):
        result = visual.handle_key("d")
        assert result.transition_to == ModeType.NORMAL
        assert result.command == "selection:delete"

    def test_mode_properties(self, visual):
        assert visual.mode_type == ModeType.VISUAL
        assert visual.display_name == "VISUAL"
        assert visual.style_color == "#bb9af7"


class TestCommandMode:
    @pytest.fixture
    def mgr(self):
        return ModeManager()

    @pytest.fixture
    def command(self, mgr):
        return mgr.get_mode(ModeType.COMMAND)

    def test_escape_returns_to_normal(self, command):
        result = command.handle_key("escape")
        assert result.transition_to == ModeType.NORMAL

    def test_typing_builds_buffer(self, command):
        command.on_enter()
        command.handle_key("w")
        assert command.buffer == "w"
        command.handle_key("q")
        assert command.buffer == "wq"

    def test_enter_executes_command(self, command):
        command.on_enter()
        command.handle_key("q")
        result = command.handle_key("enter")
        assert result.transition_to == ModeType.NORMAL
        assert result.command == "ex::q"

    def test_empty_enter_returns_to_normal(self, command):
        command.on_enter()
        result = command.handle_key("enter")
        assert result.transition_to == ModeType.NORMAL
        assert result.command is None

    def test_backspace_removes_char(self, command):
        command.on_enter()
        command.handle_key("w")
        command.handle_key("q")
        command.handle_key("backspace")
        assert command.buffer == "w"

    def test_backspace_empty_exits(self, command):
        command.on_enter()
        result = command.handle_key("backspace")
        assert result.transition_to == ModeType.NORMAL

    def test_display_text(self, command):
        command.on_enter()
        command.handle_key("w")
        assert command.display_text == ":w"

    def test_on_enter_clears_buffer(self, command):
        command.buffer = "old"
        command.on_enter()
        assert command.buffer == ""

    def test_mode_properties(self, command):
        assert command.mode_type == ModeType.COMMAND
        assert command.display_name == "COMMAND"
        assert command.style_color == "#e0af68"


class TestCommandHistory:
    def test_empty_history(self):
        h = CommandHistory()
        assert h.previous() is None
        assert h.next() is None

    def test_add_and_recall(self):
        h = CommandHistory()
        h.add("w")
        h.add("q")
        assert h.previous() == "q"
        assert h.previous() == "w"

    def test_next_after_previous(self):
        h = CommandHistory()
        h.add("w")
        h.add("q")
        h.previous()  # q
        h.previous()  # w
        assert h.next() == "q"

    def test_next_at_end_returns_empty(self):
        h = CommandHistory()
        h.add("w")
        h.previous()  # w
        assert h.next() == ""

    def test_duplicate_not_added(self):
        h = CommandHistory()
        h.add("w")
        h.add("w")
        assert len(h.entries) == 1

    def test_empty_string_not_added(self):
        h = CommandHistory()
        h.add("")
        assert len(h.entries) == 0

    def test_max_entries_respected(self):
        h = CommandHistory(max_entries=3)
        for i in range(5):
            h.add(str(i))
        assert len(h.entries) == 3
        assert h.entries == ["2", "3", "4"]

    def test_reset_clears_position(self):
        h = CommandHistory()
        h.add("w")
        h.previous()
        h.reset()
        assert h.next() is None

    def test_history_recall_in_command_mode(self):
        mgr = ModeManager()
        cmd = mgr.get_mode(ModeType.COMMAND)
        assert isinstance(cmd, CommandMode)

        # Enter command mode and execute a command
        cmd.on_enter()
        cmd.handle_key("w")
        cmd.handle_key("enter")

        # Enter command mode again
        cmd.on_enter()
        cmd.handle_key("q")
        cmd.handle_key("enter")

        # Enter command mode and recall history
        cmd.on_enter()
        cmd.handle_key("up")
        assert cmd.buffer == "q"
        cmd.handle_key("up")
        assert cmd.buffer == "w"
        cmd.handle_key("down")
        assert cmd.buffer == "q"
