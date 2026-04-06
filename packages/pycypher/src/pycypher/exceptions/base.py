"""Exception infrastructure: documentation links, environment detection, and utilities.

This module provides the shared infrastructure used by all exception classes:
documentation URL linking, terminal environment detection, and error message
sanitization.
"""

from __future__ import annotations

import dataclasses
import os
import re as _re
import sys

# Base URL for error documentation.  Change this single value when the
# docs site moves.  Set to ``""`` to suppress all doc links.
_DOCS_BASE_URL = "https://pycypher.readthedocs.io/en/latest"

# Mapping from exception class name -> docs page anchor.
_DOCS_ANCHORS: dict[str, str] = {
    "CypherSyntaxError": "/user_guide/error_handling.html#cyphersyntaxerror",
    "QueryTimeoutError": "/user_guide/error_handling.html#querytimeouterror",
    "QueryMemoryBudgetError": "/user_guide/error_handling.html#querymemorybudgeterror",
    "VariableNotFoundError": "/user_guide/error_handling.html#variablenotfounderror",
    "UnsupportedFunctionError": "/user_guide/error_handling.html#unsupportedfunctionerror",
    "QueryComplexityError": "/user_guide/error_handling.html#querycomplexityerror",
    "CyclicDependencyError": "/user_guide/error_handling.html#cyclicdependencyerror",
    "SecurityError": "/user_guide/error_handling.html#securityerror",
    "RateLimitError": "/user_guide/error_handling.html#ratelimiterror",
    "GraphTypeNotFoundError": "/user_guide/error_handling.html#graphtypenotfounderror",
    "IncompatibleOperatorError": "/user_guide/error_handling.html#incompatibleoperatorerror",
    "VariableTypeMismatchError": "/user_guide/error_handling.html#variabletypemismatcherror",
    "FunctionArgumentError": "/user_guide/error_handling.html#unsupportedfunctionerror",
    "MissingParameterError": "/user_guide/error_handling.html#catching-runtime-errors",
    "ASTConversionError": "/user_guide/error_handling.html#catching-parse-errors",
    "WrongCypherTypeError": "/user_guide/error_handling.html#incompatibleoperatorerror",
    "InvalidCastError": "/user_guide/error_handling.html#catching-runtime-errors",
    "PatternComprehensionError": "/user_guide/error_handling.html#catching-runtime-errors",
    "WorkerExecutionError": "/user_guide/error_handling.html#catching-runtime-errors",
    "CacheLockTimeoutError": "/user_guide/error_handling.html#querytimeouterror",
    "GrammarTransformerSyncError": "/user_guide/error_handling.html#catching-parse-errors",
    "TemporalArithmeticError": "/user_guide/error_handling.html#incompatibleoperatorerror",
    "UnsupportedOperatorError": "/user_guide/error_handling.html#incompatibleoperatorerror",
}


@dataclasses.dataclass(frozen=True)
class DocsLink:
    """Structured documentation link attached to an exception."""

    url: str
    exception_name: str

    def as_terminal(self) -> str:
        """Format as a clickable OSC 8 hyperlink for modern terminals."""
        # OSC 8 hyperlink: \033]8;;URL\033\\LABEL\033]8;;\033\\
        return f"\nDocs: \033]8;;{self.url}\033\\{self.url}\033]8;;\033\\"

    def as_plain(self) -> str:
        """Format as a plain-text URL (fallback for basic terminals)."""
        return f"\nDocs: {self.url}"

    def as_html(self) -> str:
        """Format as an HTML anchor for Jupyter/IPython display."""
        from html import escape

        safe_url = escape(self.url, quote=True)
        return (
            f'\nDocs: <a href="{safe_url}" target="_blank">'
            f"{escape(self.url)}</a>"
        )


_CACHED_ENVIRONMENT: str | None = None


def _detect_environment() -> str:
    """Detect the current runtime environment.

    Returns one of ``"jupyter"``, ``"terminal"``, or ``"plain"``.
    The result is cached after the first call to avoid repeated
    IPython imports (~270ms on first call).
    """
    global _CACHED_ENVIRONMENT  # noqa: PLW0603
    if _CACHED_ENVIRONMENT is not None:
        return _CACHED_ENVIRONMENT

    env = "plain"

    # Jupyter / IPython notebook -- only attempt if already imported
    if "IPython" in sys.modules:
        try:
            from IPython import get_ipython  # type: ignore[import-untyped]

            ipy = get_ipython()
            if ipy is not None and "ZMQInteractiveShell" in type(ipy).__name__:
                env = "jupyter"
        except (ImportError, NameError):
            pass

    if env == "plain":
        # Modern terminal with OSC 8 support heuristic
        if os.environ.get("TERM_PROGRAM") in (
            "iTerm2",
            "WezTerm",
            "Hyper",
            "vscode",
        ) or os.environ.get("WT_SESSION"):
            # Windows Terminal, iTerm2, WezTerm, VS Code terminal
            env = "terminal"
        # COLORTERM is a reasonable proxy for a capable terminal
        elif os.environ.get("COLORTERM") in ("truecolor", "24bit"):
            env = "terminal"

    _CACHED_ENVIRONMENT = env
    return env


def _make_docs_link(cls_name: str) -> DocsLink | None:
    """Build a :class:`DocsLink` for *cls_name*, or ``None`` if unmapped."""
    anchor = _DOCS_ANCHORS.get(cls_name, "")
    if anchor and _DOCS_BASE_URL:
        return DocsLink(url=f"{_DOCS_BASE_URL}{anchor}", exception_name=cls_name)
    return None


def _docs_hint(cls_name: str) -> str:
    """Return a doc-link hint string for the given exception class, or ``""``."""
    link = _make_docs_link(cls_name)
    if link is None:
        return ""
    env = _detect_environment()
    if env == "jupyter":
        return link.as_html()
    if env == "terminal":
        return link.as_terminal()
    return link.as_plain()


# ---------------------------------------------------------------------------
# User-facing error message sanitisation
# ---------------------------------------------------------------------------

# Patterns that may leak internal details in exception messages.
_INTERNAL_PATH_RE = _re.compile(
    r"(?:/[a-zA-Z0-9_./-]+(?:\.py|\.so|\.pyd)(?::\d+)?)",
)
_TRACEBACK_RE = _re.compile(
    r"(?:File \".+?\", line \d+|Traceback \(most recent call last\))",
)
# Matches URI credentials: scheme://user:PASSWORD@ -> scheme://user:***@
_URI_PASSWORD_RE = _re.compile(
    r"(\w+://[^:/?#]+):[^@/?#]+@",
)


def sanitize_error_message(exc: BaseException) -> str:
    """Return a user-safe error string from *exc*.

    Strips internal file paths and traceback fragments that could leak
    implementation details.  Masks URI credentials (passwords) to prevent
    credential exposure in error output.  The exception *type* name is
    preserved so the user can still identify the category of error.
    """
    msg = str(exc)
    msg = _INTERNAL_PATH_RE.sub("<internal>", msg)
    msg = _TRACEBACK_RE.sub("", msg)
    # Mask credentials in any URIs embedded in the message.
    msg = _URI_PASSWORD_RE.sub(r"\1:***@", msg)
    # Collapse any resulting double spaces.
    msg = _re.sub(r"  +", " ", msg).strip()
    return f"{type(exc).__name__}: {msg}" if msg else type(exc).__name__
