"""
This uses the ``PLY`` package to define the lexer. "Lexing" is the process of
breaking a string into tokens. This is the first step in the process of
parsing a language.

The lexer itself is defined at the end of the file (``lexer = lex.lex()``). It is
imported by the parser, which is defined in the ``__init__.py`` file.
"""
# pylint: disable=invalid-name

from typing import Any

from ply import lex

tokens: list[str] = [
    "ADDITION",
    "COLON",
    "COMMA",
    "DASH",
    "DIVIDE",
    "DOT",
    "DQUOTE",
    "EQUALS",
    "FLOAT",
    "GREATERTHAN",
    # "ID",
    "INTEGER",
    "LCURLY",
    "LESSTHAN",
    "LPAREN",
    "LENGTH",
    "LSQUARE",
    "RCURLY",
    "RPAREN",
    "RSQUARE",
    # "STAR",
    "STRING",
    "WORD",
]

t_COLON = r":"
t_COMMA = r","
t_DASH = r"-"
t_DIVIDE = r"/"
t_DOT = r"\."
t_DQUOTE = r'"'
t_EQUALS = r"="
t_FLOAT = r"\d+\.\d+"
t_GREATERTHAN = r">"
t_INTEGER = r"\d+"
t_LCURLY = r"\{"
t_LESSTHAN = r"<"
t_LPAREN = r"\("
t_LSQUARE = r"\["
t_ADDITION = r"\+"
t_RCURLY = r"\}"
t_RPAREN = r"\)"
t_RSQUARE = r"\]"
# t_STAR = r"\*"

reserved = {
    "AND": "AND",
    "AS": "AS",
    # "IF": "IF",
    "MATCH": "MATCH",
    "NOT": "NOT",
    "OR": "OR",
    "RETURN": "RETURN",
    # "THEN": "THEN",
    "WHERE": "WHERE",
    "COLLECT": "COLLECT",
    "DISTINCT": "DISTINCT",
    "WITH": "WITH",
    "SIZE": "SIZE",
    "COUNT": "COUNT",
}


tokens = tokens + list(reserved.values())


def t_WORD(t: lex.LexToken) -> Any:
    r"[a-zA-Z_][a-zA-Z_0-9]*"
    t.type = reserved.get(t.value, "WORD")  # Check for reserved words
    return t


def t_STRING(t):
    r'"[^"]*"'
    t.value = t.value[1:-1]  # Remove the surrounding quotes
    return t


def t_error(t):
    raise Exception(f"Illegal character '{t.value[0]}'")


t_ignore = " \t"
lexer = lex.lex()
