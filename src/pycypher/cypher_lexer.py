'''
Lexer
=====

This uses the ``PLY`` package to define the lexer. "Lexing" is the process of
breaking a string into tokens. This is the first step in the process of
parsing a language.

The lexer itself is defined at the end of the file (``lexer = lex.lex()``). It is
imported by the parser, which is defined in the ``__init__.py`` file.
'''

from typing import Any

import ply.lex as lex

tokens = [
    "COLON",
    "COMMA",
    "DASH",
    "DIVIDE",
    "DOT",
    "DQUOTE",
    "EQUALS",
    "FLOAT",
    "GREATERTHAN",
    "ID",
    "INTEGER",
    "LCURLY",
    "LESSTHAN",
    "LPAREN",
    "LSQUARE",
    "PLUS",
    "RCURLY",
    "RPAREN",
    "RSQUARE",
    "STAR",
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
t_PLUS = r"\+"
t_RCURLY = r"\}"
t_RPAREN = r"\)"
t_RSQUARE = r"\]"
t_STAR = r"\*"

reserved = {
    "AND": "AND",
    "AS": "AS",
    "IF": "IF",
    "MATCH": "MATCH",
    "NOT": "NOT",
    "OR": "OR",
    "RETURN": "RETURN",
    "THEN": "THEN",
    "WHERE": "WHERE",
}


tokens = tokens + list(reserved.values())


def t_WORD(t: lex.LexToken) -> Any:
    r"[a-zA-Z_][a-zA-Z_0-9]*"
    t.type = reserved.get(t.value, "WORD")  # Check for reserved words
    return t


t_ignore = " \t"
lexer = lex.lex()
