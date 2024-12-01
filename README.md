[![Install and run tests](https://github.com/zacernst/pycypher/actions/workflows/makefile.yml/badge.svg)](https://github.com/zacernst/pycypher/actions/workflows/makefile.yml)

[![Build Sphinx documentation](https://github.com/zacernst/pycypher/actions/workflows/docs.yml/badge.svg)](https://github.com/zacernst/pycypher/actions/workflows/docs.yml)

[![Deploy documentation to Github Pages](https://github.com/zacernst/pycypher/actions/workflows/pages/pages-build-deployment/badge.svg)](https://github.com/zacernst/pycypher/actions/workflows/pages/pages-build-deployment)

# Cypher AST Generator for Python

This is a *work in progress*, by which I mean, "ugly, but fixable." It is also woefully incomplete. It generates an abstract syntax tree for Cypher statements that use only a subset of the language. That subset is growing, but it's still small.

Additionally, this package contains the beginning of a query engine that is designed to accept Cypher queries and return results from arbitrary graph structures in Python. This functionality is in a _very_ early state, and works only for trivial queries.

The hope is that this will be useful for building modules that can take advantage of the Cypher query language, by eliminating the need to do all the boring work of writing a parser and generating an AST.

The documentation is [here](https://zacernst.github.io/pycypher/).