[![Install and run tests](https://github.com/zacernst/pycypher/actions/workflows/makefile.yml/badge.svg)](https://github.com/zacernst/pycypher/actions/workflows/makefile.yml)

# Cypher AST Generator for Python

This is a *work in progress*, by which I mean, "ugly, but fixable." It is also woefully incomplete. It generates an abstract syntax tree for Cypher statements that use only a subset of the language. That subset is growing, but it's still small.

Additionally, this package contains the beginning of a query engine that is designed to accept Cypher queries and return results from arbitrary graph structures in Python. This functionality is in a _very_ early state, and works only for trivial queries.

The hope is that this will be useful for building modules that can take advantage of the Cypher query language, by eliminating the need to do all the boring work of writing a parser and generating an AST.

## How to use it

Don't. But if you really want to, then:

```python
>>> from pycypher.parser import CypherParser
>>> cypher = CypherParser("MATCH (n:Thing) RETURN n.foo")
>>> cypher.parsed.print_tree()

Cypher
└── Query
    ├── Match
    │   └── Node
    │       └── NodeNameLabel
    │           ├── n
    │           └── Thing
    └── Return
        └── Projection
            └── ObjectAttributeLookup
                ├── n
                └── foo
>>> print(cypher.parsed)
Cypher(Query(Match(Node(NodeNameLabel(n, Thing), None)), Return(Projection([ObjectAttributeLookup(n, foo)]))))
```

If you want to understand what's happening, what Python classes are being built, etc., then you'll have to use the source, Luke. Check out the `__main__` function at the end of the `cypher.py` script. There are no docs yet. Like I said, this is a *work in progress*.

In addition to parsing Cypher queries, there is the beginning of support for querying your data with Cypher. It is very experimental.

The design of `pycypher`'s querying process requires a few simple steps:

* First, you define individual `Fact` objects from your graph data;
* then you put them all in a `FactCollection` object. 
* Instantiate a `CypherParser` object with your Cypher query;
* finally, call `CypherParser.solutions` with your `FactCollection`,
  which will return a list of dictionaries containing solutions to your query.

```python
################################
### Build FactCollection
################################

fact1 = FactNodeHasLabel("1", "Thing")
fact2 = FactNodeHasAttributeWithValue("1", "key", Literal("2"))
fact3 = FactNodeRelatedToNode("1", "2", "MyRelationship")
fact4 = FactNodeHasLabel("2", "OtherThing")
fact5 = FactNodeHasAttributeWithValue("2", "key", Literal(5))

fact_collection = FactCollection([fact1, fact2, fact3, fact4, fact5])

###########################################
### Define Cypher Query
###########################################

cypher_statement = """MATCH (n:Thing {key: 2}) RETURN n.key"""

###########################################
### Parse Cypher Query
###########################################

parsed = CypherParser(cypher_statement)
instances = parsed.solutions(fact_collection)
rich.print(instances)
```

which will return:

```
[{n: 1}]
```

where `n` is the node variable from your Cypher query. It says that the node whose ID is `1` can be put in for the value of `n` in your Cypher query. Alert readers will notice that the query actually asks for an attribute of `n`, not the ID of `n` itself. I know; we haven't gotten there yet.

Why is it designed this way? The idea is that if you've got a graph-like structure (say, a `networkx` graph), it would be very easy to walk the graph and create a list of simple `Fact` objects. Those can be put in a `FactCollection` and passed into your `CypherParser`. In other words, the various `Fact` classes are there to provide a simple and intuitive target to represent graph data. So long as you can get the data into a `FactCollection`, you can query it. The next logial step in developing this package is to provide out-of-the-box methods for querying various graph data formats, probably starting with `networkx`.

## Under the hood

The package is simple, but complicated in the sense of "God, this is tedious!". It contains a grammar in the old style of Lex and Yacc, which is processed by the `PLY` package. From there, an AST is generated which is constructed from a set of classes representing the semantic structure of the query (in contrast to a so-called "concrete" syntax tree which literally represents only the exact syntax).

For querying your data with the help of the AST, this package treats querying as a problem of constraint satisfaction over a finite domain. The AST yields a set of constraints such as "There is a node named `n` which has the label `Foo`". The `FactCollection` object defines the domain of the constraint satisfaction problem. When you ask for solutions, a potentially large number of partial functions over the finite domain is generated, which form the constraints. Then we apply a backtracking constraint solver to get every set of assignments of variables to the domain satisfying the constraints.

## Installation

### Mac and Linux

You'll need to be able to run `uv` in order to use the `Makefile`. To install `uv` on Linux or Mac:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

If you don't have `make` on your Mac, then you should:

```bash
brew install make
```

And if you don't have `brew`, then install it with:

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

If you're running Linux without `make`, then follow the directions for your distribution. For example,
on Ubuntu, you can:

```bash
sudo apt install make
```

### Windows 

On Windows, erase your hard drive, install Linux, and then follow the directions above.

## Setting everything up

To set up the virtual environment, install all the dependencies, install the right version of Python, build the package, install it as an editable project, run a bunch of unit tests, and build HTML documentation, do:

```bash
make all
```

To clean everything up, deleting the virtual environment, documentation, and so on, do:

```bash
make clean
```

You don't *need* to use the `Makefile`, and therefore you don't *need* to have `uv` installed on your system. But that's what all the cool kids are using these days.