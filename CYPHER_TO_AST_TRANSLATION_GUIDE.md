# Cypher to AST Translation Guide

## Overview

This document explains the complete translation pipeline from a Cypher query string to a simplified, type-safe Abstract Syntax Tree (AST). The process involves three main stages:

1. **Lexical Analysis & Parsing** - Converting text into a parse tree using Lark
2. **AST Transformation** - Converting the parse tree into a dictionary-based intermediate representation
3. **Type Conversion** - Converting dictionaries into Pydantic-based typed AST nodes

## The Three-Stage Pipeline

```
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────┐      ┌──────────────┐
│ Cypher String   │  →   │ Lark Parse Tree  │  →   │ Dictionary AST  │  →   │ Typed AST    │
│ "MATCH (n)..."  │      │ (Tree objects)   │      │ {type: "Match"} │      │ (Pydantic)   │
└─────────────────┘      └──────────────────┘      └─────────────────┘      └──────────────┘
     Input              GrammarParser.parse()   CypherASTTransformer    ASTConverter.convert()
```

### Why Three Stages?

**Stage 1 (Parsing)**: We need to validate syntax and build a structured tree. Lark provides robust parsing with error recovery.

**Stage 2 (Transformation)**: Parse trees contain too much detail (whitespace, parentheses, operator precedence). We simplify to semantic structure only.

**Stage 3 (Type Conversion)**: Dictionaries are untyped and error-prone. Pydantic models provide:
- Type safety with validation
- IDE autocomplete
- Runtime error detection
- Clean API for traversal and manipulation

## Stage 1: Lexical Analysis and Parsing

### The Grammar

The Cypher grammar is defined using Lark's EBNF syntax in `grammar_parser.py`. Here's a simplified view:

```ebnf
// Root rule
start: cypher_query

// Top-level query structure
cypher_query: statement_list

// Statements can be queries or updates
statement_list: query_statement | update_statement | ...

// Query statements
query_statement: match_clause+ where_clause? return_clause

// Patterns
match_clause: "MATCH" pattern
pattern: path_pattern ("," path_pattern)*
path_pattern: node_pattern (relationship_pattern node_pattern)*

// Node patterns
node_pattern: "(" variable_name? labels? properties? ")"
variable_name: IDENTIFIER
labels: ":" label_name (":" label_name)*
```

### How Lark Parsing Works

**Earley Parser**: We use Lark's Earley algorithm, which:
- Handles left-recursive grammars naturally
- Supports ambiguous grammars (important for complex Cypher expressions)
- Provides good error messages

**Bottom-Up Construction**: The parser builds the tree from the leaves (tokens) up to the root:

```python
# Input
"MATCH (n:Person) RETURN n"

# Tokens (lexical analysis)
["MATCH", "(", "n", ":", "Person", ")", "RETURN", "n"]

# Parse Tree (syntax analysis)
Tree('cypher_query', [
    Tree('statement_list', [
        Tree('query_statement', [
            Tree('match_clause', [
                Tree('pattern', [
                    Tree('path_pattern', [
                        Tree('node_pattern', [
                            Token('IDENTIFIER', 'n'),
                            Tree('labels', [Token('LABEL_NAME', 'Person')])
                        ])
                    ])
                ])
            ]),
            Tree('return_clause', [
                Tree('return_item', [
                    Tree('expression', [Token('IDENTIFIER', 'n')])
                ])
            ])
        ])
    ])
])
```

### Example: Parsing a Simple Query

```python
from pycypher.grammar_parser import GrammarParser

parser = GrammarParser()
query = "MATCH (n:Person) RETURN n"

# Stage 1: String → Parse Tree
tree = parser.parse(query)
print(tree.pretty())
```

**Output:**
```
cypher_query
  statement_list
    query_statement
      match_clause
        pattern
          path_pattern
            node_pattern
              n           # variable
              :Person     # label
      return_clause
        return_item
          n             # variable reference
```

**Why this structure?** The parse tree preserves the complete syntactic structure, showing:
- Where clauses appear in the query
- Nested structure of patterns
- All tokens in their grammatical roles

This is too detailed for execution, but perfect for syntax validation.

## Stage 2: AST Transformation

### The CypherASTTransformer

The transformer converts Lark's parse tree into a cleaner, semantic representation using the **Visitor Pattern**:

```python
class CypherASTTransformer(Transformer):
    """Converts parse tree → dictionary AST"""
    
    def cypher_query(self, args: List[Any]) -> Dict[str, Any]:
        """Entry point: wrap all statements in a Query container."""
        return {"type": "Query", "statements": args}
    
    def node_pattern(self, args: List[Any]) -> Dict[str, Any]:
        """Extract semantic info from node pattern syntax."""
        variable = None
        labels = []
        properties = {}
        
        for arg in args:
            if isinstance(arg, Token) and arg.type == 'IDENTIFIER':
                variable = str(arg.value)
            elif isinstance(arg, dict) and arg.get("type") == "Labels":
                labels = arg.get("labels", [])
            elif isinstance(arg, dict) and arg.get("type") == "Properties":
                properties = arg.get("properties", {})
        
        return {
            "type": "NodePattern",
            "variable": variable,
            "labels": labels,
            "properties": properties
        }
```

### How Transformation Works

**Bottom-Up Traversal**: Lark calls transformer methods from leaves to root:

1. **Leaf nodes** (tokens) are passed as-is
2. **Inner nodes** receive already-transformed children
3. Each method returns a **simplified representation**

**Example Transformation:**

```python
# Input: Tree('node_pattern', [Token('IDENTIFIER', 'n'), Tree('labels', [...])])

# Transformer receives:
args = [
    Token('IDENTIFIER', 'n'),
    {"type": "Labels", "labels": ["Person"]}  # Already transformed
]

# Method extracts semantics:
def node_pattern(self, args):
    variable = "n"
    labels = ["Person"]
    return {
        "type": "NodePattern",
        "variable": "n",
        "labels": ["Person"],
        "properties": {}
    }
```

**Why dictionaries?** This intermediate form:
- Removes syntactic noise (parentheses, keywords)
- Normalizes structure (consistent field names)
- Preserves all semantic information
- Easy to debug (can print as JSON)

### Transformation Example: Complete Query

```python
from pycypher.grammar_parser import GrammarParser

parser = GrammarParser()
query = "MATCH (n:Person {age: 30}) WHERE n.name = 'Alice' RETURN n.name AS name"

# Stage 1: String → Parse Tree
tree = parser.parse(query)

# Stage 2: Parse Tree → Dictionary AST
ast_dict = parser.transformer.transform(tree)
```

**Resulting Dictionary AST:**

```python
{
    "type": "Query",
    "statements": [{
        "type": "QueryStatement",
        "clauses": [
            {
                "type": "MatchClause",
                "optional": False,
                "pattern": {
                    "type": "Pattern",
                    "paths": [{
                        "type": "PathPattern",
                        "elements": [{
                            "type": "NodePattern",
                            "variable": "n",
                            "labels": ["Person"],
                            "properties": {
                                "age": 30
                            }
                        }]
                    }]
                }
            },
            {
                "type": "WhereClause",
                "expression": {
                    "type": "Comparison",
                    "operator": "=",
                    "left": {
                        "type": "PropertyAccess",
                        "object": "n",
                        "property": "name"
                    },
                    "right": "Alice"
                }
            }
        ],
        "return": {
            "type": "ReturnStatement",
            "distinct": False,
            "items": [{
                "type": "ReturnItem",
                "expression": {
                    "type": "PropertyAccess",
                    "object": "n",
                    "property": "name"
                },
                "alias": "name"
            }]
        }
    }]
}
```

**Key simplifications:**
- Keywords removed (MATCH, WHERE, RETURN)
- Nested structure flattened where appropriate
- All values typed (booleans, integers, strings)
- Clear separation of clauses vs return

## Stage 3: Type Conversion to Pydantic Models

### The ASTConverter

The converter transforms dictionaries into strongly-typed Pydantic models:

```python
class ASTConverter:
    """Converts dictionary AST → Pydantic AST"""
    
    def convert(self, node: Any) -> Optional[ASTNode]:
        """Main conversion dispatcher."""
        if node is None:
            return None
        
        # Handle primitives
        if not isinstance(node, dict):
            return self._convert_primitive(node)
        
        # Get node type and dispatch to specific converter
        node_type = node.get("type")
        converter_method = getattr(self, f"_convert_{node_type}", None)
        
        if converter_method:
            return converter_method(node)
        
        return self._convert_generic(node, node_type)
```

### Why Pydantic?

Pydantic provides **compile-time type safety** and **runtime validation**:

```python
# Dictionary (Stage 2) - No type safety
node = {"type": "NodePattern", "variable": "n", "labels": ["Person"]}
node["variable"] = 123  # SILENT ERROR - wrong type!
node["nonexistent"] = "oops"  # SILENT ERROR - invalid field!

# Pydantic (Stage 3) - Type safety enforced
from pycypher.ast_models import NodePattern

node = NodePattern(variable="n", labels=["Person"])
node.variable = 123  # VALIDATION ERROR - must be string
node.nonexistent = "oops"  # ATTRIBUTE ERROR - field doesn't exist

# IDE autocomplete works
node.variable  # IDE knows this is Optional[str]
node.labels    # IDE knows this is List[str]
```

### Type Conversion Process

**Pattern Matching**: The converter uses Python's match-case for type dispatch:

```python
def _convert_primitive(self, value: Any) -> Any:
    """Convert primitives to appropriate AST nodes."""
    match value:
        case None:
            return None
        case bool():
            return BooleanLiteral(value=value)
        case int():
            return IntegerLiteral(value=value)
        case float():
            return FloatLiteral(value=value)
        case str():
            # Context-dependent: could be Variable or StringLiteral
            return Variable(name=value)
        case list():
            elements = [self.convert(item) for item in value]
            return ListLiteral(elements=elements)
        case dict():
            # Structured node - dispatch to type-specific converter
            return self.convert(value)
```

**Type-Specific Converters**: Each AST node type has a dedicated converter:

```python
def _convert_NodePattern(self, node: dict) -> NodePattern:
    """Convert dictionary to NodePattern model."""
    return NodePattern(
        variable=node.get("variable"),
        labels=node.get("labels", []),
        properties=self.convert(node.get("properties", {}))
    )

def _convert_MatchClause(self, node: dict) -> MatchClause:
    """Convert dictionary to MatchClause model."""
    return MatchClause(
        optional=node.get("optional", False),
        pattern=self.convert(node.get("pattern"))
    )

def _convert_WhereClause(self, node: dict) -> WhereClause:
    """Convert dictionary to WhereClause model."""
    return WhereClause(
        expression=self.convert(node.get("expression"))
    )
```

### Complete Conversion Example

```python
from pycypher.grammar_parser import GrammarParser
from pycypher.ast_models import ASTConverter

parser = GrammarParser()
converter = ASTConverter()

query = "MATCH (n:Person {age: 30}) RETURN n.name"

# Stage 1: String → Parse Tree
tree = parser.parse(query)

# Stage 2: Parse Tree → Dictionary AST
ast_dict = parser.transformer.transform(tree)

# Stage 3: Dictionary → Typed AST
typed_ast = converter.convert(ast_dict)

# Now we have strongly-typed objects
print(type(typed_ast))  # <class 'pycypher.ast_models.Query'>
print(type(typed_ast.statements[0]))  # <class 'pycypher.ast_models.QueryStatement'>

# Type-safe access with autocomplete
query_stmt = typed_ast.statements[0]
match_clause = query_stmt.clauses[0]
node_pattern = match_clause.pattern.paths[0].elements[0]

print(node_pattern.variable)  # "n"
print(node_pattern.labels)    # ["Person"]
print(node_pattern.properties["age"])  # IntegerLiteral(value=30)
```

## Complete Example: Step-by-Step

Let's trace the complete pipeline for a realistic query:

### Input Query

```cypher
MATCH (p:Person)-[r:KNOWS]->(f:Person)
WHERE p.age > 30 AND f.active = true
RETURN p.name AS person, f.name AS friend
ORDER BY person
LIMIT 10
```

### Stage 1: Parsing

```python
parser = GrammarParser()
tree = parser.parse(query)
```

**Parse Tree (simplified):**
```
cypher_query
  statement_list
    query_statement
      match_clause
        pattern
          path_pattern
            node_pattern [variable=p, labels=[Person]]
            relationship_pattern [variable=r, types=[KNOWS], direction=RIGHT]
            node_pattern [variable=f, labels=[Person]]
      where_clause
        and_expression
          comparison [p.age > 30]
          comparison [f.active = true]
      return_clause
        return_item [p.name AS person]
        return_item [f.name AS friend]
      order_by_clause
        sort_item [expression=person, ascending=true]
      limit_clause [value=10]
```

**Why this structure?** The parse tree shows exact syntactic structure with all nesting preserved.

### Stage 2: Transformation

```python
ast_dict = parser.transformer.transform(tree)
```

**Dictionary AST:**
```python
{
    "type": "Query",
    "statements": [{
        "type": "QueryStatement",
        "clauses": [
            {
                "type": "MatchClause",
                "optional": False,
                "pattern": {
                    "type": "Pattern",
                    "paths": [{
                        "type": "PathPattern",
                        "elements": [
                            {
                                "type": "NodePattern",
                                "variable": "p",
                                "labels": ["Person"],
                                "properties": {}
                            },
                            {
                                "type": "RelationshipPattern",
                                "variable": "r",
                                "types": ["KNOWS"],
                                "direction": "RIGHT",
                                "properties": {},
                                "variable_length": None
                            },
                            {
                                "type": "NodePattern",
                                "variable": "f",
                                "labels": ["Person"],
                                "properties": {}
                            }
                        ]
                    }]
                }
            },
            {
                "type": "WhereClause",
                "expression": {
                    "type": "And",
                    "operands": [
                        {
                            "type": "Comparison",
                            "operator": ">",
                            "left": {
                                "type": "PropertyAccess",
                                "object": "p",
                                "property": "age"
                            },
                            "right": 30
                        },
                        {
                            "type": "Comparison",
                            "operator": "=",
                            "left": {
                                "type": "PropertyAccess",
                                "object": "f",
                                "property": "active"
                            },
                            "right": True
                        }
                    ]
                }
            }
        ],
        "return": {
            "type": "ReturnStatement",
            "distinct": False,
            "items": [
                {
                    "type": "ReturnItem",
                    "expression": {
                        "type": "PropertyAccess",
                        "object": "p",
                        "property": "name"
                    },
                    "alias": "person"
                },
                {
                    "type": "ReturnItem",
                    "expression": {
                        "type": "PropertyAccess",
                        "object": "f",
                        "property": "name"
                    },
                    "alias": "friend"
                }
            ],
            "order_by": [{
                "type": "SortItem",
                "expression": {"type": "Variable", "name": "person"},
                "ascending": True
            }],
            "limit": 10
        }
    }]
}
```

**Why this structure?**
- **Nested semantics preserved**: Pattern contains paths, paths contain elements
- **Types explicit**: Every node has a "type" field
- **Metadata included**: Direction, optional flags, operators
- **Clean values**: Primitives (30, True, "person") not wrapped yet

### Stage 3: Type Conversion

```python
converter = ASTConverter()
typed_ast = converter.convert(ast_dict)
```

**Typed AST (Pydantic objects):**
```python
Query(
    statements=[
        QueryStatement(
            clauses=[
                MatchClause(
                    optional=False,
                    pattern=Pattern(
                        paths=[
                            PathPattern(
                                elements=[
                                    NodePattern(
                                        variable="p",
                                        labels=["Person"],
                                        properties={}
                                    ),
                                    RelationshipPattern(
                                        variable="r",
                                        types=["KNOWS"],
                                        direction=Direction.RIGHT,
                                        properties={},
                                        variable_length=None
                                    ),
                                    NodePattern(
                                        variable="f",
                                        labels=["Person"],
                                        properties={}
                                    )
                                ]
                            )
                        ]
                    )
                ),
                WhereClause(
                    expression=And(
                        operands=[
                            Comparison(
                                operator=ComparisonOperator.GT,
                                left=PropertyAccess(
                                    expression=Variable(name="p"),
                                    property="age"
                                ),
                                right=IntegerLiteral(value=30)
                            ),
                            Comparison(
                                operator=ComparisonOperator.EQ,
                                left=PropertyAccess(
                                    expression=Variable(name="f"),
                                    property="active"
                                ),
                                right=BooleanLiteral(value=True)
                            )
                        ]
                    )
                )
            ],
            return_clause=ReturnStatement(
                distinct=False,
                items=[
                    ReturnItem(
                        expression=PropertyAccess(
                            expression=Variable(name="p"),
                            property="name"
                        ),
                        alias="person"
                    ),
                    ReturnItem(
                        expression=PropertyAccess(
                            expression=Variable(name="f"),
                            property="name"
                        ),
                        alias="friend"
                    )
                ],
                order_by=[
                    SortItem(
                        expression=Variable(name="person"),
                        ascending=True
                    )
                ],
                limit=IntegerLiteral(value=10)
            )
        )
    ]
)
```

**Why this structure?**
- **Full type safety**: IDE knows field types, validates assignments
- **Enums for constants**: `Direction.RIGHT`, `ComparisonOperator.GT`
- **Literal wrappers**: Primitives wrapped in typed nodes (`IntegerLiteral`, `BooleanLiteral`)
- **Clean API**: Can traverse with typed accessors

## Common Patterns and Design Decisions

### Pattern 1: Context-Dependent String Conversion

**Problem**: Strings can represent variables, property names, or literal strings.

**Solution**: Convert based on context:

```python
def _convert_PropertyAccess(self, node: dict) -> PropertyAccess:
    """Property access: object is a variable, property is a name."""
    return PropertyAccess(
        expression=Variable(name=node["object"]),  # String → Variable
        property=node["property"]  # String stays string
    )

def _convert_StringLiteral(self, node: dict) -> StringLiteral:
    """Explicit string literal."""
    return StringLiteral(value=node["value"])  # String → StringLiteral
```

**Why?** Different contexts require different interpretations:
- `n.name` → `n` is a Variable, `name` is a property name
- `'Alice'` → Literal string value
- `RETURN n` → `n` is a Variable

### Pattern 2: Optional Fields with Defaults

**Problem**: Not all clauses have all fields (e.g., WHERE is optional).

**Solution**: Use Pydantic's `Optional` with defaults:

```python
class MatchClause(BaseModel):
    optional: bool = False  # OPTIONAL MATCH vs MATCH
    pattern: Pattern
    where: Optional[WhereClause] = None  # WHERE is optional
```

**Why?** This allows creating nodes without all fields:

```python
# With WHERE
MatchClause(pattern=..., where=WhereClause(...))

# Without WHERE
MatchClause(pattern=...)  # where defaults to None
```

### Pattern 3: List Normalization

**Problem**: Single items vs lists in grammar (e.g., one label vs many).

**Solution**: Always use lists in AST:

```python
def _convert_Labels(self, node: dict) -> List[str]:
    """Convert labels to list, even if single label."""
    labels = node.get("labels", [])
    if isinstance(labels, str):
        return [labels]
    return labels

# Grammar allows:
# (n:Person)      → labels = ["Person"]
# (n:Person:User) → labels = ["Person", "User"]

# AST always uses:
NodePattern(labels=["Person"])  # List of 1
NodePattern(labels=["Person", "User"])  # List of 2
```

**Why?** Consistent API - code doesn't need to check if it's a list or single value.

### Pattern 4: Operator Enums

**Problem**: String operators ("=", ">", "AND") are error-prone.

**Solution**: Use Pydantic enums:

```python
class ComparisonOperator(str, Enum):
    EQ = "="
    NE = "<>"
    LT = "<"
    GT = ">"
    LTE = "<="
    GTE = ">="

class Comparison(Expression):
    operator: ComparisonOperator  # Type-safe enum
    left: Expression
    right: Expression

# Usage
comp = Comparison(operator=ComparisonOperator.GT, ...)
if comp.operator == ComparisonOperator.GT:
    # IDE autocompletes operator values
    # Typos caught at runtime
```

**Why?** 
- IDE autocomplete for valid operators
- Catch typos (`">>"` instead of `">"`)
- Self-documenting code

## Advanced Features

### Handling Ambiguity

**Problem**: Some Cypher constructs are syntactically ambiguous.

**Solution**: Lark's `_ambig` method + semantic disambiguation:

```python
class CypherASTTransformer(Transformer):
    def _ambig(self, args: List[Any]) -> Any:
        """Handle ambiguous parses."""
        # Example: Function call vs variable access
        # Both `count(n)` and `n` can parse as expressions
        
        # Choose most specific interpretation
        for arg in args:
            if isinstance(arg, dict):
                if arg.get("type") == "FunctionCall":
                    return arg  # Prefer function over variable
        
        return args[0]  # Default to first option
```

**Why?** Some syntax has multiple valid parses. We choose the most useful interpretation.

### Preserving Location Information

**Problem**: Error messages need to know where in the query errors occur.

**Solution**: Lark's `meta` attribute preserves source positions:

```python
def node_pattern(self, args: List[Any]) -> Dict[str, Any]:
    """Transform node pattern, preserving source location."""
    meta = args[0].meta if hasattr(args[0], 'meta') else None
    
    return {
        "type": "NodePattern",
        "variable": ...,
        "labels": ...,
        "_meta": {
            "line": meta.line if meta else None,
            "column": meta.column if meta else None
        }
    }
```

**Why?** Users need to know **where** errors occur:
```
Error: Variable 'x' is undefined
  at line 5, column 23
```

### Recursive Conversion

**Problem**: Nested expressions can be arbitrarily deep.

**Solution**: Recursive converter calls:

```python
def _convert_And(self, node: dict) -> And:
    """Convert AND expression, recursively converting operands."""
    return And(
        operands=[
            self.convert(op) for op in node.get("operands", [])
        ]
    )

# Example: (a AND b) AND (c AND d)
And(
    operands=[
        And(operands=[convert(a), convert(b)]),  # Recursive
        And(operands=[convert(c), convert(d)])   # Recursive
    ]
)
```

**Why?** Expressions nest arbitrarily: `a AND b AND (c OR (d AND e))`. Recursion handles any depth.

## Error Handling

### Parse Errors (Stage 1)

**Lark provides detailed syntax errors:**

```python
try:
    tree = parser.parse("MATCH (n:Person RETURN n")  # Missing )
except lark.exceptions.UnexpectedToken as e:
    print(f"Syntax error at line {e.line}, column {e.column}")
    print(f"Expected: {e.expected}")
    print(f"Got: {e.token}")

# Output:
# Syntax error at line 1, column 16
# Expected: ')'
# Got: RETURN
```

### Transformation Errors (Stage 2)

**Missing required fields:**

```python
def _convert_MatchClause(self, node: dict) -> MatchClause:
    """Convert MATCH clause."""
    pattern = node.get("pattern")
    if not pattern:
        raise ValueError(f"MATCH clause missing required 'pattern' field")
    
    return MatchClause(pattern=self.convert(pattern))
```

### Validation Errors (Stage 3)

**Pydantic catches type mismatches:**

```python
try:
    node = NodePattern(
        variable=123,  # Should be string!
        labels="Person"  # Should be list!
    )
except pydantic.ValidationError as e:
    print(e)

# Output:
# 2 validation errors for NodePattern
# variable
#   Input should be a valid string [type=string_type]
# labels
#   Input should be a valid list [type=list_type]
```

## Performance Considerations

### Why Three Stages Is Efficient

**Stage 1 (Parsing)**: 
- Fast: Lark's Earley parser is O(n³) worst-case, but typically O(n) for unambiguous input
- Only runs once per query
- Result is cached parse tree

**Stage 2 (Transformation)**:
- Fast: Single bottom-up tree traversal
- Simple dictionary construction (no heavy computation)
- Can be skipped if you only need syntax validation

**Stage 3 (Conversion)**:
- Fast: Pattern matching + object creation
- Pydantic validation is optimized
- Result can be cached for repeated queries

**Total Time**: Typically < 10ms for complex queries

### Memory Efficiency

**Three representations means three copies?** Not quite:

1. **Parse tree** discarded after transformation
2. **Dictionary AST** discarded after conversion
3. **Typed AST** is the only persistent object

Python's garbage collector reclaims stages 1 and 2 immediately.

## Practical Usage Guide

### Quick Start: Parse and Convert

```python
from pycypher.grammar_parser import GrammarParser
from pycypher.ast_models import ASTConverter

# One-liner: String → Typed AST
typed_ast = ASTConverter.from_cypher("MATCH (n) RETURN n")

# Or step-by-step for debugging
parser = GrammarParser()
converter = ASTConverter()

query = "MATCH (n:Person) RETURN n.name"
tree = parser.parse(query)                # Stage 1
ast_dict = parser.transformer.transform(tree)  # Stage 2
typed_ast = converter.convert(ast_dict)        # Stage 3
```

### Working with the AST

**Traversal:**
```python
def find_all_variables(node: ASTNode) -> List[str]:
    """Find all variable names in AST."""
    variables = []
    
    if isinstance(node, Variable):
        variables.append(node.name)
    
    # Recursively search children
    for child in node.get_children():
        variables.extend(find_all_variables(child))
    
    return variables

vars = find_all_variables(typed_ast)
print(vars)  # ['n', 'n']
```

**Pattern Matching:**
```python
def find_match_clauses(ast: Query) -> List[MatchClause]:
    """Extract all MATCH clauses from query."""
    matches = []
    
    for stmt in ast.statements:
        if isinstance(stmt, QueryStatement):
            for clause in stmt.clauses:
                if isinstance(clause, MatchClause):
                    matches.append(clause)
    
    return matches
```

**AST Modification:**
```python
# Add a WHERE clause to a MATCH
match_clause = typed_ast.statements[0].clauses[0]
match_clause.where = WhereClause(
    expression=Comparison(
        operator=ComparisonOperator.GT,
        left=PropertyAccess(
            expression=Variable(name="n"),
            property="age"
        ),
        right=IntegerLiteral(value=18)
    )
)
```

### Serialization

**To JSON:**
```python
import json

# Pydantic models can serialize to dict
ast_dict = typed_ast.model_dump()
json_str = json.dumps(ast_dict, indent=2)
```

**From JSON:**
```python
# Deserialize back to typed AST
ast_dict = json.loads(json_str)
typed_ast = converter.convert(ast_dict)
```

## Summary

The three-stage pipeline provides:

1. **Robustness**: Lark's parser handles syntax errors gracefully
2. **Simplicity**: Dictionary intermediate removes syntactic clutter
3. **Type Safety**: Pydantic models catch errors at development time
4. **Performance**: Fast parsing and transformation
5. **Debuggability**: Can inspect AST at each stage

**When to use each stage:**

- **Need syntax validation only?** Stop at Stage 1 (parse tree)
- **Need structural analysis?** Use Stage 2 (dictionary AST)
- **Need type safety and traversal?** Use Stage 3 (typed AST)

For most applications, **go straight to Stage 3** using `ASTConverter.from_cypher()`.
