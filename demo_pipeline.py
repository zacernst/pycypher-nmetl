"""
Cypher Translation Pipeline Demo

This script demonstrates the full lifecycle of a Cypher query:
1. Parsing (String -> Lark Parse Tree)
2. Transformation (Parse Tree -> Dictionary AST)
3. Type Conversion (Dictionary AST -> Typed Pydantic AST)
4. Algebra Translation (Parse Tree -> Relational Algebra Execution Plan)

Usage:
    uv run python demo_pipeline.py
"""

from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.pretty import Pretty

from pycypher.grammar_parser import GrammarParser
from pycypher.ast_models import ASTConverter
from pycypher.relational_algebra import (
    Context, 
    EntityTable, 
    QueryTranslator,
    Scan,
    Filter,
    Project
)

console = Console()

def main():
    # Define a sample query
    query = "MATCH (n:Person) WHERE n.age > 25 RETURN n.name"
    
    console.rule("[bold green]Cypher Translation Pipeline Demo[/bold green]")
    console.print(f"\n[bold]Input Query:[/bold] [green]{query}[/green]\n")

    # =========================================================================
    # Step 1: Parsing
    # =========================================================================
    console.rule("[bold blue]Step 1: Parsing (Lark)[/bold blue]")
    console.print("Converting the raw string into a syntactic parse tree using Lark.\n")
    
    parser = GrammarParser()
    tree = parser.parse(query)
    
    console.print(Panel(
        tree.pretty(), 
        title="Lark Parse Tree", 
        border_style="blue",
        expand=False
    ))

    # =========================================================================
    # Step 2: AST Transformation
    # =========================================================================
    console.rule("[bold yellow]Step 2: Dictionary AST[/bold yellow]")
    console.print("Transforming the parse tree into a simplified dictionary representation.\n")
    
    ast_dict = parser.transformer.transform(tree)
    
    console.print(Panel(
        Pretty(ast_dict),
        title="Dictionary AST",
        border_style="yellow",
        expand=False
    ))

    # =========================================================================
    # Step 3: Typed AST
    # =========================================================================
    console.rule("[bold magenta]Step 3: Typed AST (Pydantic)[/bold magenta]")
    console.print("Validating and converting the dictionary AST into strict Pydantic models.\n")
    
    try:
        converter = ASTConverter()
        typed_ast = converter.convert(ast_dict)
        console.print(Panel(
            str(typed_ast), 
            title="Typed AST (Pydantic Models)", 
            border_style="magenta", 
            expand=False
        ))
    except Exception as e:
        console.print(f"[bold red]AST Conversion Failed:[/bold red] {e}")

    # =========================================================================
    # Step 4: Relational Algebra
    # =========================================================================
    console.rule("[bold red]Step 4: Relational Algebra[/bold red]")
    console.print("Translating the Parse Tree into an executable plan of relational operators.\n")
    
    # We need a context for the translator to resolve table names and columns
    context = Context(
        entity_tables=[
            EntityTable(
                entity_type="Person",
                attributes=["id", "name", "age", "city"],
                entity_identifier_attribute="id"
            )
        ],
        relationship_tables=[],
        obj_map={}
    )
    
    translator = QueryTranslator(context)
    algebra = translator.translate(tree)
    
    console.print(Panel(
        Pretty(algebra),
        title="Relational Algebra Execution Plan",
        border_style="red",
        expand=False
    ))
    
    console.print("\n[bold green]Pipeline Complete![/bold green] 🚀")

if __name__ == "__main__":
    main()
