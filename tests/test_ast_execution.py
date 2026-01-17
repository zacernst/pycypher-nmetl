import pytest
import pandas as pd
from pycypher.ast_models import (
    Context,
    EntityTable,
    RelationshipTable,
    Filter,
    Join,
    JoinType,
    DropColumn,
    RenameColumn,
    Algebraic
)

@pytest.fixture
def context():
    """Create a test execution context."""
    # Data for Persons
    persons_df = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [30, 40, 25],
        "id": ["p1", "p2", "p3"]
    })
    
    # Data for Companies
    companies_df = pd.DataFrame({
        "name": ["Acme", "Globex"],
        "id": ["c1", "c2"]
    })
    
    # Data for KNOWS relationships
    knows_df = pd.DataFrame({
        "source": ["p1", "p2"],
        "target": ["p2", "p3"],
        "since": [2020, 2021]
    })
    
    # Setup schemas
    person_table = EntityTable(
        entity_type="Person",
        attributes=["name", "age"],
        entity_identifier_attribute="id"
    )
    
    company_table = EntityTable(
        entity_type="Company",
        attributes=["name"],
        entity_identifier_attribute="id"
    )
    
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        source_entity_type="Person",
        target_entity_type="Person",
        attributes=["since"]
    )
    
    ctx = Context(
        entity_tables=[person_table, company_table],
        relationship_tables=[knows_table],
        obj_map={
            "Person": persons_df,
            "Company": companies_df,
            "KNOWS": knows_df
        }
    )
    return ctx

class TestASTExecution:
    """Test AST model execution related methods (Relational Algebra)."""

    def test_entity_table_to_pandas(self, context):
        """Test EntityTable conversion to pandas."""
        person_table = context.entity_tables[0]
        df = person_table.to_pandas(context)
        
        # Should be a dataframe
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        
        # Columns should be hashed
        assert "name" not in df.columns # original name should be replaced
        assert "age" not in df.columns
        assert len(df.columns) == 3 # name, age, id (id is not hashed? let's check init)

    def test_relationship_table_to_pandas(self, context):
        """Test RelationshipTable conversion to pandas."""
        rel_table = context.relationship_tables[0]
        df = rel_table.to_pandas(context)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        # Rel table doesn't rename columns in to_pandas in current implementation?
        # init says it creates map, but to_pandas just returns obj_map[type].
        # Let's verify behavior.
        assert "since" in df.columns

    def test_rename_column(self, context):
        """Test RenameColumn operation."""
        # Use an existing EntityTable from context
        table = context.entity_tables[0]
        
        # We need to ensure the entity table returns a dataframe with the column we want to rename
        # The Context fixture already sets up mapping.
        # But wait, EntityTable.to_pandas() renames columns to Hash!
        # So we need to know the Hashed name or rename the Hash.
        
        # Actually, let's use a simpler approach: subclass EntityTable to control behavior
        class MockEntityTable(EntityTable):
            def to_pandas(self, ctx):
                return pd.DataFrame({"a": [1, 2], "b": [3, 4]})
            def to_ibis(self, ctx): # Implement abstract method if needed (EntityTable has it)
                pass

        # We construct it with required fields to satisfy Pydantic
        mock_table = MockEntityTable(
            entity_type="Mock", 
            attributes=["a", "b"], 
            entity_identifier_attribute="a"
        )
        # We need to trick Pydantic validation if it checks strictly for EntityTable class
        # (It shouldn't if we subclass)
        
        rename_op = RenameColumn(
            table=mock_table,
            old_column_name="a",
            new_column_name="z"
        )
        
        df = rename_op.to_pandas(context)
        assert "z" in df.columns
        assert "a" not in df.columns
        assert df["z"].tolist() == [1, 2]

    def test_drop_column(self, context):
        """Test DropColumn operation."""
        class MockEntityTable(EntityTable):
            def to_pandas(self, ctx):
                return pd.DataFrame({"a": [1, 2], "b": [3, 4]})
            def to_ibis(self, ctx): pass

        mock_table = MockEntityTable(
            entity_type="Mock", 
            attributes=["a", "b"], 
            entity_identifier_attribute="a"
        )
        
        drop_op = DropColumn(
            table=mock_table,
            column_name="a"
        )
        
        df = drop_op.to_pandas(context)
        assert "a" not in df.columns
        assert "b" in df.columns

        # Test execute=False
        drop_op_lazy = DropColumn(
            table=mock_table,
            column_name="a",
            execute=False
        )
        df_lazy = drop_op_lazy.to_pandas(context)
        assert "a" in df_lazy.columns

    def test_join_pandas(self, context):
        """Test Join operation."""
        # Join is abstract because it misses to_ibis, so we must subclass it to test logic
        class ConcreteJoin(Join):
            def to_ibis(self, ctx):
                return None

        # Create two simple tables specific to this test
        class MockTableA(EntityTable):
            def to_pandas(self, ctx):
                return pd.DataFrame({"id": [1, 2], "val": ["x", "y"]})
        
        class MockTableB(EntityTable):
            def to_pandas(self, ctx):
                return pd.DataFrame({"fk": [1, 2], "other": ["a", "b"]})

        table_a = MockTableA(entity_type="A", attributes=["id"], entity_identifier_attribute="id")
        table_b = MockTableB(entity_type="B", attributes=["fk"], entity_identifier_attribute="fk")
            
        join_op = ConcreteJoin(
            left=table_a,
            right=table_b,
            left_on="id",
            right_on="fk",
            join_type=JoinType.INNER
        )
        
        df = join_op.to_pandas(context)
        assert len(df) == 2
        # Check column presence (suffixes might be added)
        # pd.merge with collision on 'id'/'fk'? No collision on values, but columns are preserved
        assert len(df.columns) >= 4 

    def test_multijoin_pandas(self, context):
        """Test Join operation with multiple columns (previously MultiJoin)."""
        class ConcreteJoin(Join):
             def to_ibis(self, ctx): return None

        class MockTableA(EntityTable):
            def to_pandas(self, ctx):
                return pd.DataFrame({"k1": [1], "k2": [1], "v": ["a"]})
            
        class MockTableB(EntityTable):
            def to_pandas(self, ctx):
                return pd.DataFrame({"k1": [1], "k2": [1], "v": ["b"]})
            
        table_a = MockTableA(entity_type="A", attributes=["k1"], entity_identifier_attribute="k1")
        table_b = MockTableB(entity_type="B", attributes=["k1"], entity_identifier_attribute="k1")
            
        join_op = ConcreteJoin(
            left=table_a,
            right=table_b,
            left_on=["k1", "k2"],
            right_on=["k1", "k2"]
        )
        
        df = join_op.to_pandas(context)
        assert len(df) == 1
