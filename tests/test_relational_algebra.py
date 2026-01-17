"""
Unit tests for Relational Algebra module.

Tests cover all algebraic operators and the query translator:
- Context and table definitions (EntityTable, RelationshipTable, Context)
- Basic operators (Scan, Filter, Join, Project)
- Query translator (parse tree to algebra conversion)
- Edge cases and error handling
"""

import pytest
import pandas as pd
from lark import Tree

from pycypher.relational_algebra import (
    EntityTable,
    RelationshipTable,
    Context,
    Scan,
    Filter,
    Join,
    Project,
    QueryTranslator,
    JoinType,
    random_hash,
)
from pycypher.grammar_parser import GrammarParser


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def person_entity_table():
    """Create a Person entity table schema."""
    return EntityTable(
        entity_type="Person",
        attributes=["id", "name", "age", "city"],
        entity_identifier_attribute="id"
    )


@pytest.fixture
def company_entity_table():
    """Create a Company entity table schema."""
    return EntityTable(
        entity_type="Company",
        attributes=["id", "name", "industry"],
        entity_identifier_attribute="id"
    )


@pytest.fixture
def knows_relationship_table():
    """Create a KNOWS relationship table schema."""
    return RelationshipTable(
        relationship_type="KNOWS",
        source_entity_type="Person",
        target_entity_type="Person",
        attributes=["source_id", "target_id", "since"]
    )


@pytest.fixture
def works_at_relationship_table():
    """Create a WORKS_AT relationship table schema."""
    return RelationshipTable(
        relationship_type="WORKS_AT",
        source_entity_type="Person",
        target_entity_type="Company",
        attributes=["source_id", "target_id", "role"]
    )


@pytest.fixture
def person_data():
    """Create sample Person data."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "age": [30, 25, 35, 28],
        "city": ["NYC", "SF", "NYC", "LA"]
    })


@pytest.fixture
def company_data():
    """Create sample Company data."""
    return pd.DataFrame({
        "id": [101, 102, 103],
        "name": ["TechCorp", "DataInc", "AI Solutions"],
        "industry": ["Technology", "Analytics", "AI"]
    })


@pytest.fixture
def knows_data():
    """Create sample KNOWS relationship data."""
    return pd.DataFrame({
        "source_id": [1, 1, 2, 3],
        "target_id": [2, 3, 4, 4],
        "since": [2020, 2019, 2021, 2018]
    })


@pytest.fixture
def works_at_data():
    """Create sample WORKS_AT relationship data."""
    return pd.DataFrame({
        "source_id": [1, 2, 3, 4],
        "target_id": [101, 101, 102, 103],
        "role": ["Engineer", "Designer", "Analyst", "Manager"]
    })


@pytest.fixture
def basic_context(person_entity_table, person_data):
    """Create a basic context with Person table only."""
    return Context(
        entity_tables=[person_entity_table],
        relationship_tables=[],
        obj_map={"Person": person_data}
    )


@pytest.fixture
def full_context(person_entity_table, company_entity_table,
                 knows_relationship_table, works_at_relationship_table,
                 person_data, company_data, knows_data, works_at_data):
    """Create a full context with all tables."""
    return Context(
        entity_tables=[person_entity_table, company_entity_table],
        relationship_tables=[knows_relationship_table, works_at_relationship_table],
        obj_map={
            "Person": person_data,
            "Company": company_data,
            "KNOWS": knows_data,
            "WORKS_AT": works_at_data
        }
    )


@pytest.fixture
def parser():
    """Create a GrammarParser instance."""
    return GrammarParser()


# =============================================================================
# Test Utility Functions
# =============================================================================

class TestUtilityFunctions:
    """Test utility functions."""
    
    def test_random_hash_generates_hash(self):
        """Should generate a 32-character hash string."""
        hash1 = random_hash()
        assert isinstance(hash1, str)
        assert len(hash1) == 32
    
    def test_random_hash_generates_unique_hashes(self):
        """Should generate different hashes on each call."""
        hash1 = random_hash()
        hash2 = random_hash()
        assert hash1 != hash2
    
    def test_random_hash_is_hexadecimal(self):
        """Generated hashes should be valid hexadecimal."""
        hash_value = random_hash()
        # Should not raise ValueError
        int(hash_value, 16)


# =============================================================================
# Test EntityTable
# =============================================================================

class TestEntityTable:
    """Test EntityTable class."""
    
    def test_entity_table_initialization(self, person_entity_table):
        """Should initialize with correct attributes."""
        assert person_entity_table.entity_type == "Person"
        assert "id" in person_entity_table.attributes
        assert "name" in person_entity_table.attributes
        assert person_entity_table.entity_identifier_attribute == "id"
    
    def test_entity_table_creates_hash_mappings(self, person_entity_table):
        """Should create hash mappings for all attributes."""
        assert len(person_entity_table.column_name_to_hash) == 4  # id, name, age, city
        assert len(person_entity_table.hash_to_column_name) == 4
        
        # Check that mappings are inverses
        for attr in person_entity_table.attributes:
            hash_val = person_entity_table.column_name_to_hash[attr]
            assert person_entity_table.hash_to_column_name[hash_val] == attr
    
    def test_entity_table_hash_uniqueness(self, person_entity_table):
        """All hash values should be unique."""
        hashes = list(person_entity_table.column_name_to_hash.values())
        assert len(hashes) == len(set(hashes))


# =============================================================================
# Test RelationshipTable
# =============================================================================

class TestRelationshipTable:
    """Test RelationshipTable class."""
    
    def test_relationship_table_initialization(self, knows_relationship_table):
        """Should initialize with correct attributes."""
        assert knows_relationship_table.relationship_type == "KNOWS"
        assert knows_relationship_table.source_entity_type == "Person"
        assert knows_relationship_table.target_entity_type == "Person"
        assert "source_id" in knows_relationship_table.attributes
    
    def test_relationship_table_creates_hash_mappings(self, knows_relationship_table):
        """Should create hash mappings for all attributes."""
        assert len(knows_relationship_table.column_name_to_hash) == 3  # source_id, target_id, since
        assert len(knows_relationship_table.hash_to_column_name) == 3
    
    def test_relationship_table_different_entity_types(self, works_at_relationship_table):
        """Should support relationships between different entity types."""
        assert works_at_relationship_table.source_entity_type == "Person"
        assert works_at_relationship_table.target_entity_type == "Company"


# =============================================================================
# Test Context
# =============================================================================

class TestContext:
    """Test Context class."""
    
    def test_context_initialization(self, basic_context):
        """Should initialize with entity tables and data."""
        assert len(basic_context.entity_tables) == 1
        assert "Person" in basic_context.obj_map
        assert isinstance(basic_context.obj_map["Person"], pd.DataFrame)
    
    def test_get_entity_table_success(self, basic_context):
        """Should retrieve entity table by type."""
        table = basic_context.get_entity_table("Person")
        assert table.entity_type == "Person"
    
    def test_get_entity_table_not_found(self, basic_context):
        """Should raise ValueError for non-existent entity type."""
        with pytest.raises(ValueError, match="Entity table for type Unknown not found"):
            basic_context.get_entity_table("Unknown")
    
    def test_get_relationship_table_success(self, full_context):
        """Should retrieve relationship table by type."""
        table = full_context.get_relationship_table("KNOWS")
        assert table.relationship_type == "KNOWS"
    
    def test_get_relationship_table_not_found(self, full_context):
        """Should raise ValueError for non-existent relationship type."""
        with pytest.raises(ValueError, match="Relationship table for type UNKNOWN not found"):
            full_context.get_relationship_table("UNKNOWN")
    
    def test_context_with_multiple_tables(self, full_context):
        """Should support multiple entity and relationship tables."""
        assert len(full_context.entity_tables) == 2
        assert len(full_context.relationship_tables) == 2


# =============================================================================
# Test Scan Operator
# =============================================================================

class TestScan:
    """Test Scan algebraic operator."""
    
    def test_scan_entity_table(self, basic_context):
        """Should scan an entity table and apply hashing."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        result = scan.to_pandas(basic_context)
        
        # Should return a DataFrame
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4  # 4 persons
    
    def test_scan_creates_variable_mapping(self, basic_context):
        """Should create variable-to-column mapping."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        result = scan.to_pandas(basic_context)
        
        # Variable 'p' should be mapped to the id column hash
        assert "p" in scan.variables_to_columns
    
    def test_scan_without_variable(self, basic_context):
        """Should work without a variable binding."""
        scan = Scan(table_type="Person", is_entity=True, variable=None)
        result = scan.to_pandas(basic_context)
        
        assert isinstance(result, pd.DataFrame)
        assert len(scan.variables_to_columns) == 0
    
    def test_scan_applies_column_hashing(self, basic_context):
        """Column names should be hashed after scan."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        result = scan.to_pandas(basic_context)
        
        # Original column names should not be in result
        assert "name" not in result.columns
        assert "age" not in result.columns
        
        # But hash mappings should exist
        assert len(scan.column_name_to_hash) > 0


# =============================================================================
# Test Filter Operator
# =============================================================================

class TestFilter:
    """Test Filter algebraic operator."""
    
    def test_filter_equality(self, basic_context):
        """Should filter rows by equality condition."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        filter_op = Filter(input=scan, attribute="city", value="NYC", operator="=")
        result = filter_op.to_pandas(basic_context)
        
        # Should have only 2 people from NYC
        assert len(result) == 2
    
    def test_filter_greater_than(self, basic_context):
        """Should filter rows by greater than condition."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        filter_op = Filter(input=scan, attribute="age", value=30, operator=">")
        result = filter_op.to_pandas(basic_context)
        
        # Should have only Charlie (age 35)
        assert len(result) == 1
    
    def test_filter_less_than(self, basic_context):
        """Should filter rows by less than condition."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        filter_op = Filter(input=scan, attribute="age", value=30, operator="<")
        result = filter_op.to_pandas(basic_context)
        
        # Should have Bob (25) and Diana (28)
        assert len(result) == 2
    
    def test_filter_greater_equal(self, basic_context):
        """Should filter rows by greater than or equal condition."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        filter_op = Filter(input=scan, attribute="age", value=30, operator=">=")
        result = filter_op.to_pandas(basic_context)
        
        # Should have Alice (30) and Charlie (35)
        assert len(result) == 2
    
    def test_filter_less_equal(self, basic_context):
        """Should filter rows by less than or equal condition."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        filter_op = Filter(input=scan, attribute="age", value=28, operator="<=")
        result = filter_op.to_pandas(basic_context)
        
        # Should have Bob (25) and Diana (28)
        assert len(result) == 2
    
    def test_filter_not_equal(self, basic_context):
        """Should filter rows by not equal condition."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        filter_op = Filter(input=scan, attribute="city", value="NYC", operator="!=")
        result = filter_op.to_pandas(basic_context)
        
        # Should have Bob (SF) and Diana (LA)
        assert len(result) == 2
    
    def test_filter_unsupported_operator(self, basic_context):
        """Should raise error for unsupported operator."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        filter_op = Filter(input=scan, attribute="age", value=30, operator="LIKE")
        
        with pytest.raises(ValueError, match="Unsupported operator: LIKE"):
            filter_op.to_pandas(basic_context)
    
    def test_filter_invalid_attribute(self, basic_context):
        """Should raise error for non-existent attribute."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        filter_op = Filter(input=scan, attribute="nonexistent", value=123, operator="=")
        
        with pytest.raises(ValueError, match="Attribute nonexistent not found"):
            filter_op.to_pandas(basic_context)
    
    def test_filter_preserves_mappings(self, basic_context):
        """Should inherit mappings from input."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        filter_op = Filter(input=scan, attribute="age", value=30, operator=">")
        result = filter_op.to_pandas(basic_context)
        
        assert len(filter_op.column_name_to_hash) > 0
        assert len(filter_op.variables_to_columns) > 0
    
    def test_chained_filters(self, basic_context):
        """Should support chaining multiple filters."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        filter1 = Filter(input=scan, attribute="age", value=25, operator=">")
        filter2 = Filter(input=filter1, attribute="city", value="NYC", operator="=")
        result = filter2.to_pandas(basic_context)
        
        # Should have Alice (30, NYC) and Charlie (35, NYC)
        assert len(result) == 2


# =============================================================================
# Test Join Operator
# =============================================================================

class TestJoin:
    """Test Join algebraic operator."""
    
    def test_join_inner(self, basic_context):
        """Should perform inner join on two scans."""
        # Create two scans of the same table
        scan1 = Scan(table_type="Person", is_entity=True, variable="p1")
        scan2 = Scan(table_type="Person", is_entity=True, variable="p2")
        
        # Execute scans to get column mappings
        df1 = scan1.to_pandas(basic_context)
        df2 = scan2.to_pandas(basic_context)
        
        # Get the id column hash from scan1
        id_hash = scan1.column_name_to_hash["id"]
        
        # Join on id column
        join_op = Join(left=scan1, right=scan2, left_on=id_hash, right_on=id_hash)
        result = join_op.to_pandas(basic_context)
        
        # Should have 4 rows (self-join on id)
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 4
    
    def test_join_merges_mappings(self, full_context):
        """Should merge mappings from both sides."""
        scan1 = Scan(table_type="Person", is_entity=True, variable="p")
        scan2 = Scan(table_type="Company", is_entity=True, variable="c")
        
        # Execute scans
        df1 = scan1.to_pandas(full_context)
        df2 = scan2.to_pandas(full_context)
        
        # Get column hashes
        person_id_hash = scan1.column_name_to_hash["id"]
        company_id_hash = scan2.column_name_to_hash["id"]
        
        join_op = Join(left=scan1, right=scan2, left_on=person_id_hash, right_on=company_id_hash)
        result = join_op.to_pandas(full_context)
        
        # Should have mappings from both sides
        assert "id" in join_op.column_name_to_hash
        assert "p" in join_op.variables_to_columns
        assert "c" in join_op.variables_to_columns
    
    def test_join_type_inner(self):
        """Should default to INNER join type."""
        join_op = Join(
            left=Scan(table_type="Person", is_entity=True),
            right=Scan(table_type="Company", is_entity=True),
            left_on="col1",
            right_on="col2"
        )
        assert join_op.join_type == JoinType.INNER


# =============================================================================
# Test Project Operator
# =============================================================================

class TestProject:
    """Test Project algebraic operator."""
    
    def test_project_select_columns(self, basic_context):
        """Should select only specified columns."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        df = scan.to_pandas(basic_context)
        
        # Get hash for name column
        name_hash = scan.column_name_to_hash["name"]
        
        # Project only name column
        project = Project(input=scan, columns=[name_hash])
        result = project.to_pandas(basic_context)
        
        # Should have only 1 column
        assert len(result.columns) == 1
    
    def test_project_multiple_columns(self, basic_context):
        """Should select multiple specified columns."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        df = scan.to_pandas(basic_context)
        
        # Get hashes for name and age columns
        name_hash = scan.column_name_to_hash["name"]
        age_hash = scan.column_name_to_hash["age"]
        
        project = Project(input=scan, columns=[name_hash, age_hash])
        result = project.to_pandas(basic_context)
        
        # Should have 2 columns
        assert len(result.columns) == 2
    
    def test_project_with_aliases(self, basic_context):
        """Should rename columns using aliases."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        df = scan.to_pandas(basic_context)
        
        name_hash = scan.column_name_to_hash["name"]
        
        # Project with alias
        project = Project(
            input=scan,
            columns=[name_hash],
            aliases={name_hash: "person_name"}
        )
        result = project.to_pandas(basic_context)
        
        # Should have the aliased column name
        assert "person_name" in result.columns
    
    def test_project_nonexistent_columns_ignored(self, basic_context):
        """Should ignore columns that don't exist."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        
        # Try to project a non-existent column
        project = Project(input=scan, columns=["nonexistent_hash"])
        result = project.to_pandas(basic_context)
        
        # Should return empty DataFrame with no columns
        assert len(result.columns) == 0


# =============================================================================
# Test QueryTranslator
# =============================================================================

class TestQueryTranslator:
    """Test QueryTranslator class."""
    
    def test_translator_initialization(self, basic_context):
        """Should initialize with context."""
        translator = QueryTranslator(basic_context)
        assert translator.context == basic_context
        assert isinstance(translator.variable_scopes, dict)
    
    def test_translate_simple_match(self, parser, basic_context):
        """Should translate simple MATCH query."""
        tree = parser.parse("MATCH (p:Person) RETURN p")
        translator = QueryTranslator(basic_context)
        algebra = translator.translate(tree)
        
        # Should produce some algebraic expression
        assert algebra is not None
    
    def test_translate_match_with_where(self, parser, basic_context):
        """Should handle MATCH with WHERE clause."""
        tree = parser.parse("MATCH (p:Person) WHERE p.age > 25 RETURN p")
        translator = QueryTranslator(basic_context)
        algebra = translator.translate(tree)
        
        assert algebra is not None
    
    def test_extract_variable_name(self, parser, basic_context):
        """Should extract variable names from parse tree nodes."""
        tree = parser.parse("MATCH (p:Person) RETURN p")
        translator = QueryTranslator(basic_context)
        
        # Find a variable_name node
        for var_node in tree.find_data("variable_name"):
            var_name = translator._extract_variable_name(var_node)
            assert var_name in ["p"]
            break
    
    def test_extract_label_name(self, parser, basic_context):
        """Should extract label names from parse tree nodes."""
        tree = parser.parse("MATCH (p:Person) RETURN p")
        translator = QueryTranslator(basic_context)
        
        # Find label nodes
        for node_pattern in tree.find_data("node_pattern"):
            label = translator._extract_label_name(node_pattern)
            if label:
                assert label == "Person"
                break
    
    def test_find_node(self, parser, basic_context):
        """Should find nodes of specified type in tree."""
        tree = parser.parse("MATCH (p:Person) RETURN p")
        translator = QueryTranslator(basic_context)
        
        # Find match_clause
        match_clause = translator._find_node(tree, "match_clause")
        assert match_clause is not None
        
        # Find return_clause
        return_clause = translator._find_node(tree, "return_clause")
        assert return_clause is not None
        
        # Should return None for non-existent nodes
        nonexistent = translator._find_node(tree, "nonexistent_clause")
        assert nonexistent is None


# =============================================================================
# Test Edge Cases
# =============================================================================

class TestEdgeCases:
    """Test edge cases and error handling."""
    
    def test_empty_context(self):
        """Should handle context with no tables."""
        context = Context(entity_tables=[], relationship_tables=[], obj_map={})
        assert len(context.entity_tables) == 0
    
    def test_entity_table_with_no_attributes(self):
        """Should handle entity table with empty attributes list."""
        table = EntityTable(
            entity_type="EmptyEntity",
            attributes=[],
            entity_identifier_attribute="id"
        )
        assert len(table.column_name_to_hash) == 0
    
    def test_scan_missing_table(self, basic_context):
        """Should raise error when scanning non-existent table."""
        scan = Scan(table_type="NonExistent", is_entity=True, variable="x")
        
        with pytest.raises(ValueError, match="Entity table for type NonExistent not found"):
            scan.to_pandas(basic_context)
    
    def test_filter_on_empty_dataframe(self, basic_context):
        """Should handle filtering empty DataFrame."""
        # Create a filter that eliminates all rows
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        filter_op = Filter(input=scan, attribute="age", value=1000, operator=">")
        result = filter_op.to_pandas(basic_context)
        
        # Should return empty DataFrame
        assert len(result) == 0


# =============================================================================
# Integration Tests
# =============================================================================

class TestIntegration:
    """Integration tests combining multiple operators."""
    
    def test_scan_filter_project_pipeline(self, basic_context):
        """Should execute a complete scan-filter-project pipeline."""
        # Scan Person table
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        
        # Filter age > 25
        filter_op = Filter(input=scan, attribute="age", value=25, operator=">")
        
        # Project name column
        df_temp = filter_op.to_pandas(basic_context)
        name_hash = filter_op.column_name_to_hash["name"]
        project = Project(input=filter_op, columns=[name_hash])
        
        # Execute
        result = project.to_pandas(basic_context)
        
        # Should have Alice (30), Charlie (35), Diana (28)
        assert len(result) == 3
        assert len(result.columns) == 1
    
    def test_multiple_filters_chained(self, basic_context):
        """Should handle multiple chained filters correctly."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        filter1 = Filter(input=scan, attribute="age", value=20, operator=">")
        filter2 = Filter(input=filter1, attribute="age", value=35, operator="<")
        filter3 = Filter(input=filter2, attribute="city", value="NYC", operator="=")
        
        result = filter3.to_pandas(basic_context)
        
        # Should have only Alice (30, NYC)
        assert len(result) == 1
    
    def test_filter_then_project(self, basic_context):
        """Filtering then projecting should work correctly."""
        scan = Scan(table_type="Person", is_entity=True, variable="p")
        filter_op = Filter(input=scan, attribute="city", value="NYC", operator="=")
        
        df_temp = filter_op.to_pandas(basic_context)
        name_hash = filter_op.column_name_to_hash["name"]
        age_hash = filter_op.column_name_to_hash["age"]
        
        project = Project(input=filter_op, columns=[name_hash, age_hash])
        result = project.to_pandas(basic_context)
        
        # Should have 2 people from NYC with 2 columns
        assert len(result) == 2
        assert len(result.columns) == 2
