import pytest
import pandas as pd

from pycypher.algebra import (
    Algebraic,
    Context,
    EntityTable,
    Filter,
    RelationshipTable,
)
from pycypher.ast_models import (
    NodePattern,
    Algebraizable,
    Variable,
)


@pytest.fixture
def person_table() -> EntityTable:
    person_table: EntityTable = EntityTable(
        name="Person",
        entity_type="Person",
        attributes=["name", "age"],
        entity_identifier_attribute="name",
    )
    return person_table


@pytest.fixture
def city_table() -> EntityTable:
    city_table = EntityTable(
        name="City",
        entity_type="City",
        attributes=["name", "population"],
        entity_identifier_attribute="name",
    )
    return city_table

@pytest.fixture
def state_table() -> EntityTable:
    state_table = EntityTable(
        name="State",
        entity_type="State",
        attributes=["name", "mittenlike", "humid"],
        entity_identifier_attribute="name",
    )
    return state_table

@pytest.fixture
def lives_in_table() -> RelationshipTable:
    lives_in_table = RelationshipTable(
        name="LIVES_IN",
        relationship_type="LIVES_IN",
        source_entity_type="Person",
        target_entity_type="City",
        attributes=[],
    )
    return lives_in_table

@pytest.fixture
def city_in_state_table() -> RelationshipTable:
    city_in_state_table = RelationshipTable(
        name="CITY_IN_STATE",
        relationship_type="CITY_IN_STATE",
        source_entity_type="City",
        target_entity_type="State",
        attributes=[],
    )
    return city_in_state_table

@pytest.fixture
def person_node():
    # Define nodes and relationship
    person_node: NodePattern = NodePattern(variable=Variable(name="p"), labels=["Person"], properties={"name": "Alice"})
    return person_node


@pytest.fixture
def person_df():
    person_df = pd.DataFrame(
        data=[
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Carol", "age": 22},
        ],
    )
    return person_df


@pytest.fixture
def context(
    person_table: EntityTable,
    person_df: pd.DataFrame,
) -> Context:
    context = Context(
        entity_tables=[person_table],
        relationship_tables=[],
        obj_map={
            "Person": person_df,
        },
    )
    return context


def test_person_node_to_algebra(
    person_node: NodePattern,
    context: Context,
):
    """Test that person_node.to_algebra() returns an EntityTable or Filter."""
    result = person_node.to_algebra(context)
    assert isinstance(result, (EntityTable, Filter))
    assert person_node.variable in result.variables_to_columns
    print("✓ person_node.to_algebra() returns correct type and variable mapping")


# @pytest.fixture
# def city_node() -> Node:
#     city_node = Node(variable="c", label="City", attributes={})
#     return city_node
# 
# 
# @pytest.fixture
# def city_df() -> pd.DataFrame:
#     city_df = pd.DataFrame(
#         data=[
#             {"name": "Cairo", "population": 100},
#             {"name": "Alexandria", "population": 50},
#         ],
#     )
#     return city_df
# 
# 
# @pytest.fixture
# def state_node() -> Node:
#     state_node = Node(variable="s", label="State", attributes={})
#     return state_node
# 
# 
# @pytest.fixture
# def state_df() -> pd.DataFrame:
#     state_df = pd.DataFrame(
#         data=[
#             {"name": "Georgia", "humid": True, "mittenlike": False},
#             {"name": "Virginia", "humid": False, "mittenlike": False},
#             {"name": "Michigan", "humid": False, "mittenlike": True},
#         ],
#     )
#     return state_df
# 
# 
# @pytest.fixture
# def lives_in(person_node: Node, city_node: Node) -> Relationship:
#     lives_in = Relationship(
#         variable="livesin",
#         label="LIVES_IN",
#         source_node=person_node,
#         target_node=city_node,
#     )
#     return lives_in
# 
# 
# @pytest.fixture
# def lives_in_df() -> pd.DataFrame:
#     lives_in_df = pd.DataFrame(
#         data=[
#             {
#                 "source_name": "Alice",
#                 "target_name": "Cairo",
#                 "relationship_id": "r1",
#             },
#             {
#                 "source_name": "Bob",
#                 "target_name": "Alexandria",
#                 "relationship_id": "r2",
#             },
#             {
#                 "source_name": "carol",
#                 "target_name": "Cairo",
#                 "relationship_id": "r3",
#             },
#         ],
#     )
#     return lives_in_df
# 
# 
# @pytest.fixture
# def city_in_state(city_node: Node, state_node: Node) -> Relationship:
#     city_in_state = Relationship(
#         variable="citystate",
#         label="CITY_IN_STATE",
#         source_node=city_node,
#         target_node=state_node,
#     )
#     return city_in_state
# 
# 
# @pytest.fixture
# def city_in_state_df() -> pd.DataFrame:
#     city_in_state_df = pd.DataFrame(
#         data=[
#             {
#                 "source_name": "Cairo",
#                 "target_name": "Georgia",
#                 "relationship_id": "in1",
#             },
#             {
#                 "source_name": "Alexandria",
#                 "target_name": "Virginia",
#                 "relationship_id": "in2",
#             },
#             {
#                 "source_name": "Kalamazoo",
#                 "target_name": "Michigan",
#                 "relationship_id": "in3",
#             },
#         ],
#     )
#     return city_in_state_df
# 
# 
# @pytest.fixture
# def context(
#     city_table: EntityTable,
#     state_table: EntityTable,
#     person_table: EntityTable,
#     lives_in_table: RelationshipTable,
#     city_in_state_table: RelationshipTable,
#     person_df: pd.DataFrame,
#     city_df: pd.DataFrame,
#     lives_in_df: pd.DataFrame,
#     state_df: pd.DataFrame,
#     city_in_state_df: pd.DataFrame,
# ) -> Context:
#     context = Context(
#         entity_tables=[city_table, state_table, person_table],
#         relationship_tables=[lives_in_table, city_in_state_table],
#         obj_map={
#             "Person": person_df,
#             "City": city_df,
#             "LIVES_IN": lives_in_df,
#             "State": state_df,
#             "CITY_IN_STATE": city_in_state_df,
#         },
#     )
#     return context
# 
# 
# @pytest.fixture
# def city_in_state_alg(city_in_state: Relationship, context: Context) -> Algebraic:
#     city_in_state_alg = city_in_state.to_algebra(context)
#     return city_in_state_alg
# 
# 
# @pytest.fixture
# def relationship_conjunction(
#     lives_in: Relationship, city_in_state: Relationship
# ) -> RelationshipConjunction:
#     relationship_conjunction = RelationshipConjunction(
#         relationships=[lives_in, city_in_state],
#     )
#     return relationship_conjunction
# 
# 
# @pytest.fixture
# def conjunction_alg(
#     relationship_conjunction: RelationshipConjunction,
#     context: Context,
# ) -> Algebraic:
#     conjunction_alg = relationship_conjunction.to_algebra(context)
#     return conjunction_alg


# ============================================================================
# Unit Tests for to_algebra() Methods
# ============================================================================


# class TestNodeToAlgebra:
#     """Test Node.to_algebra() conversion."""
# 
#     def test_node_without_attributes_returns_entity_table(
#         self, city_node: Node, context: Context
#     ):
#         """Test that a node without attributes converts to an EntityTable."""
#         result = city_node.to_algebra(context)
#         assert isinstance(result, EntityTable)
#         assert result.entity_type == "City"
#         assert city_node.variable in result.variables_to_columns
# 
#     def test_node_with_attributes_returns_filter(
#         self, person_node: Node, context: Context
#     ):
#         """Test that a node with attributes converts to a Filter."""
#         result = person_node.to_algebra(context)
#         assert isinstance(result, (Filter, EntityTable))
#         # person_node has attributes={"name": "Alice"}
#         # Should return a Filter wrapping an EntityTable
# 
#     def test_node_variable_mapping(self, person_node: Node, context: Context):
#         """Test that node variable is correctly mapped to columns."""
#         result = person_node.to_algebra(context)
#         assert person_node.variable in result.variables_to_columns
#         # Verify the variable maps to the identifier column
# 
#     def test_state_node_conversion(self, state_node: Node, context: Context):
#         """Test state node conversion to algebra."""
#         result = state_node.to_algebra(context)
#         assert isinstance(result, EntityTable)
#         assert result.entity_type == "State"
#         assert state_node.variable in result.variables_to_columns
# 
#     def test_multiple_nodes_independent(
#         self, person_node: Node, city_node: Node, state_node: Node, context: Context
#     ):
#         """Test that multiple nodes convert independently."""
#         person_alg = person_node.to_algebra(context)
#         city_alg = city_node.to_algebra(context)
#         state_alg = state_node.to_algebra(context)
# 
#         # Each should have their own variable mappings
#         assert person_node.variable in person_alg.variables_to_columns
#         assert city_node.variable in city_alg.variables_to_columns
#         assert state_node.variable in state_alg.variables_to_columns
# 
# 
# class TestRelationshipToAlgebra:
#     """Test Relationship.to_algebra() conversion."""
# 
#     def test_relationship_returns_algebraic(
#         self, lives_in: Relationship, context: Context
#     ):
#         """Test that a relationship converts to an Algebraic type."""
#         result = lives_in.to_algebra(context)
#         assert isinstance(result, Algebraic)
#         # Relationship should produce RenameColumn or similar complex structure
# 
#     def test_relationship_has_variable_mapping(
#         self, lives_in: Relationship, context: Context
#     ):
#         """Test that relationship variable is mapped in the result."""
#         result = lives_in.to_algebra(context)
#         assert lives_in.variable in result.variables_to_columns
# 
#     def test_relationship_includes_source_and_target_variables(
#         self, lives_in: Relationship, context: Context
#     ):
#         """Test that both source and target node variables are included."""
#         result = lives_in.to_algebra(context)
#         # Should include variables from source (person) and target (city)
#         assert lives_in.source_node.variable in result.variables_to_columns
#         assert lives_in.target_node.variable in result.variables_to_columns
# 
#     def test_city_in_state_relationship(
#         self, city_in_state: Relationship, context: Context
#     ):
#         """Test conversion of city_in_state relationship."""
#         result = city_in_state.to_algebra(context)
#         assert isinstance(result, Algebraic)
#         assert city_in_state.variable in result.variables_to_columns
#         assert city_in_state.source_node.variable in result.variables_to_columns
#         assert city_in_state.target_node.variable in result.variables_to_columns
# 
#     def test_relationship_can_execute_to_pandas(
#         self, lives_in: Relationship, context: Context
#     ):
#         """Test that relationship algebra can be executed to pandas DataFrame."""
#         result = lives_in.to_algebra(context)
#         df = result.to_pandas(context)
#         assert isinstance(df, pd.DataFrame)
#         assert not df.empty  # Should have some data based on fixtures
# 
#     def test_city_in_state_executes_to_pandas(
#         self, city_in_state: Relationship, context: Context
#     ):
#         """Test city_in_state relationship execution."""
#         result = city_in_state.to_algebra(context)
#         df = result.to_pandas(context)
#         assert isinstance(df, pd.DataFrame)
# 
# 
# class TestRelationshipConjunctionToAlgebra:
#     """Test RelationshipConjunction.to_algebra() conversion."""
# 
#     def test_conjunction_returns_algebraic(
#         self, relationship_conjunction: RelationshipConjunction, context: Context
#     ):
#         """Test that conjunction converts to an Algebraic type."""
#         result = relationship_conjunction.to_algebra(context)
#         assert isinstance(result, Algebraic)
# 
#     def test_conjunction_combines_variables(
#         self, relationship_conjunction: RelationshipConjunction, context: Context
#     ):
#         """Test that conjunction includes variables from all relationships."""
#         result = relationship_conjunction.to_algebra(context)
#         # Should have variables from both relationships
#         # lives_in has: p (person), livesin, c (city)
#         # city_in_state has: c (city), citystate, s (state)
#         assert "p" in result.variables_to_columns  # person
#         assert "c" in result.variables_to_columns  # city (shared)
#         assert "s" in result.variables_to_columns  # state
#         assert "livesin" in result.variables_to_columns  # lives_in relationship
#         assert "citystate" in result.variables_to_columns  # city_in_state relationship
# 
#     def test_conjunction_executes_to_pandas(
#         self, relationship_conjunction: RelationshipConjunction, context: Context
#     ):
#         """Test that conjunction can be executed to pandas DataFrame."""
#         result = relationship_conjunction.to_algebra(context)
#         df = result.to_pandas(context)
#         assert isinstance(df, pd.DataFrame)
# 
#     def test_conjunction_joins_on_common_variable(
#         self, relationship_conjunction: RelationshipConjunction, context: Context
#     ):
#         """Test that conjunction properly joins on common variable (city)."""
#         result = relationship_conjunction.to_algebra(context)
#         df = result.to_pandas(context)
#         # The result should join person->city->state
#         # Should have columns related to all three entity types
#         assert isinstance(df, pd.DataFrame)
# 
#     def test_conjunction_fixture_alg_is_algebraic(self, conjunction_alg: Algebraic):
#         """Test that the conjunction_alg fixture is properly created."""
#         assert isinstance(conjunction_alg, Algebraic)
#         assert "p" in conjunction_alg.variables_to_columns
#         assert "c" in conjunction_alg.variables_to_columns
#         assert "s" in conjunction_alg.variables_to_columns
# 
# 
# class TestAlgebraicExecution:
#     """Test execution of Algebraic types to pandas DataFrames."""
# 
#     def test_entity_table_to_pandas(
#         self, person_table: EntityTable, context: Context
#     ):
#         """Test EntityTable execution to pandas."""
#         df = person_table.to_pandas(context)
#         assert isinstance(df, pd.DataFrame)
#         assert len(df) == 3  # Based on person_df fixture
#         # Columns are hashed, so check for the expected number of columns
#         assert len(df.columns) >= 2  # At least name and age columns
# 
#     def test_city_table_to_pandas(self, city_table: EntityTable, context: Context):
#         """Test city EntityTable execution."""
#         df = city_table.to_pandas(context)
#         assert isinstance(df, pd.DataFrame)
#         assert len(df) == 2  # Based on city_df fixture
#         # Columns are hashed, check for expected number
#         assert len(df.columns) >= 2  # At least name and population columns
# 
#     def test_state_table_to_pandas(self, state_table: EntityTable, context: Context):
#         """Test state EntityTable execution."""
#         df = state_table.to_pandas(context)
#         assert isinstance(df, pd.DataFrame)
#         assert len(df) == 3  # Based on state_df fixture
# 
#     def test_node_algebra_to_pandas(self, person_node: Node, context: Context):
#         """Test executing node algebra to pandas."""
#         result = person_node.to_algebra(context)
#         df = result.to_pandas(context)
#         assert isinstance(df, pd.DataFrame)
# 
#     def test_relationship_algebra_to_pandas(
#         self, lives_in: Relationship, context: Context
#     ):
#         """Test executing relationship algebra to pandas."""
#         result = lives_in.to_algebra(context)
#         df = result.to_pandas(context)
#         assert isinstance(df, pd.DataFrame)
#         # Should have joined data from person, lives_in, and city
# 
#     def test_conjunction_algebra_to_pandas(
#         self, relationship_conjunction: RelationshipConjunction, context: Context
#     ):
#         """Test executing conjunction algebra to pandas."""
#         result = relationship_conjunction.to_algebra(context)
#         df = result.to_pandas(context)
#         assert isinstance(df, pd.DataFrame)
# 
# 
# class TestAlgebraicProperties:
#     """Test properties of Algebraic objects."""
# 
#     def test_city_in_state_alg_fixture(self, city_in_state_alg: Algebraic):
#         """Test the city_in_state_alg fixture properties."""
#         assert isinstance(city_in_state_alg, Algebraic)
#         assert hasattr(city_in_state_alg, "variables_to_columns")
#         assert hasattr(city_in_state_alg, "column_name_to_hash")
#         assert hasattr(city_in_state_alg, "hash_to_column_name")
# 
#     def test_conjunction_alg_fixture(self, conjunction_alg: Algebraic):
#         """Test the conjunction_alg fixture properties."""
#         assert isinstance(conjunction_alg, Algebraic)
#         assert hasattr(conjunction_alg, "variables_to_columns")
#         assert len(conjunction_alg.variables_to_columns) > 0
# 
#     def test_algebraic_has_required_attributes(
#         self, city_in_state_alg: Algebraic, conjunction_alg: Algebraic
#     ):
#         """Test that Algebraic objects have required attributes."""
#         for alg in [city_in_state_alg, conjunction_alg]:
#             assert hasattr(alg, "variables_to_columns")
#             assert hasattr(alg, "column_name_to_hash")
#             assert hasattr(alg, "hash_to_column_name")
# 
# 
# class TestContextIntegration:
#     """Test that Context properly provides tables for algebra conversion."""
# 
#     def test_context_has_all_entity_tables(self, context: Context):
#         """Test that context contains all required entity tables."""
#         person_table = context.get_entity_table("Person")
#         city_table = context.get_entity_table("City")
#         state_table = context.get_entity_table("State")
# 
#         assert person_table is not None
#         assert city_table is not None
#         assert state_table is not None
# 
#     def test_context_has_all_relationship_tables(self, context: Context):
#         """Test that context contains all required relationship tables."""
#         lives_in_table = context.get_relationship_table("LIVES_IN")
#         city_in_state_table = context.get_relationship_table("CITY_IN_STATE")
# 
#         assert lives_in_table is not None
#         assert city_in_state_table is not None
# 
#     def test_context_has_data_for_tables(self, context: Context):
#         """Test that context has data for all tables."""
#         assert "Person" in context.obj_map
#         assert "City" in context.obj_map
#         assert "State" in context.obj_map
#         assert "LIVES_IN" in context.obj_map
#         assert "CITY_IN_STATE" in context.obj_map
# 
#         # Verify data is pandas DataFrames
#         for key in ["Person", "City", "State", "LIVES_IN", "CITY_IN_STATE"]:
#             assert isinstance(context.obj_map[key], pd.DataFrame)
# 
# 
# class TestComplexAlgebraConversions:
#     """Test complex scenarios combining multiple algebra conversions."""
# 
#     def test_chain_multiple_relationships(
#         self, person_node: Node, city_node: Node, state_node: Node, context: Context
#     ):
#         """Test creating a chain of relationships."""
#         # Create nodes
#         person_alg = person_node.to_algebra(context)
#         city_alg = city_node.to_algebra(context)
#         state_alg = state_node.to_algebra(context)
# 
#         # All should be Algebraic types
#         assert isinstance(person_alg, Algebraic)
#         assert isinstance(city_alg, Algebraic)
#         assert isinstance(state_alg, Algebraic)
# 
#     def test_individual_relationships_before_conjunction(
#         self, lives_in: Relationship, city_in_state: Relationship, context: Context
#     ):
#         """Test converting relationships individually before conjunction."""
#         lives_in_alg = lives_in.to_algebra(context)
#         city_in_state_alg = city_in_state.to_algebra(context)
# 
#         # Both should be Algebraic
#         assert isinstance(lives_in_alg, Algebraic)
#         assert isinstance(city_in_state_alg, Algebraic)
# 
#         # Each should have their respective variables
#         assert "p" in lives_in_alg.variables_to_columns
#         assert "c" in lives_in_alg.variables_to_columns
#         assert "c" in city_in_state_alg.variables_to_columns
#         assert "s" in city_in_state_alg.variables_to_columns
# 
#     def test_full_graph_traversal(
#         self, relationship_conjunction: RelationshipConjunction, context: Context
#     ):
#         """Test full graph traversal from person through city to state."""
#         result = relationship_conjunction.to_algebra(context)
#         df = result.to_pandas(context)
# 
#         # Should be able to traverse person -> city -> state
#         assert isinstance(df, pd.DataFrame)
#         # The DataFrame should represent the complete path
