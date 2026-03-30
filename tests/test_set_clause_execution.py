"""Unit tests for SET clause execution in Star query processor.
Tests the integration of SET clause processing into the query execution pipeline.
"""

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star


class TestSetClauseBasicExecution:
    """Test basic SET clause execution functionality."""

    @pytest.fixture
    def person_context(self):
        """Create a context with Person entities for testing."""
        person_df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "age": [25, 30, 35],
                "department": ["Engineering", "Sales", "Engineering"],
            },
        )

        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name", "age", "department"],
            source_obj_attribute_map={
                "name": "name",
                "age": "age",
                "department": "department",
            },
            attribute_map={
                "name": "name",
                "age": "age",
                "department": "department",
            },
            source_obj=person_df,
        )

        return Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )

    def test_simple_set_property_execution(self, person_context):
        """Test executing simple SET n.property = value."""
        star = Star(context=person_context)

        result = star.execute_query(
            "MATCH (p:Person) SET p.status = 'active' RETURN p.name AS name, p.status AS status",
        )

        # Check that status property was added
        assert len(result) == 3
        assert "status" in result.columns
        assert (result["status"] == "active").all()

        # Check original data preserved
        assert set(result["name"]) == {"Alice", "Bob", "Carol"}

    def test_set_property_with_expression(self, person_context):
        """Test SET with computed expression value."""
        star = Star(context=person_context)

        result = star.execute_query(
            "MATCH (p:Person) SET p.next_year_age = p.age + 1 RETURN p.name AS name, p.age AS age, p.next_year_age AS next_year_age",
        )

        # Check that computed property was added correctly
        assert len(result) == 3
        assert "next_year_age" in result.columns

        # Check computation is correct
        for _, row in result.iterrows():
            assert row["next_year_age"] == row["age"] + 1

    def test_set_modify_existing_property(self, person_context):
        """Test SET modifying existing property."""
        star = Star(context=person_context)

        result = star.execute_query(
            "MATCH (p:Person) SET p.age = 99 RETURN p.name AS name, p.age AS age",
        )

        # Check that existing property was modified
        assert len(result) == 3
        assert (result["age"] == 99).all()

        # Check other properties preserved
        assert set(result["name"]) == {"Alice", "Bob", "Carol"}

    def test_set_multiple_properties(self, person_context):
        """Test SET with multiple property assignments."""
        star = Star(context=person_context)

        result = star.execute_query(
            """MATCH (p:Person)
               SET p.status = 'employed', p.level = 5
               RETURN p.name AS name, p.status AS status, p.level AS level""",
        )

        # Check both properties were set
        assert len(result) == 3
        assert "status" in result.columns
        assert "level" in result.columns
        assert (result["status"] == "employed").all()
        assert (result["level"] == 5).all()

    def test_set_with_filtering(self, person_context):
        """Test SET clause after WHERE filtering — only matched rows are returned."""
        star = Star(context=person_context)

        # In standard Cypher, WHERE filters nodes before SET and RETURN.
        # Only the matched (Engineering) rows appear in the result.
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.department = 'Engineering' SET p.team = 'tech' RETURN p.name AS name, p.team AS team",
        )

        # Only Engineering people (Alice and Carol) appear in result
        assert len(result) == 2
        assert (result["team"] == "tech").all()
        assert set(result["name"]) == {"Alice", "Carol"}

    def test_set_labels_execution(self, person_context):
        """Test executing SET n:Label."""
        star = Star(context=person_context)

        result = star.execute_query(
            "MATCH (p:Person) SET p:Employee RETURN p.name AS name",
        )

        # Check that labels were added (implementation-dependent how labels are stored)
        assert len(result) == 3
        # Note: The exact representation of labels in the result will depend on implementation

    def test_set_chain_with_other_clauses(self, person_context):
        """Test SET clause chained with other clauses."""
        star = Star(context=person_context)

        result = star.execute_query(
            """MATCH (p:Person)
               SET p.processed = true
               WITH p.name AS name, p.age AS age, p.processed AS processed
               RETURN name, age, processed""",
        )

        # Check that SET worked and subsequent clauses see the changes
        assert len(result) == 3
        assert "processed" in result.columns
        assert (result["processed"] == True).all()


class TestSetClauseAdvancedExecution:
    """Test advanced SET clause execution scenarios."""

    @pytest.fixture
    def employee_context(self):
        """Create a context with Employee entities for advanced testing."""
        employee_df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4],
                "firstName": ["Alice", "Bob", "Carol", "Dave"],
                "lastName": ["Smith", "Jones", "Brown", "Wilson"],
                "baseSalary": [70000, 60000, 80000, 65000],
                "department": [
                    "Engineering",
                    "Sales",
                    "Engineering",
                    "Marketing",
                ],
                "startDate": [
                    "2020-01-15",
                    "2019-06-20",
                    "2021-03-10",
                    "2020-09-05",
                ],
            },
        )

        employee_table = EntityTable(
            entity_type="Employee",
            identifier="Employee",
            column_names=[
                ID_COLUMN,
                "firstName",
                "lastName",
                "baseSalary",
                "department",
                "startDate",
            ],
            source_obj_attribute_map={
                "firstName": "firstName",
                "lastName": "lastName",
                "baseSalary": "baseSalary",
                "department": "department",
                "startDate": "startDate",
            },
            attribute_map={
                "firstName": "firstName",
                "lastName": "lastName",
                "baseSalary": "baseSalary",
                "department": "department",
                "startDate": "startDate",
            },
            source_obj=employee_df,
        )

        return Context(
            entity_mapping=EntityMapping(mapping={"Employee": employee_table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )

    def test_set_all_properties_execution(self, employee_context):
        """Test executing SET n = {map}."""
        star = Star(context=employee_context)

        result = star.execute_query(
            """MATCH (e:Employee)
               SET e = {fullName: 'Updated Name', status: 'current', priority: 1}
               RETURN e.fullName AS fullName, e.status AS status, e.priority AS priority""",
        )

        # Check that all properties in map were set
        assert len(result) == 4
        assert "fullName" in result.columns
        assert "status" in result.columns
        assert "priority" in result.columns
        assert (result["fullName"] == "Updated Name").all()
        assert (result["status"] == "current").all()
        assert (result["priority"] == 1).all()

    def test_add_all_properties_execution(self, employee_context):
        """Test executing SET n += {map}."""
        star = Star(context=employee_context)

        result = star.execute_query(
            """MATCH (e:Employee)
               SET e += {bonus: e.baseSalary * 0.1, eligible: true}
               RETURN e.firstName AS firstName, e.baseSalary AS baseSalary, e.bonus AS bonus, e.eligible AS eligible""",
        )

        # Check that properties were added
        assert len(result) == 4
        assert "bonus" in result.columns
        assert "eligible" in result.columns
        assert (result["eligible"] == True).all()

        # Check bonus calculation
        for _, row in result.iterrows():
            expected_bonus = row["baseSalary"] * 0.1
            assert abs(row["bonus"] - expected_bonus) < 0.01

        # Check original properties still exist
        assert "firstName" in result.columns
        assert "baseSalary" in result.columns

    def test_complex_set_expression(self, employee_context):
        """SET using a CASE expression as the value."""
        star = Star(context=employee_context)

        result = star.execute_query(
            """MATCH (e:Employee)
               SET e.tier = CASE WHEN e.baseSalary > 70000 THEN 'senior' ELSE 'standard' END
               RETURN e.firstName AS name, e.tier AS tier
               ORDER BY e.firstName ASC""",
        )
        tiers = dict(zip(result["name"], result["tier"]))
        assert tiers["Alice"] == "standard"  # 70000 is NOT > 70000
        assert tiers["Bob"] == "standard"  # 60000
        assert tiers["Carol"] == "senior"  # 80000
        assert tiers["Dave"] == "standard"  # 65000

    def test_set_with_aggregation_context(self, employee_context):
        """SET after a WITH that computes a scalar aggregate — uses toUpper on property."""
        star = Star(context=employee_context)

        # SET an uppercased name property on each Engineering employee
        result = star.execute_query(
            """MATCH (e:Employee) WHERE e.department = 'Engineering'
               SET e.codeTag = toUpper(e.firstName)
               RETURN e.firstName AS name, e.codeTag AS tag
               ORDER BY e.firstName ASC""",
        )
        assert list(result["tag"]) == ["ALICE", "CAROL"]

    def test_multiple_set_clauses(self, employee_context):
        """Test multiple SET clauses in same query."""
        star = Star(context=employee_context)

        result = star.execute_query(
            """MATCH (e:Employee)
               SET e.processed = true
               SET e.timestamp = '2024-01-01'
               SET e.version = 1
               RETURN e.firstName AS firstName, e.processed AS processed, e.timestamp AS timestamp, e.version AS version""",
        )

        # Check all SET clauses were applied
        assert len(result) == 4
        assert "processed" in result.columns
        assert "timestamp" in result.columns
        assert "version" in result.columns
        assert (result["processed"] == True).all()
        assert (result["timestamp"] == "2024-01-01").all()
        assert (result["version"] == 1).all()


class TestSetClauseErrorHandling:
    """Test error conditions and edge cases in SET clause execution."""

    @pytest.fixture
    def simple_context(self):
        """Create a simple context for error testing."""
        df = pd.DataFrame({ID_COLUMN: [1, 2], "name": ["Alice", "Bob"]})

        table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name"],
            source_obj_attribute_map={"name": "name"},
            attribute_map={"name": "name"},
            source_obj=df,
        )

        return Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )

    def test_set_nonexistent_property_reference(self, simple_context):
        """SET using a nonexistent source property sets the new prop to null."""
        star = Star(context=simple_context)
        # p.nonexistent returns null per Cypher semantics; SET assigns null to newProp
        result = star.execute_query(
            "MATCH (p:Person) SET p.newProp = p.nonexistent RETURN p.name",
        )
        # Query must execute without raising (null propagation is valid)
        assert len(result) > 0

    def test_set_on_nonexistent_variable(self, simple_context):
        """Test SET on undefined variable."""
        star = Star(context=simple_context)

        with pytest.raises(Exception):  # Should raise appropriate exception
            star.execute_query(
                "MATCH (p:Person) SET q.name = 'test' RETURN p.name",
            )

    def test_set_with_type_mismatch(self, simple_context):
        """Test SET with incompatible type assignment."""
        star = Star(context=simple_context)

        # This might be allowed depending on implementation - could be warning rather than error
        result = star.execute_query(
            "MATCH (p:Person) SET p.name = 123 RETURN p.name AS name",
        )

        # Should complete but might convert type
        assert len(result) == 2
        # The exact behavior depends on implementation type handling

    def test_set_with_null_assignment(self, simple_context):
        """Test SET assigning null value."""
        star = Star(context=simple_context)

        result = star.execute_query(
            "MATCH (p:Person) SET p.optional = null RETURN p.name AS name, p.optional AS optional",
        )

        assert len(result) == 2
        assert "optional" in result.columns
        assert result["optional"].isna().all()

    def test_empty_set_clause(self, simple_context):
        """Test behavior with empty SET clause."""
        star = Star(context=simple_context)

        # This should be a parse error, but test documents expected behavior
        with pytest.raises(Exception):
            star.execute_query("MATCH (p:Person) SET RETURN p.name")


class TestSetClauseIntegration:
    """Test SET clause integration with other query features."""

    @pytest.fixture
    def integration_context(self):
        """Create context for integration testing."""
        person_df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "age": [25, 30, 35],
                "active": [True, False, True],
            },
        )

        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name", "age", "active"],
            source_obj_attribute_map={
                "name": "name",
                "age": "age",
                "active": "active",
            },
            attribute_map={"name": "name", "age": "age", "active": "active"},
            source_obj=person_df,
        )

        return Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )

    def test_set_with_with_clause(self, integration_context):
        """Test SET followed by WITH clause."""
        star = Star(context=integration_context)

        result = star.execute_query(
            """MATCH (p:Person)
               SET p.doubled_age = p.age * 2
               WITH p.name AS name, p.doubled_age AS doubled_age
               RETURN name, doubled_age""",
        )

        # Check SET result is visible in WITH clause
        assert len(result) == 3
        assert "doubled_age" in result.columns
        assert sorted(result["doubled_age"].tolist()) == [
            50,
            60,
            70,
        ]  # 25*2, 30*2, 35*2

    def test_set_with_aggregation(self, integration_context):
        """Aggregate functions are not valid in SET value expressions.

        In Cypher, aggregates must be computed in a prior WITH clause and
        referenced as scalars in subsequent SET clauses.  Using avg() or any
        other aggregate directly in SET raises an error.
        """
        star = Star(context=integration_context)

        with pytest.raises(Exception):
            star.execute_query(
                "MATCH (p:Person) SET p.avg_age = avg(p.age) RETURN p.name",
            )

    def test_set_preserves_query_context(self, integration_context):
        """Test that SET doesn't break subsequent WITH/RETURN operations."""
        star = Star(context=integration_context)

        # WHERE is not valid directly after SET in standard Cypher; use MATCH WHERE instead.
        result = star.execute_query(
            """MATCH (p:Person) WHERE p.active = true
               SET p.processed = true
               WITH count(*) AS active_count
               RETURN active_count""",
        )

        # Alice (active=True) and Carol (active=True) are the two active people.
        assert result["active_count"].iloc[0] == 2

    def test_nested_set_operations(self, integration_context):
        """Test SET operations that reference previously SET properties."""
        star = Star(context=integration_context)

        result = star.execute_query(
            """MATCH (p:Person)
               SET p.base_score = 100
               SET p.age_bonus = p.age * 2
               SET p.total_score = p.base_score + p.age_bonus
               RETURN p.name AS name, p.total_score AS total_score""",
        )

        # Check that chained SET operations work
        assert len(result) == 3
        assert "total_score" in result.columns

        # Check calculation: 100 + (age * 2)
        expected_scores = [
            150,
            160,
            170,
        ]  # 100 + (25*2), 100 + (30*2), 100 + (35*2)
        assert result["total_score"].tolist() == expected_scores
