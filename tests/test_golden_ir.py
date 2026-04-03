"""Golden regression tests for the query execution intermediate representation.

These tests define the contract that the IR refactor (Phase 1 onward) MUST
preserve exactly.  Every test verifies specific output values, not just
"doesn't crash".  Any refactor that breaks a test here has introduced a
regression regardless of whether other tests still pass.

Fixture design:
- Small, fixed datasets so expected values can be hand-verified.
- Relationships expressed as explicit ID linkages.
- One context per test class so each class is self-contained.

Run with:
    uv run pytest tests/test_golden_ir.py -v
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

# ===========================================================================
# Shared fixture helpers
# ===========================================================================


def _entity_table(
    entity_type: str,
    rows: list[dict],
    attrs: list[str],
) -> EntityTable:
    """Build an EntityTable from a list of dicts.  Each dict must have '__id'."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [r["__id"] for r in rows],
            **{a: [r[a] for r in rows] for a in attrs},
        },
    )
    attr_map = {a: a for a in attrs}
    return EntityTable(
        entity_type=entity_type,
        identifier=entity_type,
        column_names=[ID_COLUMN, *attrs],
        source_obj_attribute_map=attr_map,
        attribute_map=attr_map,
        source_obj=df,
    )


def _relationship_table(
    rel_type: str,
    rows: list[dict],
) -> RelationshipTable:
    """Build a RelationshipTable from a list of dicts with __id, __src, __tgt."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [r["__id"] for r in rows],
            RELATIONSHIP_SOURCE_COLUMN: [r["__src"] for r in rows],
            RELATIONSHIP_TARGET_COLUMN: [r["__tgt"] for r in rows],
        },
    )
    return RelationshipTable(
        relationship_type=rel_type,
        identifier=rel_type,
        column_names=[
            ID_COLUMN,
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
        ],
        source_col=RELATIONSHIP_SOURCE_COLUMN,
        target_col=RELATIONSHIP_TARGET_COLUMN,
        source_obj=df,
    )


# ===========================================================================
# Class 1 — Single entity, no relationships
# ===========================================================================


class TestSingleEntityPatterns:
    """Golden tests for patterns involving one entity type and no relationships.

    Dataset: 4 Person nodes with name, age, department, salary.
    All expected values are hand-computed from this dataset.
    """

    PERSONS = [
        {
            "__id": 1,
            "name": "Alice",
            "age": 30,
            "dept": "Engineering",
            "salary": 90000,
        },
        {
            "__id": 2,
            "name": "Bob",
            "age": 25,
            "dept": "Sales",
            "salary": 60000,
        },
        {
            "__id": 3,
            "name": "Carol",
            "age": 35,
            "dept": "Engineering",
            "salary": 95000,
        },
        {
            "__id": 4,
            "name": "Dave",
            "age": 28,
            "dept": "Marketing",
            "salary": 70000,
        },
    ]

    @pytest.fixture
    def ctx(self) -> Context:
        pt = _entity_table(
            "Person",
            self.PERSONS,
            ["name", "age", "dept", "salary"],
        )
        return Context(
            entity_mapping=EntityMapping(mapping={"Person": pt}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )

    @pytest.fixture
    def star(self, ctx: Context) -> Star:
        return Star(context=ctx)

    # --- Basic projection ---

    def test_match_return_single_property(self, star: Star) -> None:
        """MATCH + RETURN single property produces correct values and row count."""
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert set(result["name"]) == {"Alice", "Bob", "Carol", "Dave"}
        assert len(result) == 4

    def test_match_return_multiple_properties(self, star: Star) -> None:
        """MATCH + RETURN multiple properties produces correct columns and values."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, p.age AS age, p.dept AS dept",
        )
        assert set(result.columns) == {"name", "age", "dept"}
        assert len(result) == 4
        alice = result[result["name"] == "Alice"].iloc[0]
        assert alice["age"] == 30
        assert alice["dept"] == "Engineering"

    # --- WHERE filtering ---

    def test_where_equality_filter(self, star: Star) -> None:
        """WHERE equality filter returns only matching rows."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.dept = 'Engineering' RETURN p.name AS name",
        )
        assert set(result["name"]) == {"Alice", "Carol"}

    def test_where_numeric_comparison(self, star: Star) -> None:
        """WHERE numeric comparison (>) returns correct subset."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 29 RETURN p.name AS name",
        )
        assert set(result["name"]) == {"Alice", "Carol"}

    def test_where_and_operator(self, star: Star) -> None:
        """WHERE with AND correctly intersects conditions."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.dept = 'Engineering' AND p.age < 35 "
            "RETURN p.name AS name",
        )
        assert set(result["name"]) == {"Alice"}

    def test_where_or_operator(self, star: Star) -> None:
        """WHERE with OR correctly unions conditions."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.dept = 'Sales' OR p.dept = 'Marketing' "
            "RETURN p.name AS name",
        )
        assert set(result["name"]) == {"Bob", "Dave"}

    def test_where_not_operator(self, star: Star) -> None:
        """WHERE with NOT correctly excludes matching rows."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE NOT p.dept = 'Engineering' RETURN p.name AS name",
        )
        assert set(result["name"]) == {"Bob", "Dave"}

    # --- Arithmetic in projection ---

    def test_arithmetic_in_return(self, star: Star) -> None:
        """Arithmetic expression in RETURN produces correct per-row values."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, p.salary * 0.1 AS bonus",
        )
        by_name = result.set_index("name")["bonus"].to_dict()
        assert abs(by_name["Alice"] - 9000.0) < 0.01
        assert abs(by_name["Bob"] - 6000.0) < 0.01
        assert abs(by_name["Carol"] - 9500.0) < 0.01
        assert abs(by_name["Dave"] - 7000.0) < 0.01

    def test_arithmetic_addition_in_return(self, star: Star) -> None:
        """Addition arithmetic in RETURN is evaluated per row."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, p.age + 5 AS future_age",
        )
        by_name = result.set_index("name")["future_age"].to_dict()
        assert by_name["Alice"] == 35
        assert by_name["Bob"] == 30

    # --- Scalar functions ---

    def test_scalar_function_toUpper_in_return(self, star: Star) -> None:
        """toUpper() in RETURN produces uppercase string values."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, toUpper(p.name) AS upper_name",
        )
        for _, row in result.iterrows():
            assert row["upper_name"] == row["name"].upper()

    def test_scalar_function_size_in_return(self, star: Star) -> None:
        """size() in RETURN returns correct string lengths."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, size(p.name) AS name_len",
        )
        for _, row in result.iterrows():
            assert row["name_len"] == len(row["name"])

    def test_scalar_function_in_where(self, star: Star) -> None:
        """Scalar function in WHERE correctly filters rows."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE size(p.name) = 3 RETURN p.name AS name",
        )
        # "Bob" and "Dave" have length 3 and 4 respectively; "Bob" = 3
        assert set(result["name"]) == {"Bob"}

    def test_toString_in_return(self, star: Star) -> None:
        """toString() converts numeric values to string representation."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, toString(p.age) AS age_str",
        )
        for _, row in result.iterrows():
            assert isinstance(row["age_str"], str)
            assert (
                int(row["age_str"])
                == {"Alice": 30, "Bob": 25, "Carol": 35, "Dave": 28}[
                    row["name"]
                ]
            )

    def test_toInteger_in_return(self, star: Star) -> None:
        """toInteger() on a float truncates correctly."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, toInteger(p.salary / 1000) AS k_salary",
        )
        by_name = result.set_index("name")["k_salary"].to_dict()
        assert by_name["Alice"] == 90
        assert by_name["Bob"] == 60

    # --- WITH clause ---

    def test_with_renames_property(self, star: Star) -> None:
        """WITH clause renames a property into a new alias."""
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS n RETURN n",
        )
        assert set(result["n"]) == {"Alice", "Bob", "Carol", "Dave"}

    def test_with_computes_expression(self, star: Star) -> None:
        """WITH clause evaluates an arithmetic expression into an alias."""
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS name, p.salary / 1000 AS k RETURN name, k",
        )
        by_name = result.set_index("name")["k"].to_dict()
        assert by_name["Alice"] == 90
        assert by_name["Carol"] == 95

    def test_match_where_then_with(self, star: Star) -> None:
        """MATCH + WHERE filters rows before WITH projection."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.dept = 'Engineering' "
            "WITH p.name AS name RETURN name",
        )
        assert set(result["name"]) == {"Alice", "Carol"}

    # --- Aggregation ---

    def test_count_star(self, star: Star) -> None:
        """count(*) returns total number of matched rows."""
        result = star.execute_query(
            "MATCH (p:Person) WITH count(*) AS total RETURN total",
        )
        assert result["total"].iloc[0] == 4

    def test_count_with_filter(self, star: Star) -> None:
        """count(*) after WHERE returns filtered count."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.dept = 'Engineering' WITH count(*) AS n RETURN n",
        )
        assert result["n"].iloc[0] == 2

    def test_sum_aggregation(self, star: Star) -> None:
        """sum() aggregation returns correct total."""
        result = star.execute_query(
            "MATCH (p:Person) WITH sum(p.salary) AS total_salary RETURN total_salary",
        )
        assert result["total_salary"].iloc[0] == 315000

    def test_avg_aggregation(self, star: Star) -> None:
        """avg() aggregation returns correct mean."""
        result = star.execute_query(
            "MATCH (p:Person) WITH avg(p.age) AS mean_age RETURN mean_age",
        )
        expected = (30 + 25 + 35 + 28) / 4  # = 29.5
        assert abs(result["mean_age"].iloc[0] - expected) < 0.001

    def test_min_max_aggregation(self, star: Star) -> None:
        """min() and max() aggregation return correct extremes."""
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH min(p.age) AS youngest, max(p.age) AS oldest "
            "RETURN youngest, oldest",
        )
        assert result["youngest"].iloc[0] == 25
        assert result["oldest"].iloc[0] == 35

    def test_grouped_aggregation(self, star: Star) -> None:
        """Grouped aggregation (WITH dept, count(*)) produces one row per group."""
        result = star.execute_query(
            "MATCH (p:Person) WITH p.dept AS dept, count(*) AS n RETURN dept, n",
        )
        by_dept = result.set_index("dept")["n"].to_dict()
        assert by_dept["Engineering"] == 2
        assert by_dept["Sales"] == 1
        assert by_dept["Marketing"] == 1

    def test_grouped_sum_aggregation(self, star: Star) -> None:
        """Grouped sum produces correct totals per group."""
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH p.dept AS dept, sum(p.salary) AS total "
            "RETURN dept, total",
        )
        by_dept = result.set_index("dept")["total"].to_dict()
        assert by_dept["Engineering"] == 185000  # 90000 + 95000
        assert by_dept["Sales"] == 60000
        assert by_dept["Marketing"] == 70000

    # --- SET clause ---

    def test_set_new_property(self, star: Star) -> None:
        """SET adds a new property to all matched entities."""
        result = star.execute_query(
            "MATCH (p:Person) SET p.active = true RETURN p.name AS name, p.active AS active",
        )
        assert len(result) == 4
        assert (result["active"] == True).all()

    def test_set_computed_property(self, star: Star) -> None:
        """SET with arithmetic expression computes per-row values correctly."""
        result = star.execute_query(
            "MATCH (p:Person) SET p.bonus = p.salary * 0.15 "
            "RETURN p.name AS name, p.bonus AS bonus",
        )
        by_name = result.set_index("name")["bonus"].to_dict()
        assert abs(by_name["Alice"] - 13500.0) < 0.01
        assert abs(by_name["Bob"] - 9000.0) < 0.01

    def test_set_modifies_existing_property(self, star: Star) -> None:
        """SET overwrites an existing property with a new value."""
        result = star.execute_query(
            "MATCH (p:Person) SET p.age = p.age + 1 RETURN p.name AS name, p.age AS age",
        )
        by_name = result.set_index("name")["age"].to_dict()
        assert by_name["Alice"] == 31
        assert by_name["Bob"] == 26

    def test_set_with_filter(self, star: Star) -> None:
        """SET after WHERE modifies only matching rows; non-matching rows absent."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.dept = 'Engineering' "
            "SET p.team = 'tech' RETURN p.name AS name, p.team AS team",
        )
        assert set(result["name"]) == {"Alice", "Carol"}
        assert (result["team"] == "tech").all()

    def test_set_then_with_sees_new_property(self, star: Star) -> None:
        """A SET result is visible to a subsequent WITH clause."""
        result = star.execute_query(
            "MATCH (p:Person) "
            "SET p.score = p.salary / 1000 "
            "WITH p.name AS name, p.score AS score "
            "RETURN name, score",
        )
        by_name = result.set_index("name")["score"].to_dict()
        assert by_name["Alice"] == 90
        assert by_name["Carol"] == 95

    def test_chained_set_clauses(self, star: Star) -> None:
        """Multiple chained SET clauses each see prior SET results."""
        result = star.execute_query(
            "MATCH (p:Person) "
            "SET p.base = 100 "
            "SET p.doubled = p.base * 2 "
            "RETURN p.name AS name, p.doubled AS doubled",
        )
        assert (result["doubled"] == 200).all()

    def test_set_null_property(self, star: Star) -> None:
        """SET p.prop = null produces a null column in the result."""
        result = star.execute_query(
            "MATCH (p:Person) SET p.optional = null "
            "RETURN p.name AS name, p.optional AS optional",
        )
        assert len(result) == 4
        assert result["optional"].isna().all()


# ===========================================================================
# Class 2 — Multi-entity join via relationship (different types)
# ===========================================================================


class TestCrossTypeRelationshipPatterns:
    """Golden tests for patterns joining two different entity types via a relationship.

    Dataset:
    - Person: Alice (eng), Bob (sales)
    - Company: Acme, Globex
    - WORKS_AT: Alice→Acme, Bob→Acme, Alice→Globex (contract)
    """

    @pytest.fixture
    def ctx(self) -> Context:
        person_table = _entity_table(
            "Person",
            [
                {"__id": 1, "name": "Alice", "dept": "Engineering"},
                {"__id": 2, "name": "Bob", "dept": "Sales"},
            ],
            ["name", "dept"],
        )
        company_table = _entity_table(
            "Company",
            [
                {"__id": 10, "cname": "Acme", "industry": "Tech"},
                {"__id": 11, "cname": "Globex", "industry": "Finance"},
            ],
            ["cname", "industry"],
        )
        works_at_table = _relationship_table(
            "WORKS_AT",
            [
                {"__id": 100, "__src": 1, "__tgt": 10},  # Alice → Acme
                {"__id": 101, "__src": 2, "__tgt": 10},  # Bob → Acme
                {"__id": 102, "__src": 1, "__tgt": 11},  # Alice → Globex
            ],
        )
        return Context(
            entity_mapping=EntityMapping(
                mapping={"Person": person_table, "Company": company_table},
            ),
            relationship_mapping=RelationshipMapping(
                mapping={"WORKS_AT": works_at_table},
            ),
        )

    @pytest.fixture
    def star(self, ctx: Context) -> Star:
        return Star(context=ctx)

    def test_basic_join_returns_correct_rows(self, star: Star) -> None:
        """Simple join returns one row per relationship."""
        result = star.execute_query(
            "MATCH (p:Person)-[r:WORKS_AT]->(c:Company) "
            "RETURN p.name AS person, c.cname AS company",
        )
        assert len(result) == 3
        pairs = set(zip(result["person"], result["company"]))
        assert ("Alice", "Acme") in pairs
        assert ("Bob", "Acme") in pairs
        assert ("Alice", "Globex") in pairs

    def test_join_with_where_on_source(self, star: Star) -> None:
        """WHERE on source entity correctly filters joined rows."""
        result = star.execute_query(
            "MATCH (p:Person)-[r:WORKS_AT]->(c:Company) "
            "WHERE p.name = 'Alice' "
            "RETURN p.name AS person, c.cname AS company",
        )
        assert len(result) == 2
        assert set(result["company"]) == {"Acme", "Globex"}

    def test_join_with_where_on_target(self, star: Star) -> None:
        """WHERE on target entity correctly filters joined rows."""
        result = star.execute_query(
            "MATCH (p:Person)-[r:WORKS_AT]->(c:Company) "
            "WHERE c.industry = 'Tech' "
            "RETURN p.name AS person, c.cname AS company",
        )
        assert len(result) == 2
        assert set(result["person"]) == {"Alice", "Bob"}
        assert (result["company"] == "Acme").all()

    def test_join_aggregation_count_by_company(self, star: Star) -> None:
        """Grouped count over joined entities produces correct per-company totals."""
        result = star.execute_query(
            "MATCH (p:Person)-[r:WORKS_AT]->(c:Company) "
            "WITH c.cname AS company, count(*) AS n "
            "RETURN company, n",
        )
        by_company = result.set_index("company")["n"].to_dict()
        assert by_company["Acme"] == 2
        assert by_company["Globex"] == 1

    def test_join_with_both_entity_properties_in_return(
        self,
        star: Star,
    ) -> None:
        """RETURN can access properties from both sides of a join."""
        result = star.execute_query(
            "MATCH (p:Person)-[r:WORKS_AT]->(c:Company) "
            "RETURN p.name AS person, p.dept AS dept, c.cname AS company, c.industry AS industry",
        )
        assert set(result.columns) == {"person", "dept", "company", "industry"}
        alice_rows = result[result["person"] == "Alice"]
        assert len(alice_rows) == 2
        industries = set(alice_rows["industry"])
        assert "Tech" in industries
        assert "Finance" in industries


# ===========================================================================
# Class 3 — Multi-hop paths (different entity types at each hop)
# ===========================================================================


class TestMultiHopDifferentTypePatterns:
    """Golden tests for multi-hop path patterns where each node is a different type.

    Dataset:
    - Person: Alice, Bob
    - Team: Alpha, Beta
    - Project: Apollo, Artemis
    - MEMBER_OF: Alice→Alpha, Bob→Alpha, Bob→Beta
    - WORKS_ON: Alpha→Apollo, Beta→Artemis
    """

    @pytest.fixture
    def ctx(self) -> Context:
        person_table = _entity_table(
            "Person",
            [
                {"__id": 1, "name": "Alice"},
                {"__id": 2, "name": "Bob"},
            ],
            ["name"],
        )
        team_table = _entity_table(
            "Team",
            [
                {"__id": 10, "team_name": "Alpha"},
                {"__id": 11, "team_name": "Beta"},
            ],
            ["team_name"],
        )
        project_table = _entity_table(
            "Project",
            [
                {"__id": 20, "proj_name": "Apollo"},
                {"__id": 21, "proj_name": "Artemis"},
            ],
            ["proj_name"],
        )
        member_of = _relationship_table(
            "MEMBER_OF",
            [
                {"__id": 100, "__src": 1, "__tgt": 10},  # Alice → Alpha
                {"__id": 101, "__src": 2, "__tgt": 10},  # Bob → Alpha
                {"__id": 102, "__src": 2, "__tgt": 11},  # Bob → Beta
            ],
        )
        works_on = _relationship_table(
            "WORKS_ON",
            [
                {"__id": 200, "__src": 10, "__tgt": 20},  # Alpha → Apollo
                {"__id": 201, "__src": 11, "__tgt": 21},  # Beta → Artemis
            ],
        )
        return Context(
            entity_mapping=EntityMapping(
                mapping={
                    "Person": person_table,
                    "Team": team_table,
                    "Project": project_table,
                },
            ),
            relationship_mapping=RelationshipMapping(
                mapping={"MEMBER_OF": member_of, "WORKS_ON": works_on},
            ),
        )

    @pytest.fixture
    def star(self, ctx: Context) -> Star:
        return Star(context=ctx)

    def test_two_hop_different_types(self, star: Star) -> None:
        """Two-hop path across three different entity types returns correct rows."""
        result = star.execute_query(
            "MATCH (p:Person)-[m:MEMBER_OF]->(t:Team)-[w:WORKS_ON]->(proj:Project) "
            "RETURN p.name AS person, t.team_name AS team, proj.proj_name AS project",
        )
        # Alice → Alpha → Apollo
        # Bob → Alpha → Apollo
        # Bob → Beta → Artemis
        assert len(result) == 3
        pairs = set(zip(result["person"], result["project"]))
        assert ("Alice", "Apollo") in pairs
        assert ("Bob", "Apollo") in pairs
        assert ("Bob", "Artemis") in pairs

    def test_two_hop_with_filter_on_middle_node(self, star: Star) -> None:
        """WHERE on middle node of two-hop path filters correctly."""
        result = star.execute_query(
            "MATCH (p:Person)-[m:MEMBER_OF]->(t:Team)-[w:WORKS_ON]->(proj:Project) "
            "WHERE t.team_name = 'Beta' "
            "RETURN p.name AS person, proj.proj_name AS project",
        )
        assert len(result) == 1
        assert result["person"].iloc[0] == "Bob"
        assert result["project"].iloc[0] == "Artemis"

    def test_two_hop_aggregation(self, star: Star) -> None:
        """Aggregation over two-hop paths produces correct grouped counts."""
        result = star.execute_query(
            "MATCH (p:Person)-[m:MEMBER_OF]->(t:Team)-[w:WORKS_ON]->(proj:Project) "
            "WITH p.name AS person, count(*) AS project_count "
            "RETURN person, project_count",
        )
        by_person = result.set_index("person")["project_count"].to_dict()
        assert by_person["Alice"] == 1  # only Apollo via Alpha
        assert by_person["Bob"] == 2  # Apollo via Alpha, Artemis via Beta


# ===========================================================================
# Class 4 — Null value handling
# ===========================================================================


class TestNullHandling:
    """Golden tests for null value semantics.

    Dataset: Entities where some properties are deliberately absent (NaN).
    """

    @pytest.fixture
    def ctx(self) -> Context:
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "score": [85.0, float("nan"), 70.0],
                "bonus": [float("nan"), 500.0, float("nan")],
            },
        )
        attr_map = {"name": "name", "score": "score", "bonus": "bonus"}
        table = EntityTable(
            entity_type="Item",
            identifier="Item",
            column_names=[ID_COLUMN, "name", "score", "bonus"],
            source_obj_attribute_map=attr_map,
            attribute_map=attr_map,
            source_obj=df,
        )
        return Context(
            entity_mapping=EntityMapping(mapping={"Item": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )

    @pytest.fixture
    def star(self, ctx: Context) -> Star:
        return Star(context=ctx)

    def test_null_in_arithmetic_propagates(self, star: Star) -> None:
        """Null operand in arithmetic produces null result."""
        result = star.execute_query(
            "MATCH (i:Item) RETURN i.name AS name, i.score + 10 AS adjusted",
        )
        by_name = result.set_index("name")["adjusted"]
        assert abs(by_name["Alice"] - 95.0) < 0.001
        assert pd.isna(by_name["Bob"])
        assert abs(by_name["Carol"] - 80.0) < 0.001

    def test_coalesce_replaces_null(self, star: Star) -> None:
        """coalesce() returns the first non-null value."""
        result = star.execute_query(
            "MATCH (i:Item) RETURN i.name AS name, coalesce(i.score, 0) AS safe_score",
        )
        by_name = result.set_index("name")["safe_score"].to_dict()
        assert by_name["Alice"] == 85.0
        assert by_name["Bob"] == 0
        assert by_name["Carol"] == 70.0

    def test_avg_ignores_null(self, star: Star) -> None:
        """avg() aggregation ignores null values (Bob's score excluded)."""
        result = star.execute_query(
            "MATCH (i:Item) WITH avg(i.score) AS mean_score RETURN mean_score",
        )
        # Only Alice (85) and Carol (70) have scores: avg = 77.5
        assert abs(result["mean_score"].iloc[0] - 77.5) < 0.001

    def test_count_star_includes_null_rows(self, star: Star) -> None:
        """count(*) counts all rows regardless of null properties."""
        result = star.execute_query(
            "MATCH (i:Item) WITH count(*) AS n RETURN n",
        )
        assert result["n"].iloc[0] == 3

    def test_is_null_check(self, star: Star) -> None:
        """IS NULL correctly identifies null values."""
        result = star.execute_query(
            "MATCH (i:Item) WHERE i.score IS NULL RETURN i.name AS name",
        )
        assert set(result["name"]) == {"Bob"}

    def test_is_not_null_check(self, star: Star) -> None:
        """IS NOT NULL correctly identifies non-null values."""
        result = star.execute_query(
            "MATCH (i:Item) WHERE i.score IS NOT NULL RETURN i.name AS name",
        )
        assert set(result["name"]) == {"Alice", "Carol"}


# ===========================================================================
# Class 5 — String scalar functions
# ===========================================================================


class TestScalarFunctions:
    """Golden tests for scalar function correctness.

    Every function is verified with known inputs and hand-computed outputs.
    """

    @pytest.fixture
    def ctx(self) -> Context:
        table = _entity_table(
            "Word",
            [
                {"__id": 1, "text": "hello", "num": 42},
                {"__id": 2, "text": "  world  ", "num": 7},
                {"__id": 3, "text": "Foo", "num": 100},
            ],
            ["text", "num"],
        )
        return Context(
            entity_mapping=EntityMapping(mapping={"Word": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )

    @pytest.fixture
    def star(self, ctx: Context) -> Star:
        return Star(context=ctx)

    def test_toUpper(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (w:Word) RETURN w.text AS text, toUpper(w.text) AS up",
        )
        by_text = dict(zip(result["text"], result["up"]))
        assert by_text["hello"] == "HELLO"
        assert by_text["Foo"] == "FOO"

    def test_toLower(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (w:Word) RETURN w.text AS text, toLower(w.text) AS low",
        )
        by_text = dict(zip(result["text"], result["low"]))
        assert by_text["Foo"] == "foo"

    def test_trim(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (w:Word) RETURN w.text AS text, trim(w.text) AS trimmed",
        )
        by_text = dict(zip(result["text"], result["trimmed"]))
        assert by_text["  world  "] == "world"
        assert by_text["hello"] == "hello"

    def test_size_of_string(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (w:Word) RETURN w.text AS text, size(w.text) AS sz",
        )
        by_text = dict(zip(result["text"], result["sz"]))
        assert by_text["hello"] == 5
        assert by_text["Foo"] == 3

    def test_substring(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (w:Word) WHERE w.text = 'hello' "
            "RETURN substring(w.text, 1, 3) AS sub",
        )
        assert result["sub"].iloc[0] == "ell"

    def test_toString_of_integer(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (w:Word) RETURN w.num AS num, toString(w.num) AS s",
        )
        for _, row in result.iterrows():
            assert isinstance(row["s"], str)
            assert int(row["s"]) == row["num"]

    def test_toInteger_from_float(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (w:Word) RETURN w.num AS num, toInteger(w.num / 10) AS d",
        )
        by_num = result.set_index("num")["d"].to_dict()
        assert by_num[42] == 4
        assert by_num[100] == 10

    def test_nested_functions(self, star: Star) -> None:
        """Nested function calls: size(toUpper(text)) === size(text)."""
        result = star.execute_query(
            "MATCH (w:Word) RETURN w.text AS text, size(toUpper(w.text)) AS sz",
        )
        for _, row in result.iterrows():
            assert row["sz"] == len(row["text"])


# ===========================================================================
# Class 6 — Aggregation accuracy
# ===========================================================================


class TestAggregationAccuracy:
    """Golden tests asserting mathematical correctness of aggregation functions.

    Uses a dataset where all expected aggregation values are pre-computed.
    """

    # Values: 10, 20, 30, 40, 50
    VALUES = [10, 20, 30, 40, 50]
    EXPECTED_SUM = 150
    EXPECTED_AVG = 30.0
    EXPECTED_MIN = 10
    EXPECTED_MAX = 50
    EXPECTED_COUNT = 5

    @pytest.fixture
    def ctx(self) -> Context:
        table = _entity_table(
            "Num",
            [
                {
                    "__id": i + 1,
                    "val": v,
                    "grp": "even" if v % 20 == 0 else "odd",
                }
                for i, v in enumerate(self.VALUES)
            ],
            ["val", "grp"],
        )
        return Context(
            entity_mapping=EntityMapping(mapping={"Num": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )

    @pytest.fixture
    def star(self, ctx: Context) -> Star:
        return Star(context=ctx)

    def test_sum(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (n:Num) WITH sum(n.val) AS s RETURN s",
        )
        assert result["s"].iloc[0] == self.EXPECTED_SUM

    def test_avg(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (n:Num) WITH avg(n.val) AS a RETURN a",
        )
        assert abs(result["a"].iloc[0] - self.EXPECTED_AVG) < 0.001

    def test_min(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (n:Num) WITH min(n.val) AS m RETURN m",
        )
        assert result["m"].iloc[0] == self.EXPECTED_MIN

    def test_max(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (n:Num) WITH max(n.val) AS m RETURN m",
        )
        assert result["m"].iloc[0] == self.EXPECTED_MAX

    def test_count_star(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (n:Num) WITH count(*) AS c RETURN c",
        )
        assert result["c"].iloc[0] == self.EXPECTED_COUNT

    def test_multiple_aggregations_in_one_with(self, star: Star) -> None:
        """Multiple aggregation functions in a single WITH clause."""
        result = star.execute_query(
            "MATCH (n:Num) "
            "WITH sum(n.val) AS s, avg(n.val) AS a, min(n.val) AS lo, max(n.val) AS hi "
            "RETURN s, a, lo, hi",
        )
        row = result.iloc[0]
        assert row["s"] == self.EXPECTED_SUM
        assert abs(row["a"] - self.EXPECTED_AVG) < 0.001
        assert row["lo"] == self.EXPECTED_MIN
        assert row["hi"] == self.EXPECTED_MAX

    def test_count_after_filter(self, star: Star) -> None:
        """count(*) after WHERE counts only matching rows."""
        result = star.execute_query(
            "MATCH (n:Num) WHERE n.val > 25 WITH count(*) AS c RETURN c",
        )
        # 30, 40, 50 qualify
        assert result["c"].iloc[0] == 3

    def test_sum_after_filter(self, star: Star) -> None:
        """sum() after WHERE sums only matching rows."""
        result = star.execute_query(
            "MATCH (n:Num) WHERE n.val <= 30 WITH sum(n.val) AS s RETURN s",
        )
        assert result["s"].iloc[0] == 60  # 10 + 20 + 30


# ===========================================================================
# Class 7 — CASE expressions (promoted from xfail when implementation landed)
# ===========================================================================


class TestCaseExpressions:
    """Golden tests for CASE expression support.

    Dataset: 4 Student nodes with name and score.
    Alice=95, Bob=82, Carol=74, Dave=60.
    """

    @pytest.fixture
    def ctx(self) -> Context:
        table = _entity_table(
            "Student",
            [
                {"__id": 1, "name": "Alice", "score": 95},
                {"__id": 2, "name": "Bob", "score": 82},
                {"__id": 3, "name": "Carol", "score": 74},
                {"__id": 4, "name": "Dave", "score": 60},
            ],
            ["name", "score"],
        )
        return Context(
            entity_mapping=EntityMapping(mapping={"Student": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )

    @pytest.fixture
    def star(self, ctx: Context) -> Star:
        return Star(context=ctx)

    def test_case_in_set(self, star: Star) -> None:
        """CASE expression as the value in a SET clause assigns correct grades."""
        result = star.execute_query(
            """
            MATCH (s:Student)
            SET s.grade = CASE
                              WHEN s.score >= 90 THEN 'A'
                              WHEN s.score >= 80 THEN 'B'
                              ELSE 'C'
                          END
            RETURN s.name AS name, s.grade AS grade
            """,
        )
        by_name = result.set_index("name")["grade"].to_dict()
        assert by_name["Alice"] == "A"
        assert by_name["Bob"] == "B"
        assert by_name["Carol"] == "C"
        assert by_name["Dave"] == "C"


# ===========================================================================
# Class 8 — Same-type self-join (Groups A + B from former xfail suite)
# ===========================================================================

#
# Dataset:
#   Alice (id=1, age=30), Bob (id=2, age=25), Carol (id=3, age=35),
#   Dave  (id=4, age=28)
#
# KNOWS edges (directed):
#   Alice → Bob  (id=10)
#   Bob   → Carol (id=11)
#   Alice → Carol (id=12)
#   Carol → Dave  (id=13)
#

_PERSONS_8 = [
    {"__id": 1, "name": "Alice", "age": 30},
    {"__id": 2, "name": "Bob", "age": 25},
    {"__id": 3, "name": "Carol", "age": 35},
    {"__id": 4, "name": "Dave", "age": 28},
]

_KNOWS_8 = [
    {"__id": 10, "__src": 1, "__tgt": 2},  # Alice → Bob
    {"__id": 11, "__src": 2, "__tgt": 3},  # Bob → Carol
    {"__id": 12, "__src": 1, "__tgt": 3},  # Alice → Carol
    {"__id": 13, "__src": 3, "__tgt": 4},  # Carol → Dave
]


@pytest.fixture(name="knows_ctx_8")
def _knows_ctx_8() -> Context:
    person_table = _entity_table("Person", _PERSONS_8, ["name", "age"])
    knows_table = _relationship_table("KNOWS", _KNOWS_8)
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table},
        ),
    )


@pytest.fixture(name="knows_star_8")
def _knows_star_8(knows_ctx_8: Context) -> Star:
    return Star(context=knows_ctx_8)


class TestSameTypeSelfJoin:
    """Golden tests for same-type self-joins (formerly Group A xfail)."""

    def test_one_hop_correct_pairs(self, knows_star_8: Star) -> None:
        """MATCH (p:Person)-[:KNOWS]->(q:Person) returns one row per directed edge."""
        result = knows_star_8.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(q:Person) "
            "RETURN p.name AS source, q.name AS target",
        )
        assert len(result) == 4
        pairs = set(zip(result["source"], result["target"]))
        assert ("Alice", "Bob") in pairs
        assert ("Bob", "Carol") in pairs
        assert ("Alice", "Carol") in pairs
        assert ("Carol", "Dave") in pairs

    def test_one_hop_where_on_source(self, knows_star_8: Star) -> None:
        """WHERE on source variable filters correctly in same-type join."""
        result = knows_star_8.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(q:Person) "
            "WHERE p.name = 'Alice' "
            "RETURN p.name AS source, q.name AS target",
        )
        assert len(result) == 2
        assert set(result["target"]) == {"Bob", "Carol"}

    def test_one_hop_where_on_target(self, knows_star_8: Star) -> None:
        """WHERE on target variable filters correctly in same-type join."""
        result = knows_star_8.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(q:Person) "
            "WHERE q.name = 'Carol' "
            "RETURN p.name AS source, q.name AS target",
        )
        assert len(result) == 2
        assert set(result["source"]) == {"Bob", "Alice"}

    def test_one_hop_aggregation(self, knows_star_8: Star) -> None:
        """Grouped aggregation over same-type join counts edges per source."""
        result = knows_star_8.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(q:Person) "
            "WITH p.name AS person, count(*) AS out_degree "
            "RETURN person, out_degree",
        )
        by_person = result.set_index("person")["out_degree"].to_dict()
        assert by_person["Alice"] == 2
        assert by_person["Bob"] == 1
        assert by_person["Carol"] == 1
        assert "Dave" not in by_person

    def test_one_hop_property_from_both_ends(self, knows_star_8: Star) -> None:
        """Properties from both ends of a same-type join are independently accessible."""
        result = knows_star_8.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(q:Person) "
            "RETURN p.name AS source, p.age AS source_age, "
            "q.name AS target, q.age AS target_age",
        )
        alice_bob = result[
            (result["source"] == "Alice") & (result["target"] == "Bob")
        ].iloc[0]
        assert alice_bob["source_age"] == 30
        assert alice_bob["target_age"] == 25


class TestMultiHopSameType:
    """Golden tests for two- and three-hop same-type paths (formerly Group B xfail)."""

    def test_two_hop_row_count(self, knows_star_8: Star) -> None:
        """(a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) returns correct row count."""
        result = knows_star_8.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "RETURN a.name AS a, b.name AS b, c.name AS c",
        )
        assert len(result) == 3

    def test_two_hop_correct_paths(self, knows_star_8: Star) -> None:
        """Two-hop same-type paths contain exactly the expected (a, b, c) triples."""
        result = knows_star_8.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "RETURN a.name AS a, b.name AS b, c.name AS c",
        )
        triples = set(zip(result["a"], result["b"], result["c"]))
        assert ("Alice", "Bob", "Carol") in triples
        assert ("Alice", "Carol", "Dave") in triples
        assert ("Bob", "Carol", "Dave") in triples

    def test_two_hop_where_on_start(self, knows_star_8: Star) -> None:
        """WHERE on first variable of two-hop path filters correctly."""
        result = knows_star_8.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "WHERE a.name = 'Alice' "
            "RETURN a.name AS a, b.name AS b, c.name AS c",
        )
        assert len(result) == 2
        triples = set(zip(result["a"], result["b"], result["c"]))
        assert ("Alice", "Bob", "Carol") in triples
        assert ("Alice", "Carol", "Dave") in triples

    def test_two_hop_where_on_end(self, knows_star_8: Star) -> None:
        """WHERE on terminal variable of two-hop path filters correctly."""
        result = knows_star_8.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "WHERE c.name = 'Dave' "
            "RETURN a.name AS a, b.name AS b",
        )
        assert len(result) == 2
        pairs = set(zip(result["a"], result["b"]))
        assert ("Alice", "Carol") in pairs
        assert ("Bob", "Carol") in pairs

    def test_three_hop(self, knows_star_8: Star) -> None:
        """Three-hop same-type path: Alice→Bob→Carol→Dave."""
        result = knows_star_8.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) "
            "RETURN a.name AS a, b.name AS b, c.name AS c, d.name AS d",
        )
        assert len(result) == 1
        row = result.iloc[0]
        assert row["a"] == "Alice"
        assert row["b"] == "Bob"
        assert row["c"] == "Carol"
        assert row["d"] == "Dave"


# ===========================================================================
# Class 9 — CASE expression in RETURN / WHERE / WITH (formerly Group C xfail)
# ===========================================================================


@pytest.fixture(name="score_ctx_9")
def _score_ctx_9() -> Context:
    table = _entity_table(
        "Student",
        [
            {"__id": 1, "name": "Alice", "score": 95},
            {"__id": 2, "name": "Bob", "score": 82},
            {"__id": 3, "name": "Carol", "score": 74},
            {"__id": 4, "name": "Dave", "score": 60},
        ],
        ["name", "score"],
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Student": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


@pytest.fixture(name="score_star_9")
def _score_star_9(score_ctx_9: Context) -> Star:
    return Star(context=score_ctx_9)


class TestCaseExpressionClauses:
    """Golden tests for CASE expression in RETURN, WHERE, WITH, and aggregation."""

    def test_case_in_return_searched(self, score_star_9: Star) -> None:
        """Searched CASE in RETURN assigns correct grades."""
        result = score_star_9.execute_query(
            """
            MATCH (s:Student)
            RETURN s.name AS name,
                   CASE
                       WHEN s.score >= 90 THEN 'A'
                       WHEN s.score >= 80 THEN 'B'
                       WHEN s.score >= 70 THEN 'C'
                       ELSE 'F'
                   END AS grade
            """,
        )
        by_name = result.set_index("name")["grade"].to_dict()
        assert by_name["Alice"] == "A"
        assert by_name["Bob"] == "B"
        assert by_name["Carol"] == "C"
        assert by_name["Dave"] == "F"

    def test_case_in_where(self, score_star_9: Star) -> None:
        """CASE expression used as boolean condition in WHERE."""
        result = score_star_9.execute_query(
            """
            MATCH (s:Student)
            WHERE CASE WHEN s.score >= 80 THEN true ELSE false END = true
            RETURN s.name AS name
            """,
        )
        assert set(result["name"]) == {"Alice", "Bob"}

    def test_case_in_with(self, score_star_9: Star) -> None:
        """CASE expression in WITH produces correct computed alias."""
        result = score_star_9.execute_query(
            """
            MATCH (s:Student)
            WITH s.name AS name,
                 CASE WHEN s.score >= 80 THEN 'pass' ELSE 'fail' END AS status
            RETURN name, status
            """,
        )
        by_name = result.set_index("name")["status"].to_dict()
        assert by_name["Alice"] == "pass"
        assert by_name["Bob"] == "pass"
        assert by_name["Carol"] == "fail"
        assert by_name["Dave"] == "fail"

    def test_case_in_aggregation(self, score_star_9: Star) -> None:
        """CASE inside count() implements conditional counting."""
        result = score_star_9.execute_query(
            """
            MATCH (s:Student)
            WITH count(CASE WHEN s.score >= 80 THEN 1 END) AS passing,
                 count(CASE WHEN s.score < 80  THEN 1 END) AS failing
            RETURN passing, failing
            """,
        )
        row = result.iloc[0]
        assert row["passing"] == 2
        assert row["failing"] == 2


# ===========================================================================
# Class 10 — Cyclic patterns + variable-length paths (formerly Group A/D xfail)
# ===========================================================================


class TestCyclicAndVariableLengthPaths:
    """Golden tests for cyclic back-references and variable-length traversals."""

    def test_same_type_mutual_knows(self, knows_star_8: Star) -> None:
        """Mutual relationship detection: find pairs where both know each other."""
        # In our dataset there are NO mutual KNOWS edges, so result should be empty.
        result = knows_star_8.execute_query(
            "MATCH (p:Person)-[:KNOWS]->(q:Person)-[:KNOWS]->(p:Person) "
            "RETURN p.name AS a, q.name AS b",
        )
        assert len(result) == 0

    def test_variable_length_path_reachability(
        self,
        knows_star_8: Star,
    ) -> None:
        """MATCH (a)-[:KNOWS*1..2]->(b) finds nodes reachable in 1 or 2 hops."""
        result = knows_star_8.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "WHERE a.name = 'Alice' "
            "RETURN b.name AS reachable",
        )
        # 1-hop: Bob, Carol; 2-hop: Carol (via Bob), Dave (via Carol)
        assert set(result["reachable"]) == {"Bob", "Carol", "Dave"}

    def test_shortest_path_length(self, knows_star_8: Star) -> None:
        """Variable-length path with length() function reports hop count."""
        result = knows_star_8.execute_query(
            "MATCH p = (a:Person)-[:KNOWS*]->(b:Person) "
            "WHERE a.name = 'Alice' AND b.name = 'Dave' "
            "RETURN length(p) AS hops",
        )
        # Shortest: Alice → Carol → Dave (2 hops)
        assert result["hops"].min() == 2
