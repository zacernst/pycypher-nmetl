"""Data generation utilities for the data scientist showcase.

Generates synthetic but realistic datasets for each demo script.
All generators are deterministic (seeded) for reproducible demonstrations.

This module is NOT part of the public PyCypher API — it exists solely to
support these demo scripts.
"""

from __future__ import annotations

import random
from datetime import date, timedelta

import pandas as pd


def _seed(seed: int = 42) -> random.Random:
    """Return a seeded Random instance for reproducible data."""
    return random.Random(seed)


# ---------------------------------------------------------------------------
# Script 1: Quick Start — small, clean datasets
# ---------------------------------------------------------------------------


def quick_start_people(n: int = 8) -> pd.DataFrame:
    """Generate a small people dataset for the quick start demo.

    Returns a DataFrame with __ID__, name, age, and role columns.
    """
    rng = _seed(1)
    names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank"]
    roles = ["Engineer", "Analyst", "Manager", "Scientist", "Designer"]
    return pd.DataFrame(
        {
            "__ID__": list(range(1, n + 1)),
            "name": names[:n],
            "age": [rng.randint(24, 55) for _ in range(n)],
            "role": [rng.choice(roles) for _ in range(n)],
        }
    )


def quick_start_friendships(people_df: pd.DataFrame) -> pd.DataFrame:
    """Generate friendship relationships between people.

    Creates a realistic social graph where each person knows 1-3 others.
    """
    rng = _seed(2)
    ids = people_df["__ID__"].tolist()
    edges: list[dict] = []
    edge_id = 1
    seen: set[tuple[int, int]] = set()

    for person_id in ids:
        n_friends = rng.randint(1, 3)
        candidates = [x for x in ids if x != person_id]
        friends = rng.sample(candidates, min(n_friends, len(candidates)))
        for friend_id in friends:
            key = (min(person_id, friend_id), max(person_id, friend_id))
            if key not in seen:
                seen.add(key)
                edges.append(
                    {
                        "__ID__": edge_id,
                        "__SOURCE__": person_id,
                        "__TARGET__": friend_id,
                        "since": rng.randint(2018, 2025),
                    }
                )
                edge_id += 1

    return pd.DataFrame(edges)


# ---------------------------------------------------------------------------
# Script 2: Backend Performance — scalable datasets
# ---------------------------------------------------------------------------


def scalable_entities(n: int = 1000) -> pd.DataFrame:
    """Generate a dataset of configurable size for backend benchmarks.

    Args:
        n: Number of rows to generate.

    Returns:
        DataFrame with __ID__, name, value, category, and timestamp columns.
    """
    rng = _seed(10)
    categories = ["alpha", "beta", "gamma", "delta", "epsilon"]
    base_date = date(2020, 1, 1)

    return pd.DataFrame(
        {
            "__ID__": list(range(1, n + 1)),
            "name": [f"entity_{i:06d}" for i in range(1, n + 1)],
            "value": [round(rng.uniform(1.0, 10000.0), 2) for _ in range(n)],
            "category": [rng.choice(categories) for _ in range(n)],
            "timestamp": [
                (base_date + timedelta(days=rng.randint(0, 1000))).isoformat()
                for _ in range(n)
            ],
        }
    )


def scalable_relationships(entity_df: pd.DataFrame, density: float = 2.0) -> pd.DataFrame:
    """Generate relationships for scalable entities.

    Args:
        entity_df: The entity DataFrame to link.
        density: Average number of relationships per entity.

    Returns:
        DataFrame with relationship edges.
    """
    rng = _seed(11)
    ids = entity_df["__ID__"].tolist()
    n_edges = int(len(ids) * density)
    edges: list[dict] = []

    for i in range(1, n_edges + 1):
        src = rng.choice(ids)
        tgt = rng.choice([x for x in ids if x != src])
        edges.append(
            {
                "__ID__": i,
                "__SOURCE__": src,
                "__TARGET__": tgt,
                "weight": round(rng.uniform(0.1, 1.0), 3),
            }
        )

    return pd.DataFrame(edges)


# ---------------------------------------------------------------------------
# Script 3: Real-World Messiness — messy government-style data
# ---------------------------------------------------------------------------


def messy_contractors() -> pd.DataFrame:
    """Generate messy contractor data mimicking government datasets.

    Includes inconsistent casing, missing values, mixed date formats,
    and duplicate-like entries — typical of real-world public data.
    """
    rng = _seed(20)
    rows = [
        {"__ID__": 1, "vendor_name": "Acme Corp", "state": "VA", "amount": 1500000.00, "award_date": "2024-01-15", "category": "IT Services"},
        {"__ID__": 2, "vendor_name": "ACME CORP.", "state": "va", "amount": 2300000.00, "award_date": "02/28/2024", "category": "it services"},
        {"__ID__": 3, "vendor_name": "acme corporation", "state": "VA", "amount": None, "award_date": "2024-03-10", "category": "IT"},
        {"__ID__": 4, "vendor_name": "Globex Industries", "state": "MD", "amount": 890000.50, "award_date": "2024-01-20", "category": "Consulting"},
        {"__ID__": 5, "vendor_name": "Globex Industries LLC", "state": "MD", "amount": 1200000.00, "award_date": "Jan 5, 2024", "category": "consulting"},
        {"__ID__": 6, "vendor_name": "Initech", "state": "TX", "amount": 450000.00, "award_date": "2024-02-14", "category": "Software"},
        {"__ID__": 7, "vendor_name": "INITECH INC", "state": "tx", "amount": 675000.00, "award_date": "2024-04-01", "category": "software dev"},
        {"__ID__": 8, "vendor_name": "Umbrella Corp", "state": "CA", "amount": 3200000.00, "award_date": "2024-01-30", "category": "Research"},
        {"__ID__": 9, "vendor_name": "Umbrella Corporation", "state": "ca", "amount": 1800000.00, "award_date": "03/15/2024", "category": "R&D"},
        {"__ID__": 10, "vendor_name": "Stark Industries", "state": "NY", "amount": 5600000.00, "award_date": "2024-02-01", "category": "Defense"},
        {"__ID__": 11, "vendor_name": "stark industries", "state": "ny", "amount": 4200000.00, "award_date": "2024-05-20", "category": "defense tech"},
        {"__ID__": 12, "vendor_name": "Wayne Enterprises", "state": "NJ", "amount": 920000.00, "award_date": "2024-03-01", "category": None},
    ]
    return pd.DataFrame(rows)


def messy_agencies() -> pd.DataFrame:
    """Generate government agency data with typical inconsistencies."""
    return pd.DataFrame(
        {
            "__ID__": list(range(101, 107)),
            "agency_name": [
                "Dept. of Defense",
                "Department of Defense",
                "Dept of Energy",
                "Health & Human Services",
                "HHS",
                "General Services Admin",
            ],
            "budget_millions": [750.0, None, 120.0, 340.0, None, 85.0],
            "state": ["DC", "DC", "DC", "DC", "DC", "DC"],
        }
    )


def messy_awards(contractors_df: pd.DataFrame, agencies_df: pd.DataFrame) -> pd.DataFrame:
    """Generate contract award relationships between agencies and contractors."""
    rng = _seed(21)
    c_ids = contractors_df["__ID__"].tolist()
    a_ids = agencies_df["__ID__"].tolist()
    edges: list[dict] = []

    for i in range(1, 16):
        edges.append(
            {
                "__ID__": i,
                "__SOURCE__": rng.choice(a_ids),
                "__TARGET__": rng.choice(c_ids),
                "fiscal_year": rng.choice([2023, 2024]),
                "status": rng.choice(["Active", "ACTIVE", "active", "Completed", "completed"]),
            }
        )

    return pd.DataFrame(edges)


# ---------------------------------------------------------------------------
# Script 4: Multi-Dataset Integration — multiple related sources
# ---------------------------------------------------------------------------


def integration_employees(n: int = 25) -> pd.DataFrame:
    """Generate employee records for the integration demo."""
    rng = _seed(30)
    first_names = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer",
                   "Michael", "Linda", "David", "Elizabeth", "William", "Barbara",
                   "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah",
                   "Christopher", "Karen", "Daniel", "Lisa", "Matthew", "Nancy", "Anthony"]
    departments = ["Engineering", "Sales", "Marketing", "Finance", "Operations"]
    levels = ["Junior", "Mid", "Senior", "Lead", "Principal"]

    return pd.DataFrame(
        {
            "__ID__": list(range(1, n + 1)),
            "name": first_names[:n],
            "department": [rng.choice(departments) for _ in range(n)],
            "level": [rng.choice(levels) for _ in range(n)],
            "salary": [rng.randint(60, 200) * 1000 for _ in range(n)],
            "hire_date": [
                (date(2018, 1, 1) + timedelta(days=rng.randint(0, 2000))).isoformat()
                for _ in range(n)
            ],
        }
    )


def integration_projects(n: int = 10) -> pd.DataFrame:
    """Generate project records for the integration demo."""
    rng = _seed(31)
    project_names = [
        "Atlas", "Beacon", "Catalyst", "Delta", "Eclipse",
        "Frontier", "Genesis", "Horizon", "Ion", "Jupiter",
    ]
    statuses = ["Active", "Planning", "Completed", "On Hold"]

    return pd.DataFrame(
        {
            "__ID__": list(range(201, 201 + n)),
            "name": project_names[:n],
            "budget": [rng.randint(50, 500) * 1000 for _ in range(n)],
            "status": [rng.choice(statuses) for _ in range(n)],
            "priority": [rng.choice(["High", "Medium", "Low"]) for _ in range(n)],
        }
    )


def integration_departments() -> pd.DataFrame:
    """Generate department records with budget information."""
    return pd.DataFrame(
        {
            "__ID__": list(range(301, 306)),
            "name": ["Engineering", "Sales", "Marketing", "Finance", "Operations"],
            "budget": [2000000, 1500000, 800000, 600000, 1200000],
            "headcount_target": [50, 30, 20, 15, 25],
        }
    )


def integration_assignments(
    employees_df: pd.DataFrame, projects_df: pd.DataFrame
) -> pd.DataFrame:
    """Generate employee-to-project assignment relationships."""
    rng = _seed(32)
    emp_ids = employees_df["__ID__"].tolist()
    proj_ids = projects_df["__ID__"].tolist()
    edges: list[dict] = []
    edge_id = 1

    for emp_id in emp_ids:
        n_projects = rng.randint(1, 3)
        assigned = rng.sample(proj_ids, min(n_projects, len(proj_ids)))
        for proj_id in assigned:
            edges.append(
                {
                    "__ID__": edge_id,
                    "__SOURCE__": emp_id,
                    "__TARGET__": proj_id,
                    "role": rng.choice(["contributor", "lead", "reviewer"]),
                    "hours_per_week": rng.randint(5, 40),
                }
            )
            edge_id += 1

    return pd.DataFrame(edges)


def integration_mentors(employees_df: pd.DataFrame) -> pd.DataFrame:
    """Generate mentor-mentee relationships between employees."""
    rng = _seed(33)
    ids = employees_df["__ID__"].tolist()
    edges: list[dict] = []
    edge_id = 1

    # Senior people mentor junior people
    for i, emp_id in enumerate(ids):
        if i < len(ids) // 2:  # Roughly half are mentors
            n_mentees = rng.randint(1, 2)
            mentee_pool = [x for x in ids if x != emp_id]
            mentees = rng.sample(mentee_pool, min(n_mentees, len(mentee_pool)))
            for mentee_id in mentees:
                edges.append(
                    {
                        "__ID__": edge_id,
                        "__SOURCE__": emp_id,
                        "__TARGET__": mentee_id,
                        "started": rng.randint(2020, 2025),
                    }
                )
                edge_id += 1

    return pd.DataFrame(edges)


# ---------------------------------------------------------------------------
# Script 6: Advanced Analytics — complex graph patterns
# ---------------------------------------------------------------------------


def analytics_network(n_nodes: int = 30, n_edges: int = 80) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate a rich network for advanced analytics demos.

    Returns:
        Tuple of (nodes_df, edges_df) with diverse attributes.
    """
    rng = _seed(50)
    clusters = ["research", "engineering", "management", "operations"]

    nodes = pd.DataFrame(
        {
            "__ID__": list(range(1, n_nodes + 1)),
            "name": [f"node_{i:03d}" for i in range(1, n_nodes + 1)],
            "cluster": [rng.choice(clusters) for _ in range(n_nodes)],
            "influence": [round(rng.uniform(0.1, 10.0), 2) for _ in range(n_nodes)],
            "active": [rng.choice([True, True, True, False]) for _ in range(n_nodes)],
        }
    )

    edges: list[dict] = []
    for i in range(1, n_edges + 1):
        src = rng.randint(1, n_nodes)
        tgt = rng.randint(1, n_nodes)
        while tgt == src:
            tgt = rng.randint(1, n_nodes)
        edges.append(
            {
                "__ID__": i,
                "__SOURCE__": src,
                "__TARGET__": tgt,
                "strength": round(rng.uniform(0.1, 1.0), 3),
                "type": rng.choice(["collaborates", "reports_to", "advises"]),
            }
        )

    return nodes, pd.DataFrame(edges)
