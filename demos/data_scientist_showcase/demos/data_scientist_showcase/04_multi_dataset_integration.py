#!/usr/bin/env python3
"""Script 4: Multi-Dataset Integration — Cross-Source Data Fusion.

The hardest part of data science isn't the analysis — it's getting data from
multiple sources into a shape where you can ask questions across all of them.

This script shows how PyCypher makes cross-dataset integration natural. Load
employees, projects, departments, and mentorship data from separate sources,
then query across them all with Cypher — no complex JOIN logic, no ETL
pipelines, no schema alignment headaches.

Demonstrates:
  - Loading multiple datasets from different "sources"
  - Creating entity and relationship types from separate DataFrames
  - Cross-entity traversals (Employee → Project → Department)
  - Multi-hop relationship queries (mentor chains)
  - Combining organizational, project, and people data
  - Analytical queries across fused datasets

Run with:
    uv run python demos/data_scientist_showcase/04_multi_dataset_integration.py
"""

from __future__ import annotations

import random
import sys
from datetime import date, timedelta
from pathlib import Path

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import done, section, setup_demo, show_result, timed

import pandas as pd
from pycypher import ContextBuilder, Star


def main() -> None:
    setup_demo("Script 4: Multi-Dataset Integration — Cross-Source Data Fusion")

    # ------------------------------------------------------------------
    # 1. Multiple Data Sources
    # ------------------------------------------------------------------
    section("1. Multiple Data Sources — The Integration Challenge")

    print("Imagine these arrive from different systems:")
    print("  - HR system: employee records")
    print("  - PMO tool: project data")
    print("  - Finance: department budgets")
    print("  - HR again: mentorship pairings")
    print("  - PMO again: project assignments")
    print()
    print("In SQL, you'd need a star schema, foreign keys, and JOINs.")
    print("With PyCypher, you just load each source as an entity or relationship.\n")

    # Source 1: HR System — Employee records
    rng = random.Random(30)
    departments = ["Engineering", "Sales", "Marketing", "Finance", "Operations"]
    levels = ["Junior", "Mid", "Senior", "Lead", "Principal"]
    first_names = [
        "James", "Mary", "Robert", "Patricia", "John", "Jennifer",
        "Michael", "Linda", "David", "Elizabeth", "William", "Barbara",
        "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah",
        "Christopher", "Karen", "Daniel", "Lisa", "Matthew", "Nancy",
        "Anthony",
    ]

    employees = pd.DataFrame({
        "__ID__": list(range(1, 26)),
        "name": first_names[:25],
        "department": [rng.choice(departments) for _ in range(25)],
        "level": [rng.choice(levels) for _ in range(25)],
        "salary": [rng.randint(60, 200) * 1000 for _ in range(25)],
        "hire_date": [
            (date(2018, 1, 1) + timedelta(days=rng.randint(0, 2000))).isoformat()
            for _ in range(25)
        ],
    })

    # Source 2: PMO — Project records
    project_names = [
        "Atlas", "Beacon", "Catalyst", "Delta", "Eclipse",
        "Frontier", "Genesis", "Horizon", "Ion", "Jupiter",
    ]
    rng2 = random.Random(31)
    projects = pd.DataFrame({
        "__ID__": list(range(201, 211)),
        "name": project_names,
        "budget": [rng2.randint(50, 500) * 1000 for _ in range(10)],
        "status": [rng2.choice(["Active", "Planning", "Completed", "On Hold"]) for _ in range(10)],
        "priority": [rng2.choice(["High", "Medium", "Low"]) for _ in range(10)],
    })

    # Source 3: Finance — Department budgets
    dept_records = pd.DataFrame({
        "__ID__": list(range(301, 306)),
        "name": ["Engineering", "Sales", "Marketing", "Finance", "Operations"],
        "budget": [2000000, 1500000, 800000, 600000, 1200000],
        "headcount_target": [50, 30, 20, 15, 25],
    })

    # Source 4: PMO — Employee-to-project assignments
    rng3 = random.Random(32)
    emp_ids = list(range(1, 26))
    proj_ids = list(range(201, 211))
    assignment_rows = []
    edge_id = 1
    for emp_id in emp_ids:
        n_projects = rng3.randint(1, 3)
        assigned = rng3.sample(proj_ids, min(n_projects, len(proj_ids)))
        for proj_id in assigned:
            assignment_rows.append({
                "__ID__": edge_id,
                "__SOURCE__": emp_id,
                "__TARGET__": proj_id,
                "role": rng3.choice(["contributor", "lead", "reviewer"]),
                "hours_per_week": rng3.randint(5, 40),
            })
            edge_id += 1

    assignments = pd.DataFrame(assignment_rows)

    # Source 5: HR — Mentorship relationships
    rng4 = random.Random(33)
    mentor_rows = []
    mentor_id = 1
    for i, emp_id in enumerate(emp_ids):
        if i < len(emp_ids) // 2:
            n_mentees = rng4.randint(1, 2)
            mentee_pool = [x for x in emp_ids if x != emp_id]
            mentees = rng4.sample(mentee_pool, min(n_mentees, len(mentee_pool)))
            for mentee_id in mentees:
                mentor_rows.append({
                    "__ID__": mentor_id,
                    "__SOURCE__": emp_id,
                    "__TARGET__": mentee_id,
                    "started": rng4.randint(2020, 2025),
                })
                mentor_id += 1

    mentors = pd.DataFrame(mentor_rows)

    print(f"Employees:   {len(employees)} records (from HR)")
    print(f"Projects:    {len(projects)} records (from PMO)")
    print(f"Departments: {len(dept_records)} records (from Finance)")
    print(f"Assignments: {len(assignments)} relationships (from PMO)")
    print(f"Mentorships: {len(mentors)} relationships (from HR)")

    # ------------------------------------------------------------------
    # 2. Build a Unified Graph
    # ------------------------------------------------------------------
    section("2. Build a Unified Graph — One Line of Code")

    print("All five sources become one queryable graph:\n")

    with timed("Unified context build"):
        context = ContextBuilder.from_dict({
            # Entities (nodes)
            "Employee": employees,
            "Project": projects,
            "Department": dept_records,
            # Relationships (edges)
            "ASSIGNED_TO": assignments,
            "MENTORS": mentors,
        })
        star = Star(context=context)

    print("Five separate data sources → one graph. No schema alignment needed.\n")

    # ------------------------------------------------------------------
    # 3. Cross-Source Queries: Employee → Project
    # ------------------------------------------------------------------
    section("3. Cross-Source: Employee → Project Assignments")

    print("Query across HR data and PMO data in one Cypher statement.\n")

    with timed("Employee-project traversal"):
        result = star.execute_query("""
            MATCH (e:Employee)-[a:ASSIGNED_TO]->(p:Project)
            WHERE p.status = 'Active'
            RETURN e.name AS employee,
                   e.level AS level,
                   p.name AS project,
                   a.role AS project_role,
                   a.hours_per_week AS hours
            ORDER BY p.name, e.name
        """)
    show_result(result, label="Active project team composition (HR + PMO data fused)")

    # ------------------------------------------------------------------
    # 4. Project Leads
    # ------------------------------------------------------------------
    section("4. Finding Project Leads Across Datasets")

    print("Who leads each project? This joins PMO assignment data with HR.\n")

    with timed("Project leads"):
        result = star.execute_query("""
            MATCH (e:Employee)-[a:ASSIGNED_TO]->(p:Project)
            WHERE a.role = 'lead'
            RETURN p.name AS project,
                   p.status AS status,
                   p.priority AS priority,
                   e.name AS lead,
                   e.level AS level,
                   e.department AS dept
            ORDER BY p.name
        """)
    show_result(result, label="Project leads with their HR details")

    # ------------------------------------------------------------------
    # 5. Mentorship Network
    # ------------------------------------------------------------------
    section("5. Mentorship Network Traversal")

    print("Explore who mentors whom — from HR's mentorship data.\n")

    with timed("Mentor relationships"):
        result = star.execute_query("""
            MATCH (mentor:Employee)-[m:MENTORS]->(mentee:Employee)
            RETURN mentor.name AS mentor,
                   mentor.level AS mentor_level,
                   mentee.name AS mentee,
                   mentee.level AS mentee_level,
                   m.started AS since
            ORDER BY mentor.name, mentee.name
        """)
    show_result(result, label="Mentorship pairs with career levels")

    # ------------------------------------------------------------------
    # 6. Cross-Dataset: Mentors Who Also Lead Projects
    # ------------------------------------------------------------------
    section("6. Cross-Dataset Discovery: Mentors Who Lead Projects")

    print("Find people who both mentor others AND lead projects.")
    print("This requires joining across HR mentorship + PMO assignment data.\n")

    with timed("Mentor-leads cross query"):
        result = star.execute_query("""
            MATCH (e:Employee)-[:MENTORS]->(mentee:Employee)
            RETURN DISTINCT e.name AS mentor_name,
                   e.level AS level,
                   e.department AS dept
            ORDER BY e.name
        """)
    show_result(result, label="Active mentors (from HR)")

    with timed("Project leads"):
        result = star.execute_query("""
            MATCH (e:Employee)-[a:ASSIGNED_TO]->(p:Project)
            WHERE a.role = 'lead'
            RETURN DISTINCT e.name AS lead_name,
                   e.level AS level,
                   p.name AS project
            ORDER BY e.name
        """)
    show_result(result, label="Project leads (from PMO)")

    # ------------------------------------------------------------------
    # 7. Department-Level Analysis
    # ------------------------------------------------------------------
    section("7. Department-Level Insights")

    print("Analyze departments — combining Finance budget data with HR records.\n")

    with timed("Department overview"):
        result = star.execute_query("""
            MATCH (d:Department)
            RETURN d.name AS department,
                   d.budget AS annual_budget,
                   d.headcount_target AS target_headcount
            ORDER BY d.budget DESC
        """)
    show_result(result, label="Department budgets (from Finance)")

    with timed("Employee distribution"):
        result = star.execute_query("""
            MATCH (e:Employee)
            RETURN DISTINCT e.department AS department,
                   e.level AS level,
                   e.name AS employee
            ORDER BY e.department, e.level
        """)
    show_result(result, label="Employee distribution by department and level (from HR)")

    # ------------------------------------------------------------------
    # 8. High-Priority Project Teams
    # ------------------------------------------------------------------
    section("8. High-Priority Project Deep Dive")

    print("Zoom into high-priority projects — who's on them and at what capacity.\n")

    with timed("High-priority teams"):
        result = star.execute_query("""
            MATCH (e:Employee)-[a:ASSIGNED_TO]->(p:Project)
            WHERE p.priority = 'High'
            RETURN p.name AS project,
                   p.budget AS project_budget,
                   e.name AS team_member,
                   e.level AS level,
                   a.role AS role,
                   a.hours_per_week AS hours
            ORDER BY p.name, a.role, e.name
        """)
    show_result(result, label="High-priority project teams (full cross-source detail)")

    # ------------------------------------------------------------------
    # 9. Finding Overloaded Employees
    # ------------------------------------------------------------------
    section("9. Finding Overloaded Employees")

    print("Who is assigned to the most projects? Potential workload issues.\n")

    with timed("Multi-project employees"):
        result = star.execute_query("""
            MATCH (e:Employee)-[a:ASSIGNED_TO]->(p:Project)
            RETURN e.name AS employee,
                   e.level AS level,
                   e.department AS dept,
                   p.name AS project,
                   a.hours_per_week AS hours
            ORDER BY e.name, p.name
        """)
    show_result(result, label="All employee-project assignments (spot multi-project people)")

    # ------------------------------------------------------------------
    # 10. Key Takeaways
    # ------------------------------------------------------------------
    section("10. Key Takeaways")

    print("What we just demonstrated:")
    print()
    print("  1. FIVE SOURCES → ONE GRAPH — HR, PMO, Finance loaded together")
    print("  2. NO SCHEMA ALIGNMENT — each source keeps its own columns")
    print("  3. CROSS-SOURCE QUERIES — Employee → Project joins are natural")
    print("  4. RELATIONSHIP DISCOVERY — mentorship + project lead patterns")
    print("  5. DEPARTMENT ANALYSIS — Finance budgets + HR headcounts combined")
    print("  6. WORKLOAD INSIGHTS — multi-project assignment visibility")
    print()
    print("In traditional approaches, you'd need:")
    print("  - A data warehouse with star/snowflake schema")
    print("  - ETL pipelines to normalize and load each source")
    print("  - Complex SQL with 4-5 JOINs for cross-source queries")
    print("  - Separate queries for each analytical perspective")
    print()
    print("With PyCypher: load each source, name the relationships,")
    print("and query across everything naturally.")

    done()


if __name__ == "__main__":
    main()
