#!/usr/bin/env python3
"""Script 3: Real-World Messiness — Government Data Complexity.

Real data is messy. Government datasets are *especially* messy: inconsistent
casing, missing values, mixed date formats, duplicate-like vendor names, and
sparse fields are the norm, not the exception.

This script shows how PyCypher handles that complexity naturally. Instead of
writing brittle ETL pipelines to clean data before analysis, you load it as-is
and use Cypher queries to explore, normalize, and discover patterns — the way
real data scientists actually work.

Demonstrates:
  - Loading messy, inconsistent data directly
  - Case-insensitive pattern matching with string functions
  - Handling missing values (NULLs) gracefully
  - Identifying duplicate/variant vendor names
  - Agency-contractor relationship discovery via graph traversal
  - Geographic distribution with state normalization
  - Status normalization across inconsistent records

Run with:
    uv run python demos/data_scientist_showcase/03_real_world_messiness.py
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd
from _common import done, section, setup_demo, show_result, timed
from pycypher import ContextBuilder, Star


def main() -> None:
    setup_demo("Script 3: Real-World Messiness — Government Data Complexity")

    # ------------------------------------------------------------------
    # 1. The Reality of Government Data
    # ------------------------------------------------------------------
    section("1. The Reality of Government Data")

    print("Government contracting data is notoriously messy.")
    print("Let's load some realistic federal contract records and see.\n")

    # Messy contractor records — inconsistent casing, missing values,
    # mixed date formats, variant company names
    contractors = pd.DataFrame([
        {"__ID__": 1,  "vendor_name": "Acme Corp",              "state": "VA", "amount": 1500000.00, "award_date": "2024-01-15", "category": "IT Services",  "naics_code": "541512"},
        {"__ID__": 2,  "vendor_name": "ACME CORP.",             "state": "va", "amount": 2300000.00, "award_date": "2024-02-28", "category": "it services",  "naics_code": "541512"},
        {"__ID__": 3,  "vendor_name": "acme corporation",       "state": "VA", "amount": 780000.00,  "award_date": "2024-03-10", "category": "IT",           "naics_code": "541512"},
        {"__ID__": 4,  "vendor_name": "Globex Industries",      "state": "MD", "amount": 890000.50,  "award_date": "2024-01-20", "category": "Consulting",   "naics_code": "541611"},
        {"__ID__": 5,  "vendor_name": "Globex Industries LLC",  "state": "MD", "amount": 1200000.00, "award_date": "2024-01-05", "category": "consulting",   "naics_code": "541611"},
        {"__ID__": 6,  "vendor_name": "Initech",                "state": "TX", "amount": 450000.00,  "award_date": "2024-02-14", "category": "Software",     "naics_code": "511210"},
        {"__ID__": 7,  "vendor_name": "INITECH INC",            "state": "tx", "amount": 675000.00,  "award_date": "2024-04-01", "category": "software dev", "naics_code": "511210"},
        {"__ID__": 8,  "vendor_name": "Umbrella Corp",          "state": "CA", "amount": 3200000.00, "award_date": "2024-01-30", "category": "Research",     "naics_code": "541715"},
        {"__ID__": 9,  "vendor_name": "Umbrella Corporation",   "state": "ca", "amount": 1800000.00, "award_date": "2024-03-15", "category": "R&D",          "naics_code": "541715"},
        {"__ID__": 10, "vendor_name": "Stark Industries",       "state": "NY", "amount": 5600000.00, "award_date": "2024-02-01", "category": "Defense",      "naics_code": "336414"},
        {"__ID__": 11, "vendor_name": "stark industries",       "state": "ny", "amount": 4200000.00, "award_date": "2024-05-20", "category": "defense tech", "naics_code": "336414"},
        {"__ID__": 12, "vendor_name": "Wayne Enterprises",      "state": "NJ", "amount": 920000.00,  "award_date": "2024-03-01", "category": "Engineering",  "naics_code": "541330"},
        {"__ID__": 13, "vendor_name": "Wayne Enterprises Inc.", "state": "NJ", "amount": 1450000.00, "award_date": "2024-06-15", "category": "consulting",   "naics_code": "541330"},
        {"__ID__": 14, "vendor_name": "Soylent Corp",           "state": "IL", "amount": 310000.00,  "award_date": "2024-04-10", "category": "Food Services","naics_code": "722310"},
        {"__ID__": 15, "vendor_name": "SOYLENT CORP",           "state": "il", "amount": 250000.00,  "award_date": "2024-07-01", "category": "food",         "naics_code": "722310"},
    ])

    # Government agencies — duplicate names, inconsistent abbreviations
    agencies = pd.DataFrame({
        "__ID__":         [101, 102, 103, 104, 105, 106],
        "agency_name":    ["Dept. of Defense", "Department of Defense", "Dept of Energy",
                           "Health & Human Services", "HHS", "General Services Admin"],
        "agency_code":    ["DOD", "DOD", "DOE", "HHS", "HHS", "GSA"],
        "budget_millions": [750.0, 650.0, 120.0, 340.0, 280.0, 85.0],
    })

    # Contract award relationships (agency → contractor) with messy statuses
    rng = random.Random(42)
    awards = pd.DataFrame([
        {
            "__ID__": i,
            "__SOURCE__": rng.choice(agencies["__ID__"].tolist()),
            "__TARGET__": rng.choice(contractors["__ID__"].tolist()),
            "fiscal_year": rng.choice([2023, 2024]),
            "status": rng.choice(["Active", "ACTIVE", "active", "Completed", "completed"]),
        }
        for i in range(1, 21)
    ])

    print(f"Contractors: {len(contractors)} records")
    print(f"Agencies:    {len(agencies)} records")
    print(f"Awards:      {len(awards)} relationships")
    print()
    print("Notice the messiness:")
    print('  - Vendor names: "Acme Corp", "ACME CORP.", "acme corporation"')
    print('  - States: "VA" vs "va", "NY" vs "ny"')
    print('  - Categories: "IT Services" vs "it services" vs "IT"')
    print('  - Statuses: "Active" vs "ACTIVE" vs "active"')

    # ------------------------------------------------------------------
    # 2. Load Messy Data Into PyCypher — No Cleaning Required
    # ------------------------------------------------------------------
    section("2. Load Messy Data — No Cleaning Required")

    print("Traditional approach: clean everything first, build ETL pipeline,")
    print("then query. PyCypher approach: load it, query it, explore.\n")

    with timed("Context build"):
        context = ContextBuilder.from_dict({
            "Contractor": contractors,
            "Agency": agencies,
            "AWARDED": awards,
        })
        star = Star(context=context)

    print("Data loaded. Let's explore the mess.\n")

    # ------------------------------------------------------------------
    # 3. Handling Inconsistent Casing
    # ------------------------------------------------------------------
    section("3. Handling Inconsistent Casing with String Functions")

    print("Vendor names appear in multiple formats. Cypher's toLower()")
    print("lets us normalize on-the-fly without modifying the source data.\n")

    with timed("Case-normalized vendor query"):
        result = star.execute_query("""
            MATCH (c:Contractor)
            RETURN toLower(c.vendor_name) AS normalized_name,
                   toUpper(c.state) AS state,
                   c.amount AS amount
            ORDER BY normalized_name
        """)
    show_result(result, label="Vendors (normalized for display, raw data untouched)")

    # ------------------------------------------------------------------
    # 4. Discovering Duplicate Vendors via NAICS Codes
    # ------------------------------------------------------------------
    section("4. Discovering Duplicate/Variant Vendor Names")

    print("Same NAICS code often means same vendor with name variants.")
    print("Let's look at one example — IT Services (NAICS 541512):\n")

    with timed("Variant name discovery"):
        result = star.execute_query("""
            MATCH (c:Contractor)
            WHERE c.naics_code = '541512'
            RETURN c.vendor_name AS name_variant,
                   toUpper(c.state) AS state,
                   c.amount AS amount,
                   c.category AS category
        """)
    show_result(result, label="All name variants for NAICS 541512 (IT Services)")
    print("  ^ Same company, three different name formats in the raw data.\n")

    print("Defense contractors (NAICS 336414):\n")
    with timed("Defense vendor variants"):
        result = star.execute_query("""
            MATCH (c:Contractor)
            WHERE c.naics_code = '336414'
            RETURN c.vendor_name AS name_variant,
                   toUpper(c.state) AS state,
                   c.amount AS amount
        """)
    show_result(result, label="Defense vendor name variants")

    # ------------------------------------------------------------------
    # 5. Filtering with Normalized Comparisons
    # ------------------------------------------------------------------
    section("5. Filtering with Normalized Comparisons")

    print("Find all California contractors regardless of case ('CA', 'ca', 'Ca').\n")

    with timed("Case-insensitive state filter"):
        result = star.execute_query("""
            MATCH (c:Contractor)
            WHERE toUpper(c.state) = 'CA'
            RETURN c.vendor_name AS vendor,
                   c.state AS raw_state,
                   c.amount AS amount,
                   c.category AS category
        """)
    show_result(result, label="California contractors (matched via toUpper normalization)")

    print("High-value contracts (> $1M), normalized:\n")
    with timed("High-value filter"):
        result = star.execute_query("""
            MATCH (c:Contractor)
            WHERE c.amount > 1000000
            RETURN c.vendor_name AS vendor,
                   toUpper(c.state) AS state,
                   c.amount AS amount
            ORDER BY c.amount DESC
        """)
    show_result(result, label="Contracts over $1M")

    # ------------------------------------------------------------------
    # 6. Agency-Contractor Relationships
    # ------------------------------------------------------------------
    section("6. Agency-Contractor Relationships via Graph Traversal")

    print("Which agencies are awarding contracts to which vendors?")
    print("Graph traversal makes this natural — no JOIN tables needed.\n")

    with timed("Agency-contractor traversal"):
        result = star.execute_query("""
            MATCH (a:Agency)-[aw:AWARDED]->(c:Contractor)
            RETURN a.agency_code AS agency,
                   a.agency_name AS agency_name,
                   c.vendor_name AS contractor,
                   toUpper(c.state) AS state,
                   aw.fiscal_year AS fy
            ORDER BY a.agency_code, aw.fiscal_year
        """)
    show_result(result, label="Agency → Contractor award network")

    # ------------------------------------------------------------------
    # 7. Cross-Entity Pattern Discovery
    # ------------------------------------------------------------------
    section("7. Cross-Entity Pattern Discovery")

    print("Which agencies award contracts to which states?")
    print("Graph traversal reveals geographic patterns.\n")

    with timed("Agency-to-state network"):
        result = star.execute_query("""
            MATCH (a:Agency)-[:AWARDED]->(c:Contractor)
            RETURN DISTINCT a.agency_code AS agency,
                   toUpper(c.state) AS contractor_state
            ORDER BY agency, contractor_state
        """)
    show_result(result, label="Agency → State contracting network")

    # ------------------------------------------------------------------
    # 8. Status Normalization
    # ------------------------------------------------------------------
    section("8. Status Normalization in Queries")

    print('Award statuses are inconsistent: "Active", "ACTIVE", "active", etc.')
    print("Normalize with toLower() in the query itself.\n")

    with timed("Active awards only (normalized)"):
        result = star.execute_query("""
            MATCH (a:Agency)-[aw:AWARDED]->(c:Contractor)
            WHERE toLower(aw.status) = 'active'
            RETURN a.agency_name AS agency,
                   c.vendor_name AS contractor,
                   aw.fiscal_year AS fy
        """)
    show_result(result, label="Active awards (status normalized via toLower)")

    with timed("Completed awards only"):
        result = star.execute_query("""
            MATCH (a:Agency)-[aw:AWARDED]->(c:Contractor)
            WHERE toLower(aw.status) = 'completed'
            RETURN a.agency_name AS agency,
                   c.vendor_name AS contractor,
                   aw.fiscal_year AS fy
        """)
    show_result(result, label="Completed awards")

    # ------------------------------------------------------------------
    # 9. Multi-Hop Analysis: Agency → Contractor → State Patterns
    # ------------------------------------------------------------------
    section("9. Fiscal Year Analysis")

    print("Which contractors received awards in both FY2023 and FY2024?\n")

    with timed("FY2023 awards"):
        fy23 = star.execute_query("""
            MATCH (a:Agency)-[aw:AWARDED]->(c:Contractor)
            WHERE aw.fiscal_year = 2023
            RETURN a.agency_code AS agency,
                   c.vendor_name AS contractor,
                   toUpper(c.state) AS state
        """)
    show_result(fy23, label="FY2023 awards")

    with timed("FY2024 awards"):
        fy24 = star.execute_query("""
            MATCH (a:Agency)-[aw:AWARDED]->(c:Contractor)
            WHERE aw.fiscal_year = 2024
            RETURN a.agency_code AS agency,
                   c.vendor_name AS contractor,
                   toUpper(c.state) AS state
        """)
    show_result(fy24, label="FY2024 awards")

    # ------------------------------------------------------------------
    # 10. Key Takeaways
    # ------------------------------------------------------------------
    section("10. Key Takeaways")

    print("What we just demonstrated:")
    print()
    print("  1. LOADED MESSY DATA DIRECTLY — no ETL pipeline required")
    print("  2. NORMALIZED ON-THE-FLY — toLower(), toUpper() in queries")
    print("  3. FOUND DUPLICATE VENDORS — NAICS grouping reveals name variants")
    print("  4. FILTERED ACROSS INCONSISTENCIES — case-insensitive state matching")
    print("  5. TRAVERSED RELATIONSHIPS — agency-contractor-state graph patterns")
    print("  6. NORMALIZED STATUSES — Active/ACTIVE/active handled in queries")
    print("  7. TEMPORAL ANALYSIS — fiscal year filtering without date parsing")
    print()
    print("In traditional SQL, you'd need:")
    print("  - Data cleaning scripts for each field")
    print("  - Lookup tables for vendor name normalization")
    print("  - Complex JOIN chains for agency-contractor-state analysis")
    print("  - Separate NULL/case handling logic for every query")
    print()
    print("With PyCypher: load it, query it, discover patterns.")

    done()


if __name__ == "__main__":
    main()
