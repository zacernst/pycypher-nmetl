#!/usr/bin/env python3
"""Retail Analytics ETL Pipeline - Programmatic Execution

Demonstrates a complete multi-stage ETL pipeline with query composition
and dependencies using PyCypher's programmatic API.

This script shows the same pipeline as pipeline.yaml but executed
programmatically, giving you full control over the execution flow
and intermediate results.

Usage:
    uv run python examples/retail_analytics/run_pipeline.py
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.star import Star


def setup_logging() -> None:
    """Configure logging for pipeline execution."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def load_pipeline_context() -> tuple[ContextBuilder, Star]:
    """Load CSV data into PyCypher context."""
    base_path = Path("examples/retail_analytics/data")

    print("🔄 Loading data sources...")

    context = (
        ContextBuilder()
        .add_entity("Customer", str(base_path / "customers.csv"), id_col="customer_id")
        .add_entity("Product", str(base_path / "products.csv"), id_col="product_id")
        .add_entity("CustomerOrder", str(base_path / "orders.csv"), id_col="order_id")
        .build()
    )

    star = Star(context=context)

    # Quick test to verify data is loaded
    test_customers = star.execute_query(
        "MATCH (c:Customer) RETURN count(c) AS customer_count",
    )
    test_orders = star.execute_query(
        "MATCH (o:CustomerOrder) RETURN count(o) AS order_count",
    )

    # Test join condition
    join_test = star.execute_query("""
        MATCH (c:Customer), (o:CustomerOrder)
        WHERE c.customer_id = o.customer_id
        RETURN count(*) AS join_count
    """)

    # Check what properties are actually available
    customer_props = star.execute_query("MATCH (c:Customer) RETURN c LIMIT 1")
    order_props = star.execute_query("MATCH (o:CustomerOrder) RETURN o LIMIT 1")

    print("✅ Data sources loaded successfully")
    print(f"   📊 Customers: {test_customers.iloc[0]['customer_count']}")
    print(f"   📊 Orders: {test_orders.iloc[0]['order_count']}")
    print(f"   🔗 Join matches: {join_test.iloc[0]['join_count']}")
    print(f"   🔍 Customer properties: {list(customer_props.columns)}")
    print(f"   🔍 Order properties: {list(order_props.columns)}")

    return context, star


def execute_stage_1_customer_metrics(star: Star) -> pd.DataFrame:
    """Stage 1: Calculate customer RFM metrics."""
    print("\n🔄 Stage 1: Calculating customer metrics...")

    query = """
    MATCH (c:Customer)
    WITH ID(c) AS customer_id,
         c.name AS customer_name,
         c.city AS customer_city,
         c.signup_date AS signup_date
    MATCH (o:CustomerOrder)
    WHERE o.customer_id = customer_id

    WITH customer_id, customer_name, customer_city, signup_date,
         count(DISTINCT ID(o)) AS total_orders,
         sum(o.quantity * o.unit_price) AS total_spend,
         avg(o.quantity * o.unit_price) AS avg_order_value,
         max(o.order_date) AS last_order_date,
         min(o.order_date) AS first_order_date

    WITH customer_id, customer_name, customer_city, signup_date,
         total_orders, total_spend, avg_order_value,
         last_order_date, first_order_date,
         CASE
           WHEN total_orders >= 5 THEN 30
           WHEN total_orders >= 3 THEN 90
           WHEN total_orders >= 1 THEN 180
           ELSE 365
         END AS days_since_last_order

    CREATE (cm:CustomerMetrics {
      customer_id: customer_id,
      name: customer_name,
      city: customer_city,
      signup_date: signup_date,
      total_orders: total_orders,
      total_spend: total_spend,
      avg_order_value: avg_order_value,
      days_since_last_order: days_since_last_order,
      first_order_date: first_order_date,
      last_order_date: last_order_date
    })

    RETURN cm.customer_id AS customer_id,
           cm.name AS name,
           cm.total_spend AS total_spend,
           cm.total_orders AS total_orders,
           cm.days_since_last_order AS days_since_last_order
    ORDER BY cm.total_spend DESC
    """

    result = star.execute_query(query)
    print(f"✅ Created CustomerMetrics for {len(result)} customers")
    if len(result) > 0:
        print(
            f"📊 Top customer: {result.iloc[0]['name']} (${result.iloc[0]['total_spend']:.2f})",
        )
    else:
        print("⚠️ No customers found - check data join conditions")

    return result


def execute_stage_2_customer_segmentation(star: Star) -> pd.DataFrame:
    """Stage 2: Classify customers into segments."""
    print("\n🔄 Stage 2: Segmenting customers...")

    query = """
    MATCH (cm:CustomerMetrics)

    WITH cm.customer_id AS customer_id,
         cm.name AS customer_name,
         cm.total_spend AS total_spend,
         cm.total_orders AS total_orders,
         cm.days_since_last_order AS days_since_last_order,

         CASE
           WHEN cm.total_spend >= 1000 AND cm.total_orders >= 3
             THEN 'VIP'
           WHEN cm.total_spend >= 300 AND cm.total_orders >= 2
             THEN 'Regular'
           WHEN cm.total_spend >= 500
             THEN 'At-Risk'
           WHEN cm.total_orders <= 2
             THEN 'New'
           ELSE 'Inactive'
         END AS segment,

         CASE
           WHEN cm.total_spend >= 1000 AND cm.total_orders >= 3 THEN 5
           WHEN cm.total_spend >= 300 AND cm.total_orders >= 2 THEN 4
           WHEN cm.total_spend >= 500 THEN 3
           WHEN cm.total_orders <= 2 THEN 2
           ELSE 1
         END AS segment_priority

    CREATE (cs:CustomerSegment {
      customer_id: customer_id,
      name: customer_name,
      segment: segment,
      segment_priority: segment_priority,
      total_spend: total_spend,
      total_orders: total_orders,
      days_since_last_order: days_since_last_order
    })

    RETURN cs.segment AS segment,
           count(cs) AS customer_count,
           avg(cs.total_spend) AS avg_spend_per_segment,
           sum(cs.total_spend) AS total_segment_revenue
    ORDER BY cs.segment_priority DESC, customer_count DESC
    """

    result = star.execute_query(query)
    print(f"✅ Classified customers into {len(result)} segments:")
    for _, row in result.iterrows():
        print(
            f"   📋 {row['segment']}: {row['customer_count']} customers (${row['total_segment_revenue']:.2f} revenue)",
        )

    return result


def execute_stage_3_product_analytics(star: Star) -> pd.DataFrame:
    """Stage 3: Analyze product performance."""
    print("\n🔄 Stage 3: Analyzing product performance...")

    query = """
    MATCH (p:Product)
    WITH ID(p) AS product_id,
         p.name AS product_name,
         p.category AS category,
         p.price AS list_price,
         p.cost AS product_cost
    MATCH (o:CustomerOrder)
    WHERE o.product_id = product_id

    WITH product_id, product_name, category, list_price, product_cost,
         count(ID(o)) AS order_count,
         sum(o.quantity) AS units_sold,
         sum(o.quantity * o.unit_price) AS revenue,
         avg(o.unit_price) AS avg_selling_price,
         max(o.order_date) AS last_sold_date

    WITH product_id, product_name, category, list_price, product_cost,
         order_count, units_sold, revenue, avg_selling_price, last_sold_date,
         revenue - (units_sold * product_cost) AS gross_profit,
         CASE
           WHEN units_sold >= 10 AND revenue >= 1000 THEN 'High'
           WHEN units_sold >= 5 AND revenue >= 500 THEN 'Medium'
           ELSE 'Low'
         END AS performance_tier,
         CASE
           WHEN units_sold >= 10 THEN 30
           WHEN units_sold >= 5 THEN 90
           WHEN units_sold >= 1 THEN 180
           ELSE 365
         END AS days_since_last_sale

    CREATE (pa:ProductAnalytics {
      product_id: product_id,
      name: product_name,
      category: category,
      list_price: list_price,
      units_sold: units_sold,
      revenue: revenue,
      gross_profit: gross_profit,
      order_count: order_count,
      avg_selling_price: avg_selling_price,
      performance_tier: performance_tier,
      days_since_last_sale: days_since_last_sale,
      profit_margin: round((gross_profit / revenue) * 100, 2)
    })

    RETURN pa.category AS category,
           count(pa) AS products_in_category,
           sum(pa.revenue) AS category_revenue,
           sum(pa.gross_profit) AS category_profit,
           avg(pa.profit_margin) AS avg_profit_margin,
           sum(pa.units_sold) AS total_units_sold
    ORDER BY category_revenue DESC
    """

    result = star.execute_query(query)
    print(
        f"✅ Analyzed {sum(result['products_in_category'])} products across {len(result)} categories:",
    )
    for _, row in result.iterrows():
        print(
            f"   📦 {row['category']}: ${row['category_revenue']:.2f} revenue ({row['avg_profit_margin']:.1f}% margin)",
        )

    return result


def execute_stage_4_business_report(star: Star) -> pd.DataFrame:
    """Stage 4: Generate executive business report."""
    print("\n🔄 Stage 4: Generating executive report...")

    query = """
    MATCH (cs:CustomerSegment)
    WITH count(cs) AS total_customers,
         sum(cs.total_spend) AS total_revenue,
         avg(cs.total_spend) AS avg_customer_ltv

    MATCH (pa:ProductAnalytics)
    WITH total_customers, total_revenue, avg_customer_ltv,
         count(pa) AS total_products,
         sum(pa.gross_profit) AS total_profit,
         sum(pa.units_sold) AS total_units_sold

    CREATE (br:BusinessReport {
      report_date: '2024-12-01',
      total_customers: total_customers,
      total_revenue: total_revenue,
      total_profit: total_profit,
      total_products: total_products,
      total_units_sold: total_units_sold,
      avg_customer_ltv: avg_customer_ltv,
      profit_margin: round((total_profit / total_revenue) * 100, 2),
      top_segment: 'VIP',
      top_category: 'Electronics',
      focus_area: 'At-Risk customer retention',
      growth_opportunity: 'Sports category expansion'
    })

    RETURN br.report_date AS report_date,
           br.total_customers AS total_customers,
           br.total_revenue AS total_revenue,
           br.total_profit AS total_profit,
           br.profit_margin AS profit_margin_percent,
           br.avg_customer_ltv AS avg_customer_lifetime_value,
           br.focus_area AS strategic_focus,
           br.growth_opportunity AS growth_recommendation
    """

    result = star.execute_query(query)
    print("✅ Generated executive report:")
    row = result.iloc[0]
    print(f"   💰 Total Revenue: ${row['total_revenue']:,.2f}")
    print(
        f"   📈 Total Profit: ${row['total_profit']:,.2f} ({row['profit_margin_percent']}% margin)",
    )
    print(f"   👥 Customer LTV: ${row['avg_customer_lifetime_value']:,.2f}")
    print(f"   🎯 Strategic Focus: {row['strategic_focus']}")
    print(f"   🚀 Growth Opportunity: {row['growth_recommendation']}")

    return result


def save_results(result: pd.DataFrame, filename: str) -> None:
    """Save query results to CSV file."""
    output_dir = Path("examples/retail_analytics/output")
    output_dir.mkdir(exist_ok=True)

    filepath = output_dir / filename
    result.to_csv(filepath, index=False)
    print(f"💾 Saved results to {filepath}")


def main() -> None:
    """Execute the complete retail analytics ETL pipeline."""
    print("🚀 Starting Retail Analytics ETL Pipeline")
    print("=" * 50)

    setup_logging()

    # Load data sources
    context, star = load_pipeline_context()

    # Execute pipeline stages in dependency order
    try:
        # Stage 1: Customer Metrics (depends on raw data)
        metrics_result = execute_stage_1_customer_metrics(star)
        save_results(metrics_result, "customer_metrics.csv")

        # Stage 2: Customer Segmentation (depends on Stage 1)
        segments_result = execute_stage_2_customer_segmentation(star)
        save_results(segments_result, "customer_segments.csv")

        # Stage 3: Product Analytics (independent, depends on raw data)
        products_result = execute_stage_3_product_analytics(star)
        save_results(products_result, "product_performance.csv")

        # Stage 4: Business Report (depends on Stages 2 & 3)
        report_result = execute_stage_4_business_report(star)
        save_results(report_result, "executive_report.csv")

        print("\n🎉 Pipeline completed successfully!")
        print("📁 Results saved to examples/retail_analytics/output/")

    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()
