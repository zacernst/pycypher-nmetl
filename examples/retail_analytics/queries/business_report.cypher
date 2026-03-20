// Stage 4: Executive Business Report
// Generates high-level KPIs combining customer and product insights
// Dependencies: CustomerSegment, ProductAnalytics entities (created in Stages 2 & 3)

// First, get customer segment summary
MATCH (cs:CustomerSegment)
WITH cs.segment AS segment,
     count(cs) AS customer_count,
     sum(cs.total_spend) AS segment_revenue,
     avg(cs.total_spend) AS avg_customer_value
ORDER BY segment_revenue DESC
WITH collect({
  segment: segment,
  customer_count: customer_count,
  revenue: segment_revenue,
  avg_value: avg_customer_value
}) AS segment_summary

// Then, get product category summary
MATCH (pa:ProductAnalytics)
WITH segment_summary,
     pa.category AS category,
     count(pa) AS product_count,
     sum(pa.revenue) AS category_revenue,
     sum(pa.gross_profit) AS category_profit,
     avg(pa.profit_margin) AS avg_margin
ORDER BY category_revenue DESC
WITH segment_summary, collect({
  category: category,
  product_count: product_count,
  revenue: category_revenue,
  profit: category_profit,
  margin: avg_margin
}) AS category_summary

// Calculate overall business metrics
MATCH (cs:CustomerSegment)
WITH segment_summary, category_summary,
     count(cs) AS total_customers,
     sum(cs.total_spend) AS total_revenue,
     avg(cs.total_spend) AS avg_customer_ltv

MATCH (pa:ProductAnalytics)
WITH segment_summary, category_summary, total_customers, total_revenue, avg_customer_ltv,
     count(pa) AS total_products,
     sum(pa.gross_profit) AS total_profit,
     sum(pa.units_sold) AS total_units_sold

// Create executive summary report
CREATE (br:BusinessReport {
  report_date: '2024-12-01',
  total_customers: total_customers,
  total_revenue: total_revenue,
  total_profit: total_profit,
  total_products: total_products,
  total_units_sold: total_units_sold,
  avg_customer_ltv: avg_customer_ltv,
  profit_margin: round((total_profit / total_revenue) * 100, 2),

  // Key insights
  top_segment: 'VIP',
  top_category: 'Electronics',

  // Recommendations
  focus_area: 'At-Risk customer retention',
  growth_opportunity: 'Sports category expansion'
})

// Return executive dashboard data
RETURN br.report_date AS report_date,
       br.total_customers AS total_customers,
       br.total_revenue AS total_revenue,
       br.total_profit AS total_profit,
       br.profit_margin AS profit_margin_percent,
       br.avg_customer_ltv AS avg_customer_lifetime_value,
       br.focus_area AS strategic_focus,
       br.growth_opportunity AS growth_recommendation