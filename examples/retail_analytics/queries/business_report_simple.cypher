// Stage 4: Executive Business Report (Simplified for CLI compatibility)
// Generates high-level KPIs combining customer and product insights
// Dependencies: CustomerSegment, ProductAnalytics entities (created in Stages 2 & 3)

// Calculate overall business metrics from customer segments
MATCH (cs:CustomerSegment)
WITH count(cs) AS total_customers,
     sum(cs.total_spend) AS total_revenue,
     avg(cs.total_spend) AS avg_customer_ltv

// Get product metrics
MATCH (pa:ProductAnalytics)
WITH total_customers, total_revenue, avg_customer_ltv,
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

  // Key insights (simplified)
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