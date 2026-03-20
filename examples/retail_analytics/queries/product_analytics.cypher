// Stage 3: Product Analytics
// Analyzes product performance and category trends
// Dependencies: Product, CustomerOrder entities

MATCH (p:Product)
WITH ID(p) AS product_id,
     p.name AS product_name,
     p.category AS category,
     p.price AS list_price,
     p.cost AS product_cost
MATCH (o:CustomerOrder)
WHERE o.product_id = product_id

// Calculate product performance metrics
WITH product_id, product_name, category, list_price, product_cost,
     count(ID(o)) AS order_count,
     sum(o.quantity) AS units_sold,
     sum(o.quantity * o.unit_price) AS revenue,
     avg(o.unit_price) AS avg_selling_price,
     max(o.order_date) AS last_sold_date

// Calculate profit metrics and performance indicators
WITH product_id, product_name, category, list_price, product_cost,
     order_count, units_sold, revenue, avg_selling_price, last_sold_date,

     // Calculate profit margin
     revenue - (units_sold * product_cost) AS gross_profit,

     // Calculate performance score
     CASE
       WHEN units_sold >= 10 AND revenue >= 1000 THEN 'High'
       WHEN units_sold >= 5 AND revenue >= 500 THEN 'Medium'
       ELSE 'Low'
     END AS performance_tier,

     // Days since last sale (approximated by unit sales)
     CASE
       WHEN units_sold >= 10 THEN 30
       WHEN units_sold >= 5 THEN 90
       WHEN units_sold >= 1 THEN 180
       ELSE 365
     END AS days_since_last_sale

// Create ProductAnalytics entity
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

// Return category-level analytics
RETURN pa.category AS category,
       count(pa) AS products_in_category,
       sum(pa.revenue) AS category_revenue,
       sum(pa.gross_profit) AS category_profit,
       avg(pa.profit_margin) AS avg_profit_margin,
       sum(pa.units_sold) AS total_units_sold
ORDER BY category_revenue DESC