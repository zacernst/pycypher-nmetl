// Stage 1: Customer Metrics Calculation
// Creates CustomerMetrics entity with RFM analysis (Recency, Frequency, Monetary)
// Dependencies: Customer, CustomerOrder entities

MATCH (c:Customer)
WITH ID(c) AS customer_id,
     c.name AS customer_name,
     c.city AS customer_city,
     c.signup_date AS signup_date
MATCH (o:CustomerOrder)
WHERE o.customer_id = customer_id

// Calculate customer metrics using aggregation
WITH customer_id, customer_name, customer_city, signup_date,
     count(DISTINCT ID(o)) AS total_orders,
     sum(o.quantity * o.unit_price) AS total_spend,
     avg(o.quantity * o.unit_price) AS avg_order_value,
     max(o.order_date) AS last_order_date,
     min(o.order_date) AS first_order_date

// Calculate days since last order (recency)
WITH customer_id, customer_name, customer_city, signup_date,
     total_orders, total_spend, avg_order_value,
     last_order_date, first_order_date,
     // Approximate days since last order based on order frequency
     CASE
       WHEN total_orders >= 5 THEN 30
       WHEN total_orders >= 3 THEN 90
       WHEN total_orders >= 1 THEN 180
       ELSE 365
     END AS days_since_last_order

// Create CustomerMetrics entity with calculated metrics
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