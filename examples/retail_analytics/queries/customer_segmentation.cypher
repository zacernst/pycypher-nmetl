// Stage 2: Customer Segmentation
// Classifies customers into value-based segments using RFM analysis
// Dependencies: CustomerMetrics entity (created in Stage 1)

MATCH (cm:CustomerMetrics)

// RFM-based segmentation logic
WITH cm.customer_id AS customer_id,
     cm.name AS customer_name,
     cm.total_spend AS total_spend,
     cm.total_orders AS total_orders,
     cm.days_since_last_order AS days_since_last_order,

     // Determine segment based on spend and frequency criteria
     CASE
       // VIP: High spend and frequent orders
       WHEN cm.total_spend >= 1000 AND cm.total_orders >= 3
         THEN 'VIP'

       // Regular: Moderate spend and activity
       WHEN cm.total_spend >= 300 AND cm.total_orders >= 2
         THEN 'Regular'

       // At-Risk: Previous good customers with high spend
       WHEN cm.total_spend >= 500
         THEN 'At-Risk'

       // New: Low order count (potential for growth)
       WHEN cm.total_orders <= 2
         THEN 'New'

       // Inactive: Low engagement overall
       ELSE 'Inactive'
     END AS segment,

     // Calculate segment priority score for ranking
     CASE
       WHEN cm.total_spend >= 1000 AND cm.total_orders >= 3 THEN 5
       WHEN cm.total_spend >= 300 AND cm.total_orders >= 2 THEN 4
       WHEN cm.total_spend >= 500 THEN 3
       WHEN cm.total_orders <= 2 THEN 2
       ELSE 1
     END AS segment_priority

// Create CustomerSegment entity
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