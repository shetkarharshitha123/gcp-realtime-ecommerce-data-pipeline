
SELECT  
    product_id, 
    total_items_sold, 
    total_orders, 
    aggregated_at 
FROM `snappy-mapper-498509-e0.ecom_analytics.daily_product_summary` 
ORDER BY total_items_sold DESC;

 
