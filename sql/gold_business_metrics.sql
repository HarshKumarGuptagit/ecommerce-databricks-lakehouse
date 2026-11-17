-- 1. Executive KPI Summary
SELECT
    COUNT(*) AS total_orders,
    SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_orders,
    ROUND(100.0 * SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate_pct,
    ROUND(SUM(total_item_price), 2) AS total_item_revenue,
    ROUND(AVG(CASE WHEN order_status = 'delivered' THEN total_order_value END), 2) AS avg_order_value,
    ROUND(100.0 * SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END), 0), 2) AS late_delivery_pct
FROM workspace.ecommerce_gold.fact_orders;

-- 2. Monthly Revenue Trend
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(f.order_id) AS orders_count,
    ROUND(SUM(f.total_item_price), 2) AS monthly_revenue
FROM workspace.ecommerce_gold.fact_orders f
JOIN workspace.ecommerce_gold.dim_date d
    ON f.order_date_key = d.date_key
WHERE f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- 3. Top 10 Product Categories by Revenue
SELECT
    p.product_category_name,
    COUNT(DISTINCT f.order_id) AS orders_count,
    ROUND(SUM(f.price), 2) AS category_revenue
FROM workspace.ecommerce_gold.fact_order_items f
JOIN workspace.ecommerce_gold.dim_products p
    ON f.product_sk = p.product_sk
GROUP BY p.product_category_name
ORDER BY category_revenue DESC
LIMIT 10;

-- 4. Payment Method Distribution
SELECT
    payment_type,
    COUNT(*) AS payment_count,
    ROUND(SUM(payment_value), 2) AS total_paid,
    ROUND(AVG(payment_installments), 1) AS avg_installments
FROM workspace.ecommerce_gold.fact_payments
GROUP BY payment_type
ORDER BY total_paid DESC;