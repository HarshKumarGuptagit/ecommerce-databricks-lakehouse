CATALOG = "workspace"
BRONZE_SCHEMA = f"{CATALOG}.ecommerce_bronze"
SILVER_SCHEMA = f"{CATALOG}.ecommerce_silver"
GOLD_SCHEMA = f"{CATALOG}.ecommerce_gold"
OPS_SCHEMA = f"{CATALOG}.ecommerce_ops"

SOURCE_VOLUME_PATH = f"/Volumes/{CATALOG}/ecommerce_bronze/raw_files/"
AUDIT_TABLE = f"{OPS_SCHEMA}.pipeline_audit_log"

RAW_SCHEMAS = {
    "orders": [
        "order_id", "customer_id", "order_status", "order_purchase_timestamp",
        "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date",
    ],
    "order_items": [
        "order_id", "order_item_id", "product_id", "seller_id",
        "shipping_limit_date", "price", "freight_value",
    ],
    "customers": [
        "customer_id", "customer_unique_id", "customer_zip_code_prefix",
        "customer_city", "customer_state",
    ],
    "products": [
        "product_id", "product_category_name", "product_name_lenght",
        "product_description_lenght", "product_photos_qty", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm",
    ],
    "payments": [
        "order_id", "payment_sequential", "payment_type",
        "payment_installments", "payment_value",
    ],
    "sellers": [
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state",
    ],
    "reviews": [
        "review_id", "order_id", "review_score", "review_comment_title",
        "review_comment_message", "review_creation_date", "review_answer_timestamp",
    ],
}

DATASETS = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
}
