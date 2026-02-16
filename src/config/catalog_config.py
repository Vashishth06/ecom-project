"""
Unity Catalog configuration
"""

CATALOG_NAME = "ecommerce"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Full table names
BRONZE_ORDER_ITEMS_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.order_items"
SILVER_ORDER_ITEMS_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA}.order_items"
GOLD_ORDER_ITEMS_TABLE = f"{CATALOG_NAME}.{GOLD_SCHEMA}.order_items"