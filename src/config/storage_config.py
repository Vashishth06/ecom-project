"""
Storage paths and container configurations
"""

# Storage account
STORAGE_ACCOUNT = "ecomdata2026"

# Container paths
BRONZE_CONTAINER = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_CONTAINER = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_CONTAINER = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows"

# Table paths
BRONZE_ORDER_ITEMS = f"{BRONZE_CONTAINER}/order_items_large_dataset"
SILVER_ORDER_ITEMS = f"{SILVER_CONTAINER}/order_items"
GOLD_ORDER_METRICS = f"{GOLD_CONTAINER}/order_metrics"

# Checkpoint location
CHECKPOINT_PATH = f"{BRONZE_CONTAINER}/_checkpoints"