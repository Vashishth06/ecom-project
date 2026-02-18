"""
Central table registry - Add new tables here ONLY
All paths and table names auto-generated
"""

import os

# ============================================================
# PROJECT PATHS
# ============================================================
# Users should update this to their Azure account email
WORKSPACE_USER = "<email used to register Azure account>"
PROJECT_NAME = "ecom-project"

# Auto-constructed paths
WORKSPACE_BASE = f"/Workspace/Users/{WORKSPACE_USER}"
PROJECT_PATH = f"{WORKSPACE_BASE}/{PROJECT_NAME}/src"

# ============================================================
# AZURE STORAGE
# ============================================================
# Update with your storage account name
STORAGE_ACCOUNT = "<your-storage-account-name>"

# Container base paths
BRONZE_BASE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_BASE = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_BASE   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

CATALOG_NAME  = "ecommerce"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"

# ============================================================
# TABLE REGISTRY - ADD NEW TABLES HERE ONLY
# Format: "table_name": "raw_folder_name"
# ============================================================
TABLE_REGISTRY = {
    "order_items":  "order_items_large_dataset",  # folder name in bronze storage
    "customers":    "customers_large_dataset",
    "sellers":      "sellers_large_dataset",
    "orders":       "orders_large_dataset",
    "orders_new":   "orders_new_large_dataset"
}

# ============================================================
# AUTO-GENERATED PATHS (no need to touch below)
# ============================================================

def _generate_configs():
    """Auto-generate all paths and table names from registry"""
    
    configs = {}
    
    for table_name, folder_name in TABLE_REGISTRY.items():
        upper = table_name.upper()
        
        # Storage paths
        configs[f"BRONZE_{upper}"]       = f"{BRONZE_BASE}/{folder_name}"
        configs[f"SILVER_{upper}"]       = f"{SILVER_BASE}/{table_name}"
        configs[f"GOLD_{upper}"]         = f"{GOLD_BASE}/{table_name}"
        
        # Iceberg table names
        configs[f"BRONZE_{upper}_TABLE"] = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.{table_name}"
        configs[f"SILVER_{upper}_TABLE"] = f"{CATALOG_NAME}.{SILVER_SCHEMA}.{table_name}"
        configs[f"GOLD_{upper}_TABLE"]   = f"{CATALOG_NAME}.{GOLD_SCHEMA}.{table_name}"
    
    return configs

# Generate and inject into module namespace
_configs = _generate_configs()
globals().update(_configs)

# Checkpoint
CHECKPOINT_PATH = f"{BRONZE_BASE}/_checkpoints"
WATERMARK_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.processing_watermark"