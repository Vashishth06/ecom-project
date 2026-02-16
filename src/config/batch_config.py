"""
Batch processing configuration
"""

# Batch size for incremental loads
BATCH_SIZE_GB = 10
BATCH_SIZE_ROWS = 10_000_000 # Process 10M rows at a time

# Processing metadata
WATERMARK_TABLE = "ecommerce.bronze.processing_watermark"