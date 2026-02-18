"""
Reusable data quality checking functions
"""

import builtins
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when
from utils.logger import log_info, log_metric, log_error


def check_nulls(df: DataFrame, table_name: str) -> dict:
    """
    Analyze nulls in the table
    Args: 
        df: Spark DataFrame 
        table_name: name of the table
    Returns:
        dict: column -> null count mapping
    """
    log_info(f"Checking nulls in {table_name}...")

    # Calculate null counts
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) 
        for c in df.columns
    ]).collect()[0].asDict()
    
    # Analyze results
    columns_with_nulls = {k: v for k, v in null_counts.items() if v > 0}
    
    if len(columns_with_nulls) == 0:
        log_info("No null values found in any column")
    else:
        log_info(f"Nulls found in {len(columns_with_nulls)} columns:")
        for col_name, null_count in columns_with_nulls.items():
            pct = (null_count / df.count()) * 100
            print(f"{col_name}: {null_count:,} nulls ({pct:.2f}%)")
    
    return null_counts


def check_partition_skew(
    df: DataFrame,
    partition_col: str,
    table_name: str,
    skew_factor_threshold: float = 2.0
) -> DataFrame:
    """
    Analyze partition skew for any partitioned table.
    Detects skew by comparing partition sizes to median.
    Args:
        df: Spark DataFrame
        partition_col: name of the partition column
        table_name: name of the table
        skew_factor_threshold: flag partitions larger than this multiple of median
    Returns:
        DataFrame: partition skew stats
    """
    log_info(f"Checking partition skew in {table_name}...")

    # Get partition sizes
    partition_stats = df.groupBy(partition_col) \
        .agg(count("*").alias("row_count")) \
        .orderBy(col("row_count").desc())

    # Collect for analysis
    stats_list = partition_stats.collect()
    total_partitions = len(stats_list)

    if total_partitions == 0:
        log_error(f"No partitions found in {table_name}")
        return partition_stats
    
    # Calculate statistics
    row_counts = [row['row_count'] for row in stats_list]

    # Median calculation
    sorted_counts = sorted(row_counts)
    mid = len(sorted_counts) // 2
    if len(sorted_counts) % 2 == 0:
        median = (sorted_counts[mid - 1] + sorted_counts[mid]) / 2
    else:
        median = sorted_counts[mid]

    # Mean calculation
    mean = builtins.sum(row_counts) / total_partitions

    # Max and min
    max_rows = max(row_counts)
    min_rows = min(row_counts)

    # Skew factor
    skew_factor = max_rows / median if median > 0 else 0
    
    # Log statistics
    log_info(f"\nPartition Statistics for {table_name}:")
    log_info(f"Total partitions: {total_partitions}")
    log_metric("Max partition rows", f"{max_rows:,}")
    log_metric("Min partition rows", f"{min_rows:,}")
    log_metric("Mean rows per partition", f"{mean:,.0f}")
    log_metric("Median rows per partition", f"{median:,.0f}")
    log_metric("Skew factor (max/median)", f"{skew_factor:.2f}x")

    # Identify skewed partitions
    skewed_partitions = [
        row for row in stats_list if row['row_count'] > (median * skew_factor_threshold)
    ]

    # Report findings
    log_info(f"\nSkew Analysis (threshold: {skew_factor_threshold}x median):")
    
    if len(skewed_partitions) == 0:
        log_info("No significant skew detected")
        log_info(f"All partitions within {skew_factor_threshold}x of median")
    else:
        log_info(f"{len(skewed_partitions)} skewed partitions detected:")
        for p in skewed_partitions:
            factor = p['row_count'] / median
            print(f"Partition {p[partition_col]}: {p['row_count']:,} rows ({factor:.1f}x median)")
        
        log_info(f"\nNOTE: Spark AQE is enabled and will handle this automatically")
        log_info(f"Documenting for awareness only")
    
    # Show top 20 partitions
    log_info("\nTop 20 partitions by size:")
    partition_stats.show(20, truncate=False)
    
    return partition_stats


def check_duplicates(df: DataFrame, key_columns: List[str], table_name: str) -> int:
    """
    Check for duplicate records in the table
    Args:
        df: Spark DataFrame
        key_columns: list of columns to use as key
        table_name: name of the table
    Returns:
        int: number of duplicate records
    """
    log_info(f"Checking for duplicates in {table_name} on {key_columns}...")
    
    # Calculate duplicates
    total_rows = df.count()
    unique_rows = df.dropDuplicates(key_columns).count()
    duplicate_count = total_rows - unique_rows
    duplicate_pct = (duplicate_count / total_rows) * 100 if total_rows > 0 else 0
    
    log_metric("Total rows", f"{total_rows:,}")
    log_metric("Unique rows", f"{unique_rows:,}")
    log_metric("Duplicate rows", f"{duplicate_count:,}")
    log_metric("Duplicate percentage", f"{duplicate_pct:.4f}%")
    
    if duplicate_count == 0:
        log_info("No duplicates found")
    else:
        log_info(f"{duplicate_count:,} duplicates detected ({duplicate_pct:.4f}%)")
        log_info(f"These will be removed in silver layer transformation")
    
    return duplicate_count


def check_value_ranges(df: DataFrame, table_name: str, columns: list = None) -> None:
    """
    Check value ranges and basic statistics for numeric columns.
    
    Args:
        df: Spark DataFrame
        table_name: name of the table
        columns: list of columns to check (None = all numeric)
    """
    
    log_info(f"Checking value ranges in {table_name}...")
    
    # Get numeric columns
    numeric_cols = [
        f.name for f in df.schema.fields 
        if str(f.dataType) in ['IntegerType', 'LongType', 'DoubleType', 'FloatType']
    ] if columns is None else columns
    
    if not numeric_cols:
        log_info("No numeric columns found")
        return
    
    # Calculate stats
    stats = df.select(numeric_cols).describe()
    
    log_info("Column statistics:")
    stats.show(truncate=False)
    
    # Check for negative values (often invalid)
    for c in numeric_cols:
        negative_count = df.filter(col(c) < 0).count()
        if negative_count > 0:
            log_info(f"{c}: {negative_count:,} negative values found")
        else:
            log_info(f"{c}: No negative values")


def run_all_checks(
    df: DataFrame,
    table_name: str,
    key_columns: list,
    partition_col: str = None
) -> dict:
    """
    Run all data quality checks for a table.
    Returns results dict for programmatic use.
    
    Args:
        df: Spark DataFrame
        table_name: name of the table
        key_columns: columns that should be unique
        partition_col: partition column (None if not partitioned)
    Returns:
        dict: {
            'nulls': {col_name: null_count, ...},
            'duplicates': duplicate_count,
            'skew': DataFrame (if partitioned)
        }
    """
    
    log_info("=" * 60)
    log_info(f"DATA QUALITY REPORT: {table_name}")
    log_info("=" * 60)
    
    results = {}
    
    # 1. Null checks
    log_info("\n1. NULL VALUE ANALYSIS")
    results['nulls'] = check_nulls(df, table_name)
    
    # 2. Duplicate checks
    log_info("\n2. DUPLICATE ANALYSIS")
    results['duplicates'] = check_duplicates(df, key_columns, table_name)
    
    # 3. Value ranges
    log_info("\n3. VALUE RANGE ANALYSIS")
    check_value_ranges(df, table_name)
    
    # 4. Partition skew (only if partitioned)
    if partition_col:
        log_info("\n4. PARTITION SKEW ANALYSIS")
        results['skew'] = check_partition_skew(df, partition_col, table_name)
    
    log_info("=" * 60)
    log_info(f"DATA QUALITY REPORT COMPLETE: {table_name}")
    log_info("=" * 60)
    
    return results