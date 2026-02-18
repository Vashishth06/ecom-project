"""
Reusable silver layer transformation utilities
Optimized for large tables - uses repartitioning, skips silver DQA
"""

from typing import List, Optional, Callable, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit
from utils.logger import log_info, log_metric, log_error


class SilverTransformation:
    """
    Handles silver layer transformation for any table.
    Uses parallel processing via repartitioning.
    Skips silver DQA to avoid OOM - uses metadata comparison instead.
    """

    def __init__(
        self,
        spark: SparkSession,
        table_display_name: str,
        source_table: str,
        target_table: str
    ):
        self.spark = spark
        self.table_display_name = table_display_name.upper()
        self.source_table = source_table
        self.target_table = target_table
        self.table_name = target_table.split(".")[-1]
        self.bronze_df = None
        self.silver_df = None
        self.bronze_dqa_results = None
        self.dqa_sample_fraction = 0.01

    def read_bronze(self) -> DataFrame:
        """Read bronze and get metrics from Iceberg metadata (no scan)."""
        
        log_info(f"Reading bronze ({self.table_display_name})...")
        self.bronze_df = self.spark.table(self.source_table)
        
        try:
            bronze_detail = self.spark.sql(
                f"DESCRIBE DETAIL {self.source_table}"
            ).collect()[0]
            
            bronze_gb = bronze_detail['sizeInBytes'] / (1024**3)
            num_files = bronze_detail['numFiles']
            
            log_metric(f"Bronze size ({self.table_display_name})", f"{bronze_gb:.2f} GB")
            log_metric(f"Bronze files ({self.table_display_name})", num_files)
        except Exception as e:
            log_error(f"Could not read metadata: {str(e)}")
        
        return self.bronze_df

    def run_bronze_dqa(
        self,
        key_columns: List[str],
        partition_col: Optional[str] = None,
        sample_fraction: float = 0.01
    ) -> Dict:
        """Run DQA on bronze sample to identify issues."""
        from utils.data_quality_check import run_all_checks
        
        self.dqa_sample_fraction = sample_fraction
        log_info(f"Running bronze DQA on {sample_fraction*100:.1f}% sample ({self.table_display_name})...")
        
        df_sample = self.bronze_df.sample(fraction=sample_fraction, seed=42)
        
        dqa_results = run_all_checks(
            df=df_sample,
            table_name=self.source_table,
            key_columns=key_columns,
            partition_col=partition_col
        )
        
        return dqa_results

    def transform(
        self,
        key_columns: List[str],
        drop_cols: Optional[List[str]] = None,
        custom_transform: Optional[Callable] = None,
        num_partitions: int = 16,
        remove_nulls: bool = False,
        null_columns: Optional[List[str]] = None
    ) -> DataFrame:
        """Apply transformations with parallel processing (lazy execution)."""
        
        log_info(f"Building transformation plan ({self.table_display_name})...")
        
        log_info(f"Repartitioning into {num_partitions} partitions for parallel processing...")
        self.silver_df = self.bronze_df.repartition(num_partitions)
        
        if remove_nulls and null_columns:
            log_info(f"Removing nulls from columns: {null_columns}")
            for col_name in null_columns:
                self.silver_df = self.silver_df.filter(
                    self.silver_df[col_name].isNotNull()
                )
        
        log_info(f"Deduplicating on {key_columns}...")
        self.silver_df = self.silver_df.dropDuplicates(key_columns)
        
        if custom_transform:
            log_info(f"Applying custom transformations ({self.table_display_name})...")
            self.silver_df = custom_transform(self.silver_df)
        
        if drop_cols:
            log_info(f"Dropping columns: {drop_cols}")
            self.silver_df = self.silver_df.drop(*drop_cols)
        
        self.silver_df = self.silver_df \
            .withColumn("ingested_at", current_timestamp()) \
            .withColumn("data_source", lit(self.source_table))
        
        log_info(f"Transformation plan complete ({self.table_display_name})")
        log_info(f"Silver schema:")
        self.silver_df.printSchema()
        
        return self.silver_df

    def merge_sources(
        self,
        additional_sources: List[str],
        key_columns: List[str],
        drop_cols: Optional[List[str]] = None,
        merge_transform: Optional[Callable] = None,
        num_partitions: int = 32
    ) -> DataFrame:
        """Merge multiple bronze sources into one silver DataFrame."""
        
        log_info(f"Merging multiple sources ({self.table_display_name})...")
        log_metric("Primary source", self.source_table)
        
        all_dfs = [self.bronze_df]
        
        for source_table in additional_sources:
            log_info(f"Reading additional source: {source_table}")
            additional_df = self.spark.table(source_table)
            all_dfs.append(additional_df)
        
        log_metric("Total sources to merge", len(all_dfs))
        
        if merge_transform:
            log_info("Applying custom merge transformation...")
            merged_df = merge_transform(all_dfs)
        else:
            log_info("No custom transform - performing simple union...")
            merged_df = all_dfs[0]
            for df in all_dfs[1:]:
                merged_df = merged_df.unionByName(df, allowMissingColumns=True)
        
        log_info(f"Repartitioning merged data into {num_partitions} partitions...")
        self.silver_df = merged_df.repartition(num_partitions)
        
        log_info(f"Deduplicating on {key_columns}...")
        self.silver_df = self.silver_df.dropDuplicates(key_columns)
        
        if drop_cols:
            log_info(f"Dropping columns: {drop_cols}")
            self.silver_df = self.silver_df.drop(*drop_cols)
        
        self.silver_df = self.silver_df \
            .withColumn("ingested_at", current_timestamp()) \
            .withColumn("data_source", lit("merged"))
        
        log_info(f"Merge transformation complete ({self.table_display_name})")
        self.silver_df.printSchema()
        
        return self.silver_df

    def write_iceberg(
        self,
        catalog_name: str,
        silver_schema: str,
        silver_container: str,
        partition_cols: Optional[List[str]] = None,
        compression: str = "zstd"
    ) -> None:
        """Write to Iceberg - single action executes full plan."""
        
        if self.silver_df is None:
            log_error("No data to write. Call transform() first.")
            return
        
        log_info(f"Writing to silver Iceberg ({self.table_display_name})...")
        log_info("This will execute the full transformation plan (dedup, metadata, etc.)")
        
        self.spark.sql(f"USE CATALOG {catalog_name}")
        self.spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {silver_schema}
            MANAGED LOCATION '{silver_container}'
        """)
        self.spark.sql(f"USE SCHEMA {silver_schema}")
        
        self.spark.sql(f"DROP TABLE IF EXISTS {self.table_name}")
        
        writer = self.silver_df.writeTo(self.table_name) \
            .using("iceberg") \
            .option("write.parquet.compression-codec", compression)
        
        if partition_cols:
            writer = writer.partitionedBy(*partition_cols)
        
        writer.create()
        
        log_info(f"Silver table created: {self.target_table}")
        
        self._show_comparison()

    def _show_comparison(self) -> None:
        """Show bronze→silver metrics from Iceberg metadata (no scan)."""
        
        try:
            bronze_detail = self.spark.sql(
                f"DESCRIBE DETAIL {self.source_table}"
            ).collect()[0]
            
            silver_detail = self.spark.sql(
                f"DESCRIBE DETAIL {self.target_table}"
            ).collect()[0]
            
            bronze_gb = bronze_detail['sizeInBytes'] / (1024**3)
            silver_gb = silver_detail['sizeInBytes'] / (1024**3)
            size_reduction = ((bronze_gb - silver_gb) / bronze_gb) * 100 if bronze_gb > 0 else 0
            
            log_info("=" * 60)
            log_info(f"BRONZE → SILVER COMPARISON ({self.table_display_name}):")
            log_metric("Bronze size", f"{bronze_gb:.2f} GB")
            log_metric("Silver size", f"{silver_gb:.2f} GB")
            log_metric("Size reduction", f"{size_reduction:.1f}%")
            log_metric("Bronze files", bronze_detail['numFiles'])
            log_metric("Silver files", silver_detail['numFiles'])
            log_info("=" * 60)
            
            if self.bronze_dqa_results:
                self._show_cleaning_summary()
            
        except Exception as e:
            log_error(f"Could not retrieve comparison: {str(e)}")

    def _show_cleaning_summary(self) -> None:
        """Show cleaning metrics estimated from bronze DQA."""
        
        log_info("")
        log_info("=" * 60)
        log_info(f"CLEANING SUMMARY ({self.table_display_name}):")
        log_info(f"(Based on bronze DQA {self.dqa_sample_fraction*100:.1f}% sample)")
        log_info("=" * 60)
        
        bronze_dup = self.bronze_dqa_results.get('duplicates', 0)
        scale_factor = 1 / self.dqa_sample_fraction
        estimated_dup_removed = int(bronze_dup * scale_factor)
        
        log_metric("Duplicates in bronze (estimated)", f"{estimated_dup_removed:,}")
        log_metric("Duplicates in silver (after dedup)", "0")
        log_metric("Duplicates removed (estimated)", f"{estimated_dup_removed:,}")
        
        bronze_nulls = self.bronze_dqa_results.get('nulls', {})
        
        null_removed_any = False
        for col_name, bronze_null_count in bronze_nulls.items():
            if bronze_null_count > 0:
                estimated_nulls = int(bronze_null_count * scale_factor)
                log_metric(f"Nulls in '{col_name}' (bronze, estimated)", f"{estimated_nulls:,}")
                null_removed_any = True
        
        if not null_removed_any:
            log_info("No null values found in bronze")
        
        log_info("=" * 60)

    def validate(self) -> None:
        """Lightweight validation using Iceberg metadata."""
        
        log_info("")
        log_info("=" * 60)
        log_info(f"SILVER VALIDATION: {self.table_display_name}")
        log_info("=" * 60)
        
        self.spark.sql(
            f"DESCRIBE DETAIL {self.target_table}"
        ).show(truncate=False, vertical=True)
        
        log_info(f"Silver transformation complete: {self.table_display_name}")
        log_info("=" * 60)

    def run(
        self,
        catalog_name: str,
        silver_schema: str,
        silver_container: str,
        key_columns: List[str],
        drop_cols: Optional[List[str]] = None,
        partition_cols: Optional[List[str]] = None,
        bronze_partition_col: Optional[str] = None,
        custom_transform: Optional[Callable] = None,
        skip_bronze_dqa: bool = False,
        dqa_sample_fraction: float = 0.01,
        num_partitions: int = 16,
        remove_nulls: bool = False,
        null_columns: Optional[List[str]] = None
    ) -> bool:
        """Run full silver transformation pipeline."""
        
        log_info("=" * 60)
        log_info(f"SILVER TRANSFORMATION: {self.table_display_name}")
        log_info(f"Source: {self.source_table}")
        log_info(f"Target: {self.target_table}")
        log_info("=" * 60)
        
        self.read_bronze()
        
        if not skip_bronze_dqa:
            self.bronze_dqa_results = self.run_bronze_dqa(
                key_columns=key_columns,
                partition_col=bronze_partition_col,
                sample_fraction=dqa_sample_fraction
            )
        
        self.transform(
            key_columns=key_columns,
            drop_cols=drop_cols,
            custom_transform=custom_transform,
            num_partitions=num_partitions,
            remove_nulls=remove_nulls,
            null_columns=null_columns
        )
        
        self.write_iceberg(
            catalog_name=catalog_name,
            silver_schema=silver_schema,
            silver_container=silver_container,
            partition_cols=partition_cols
        )
        
        self.validate()
        
        return True

    def run_merge(
        self,
        catalog_name: str,
        silver_schema: str,
        silver_container: str,
        additional_sources: List[str],
        key_columns: List[str],
        drop_cols: Optional[List[str]] = None,
        partition_cols: Optional[List[str]] = None,
        merge_transform: Optional[Callable] = None,
        skip_bronze_dqa: bool = False,
        dqa_sample_fraction: float = 0.01,
        num_partitions: int = 32
    ) -> bool:
        """Run silver transformation for MULTIPLE sources (merge scenario)."""
        
        log_info("=" * 60)
        log_info(f"SILVER MERGE TRANSFORMATION: {self.table_display_name}")
        log_info(f"Primary source: {self.source_table}")
        log_info(f"Additional sources: {additional_sources}")
        log_info(f"Target: {self.target_table}")
        log_info("=" * 60)
        
        self.read_bronze()
        
        if not skip_bronze_dqa:
            self.bronze_dqa_results = self.run_bronze_dqa(
                key_columns=key_columns,
                partition_col=None,
                sample_fraction=dqa_sample_fraction
            )
        
        self.merge_sources(
            additional_sources=additional_sources,
            key_columns=key_columns,
            drop_cols=drop_cols,
            merge_transform=merge_transform,
            num_partitions=num_partitions
        )
        
        self.write_iceberg(
            catalog_name=catalog_name,
            silver_schema=silver_schema,
            silver_container=silver_container,
            partition_cols=partition_cols
        )
        
        self.validate()
        
        return True