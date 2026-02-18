"""
Reusable gold layer transformation utilities
Handles dimension and fact table creation with SCD support
"""

from typing import List, Optional, Callable, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit
from utils.logger import log_info, log_metric, log_error


class GoldTransformation:
    """
    Handles gold layer transformation for dimension and fact tables.
    Supports SCD Type 1/2, incremental loads, and aggregations.
    """

    def __init__(
        self,
        spark: SparkSession,
        table_display_name: str,
        table_type: str,  # "dimension" or "fact"
        source_tables: List[str],  # Can read from silver AND gold
        target_table: str
    ):
        self.spark = spark
        self.table_display_name = table_display_name.upper()
        self.table_type = table_type.lower()
        self.source_tables = source_tables if isinstance(source_tables, list) else [source_tables]
        self.target_table = target_table
        self.table_name = target_table.split(".")[-1]
        self.source_dfs = {}
        self.gold_df = None

    def read_sources(self) -> Dict[str, DataFrame]:
        """Read all source tables (from silver or gold)."""
        
        log_info(f"Reading source tables for {self.table_display_name}...")
        
        for source_table in self.source_tables:
            log_info(f"Reading: {source_table}")
            self.source_dfs[source_table] = self.spark.table(source_table)
            
            try:
                detail = self.spark.sql(f"DESCRIBE DETAIL {source_table}").collect()[0]
                size_gb = detail['sizeInBytes'] / (1024**3)
                log_metric(f"Source size ({source_table})", f"{size_gb:.2f} GB")
            except:
                pass
        
        log_metric("Total sources read", len(self.source_dfs))
        return self.source_dfs

    def transform(
        self,
        transform_logic: Callable,
        scd_type: int = 1,  # 1 = overwrite, 2 = track history
        add_metadata: bool = True
    ) -> DataFrame:
        """
        Apply custom transformation logic.
        
        Args:
            transform_logic: Function(dict[table_name: DataFrame]) -> DataFrame
            scd_type: 1 (overwrite) or 2 (track history)
            add_metadata: Add created_at/updated_at columns
        """
        
        log_info(f"Applying {self.table_type} transformation ({self.table_display_name})...")
        log_metric("SCD Type", scd_type)
        
        # Apply custom logic
        self.gold_df = transform_logic(self.source_dfs)
        
        # Add SCD Type 2 columns if needed
        if scd_type == 2:
            log_info("Adding SCD Type 2 columns...")
            self.gold_df = self.gold_df \
                .withColumn("valid_from", current_timestamp()) \
                .withColumn("valid_to", lit(None).cast("timestamp")) \
                .withColumn("is_current", lit(True))
        
        # Add metadata
        if add_metadata:
            if scd_type == 1:
                self.gold_df = self.gold_df \
                    .withColumn("created_at", current_timestamp()) \
                    .withColumn("updated_at", current_timestamp())
            # SCD Type 2 already has valid_from/valid_to
        
        log_info(f"{self.table_type.capitalize()} schema:")
        self.gold_df.printSchema()
        
        log_info(f"Sample {self.table_display_name}:")
        self.gold_df.show(5, truncate=False)
        
        return self.gold_df

    def write_iceberg(
        self,
        catalog_name: str,
        gold_schema: str,
        gold_container: str,
        partition_cols: Optional[List[str]] = None,
        clustering_cols: Optional[List[str]] = None,
        write_mode: str = "overwrite",  # "overwrite", "append", "merge"
        merge_keys: Optional[List[str]] = None,
        compression: str = "zstd"
    ) -> None:
        """
        Write to gold Iceberg table.
        
        Args:
            write_mode: "overwrite" (SCD Type 1), "append" (incremental), "merge" (upsert)
            merge_keys: Keys for merge operation (required if write_mode="merge")
        """
        
        if self.gold_df is None:
            log_error("No data to write. Call transform() first.")
            return
        
        log_info(f"Writing {self.table_type} to Gold ({self.table_display_name})...")
        log_metric("Write mode", write_mode)
        
        # Setup schema
        self.spark.sql(f"USE CATALOG {catalog_name}")
        self.spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {gold_schema}
            MANAGED LOCATION '{gold_container}'
        """)
        self.spark.sql(f"USE SCHEMA {gold_schema}")
        
        # Check if table exists
        table_exists = self.spark.catalog.tableExists(self.table_name)
        
        if write_mode == "overwrite":
            # Drop and recreate
            self.spark.sql(f"DROP TABLE IF EXISTS {self.table_name}")
            
            writer = self.gold_df.writeTo(self.table_name) \
                .using("iceberg") \
                .option("write.parquet.compression-codec", compression)
            
            if partition_cols:
                writer = writer.partitionedBy(*partition_cols)
            elif clustering_cols:
                writer = writer.partitionedBy(*clustering_cols)
            
            writer.create()
            
        elif write_mode == "append":
            # Append to existing table
            if not table_exists:
                log_error(f"Table {self.table_name} doesn't exist. Use 'overwrite' mode first.")
                return
            
            self.gold_df.writeTo(self.table_name) \
                .using("iceberg") \
                .append()
        
        elif write_mode == "merge":
            # Merge (upsert) - SCD Type 2 or incremental updates
            if not merge_keys:
                log_error("merge_keys required for merge mode")
                return
            
            if not table_exists:
                log_error(f"Table {self.table_name} doesn't exist. Use 'overwrite' mode first.")
                return
            
            # Create temp view for merge
            self.gold_df.createOrReplaceTempView("temp_gold_updates")
            
            # Build merge statement
            merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
            
            self.spark.sql(f"""
                MERGE INTO {self.table_name} AS target
                USING temp_gold_updates AS source
                ON {merge_condition}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        
        log_info(f"{self.table_type.capitalize()} table written: {self.target_table}")
        self._show_summary()

    def _show_summary(self) -> None:
        """Show table summary from metadata."""
        
        try:
            detail = self.spark.sql(f"DESCRIBE DETAIL {self.target_table}").collect()[0]
            
            size_gb = detail['sizeInBytes'] / (1024**3)
            num_files = detail['numFiles']
            
            log_info("=" * 60)
            log_info(f"{self.table_type.upper()} TABLE SUMMARY ({self.table_display_name}):")
            log_metric("Size", f"{size_gb:.2f} GB")
            log_metric("Files", num_files)
            log_info("=" * 60)
            
        except Exception as e:
            log_error(f"Could not retrieve summary: {str(e)}")

    def validate(
        self,
        show_sample: bool = True,
        run_dqa: bool = False,
        key_columns: Optional[List[str]] = None
    ) -> None:
        """
        Validate table.
        
        Args:
            show_sample: Show sample data
            run_dqa: Run data quality checks
            key_columns: Key columns for DQA
        """
        
        log_info("")
        log_info("=" * 60)
        log_info(f"{self.table_type.upper()} VALIDATION: {self.table_display_name}")
        log_info("=" * 60)
        
        # Show table details
        self.spark.sql(f"DESCRIBE DETAIL {self.target_table}").show(truncate=False, vertical=True)
        
        if show_sample:
            log_info(f"Sample data from {self.table_display_name}:")
            self.spark.table(self.target_table).show(10, truncate=False)
        
        # Optional DQA
        if run_dqa and key_columns:
            from utils.data_quality_check import run_all_checks
            
            log_info("Running data quality checks...")
            df = self.spark.table(self.target_table)
            run_all_checks(
                df=df.sample(0.01),  # 1% sample
                table_name=self.target_table,
                key_columns=key_columns,
                partition_col=None
            )
        
        log_info(f"{self.table_type.capitalize()} complete: {self.table_display_name}")
        log_info("=" * 60)

    def run(
        self,
        catalog_name: str,
        gold_schema: str,
        gold_container: str,
        transform_logic: Callable,
        partition_cols: Optional[List[str]] = None,
        clustering_cols: Optional[List[str]] = None,
        scd_type: int = 1,
        write_mode: str = "overwrite",
        merge_keys: Optional[List[str]] = None,
        add_metadata: bool = True,
        run_dqa: bool = False,
        key_columns: Optional[List[str]] = None
    ) -> bool:
        """Run full gold transformation pipeline."""
        
        log_info("=" * 60)
        log_info(f"GOLD {self.table_type.upper()}: {self.table_display_name}")
        log_info(f"Sources: {self.source_tables}")
        log_info(f"Target: {self.target_table}")
        log_info("=" * 60)
        
        self.read_sources()
        
        self.transform(
            transform_logic=transform_logic,
            scd_type=scd_type,
            add_metadata=add_metadata
        )
        
        self.write_iceberg(
            catalog_name=catalog_name,
            gold_schema=gold_schema,
            gold_container=gold_container,
            partition_cols=partition_cols,
            clustering_cols=clustering_cols,
            write_mode=write_mode,
            merge_keys=merge_keys
        )
        
        self.validate(
            show_sample=True,
            run_dqa=run_dqa,
            key_columns=key_columns
        )
        
        return True