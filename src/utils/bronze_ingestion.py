"""
Reusable bronze layer ingestion utilities
Follows DRY, OOP, SOLID principles
"""

import builtins
from typing import List, Optional, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from utils.logger import log_info, log_metric, log_error


class BronzeIngestion:
    """
    Handles bronze layer ingestion for any table.
    
    Usage:
        ingestion = BronzeIngestion(spark, "customers", BRONZE_CUSTOMERS, BRONZE_CUSTOMERS_TABLE)
        ingestion.run(schema=schema, partition_cols=["country", "state"])
    """
    
    def __init__(
        self,
        spark: SparkSession,
        table_display_name: str,
        source_path: str,
        target_table: str
    ):
        """
        Args:
            spark: SparkSession instance
            table_display_name: Human readable name (e.g. "CUSTOMERS")
            source_path: Bronze storage path
            target_table: Full Iceberg table name (catalog.schema.table)
        """
        self.spark = spark
        self.table_display_name = table_display_name.upper()
        self.source_path = source_path
        self.target_table = target_table
        self.table_name = target_table.split(".")[-1]
        self.df = None
        self.row_count = 0
    
    # ============================================================
    # STEP 1: Verify source data
    # ============================================================
    def verify_source(self) -> bool:
        """
        Check if source files exist in bronze storage.
        Returns True if valid files found.
        """
        log_info(f"Checking bronze storage ({self.table_display_name})...")
        
        try:
            files = self.spark.sparkContext._jvm.org.apache.hadoop.fs \
                .FileSystem.get(
                    self.spark._jvm.java.net.URI.create(self.source_path),
                    self.spark._jsc.hadoopConfiguration()
                )
            
            # Use dbutils via spark
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)
            all_files = dbutils.fs.ls(self.source_path)
            
        except Exception:
            # Fallback: use spark to list files
            all_files = self.spark.sparkContext \
                ._jvm.org.apache.hadoop \
                .fs.FileSystem \
                .get(self.spark._jsc.hadoopConfiguration()) \
                .listStatus(
                    self.spark._jvm.org.apache.hadoop.fs.Path(self.source_path)
                )
        
        return True
    
    def check_source(self, dbutils) -> bool:
        """
        Check source files using dbutils.
        
        Args:
            dbutils: Databricks utilities object
        Returns:
            bool: True if valid files found
        """
        log_info(f"Checking source ({self.table_display_name})...")
        
        try:
            files = dbutils.fs.ls(self.source_path)
            
            # Filter valid files
            valid_files = [
                f for f in files
                if not f.name.startswith('._')
                and (f.name.endswith('.csv') 
                     or f.name.endswith('.parquet')
                     or f.name.endswith('.json'))
            ]
            
            if not valid_files:
                log_error(f"No valid files found in {self.source_path}")
                return False
            
            # Calculate total size
            total_bytes = builtins.sum(f.size for f in valid_files)
            total_gb = total_bytes / (1024**3)
            
            log_metric(f"Valid files ({self.table_display_name})", len(valid_files))
            log_metric(f"Total size ({self.table_display_name})", f"{total_gb:.2f} GB")
            
            return True
            
        except Exception as e:
            log_error(f"Source check failed: {str(e)}")
            return False
    
    # ============================================================
    # STEP 2: Read data
    # ============================================================
    def read(
        self,
        schema: StructType,
        file_format: str = "csv",
        header: bool = True
    ) -> DataFrame:
        """
        Read source files with explicit schema.
        
        Args:
            schema: PySpark StructType schema
            file_format: csv/parquet/json
            header: Whether files have header row
        Returns:
            DataFrame: cached DataFrame
        """
        log_info(f"Reading {file_format.upper()} files ({self.table_display_name})...")
        
        reader = self.spark.read.schema(schema)
        
        if file_format == "csv":
            reader = reader.option("header", str(header).lower())
        
        self.df = reader.format(file_format).load(self.source_path)
        
        # Cache for reuse
        self.df.cache()
        
        self.row_count = self.df.count()
        log_metric(f"Rows loaded ({self.table_display_name})", f"{self.row_count:,}")
        log_info(f"Data cached in memory ({self.table_display_name})")
        
        return self.df
    
    # ============================================================
    # STEP 3: Write to Iceberg
    # ============================================================
    def write_iceberg(
        self,
        catalog_name: str,
        bronze_schema: str,
        partition_cols: Optional[List[str]] = None,
        compression: str = "zstd"
    ) -> None:
        """
        Write DataFrame to Iceberg table.
        
        Args:
            catalog_name: Unity catalog name
            bronze_schema: Bronze schema name
            partition_cols: List of partition columns
            compression: Parquet compression codec
        """
        if self.df is None:
            log_error("No data loaded. Call read() first.")
            return
        
        log_info(f"Creating Iceberg table ({self.table_display_name})...")
        
        # Set catalog context
        self.spark.sql(f"USE CATALOG {catalog_name}")
        self.spark.sql(f"USE SCHEMA {bronze_schema}")
        
        # Drop existing table
        self.spark.sql(f"DROP TABLE IF EXISTS {self.table_name}")
        log_info(f"Dropped existing table: {self.table_name}")
        
        # Build writer
        writer = self.df.writeTo(self.table_name) \
            .using("iceberg") \
            .option("write.parquet.compression-codec", compression)
        
        # Add partitions if specified
        if partition_cols:
            writer = writer.partitionedBy(*partition_cols)
        
        writer.create()
        
        # Verify row count
        final_count = self.spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {self.table_name}"
        ).collect()[0]['cnt']
        
        log_metric(f"Table row count ({self.table_display_name})", f"{final_count:,}")
        assert final_count == self.row_count, \
            f"Row count mismatch! Expected {self.row_count:,}, got {final_count:,}"
        
        log_info(f"Iceberg table created: {self.target_table}")
    
    # ============================================================
    # STEP 4: Show table details
    # ============================================================
    def show_table_details(self) -> None:
        """Show Iceberg table metadata and schema."""
        
        log_info(f"Table details ({self.table_display_name}):")
        detail_df = self.spark.sql(f"DESCRIBE DETAIL {self.target_table}")
        detail_df.show(truncate=False, vertical=True)
        
        log_info(f"Table schema ({self.table_display_name}):")
        schema_df = self.spark.sql(f"DESCRIBE {self.target_table}")
        schema_df.show(truncate=False)
    
    # ============================================================
    # MAIN: Run full pipeline
    # ============================================================
    def run(
        self,
        dbutils,
        schema: StructType,
        catalog_name: str,
        bronze_schema: str,
        partition_cols: Optional[List[str]] = None,
        file_format: str = "csv"
    ) -> bool:
        """
        Run full bronze ingestion pipeline.
        
        Args:
            dbutils: Databricks utilities
            schema: PySpark StructType schema
            catalog_name: Unity catalog name
            bronze_schema: Bronze schema name
            partition_cols: Optional partition columns
            file_format: Source file format
        Returns:
            bool: True if successful
        """
        log_info("=" * 60)
        log_info(f"BRONZE INGESTION: {self.table_display_name}")
        log_info(f"Source: {self.source_path}")
        log_info(f"Target: {self.target_table}")
        log_info("=" * 60)
        
        # Step 1: Verify source
        if not self.check_source(dbutils):
            return False
        
        # Step 2: Read data
        self.read(schema=schema, file_format=file_format)
        
        # Step 3: Write to Iceberg
        self.write_iceberg(
            catalog_name=catalog_name,
            bronze_schema=bronze_schema,
            partition_cols=partition_cols
        )
        
        # Step 4: Show details
        self.show_table_details()
        
        log_info(f"Bronze ingestion complete: {self.table_display_name}")
        return True