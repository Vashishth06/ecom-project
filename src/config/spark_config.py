"""
Minimal Spark configuration for skew handling
"""

def configure_spark(spark):
    """
    Apply essential Spark configurations for data skew
    
    Args:
        spark: SparkSession instance
    """
    
    # Enable Adaptive Query Execution
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    
    # Enable skew join optimization
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    
    # Skew thresholds
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    
    print("Spark AQE and skew handling enabled")


def get_spark_config_summary(spark):
    """Get current Spark AQE settings"""
    return {
        "adaptive_enabled": spark.conf.get("spark.sql.adaptive.enabled"),
        "skew_join_enabled": spark.conf.get("spark.sql.adaptive.skewJoin.enabled")
    }