"""
Spark Structured Streaming job: Kafka CDC → Iceberg (Nessie catalog)

Reads Debezium CDC events from Kafka topics and upserts them
into Iceberg tables on MinIO via Nessie catalog.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, expr
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType,
    MapType, IntegerType,
)

# ── Iceberg + Nessie config ──────────────────

WAREHOUSE = "s3://iceberg-warehouse/"
NESSIE_URI = "http://nessie:19120/api/v2"
MINIO_ENDPOINT = "http://minio:9000"

def create_spark():
    return (
        SparkSession.builder
        .appName("cdc-to-iceberg")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2,software.amazon.awssdk:s3:2.24.13")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.type", "nessie")
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.nessie.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.sql.catalog.nessie.s3.access-key-id", "admin")
        .config("spark.sql.catalog.nessie.s3.secret-access-key", "password123")
        .config("spark.sql.catalog.nessie.s3.region", "us-east-1")
        .getOrCreate()
    )


# ── Flat CDC Schema (no schema envelope) ─────
# With JsonConverter + schemas.enable=false + decimal.handling.mode=double

SOURCE_SCHEMA = StructType([
    StructField("version", StringType()),
    StructField("connector", StringType()),
    StructField("name", StringType()),
    StructField("ts_ms", LongType()),
    StructField("snapshot", StringType()),
    StructField("db", StringType()),
    StructField("schema", StringType()),
    StructField("table", StringType()),
    StructField("txId", LongType()),
    StructField("lsn", LongType()),
])

CDC_EVENT_SCHEMA = StructType([
    StructField("before", MapType(StringType(), StringType()), True),
    StructField("after", MapType(StringType(), StringType()), True),
    StructField("source", SOURCE_SCHEMA),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
])


def process_batch(df, epoch_id):
    """Process each micro-batch: parse CDC events and upsert into Iceberg."""
    if df.isEmpty():
        return

    spark = df.sparkSession

    parsed = (
        df.select(
            from_json(col("value").cast("string"), CDC_EVENT_SCHEMA).alias("cdc"),
        )
        .filter(col("cdc").isNotNull())
        .select(
            col("cdc.op").alias("operation"),
            col("cdc.after").alias("after"),
            col("cdc.source.table").alias("source_table"),
            col("cdc.ts_ms").alias("event_ts_ms"),
        )
    )

    tables = parsed.select("source_table").distinct().collect()
    for row in tables:
        table_name = row["source_table"]
        table_df = parsed.filter(col("source_table") == table_name)

        if table_df.isEmpty():
            continue

        flat = table_df.select(
            col("operation"),
            col("after")["customer_id"].cast("int").alias("customer_id"),
            col("after")["name"].alias("name"),
            col("after")["email"].alias("email"),
            col("after")["phone"].alias("phone"),
            col("after")["loyalty_tier"].alias("loyalty_tier"),
            col("after")["order_id"].cast("int").alias("order_id"),
            col("after")["total_amount"].cast("double").alias("total_amount"),
            col("after")["status"].alias("status"),
            col("after")["payment_method"].alias("payment_method"),
            col("after")["item_id"].cast("int").alias("item_id"),
            col("after")["product_name"].alias("product_name"),
            col("after")["quantity"].cast("int").alias("quantity"),
            col("after")["unit_price"].cast("double").alias("unit_price"),
            col("after")["product_id"].cast("int").alias("product_id"),
            col("after")["sku"].alias("sku"),
            col("after")["category"].alias("category"),
            col("after")["stock_quantity"].cast("int").alias("stock_quantity"),
            col("event_ts_ms").alias("_cdc_event_ts"),
        )

        if table_name == "customers":
            upsert_customers(spark, flat)
        elif table_name == "orders":
            upsert_orders(spark, flat)
        elif table_name == "inventory":
            upsert_inventory(spark, flat)

    print(f"  ✓ Batch {epoch_id} processed")


def upsert_customers(spark, df):
    df.createOrReplaceTempView("cdc_customers")
    spark.sql("""
        MERGE INTO nessie.iceberg_warehouse.customers AS target
        USING cdc_customers AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED AND source.operation = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET
            name = source.name,
            email = source.email,
            phone = source.phone,
            loyalty_tier = source.loyalty_tier
        WHEN NOT MATCHED THEN INSERT *
    """)


def upsert_orders(spark, df):
    df.createOrReplaceTempView("cdc_orders")
    spark.sql("""
        MERGE INTO nessie.iceberg_warehouse.orders AS target
        USING cdc_orders AS source
        ON target.order_id = source.order_id
        WHEN MATCHED AND source.operation = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET
            total_amount = source.total_amount,
            status = source.status,
            payment_method = source.payment_method
        WHEN NOT MATCHED THEN INSERT *
    """)


def upsert_inventory(spark, df):
    df.createOrReplaceTempView("cdc_inventory")
    spark.sql("""
        MERGE INTO nessie.iceberg_warehouse.inventory AS target
        USING cdc_inventory AS source
        ON target.sku = source.sku
        WHEN MATCHED THEN UPDATE SET
            stock_quantity = source.stock_quantity,
            unit_price = source.unit_price,
            category = source.category,
            product_name = source.product_name
        WHEN NOT MATCHED THEN INSERT *
    """)


def create_iceberg_tables(spark):
    """Initialize Iceberg tables if they don't exist."""
    spark.sql("CREATE DATABASE IF NOT EXISTS nessie.iceberg_warehouse")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.iceberg_warehouse.customers (
            customer_id INT,
            name STRING,
            email STRING,
            phone STRING,
            loyalty_tier STRING,
            _cdc_event_ts BIGINT
        ) USING iceberg
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.iceberg_warehouse.orders (
            order_id INT,
            customer_id INT,
            total_amount DOUBLE,
            status STRING,
            payment_method STRING,
            _cdc_event_ts BIGINT
        ) USING iceberg
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.iceberg_warehouse.inventory (
            product_id INT,
            sku STRING,
            product_name STRING,
            category STRING,
            stock_quantity INT,
            unit_price DOUBLE,
            _cdc_event_ts BIGINT
        ) USING iceberg
    """)
    print("  ✓ Iceberg tables initialized")


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    create_iceberg_tables(spark)

    print("Starting CDC streaming from Kafka → Iceberg...")

    kafka_topic_pattern = "cdc.sales.*"

    df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribePattern", kafka_topic_pattern)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "100")
        .load()
    )

    query = (
        df
        .writeStream
        .foreachBatch(process_batch)
        .outputMode("update")
        .option("checkpointLocation", "/tmp/spark-checkpoints/cdc")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
