"""CDC streaming: Kafka -> Iceberg via separate per-table streams"""
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, MapType, IntegerType

WAREHOUSE = "s3://iceberg-warehouse/"
NESSIE_URI = "http://nessie:19120/api/v2"
MINIO_ENDPOINT = "http://minio:9000"

CDC_SCHEMA = StructType([
    StructField("before", MapType(StringType(), StringType()), True),
    StructField("after", MapType(StringType(), StringType()), True),
    StructField("source", StructType([
        StructField("table", StringType()),
    ])),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
])

TABLE_CONF = {
    "customers": {
        "topic": "cdc.sales.customers",
        "key": "customer_id",
        "columns": ["customer_id", "name", "email", "phone", "loyalty_tier"],
        "set": "name = source.name, email = source.email, phone = source.phone, loyalty_tier = source.loyalty_tier",
    },
    "orders": {
        "topic": "cdc.sales.orders",
        "key": "order_id",
        "columns": ["order_id", "customer_id", "total_amount", "status", "payment_method"],
        "set": "customer_id = source.customer_id, total_amount = source.total_amount, status = source.status, payment_method = source.payment_method",
    },
    "inventory": {
        "topic": "cdc.sales.inventory",
        "key": "sku",
        "columns": ["product_id", "sku", "product_name", "category", "stock_quantity", "unit_price"],
        "set": "product_id = source.product_id, product_name = source.product_name, category = source.category, stock_quantity = source.stock_quantity, unit_price = source.unit_price",
    },
}

SPARK_CFG = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.nessie.type": "nessie",
    "spark.sql.catalog.nessie.uri": NESSIE_URI,
    "spark.sql.catalog.nessie.ref": "main",
    "spark.sql.catalog.nessie.warehouse": WAREHOUSE,
    "spark.sql.catalog.nessie.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.nessie.s3.endpoint": MINIO_ENDPOINT,
    "spark.sql.catalog.nessie.s3.path-style-access": "true",
    "spark.sql.catalog.nessie.s3.access-key-id": "admin",
    "spark.sql.catalog.nessie.s3.secret-access-key": "password123",
    "spark.sql.catalog.nessie.s3.region": "us-east-1",
}


def process_table(spark, table_name, conf):
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", conf["topic"])
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "20")
        .load()
    )

    def _col_type(c):
        if c in ("customer_id", "order_id", "product_id", "stock_quantity"):
            return "int"
        if c in ("total_amount", "unit_price"):
            return "double"
        return "string"

    def upsert_batch(batch_df, epoch_id):
        if batch_df.isEmpty():
            return

        parsed = (
            batch_df
            .select(from_json(col("value").cast("string"), CDC_SCHEMA).alias("cdc"))
            .filter(col("cdc").isNotNull())
            .select("cdc.op", "cdc.after", "cdc.ts_ms")
        )

        rows = (
            parsed
            .select(
                col("op"),
                *[col("after")[c].cast(_col_type(c)).alias(c) for c in conf["columns"]],
                col("ts_ms").alias("_ts")
            )
            .orderBy(col("_ts").desc())
            .dropDuplicates([conf["key"]])
        )

        if rows.isEmpty():
            return

        tbl = f"nessie.iceberg_warehouse.{table_name}"
        key = conf["key"]
        cols = conf["columns"] + ["_ts"]

        deletes = rows.filter(col("op") == "d").select(*[col(c) for c in [key]])
        upserts = rows.filter(col("op") != "d").select(*[col(c) for c in cols])

        if not deletes.isEmpty():
            deletes.createOrReplaceTempView("del")
            spark.sql(f"DELETE FROM {tbl} WHERE {key} IN (SELECT {key} FROM del)")

        if not upserts.isEmpty():
            upserts.createOrReplaceTempView("src")
            try:
                spark.sql(f"""
                    MERGE INTO {tbl} AS t
                    USING src AS s
                    ON t.{key} = s.{key}
                    WHEN MATCHED THEN UPDATE SET {conf["set"]}
                    WHEN NOT MATCHED THEN INSERT *
                """)
            except Exception:
                upserts.write.format("iceberg").mode("append").save(tbl)

        print(f"  [{table_name}] batch {epoch_id} done")

    return (
        df.writeStream
        .foreachBatch(upsert_batch)
        .outputMode("update")
        .option("checkpointLocation", f"/tmp/checkpoint/{table_name}")
        .trigger(processingTime="5 seconds")
        .start()
    )


def main():
    spark = SparkSession.builder.appName("cdc-to-iceberg")
    for k, v in SPARK_CFG.items():
        spark = spark.config(k, v)
    spark = spark.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE DATABASE IF NOT EXISTS nessie.iceberg_warehouse")
    type_map = {
        "customer_id": "INT", "order_id": "INT", "product_id": "INT", "stock_quantity": "INT",
        "total_amount": "DOUBLE", "unit_price": "DOUBLE",
    }
    for tn, conf in TABLE_CONF.items():
        cols = ", ".join(f"{c} {type_map.get(c, 'STRING')}" for c in conf["columns"])
        spark.sql(f"CREATE TABLE IF NOT EXISTS nessie.iceberg_warehouse.{tn} ({cols}, _ts BIGINT) USING iceberg")

    print("Starting per-table CDC streaming...")
    queries = [process_table(spark, tn, conf) for tn, conf in TABLE_CONF.items()]

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
