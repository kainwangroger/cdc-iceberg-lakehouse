#!/bin/bash
# Soumet le job Spark Streaming CDC → Iceberg
# Usage: bash submit_job.sh

SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"

echo "Submitting CDC-to-Iceberg Spark job..."

/opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER}" \
  --deploy-mode client \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2,software.amazon.awssdk:s3:2.24.13,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.nessie.type=nessie \
  --conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v2 \
  --conf spark.sql.catalog.nessie.ref=main \
  --conf spark.sql.catalog.nessie.warehouse=s3://iceberg-warehouse/ \
  --conf spark.sql.catalog.nessie.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.nessie.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.nessie.s3.path-style-access=true \
  --conf spark.sql.catalog.nessie.s3.access-key-id=admin \
  --conf spark.sql.catalog.nessie.s3.secret-access-key=password123 \
  --conf spark.sql.catalog.nessie.s3.region=us-east-1 \
  /opt/spark-apps/cdc_to_iceberg.py
