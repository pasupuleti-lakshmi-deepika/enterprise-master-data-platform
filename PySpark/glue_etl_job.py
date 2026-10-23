"""
Enterprise Master Data Integration Platform - AWS Glue ETL Job (PySpark)

Hands-on project (DataMites training): consolidates customer, product and
supplier datasets from an Amazon S3 raw zone into Snowflake staging tables.

Flow:
    S3 raw CSVs -> clean & standardize (PySpark) -> dedupe -> Snowflake staging

Run in AWS Glue with the Snowflake Spark connector, or locally with PySpark
by replacing the S3 paths with local CSV paths.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, upper

# ---------------------------------------------------------------------------
# Config - replace the placeholder values with your own bucket / warehouse
# ---------------------------------------------------------------------------
S3_RAW = "s3://my-master-data-bucket/raw"
SNOWFLAKE_OPTIONS = {
    "sfURL": "my_account.snowflakecomputing.com",
    "sfUser": "MY_USER",
    "sfPassword": "MY_PASSWORD",  # use AWS Secrets Manager in a real Glue job
    "sfDatabase": "MASTER_DATA_DW",
    "sfSchema": "STAGING",
    "sfWarehouse": "COMPUTE_WH",
}


def clean_customers(df):
    """Standardize customer records and drop exact duplicates."""
    return (
        df.select(
            col("customer_id").cast("int").alias("customer_id"),
            trim(col("customer_name")).alias("customer_name"),
            lower(trim(col("email"))).alias("email"),
            trim(col("city")).alias("city"),
            upper(trim(col("country"))).alias("country"),
        )
        .filter(col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )


def clean_products(df):
    """Standardize product records and drop exact duplicates."""
    return (
        df.select(
            col("product_id").cast("int").alias("product_id"),
            trim(col("product_name")).alias("product_name"),
            trim(col("category")).alias("category"),
            col("unit_price").cast("double").alias("unit_price"),
        )
        .filter(col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
    )


def clean_suppliers(df):
    """Standardize supplier records and drop exact duplicates."""
    return (
        df.select(
            col("supplier_id").cast("int").alias("supplier_id"),
            trim(col("supplier_name")).alias("supplier_name"),
            lower(trim(col("contact_email"))).alias("contact_email"),
            upper(trim(col("country"))).alias("country"),
        )
        .filter(col("supplier_id").isNotNull())
        .dropDuplicates(["supplier_id"])
    )


def write_to_snowflake(df, table):
    """Load a cleaned DataFrame into a Snowflake staging table."""
    (
        df.write.format("snowflake")
        .options(**SNOWFLAKE_OPTIONS)
        .option("dbtable", table)
        .mode("overwrite")
        .save()
    )
    print(f"Loaded {df.count()} rows into STAGING.{table}")


def main():
    spark = SparkSession.builder.appName("master-data-glue-etl").getOrCreate()

    customers = clean_customers(spark.read.csv(f"{S3_RAW}/customers.csv", header=True))
    products = clean_products(spark.read.csv(f"{S3_RAW}/products.csv", header=True))
    suppliers = clean_suppliers(spark.read.csv(f"{S3_RAW}/suppliers.csv", header=True))

    write_to_snowflake(customers, "STG_CUSTOMERS")
    write_to_snowflake(products, "STG_PRODUCTS")
    write_to_snowflake(suppliers, "STG_SUPPLIERS")

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
