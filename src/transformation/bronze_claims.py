import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType
)

# --------------------------------------------------
# Python environment
# --------------------------------------------------

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


# --------------------------------------------------
# Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("BronzeClaims")
    .master("local[*]")
    .getOrCreate()
)


# --------------------------------------------------
# Input / Output
# --------------------------------------------------

input_file = "data/source/medical_claims.csv"
output_path = "data/bronze/claims"


# --------------------------------------------------
# Schema
# --------------------------------------------------

claims_schema = StructType([
    StructField("claim_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("provider_id", StringType(), True),
    StructField("hospital_id", StringType(), True),
    StructField("claim_date", DateType(), True),
    StructField("claim_amount", DoubleType(), True),
    StructField("claim_status", StringType(), True),
    StructField("claim_type", StringType(), True)
])


# --------------------------------------------------
# Read source data
# --------------------------------------------------

claims_df = (
    spark.read
    .option("header", True)
    .schema(claims_schema)
    .csv(input_file)
)


# --------------------------------------------------
# Add ingestion metadata
# --------------------------------------------------

claims_df = (
    claims_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
)


# --------------------------------------------------
# Display Bronze input
# --------------------------------------------------

print("======================================")
print("BRONZE CLAIMS")
print("======================================")

print("Claims Data:")
claims_df.show(5, truncate=False)

print("Claims Schema:")
claims_df.printSchema()

print("Total Records:", claims_df.count())


# --------------------------------------------------
# Write Bronze data
# --------------------------------------------------

(
    claims_df
    .write
    .mode("overwrite")
    .parquet(output_path)
)

print("Bronze claims data written successfully.")
print("Output path:", output_path)


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()