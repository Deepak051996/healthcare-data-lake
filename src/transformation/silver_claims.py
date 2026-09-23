import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    year,
    month,
    trim,
    upper
)


# ============================================================
# PYSPARK CONFIGURATION
# ============================================================

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


spark = (
    SparkSession.builder
    .appName("SilverClaims")
    .master("local[*]")
    .getOrCreate()
)


# ============================================================
# PATHS
# ============================================================

INPUT_PATH = "data/bronze/claims"
OUTPUT_PATH = "data/silver/claims"


# ============================================================
# READ BRONZE CLAIMS
# ============================================================

claims_df = spark.read.parquet(INPUT_PATH)

print("======================================")
print("SILVER CLAIMS TRANSFORMATION")
print("======================================")

print("Bronze Records:", claims_df.count())


# ============================================================
# 1. REMOVE DUPLICATE CLAIMS
# ============================================================

claims_df = claims_df.dropDuplicates(["claim_id"])


# ============================================================
# 2. REMOVE RECORDS WITH CRITICAL NULL VALUES
# ============================================================

claims_df = claims_df.filter(
    col("claim_id").isNotNull()
    & col("patient_id").isNotNull()
    & col("provider_id").isNotNull()
    & col("hospital_id").isNotNull()
    & col("claim_date").isNotNull()
)


# ============================================================
# 3. CLEAN STRING COLUMNS
# ============================================================

claims_df = (
    claims_df
    .withColumn("claim_id", trim(col("claim_id")))
    .withColumn("patient_id", trim(col("patient_id")))
    .withColumn("provider_id", trim(col("provider_id")))
    .withColumn("hospital_id", trim(col("hospital_id")))
    .withColumn("claim_status", trim(col("claim_status")))
    .withColumn("claim_type", trim(col("claim_type")))
)


# ============================================================
# 4. STANDARDIZE STATUS
# ============================================================

claims_df = claims_df.withColumn(
    "claim_status",
    upper(col("claim_status"))
)


# ============================================================
# 5. STANDARDIZE CLAIM TYPE
# ============================================================

claims_df = claims_df.withColumn(
    "claim_type",
    upper(col("claim_type"))
)


# ============================================================
# 6. VALIDATE CLAIM AMOUNT
# ============================================================

claims_df = claims_df.filter(
    col("claim_amount").isNotNull()
    & (col("claim_amount") >= 0)
)


# ============================================================
# 7. ADD CLAIM YEAR
# ============================================================

claims_df = claims_df.withColumn(
    "claim_year",
    year(col("claim_date"))
)


# ============================================================
# 8. ADD CLAIM MONTH
# ============================================================

claims_df = claims_df.withColumn(
    "claim_month",
    month(col("claim_date"))
)


# ============================================================
# 9. ADD SILVER INGESTION TIMESTAMP
# ============================================================

claims_df = claims_df.withColumn(
    "silver_ingestion_timestamp",
    current_timestamp()
)


# ============================================================
# SHOW SILVER DATA
# ============================================================

print("Silver Claims Data:")

claims_df.show(
    5,
    truncate=False
)


# ============================================================
# PRINT SCHEMA
# ============================================================

print("Silver Claims Schema:")

claims_df.printSchema()


# ============================================================
# COUNT RECORDS
# ============================================================

print(
    "Silver Records:",
    claims_df.count()
)


# ============================================================
# WRITE SILVER DATA
# ============================================================

(
    claims_df
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)


print("======================================")
print("Silver claims data written successfully.")
print("Output path:", OUTPUT_PATH)
print("======================================")


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()