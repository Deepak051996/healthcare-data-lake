import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    round,
    current_timestamp
)

# --------------------------------------------------
# Python / Spark configuration
# --------------------------------------------------

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


# --------------------------------------------------
# Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("GoldClaimsFact")
    .master("local[*]")
    .getOrCreate()
)


# --------------------------------------------------
# Input paths
# --------------------------------------------------

claims_path = "data/silver/claims"
patient_path = "data/gold/dim_patient"

output_path = "data/gold/fact_claims"


# --------------------------------------------------
# Read Silver Claims
# --------------------------------------------------

claims_df = spark.read.parquet(claims_path)

print("=" * 40)
print("GOLD CLAIMS FACT")
print("=" * 40)

print("Silver Claims Records:", claims_df.count())


# --------------------------------------------------
# Read Gold Patient Dimension
# --------------------------------------------------

patient_df = spark.read.parquet(patient_path)

print("Gold Patient Dimension Records:", patient_df.count())


# --------------------------------------------------
# Select required patient columns
# --------------------------------------------------

patient_lookup = patient_df.select(
    "patient_id",
    "patient_name",
    "gender",
    "age",
    "city",
    "state"
)


# --------------------------------------------------
# Join Claims with Patient Dimension
# --------------------------------------------------

fact_df = (
    claims_df
    .join(
        patient_lookup,
        on="patient_id",
        how="left"
    )
)


# --------------------------------------------------
# Create business columns
# --------------------------------------------------

fact_df = (
    fact_df

    # Claim amount rounded to 2 decimal places
    .withColumn(
        "claim_amount",
        round(col("claim_amount"), 2)
    )

    # Claim amount category
    .withColumn(
        "claim_amount_category",
        when(col("claim_amount") < 10000, "LOW")
        .when(
            (col("claim_amount") >= 10000) &
            (col("claim_amount") < 100000),
            "MEDIUM"
        )
        .when(
            col("claim_amount") >= 100000,
            "HIGH"
        )
        .otherwise("UNKNOWN")
    )

    # Approved flag
    .withColumn(
        "is_approved",
        when(col("claim_status") == "APPROVED", 1)
        .otherwise(0)
    )

    # Rejected flag
    .withColumn(
        "is_rejected",
        when(col("claim_status") == "REJECTED", 1)
        .otherwise(0)
    )

    # Gold processing timestamp
    .withColumn(
        "gold_ingestion_timestamp",
        current_timestamp()
    )
)


# --------------------------------------------------
# Select final Gold columns
# --------------------------------------------------

fact_df = fact_df.select(
    "claim_id",
    "patient_id",
    "patient_name",
    "gender",
    "age",
    "city",
    "state",
    "provider_id",
    "hospital_id",
    "claim_date",
    "claim_year",
    "claim_month",
    "claim_amount",
    "claim_amount_category",
    "claim_status",
    "claim_type",
    "is_approved",
    "is_rejected",
    "ingestion_timestamp",
    "source_file",
    "gold_ingestion_timestamp"
)


# --------------------------------------------------
# Display Gold data
# --------------------------------------------------

print("Gold Claims Fact:")

fact_df.show(
    5,
    truncate=False
)


# --------------------------------------------------
# Display Schema
# --------------------------------------------------

print("Gold Claims Fact Schema:")

fact_df.printSchema()


# --------------------------------------------------
# Record Count
# --------------------------------------------------

gold_count = fact_df.count()

print("Gold Records:", gold_count)


# --------------------------------------------------
# Write Gold Fact Table
# --------------------------------------------------

(
    fact_df
    .write
    .mode("overwrite")
    .parquet(output_path)
)


print("=" * 40)
print("Gold claims fact data written successfully.")
print("Output path:", output_path)
print("=" * 40)


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()