import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# --------------------------------------------------
# Configure Python for PySpark on Windows
# --------------------------------------------------

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


# --------------------------------------------------
# Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("HealthcareProviderValidation")
    .master("local[*]")
    .getOrCreate()
)


# --------------------------------------------------
# Read Provider Data
# --------------------------------------------------

input_file = "data/simulated/providers.csv"

df = spark.read.csv(
    input_file,
    header=True,
    inferSchema=True
)


# --------------------------------------------------
# Basic Data Inspection
# --------------------------------------------------

print("Provider Data:")
df.show(5, truncate=False)

print("Schema:")
df.printSchema()

print("Total Records:", df.count())


# --------------------------------------------------
# 1. Detect Duplicate Provider IDs
# --------------------------------------------------

duplicate_providers = (
    df.groupBy("provider_id")
      .count()
      .filter("count > 1")
)

print("Duplicate Provider IDs:")
duplicate_providers.show(truncate=False)

print(
    "Number of duplicate provider IDs:",
    duplicate_providers.count()
)


# --------------------------------------------------
# 2. Detect Missing Specialization
# --------------------------------------------------

missing_specialization_records = df.filter(
    col("specialization").isNull()
)

print(
    "Number of missing specializations:",
    missing_specialization_records.count()
)

print("Records with missing specialization:")
missing_specialization_records.show(
    5,
    truncate=False
)


# --------------------------------------------------
# 3. Read Hospital Master Data
# --------------------------------------------------

hospital_file = "data/source/hospitals.csv"

hospitals_df = spark.read.csv(
    hospital_file,
    header=True,
    inferSchema=True
)

print("Total Hospitals:", hospitals_df.count())


# --------------------------------------------------
# 4. Detect Invalid Hospital IDs
# --------------------------------------------------

invalid_hospital_records = df.join(
    hospitals_df,
    df["hospital_id"] == hospitals_df["hospital_id"],
    "left_anti"
)

print(
    "Number of providers with invalid hospital IDs:",
    invalid_hospital_records.count()
)

print("Providers with invalid hospital IDs:")
invalid_hospital_records.show(
    10,
    truncate=False
)

# Detect missing provider IDs

missing_provider_id_records = df.filter(
    col("provider_id").isNull()
)

print(
    "Number of missing provider IDs:",
    missing_provider_id_records.count()
)

print("Records with missing provider ID:")
missing_provider_id_records.show(5, truncate=False)

# Detect invalid experience years

invalid_experience_records = df.filter(
    col("experience_years") < 0
)

print(
    "Number of providers with invalid experience:",
    invalid_experience_records.count()
)

print("Providers with invalid experience:")
invalid_experience_records.show(5, truncate=False)
# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()