import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configure Python for PySpark on Windows

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


# Create Spark Session

spark = (
    SparkSession.builder
    .appName("HealthcareHospitalValidation")
    .master("local[*]")
    .getOrCreate()
)


# Read Hospital Data

input_file = "data/source/hospitals.csv"

df = spark.read.csv(
    input_file,
    header=True,
    inferSchema=True
)


# Basic Data Inspection

print("Hospital Data:")
df.show(5, truncate=False)

print("Schema:")
df.printSchema()

print("Total Records:", df.count())

# Detect duplicate hospital IDs

duplicate_hospitals = (
    df.groupBy("hospital_id")
      .count()
      .filter("count > 1")
)

print("Duplicate Hospital IDs:")
duplicate_hospitals.show(truncate=False)

print(
    "Number of duplicate hospital IDs:",
    duplicate_hospitals.count()
)

# Detect missing hospital IDs

missing_hospital_id_records = df.filter(
    col("hospital_id").isNull()
)

print(
    "Number of missing hospital IDs:",
    missing_hospital_id_records.count()
)

print("Records with missing hospital ID:")
missing_hospital_id_records.show(5, truncate=False)

# Detect invalid bed count

invalid_bed_count_records = df.filter(
    col("bed_count") < 0
)

print(
    "Number of hospitals with invalid bed count:",
    invalid_bed_count_records.count()
)

print("Hospitals with invalid bed count:")
invalid_bed_count_records.show(5, truncate=False)

# Validate hospital type

valid_hospital_types = [
    "Private",
    "Government",
    "Trust"
]

invalid_hospital_type_records = df.filter(
    ~col("hospital_type").isin(valid_hospital_types)
)

print(
    "Number of hospitals with invalid hospital type:",
    invalid_hospital_type_records.count()
)

print("Hospitals with invalid hospital type:")
invalid_hospital_type_records.show(5, truncate=False)

# Stop Spark

spark.stop()