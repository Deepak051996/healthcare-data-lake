import os
import sys

from pyspark.sql.functions import current_timestamp, input_file_name

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType
)


# --------------------------------------------------
# Configure Python for PySpark
# --------------------------------------------------

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


# --------------------------------------------------
# Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("BronzePatients")
    .master("local[*]")
    .getOrCreate()
)

# --------------------------------------------------
# Define Patient Schema
# --------------------------------------------------

patient_schema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("patient_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("phone", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("registration_date", DateType(), True)
])


# --------------------------------------------------
# Read Source CSV
# --------------------------------------------------

input_file = "data/source/patients.csv"

patients_df = (
    spark.read
    .option("header", True)
    .schema(patient_schema)
    .csv(input_file)
)

patients_df = patients_df.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)

patients_df = patients_df.withColumn(
    "source_file",
    input_file_name()
)


# --------------------------------------------------
# Validate Input
# --------------------------------------------------

print("Patients Bronze Input:")

patients_df.show(5, truncate=False)

print("Patients Schema:")

patients_df.printSchema()

print("Total Records:", patients_df.count())

# ---------------------------------------------------

output_path = "data/bronze/patients"

patients_df.write \
    .mode("overwrite") \
    .parquet(output_path)

print("Bronze data written successfully.")
print("Output path:", output_path)


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()