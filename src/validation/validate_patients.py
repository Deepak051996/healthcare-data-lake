import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,current_date


# Use the current Python environment for PySpark
python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


spark = (
    SparkSession.builder
    .appName("HealthcarePatientValidation")
    .master("local[*]")
    .getOrCreate()
)


input_file = "data/simulated/patients.csv"

df = spark.read.csv(
    input_file,
    header=True,
    inferSchema=True
)


print("Patient Data:")
df.show(5, truncate=False)

print("Schema:")
df.printSchema()

print("Total Records:", df.count())

# Detect duplicate patient IDs

duplicate_patients = (
    df.groupBy("patient_id")
      .count()
      .filter("count > 1")
)

print("Duplicate Patient IDs:")
duplicate_patients.show(truncate=False)

print(
    "Number of duplicate patient IDs:",
    duplicate_patients.count()
)

# Detect missing email values

missing_email_records = df.filter(
    col("email").isNull()
)

print(
    "Number of missing emails:",
    missing_email_records.count()
)

print("Records with missing email:")
missing_email_records.show(5, truncate=False)

# Detect invalid phone numbers

invalid_phone_records = df.filter(
    ~col("phone").rlike("^[0-9]{10}$")
)

print(
    "Number of invalid phone numbers:",
    invalid_phone_records.count()
)

print("Records with invalid phone numbers:")
invalid_phone_records.show(5, truncate=False)

# Detect missing patient IDs

missing_patient_id_records = df.filter(
    col("patient_id").isNull()
)

print(
    "Number of missing patient IDs:",
    missing_patient_id_records.count()
)

print("Records with missing patient ID:")
missing_patient_id_records.show(5, truncate=False)

# Detect future date of birth

future_dob_records = df.filter(
    col("date_of_birth") > current_date()
)

print(
    "Number of future date of birth records:",
    future_dob_records.count()
)

print("Records with future date of birth:")
future_dob_records.show(5, truncate=False)

spark.stop()