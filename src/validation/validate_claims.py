import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, trim


# --------------------------------------------------
# Configure Python for PySpark on Windows
# --------------------------------------------------

validation_results = []


def add_validation_result(check_name, failed_records):

    status = "PASS" if failed_records == 0 else "FAIL"

    validation_results.append({
        "check_name": check_name,
        "failed_records": failed_records,
        "status": status
    })


python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


# --------------------------------------------------
# Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("HealthcareClaimsValidation")
    .master("local[*]")
    .getOrCreate()
)


# --------------------------------------------------
# Read Claims Data
# --------------------------------------------------

input_file = "data/simulated/medical_claims.csv"

df = spark.read.csv(
    input_file,
    header=True,
    inferSchema=True
)


# --------------------------------------------------
# Basic Data Inspection
# --------------------------------------------------

print("Claims Data:")

df.show(
    5,
    truncate=False
)

print("Schema:")

df.printSchema()

print("Total Records:", df.count())


# --------------------------------------------------
# Validate Duplicate Claim IDs
# --------------------------------------------------

duplicate_claims = (
    df.groupBy("claim_id")
      .count()
      .filter("count > 1")
)

duplicate_claim_count = duplicate_claims.count()

print("Duplicate Claim IDs:")

duplicate_claims.show(
    truncate=False
)

print(
    "Number of duplicate claim IDs:",
    duplicate_claim_count
)

add_validation_result(
    "Duplicate Claim IDs",
    duplicate_claim_count
)


# --------------------------------------------------
# Validate Invalid Patient IDs
# --------------------------------------------------

patients_file = "data/source/patients.csv"

patients_df = spark.read.csv(
    patients_file,
    header=True,
    inferSchema=True
)

invalid_patient_records = df.join(
    patients_df,
    df["patient_id"] == patients_df["patient_id"],
    "left_anti"
)

invalid_patient_count = invalid_patient_records.count()

print("Number of claims with invalid patient IDs:", invalid_patient_count)

print("Claims with invalid patient IDs:")

invalid_patient_records.show(
    10,
    truncate=False
)

add_validation_result(
    "Invalid Patient IDs",
    invalid_patient_count
)

# --------------------------------------------------
# Validate Invalid Provider IDs
# --------------------------------------------------

providers_file = "data/source/providers.csv"

providers_df = spark.read.csv(
    providers_file,
    header=True,
    inferSchema=True
)

invalid_provider_records = df.join(
    providers_df,
    df["provider_id"] == providers_df["provider_id"],
    "left_anti"
)

invalid_provider_count = invalid_provider_records.count()

print(
    "Number of claims with invalid provider IDs:",
    invalid_provider_count
)

print("Claims with invalid provider IDs:")

invalid_provider_records.show(
    10,
    truncate=False
)

add_validation_result(
    "Invalid Provider IDs",
    invalid_provider_count
)

# --------------------------------------------------
# Validate Provider-Hospital Consistency
# --------------------------------------------------

provider_hospital_df = providers_df.select(
    "provider_id",
    "hospital_id"
)

claim_provider_check = df.join(
    provider_hospital_df,
    df["provider_id"] == provider_hospital_df["provider_id"],
    "inner"
)

invalid_provider_hospital_records = claim_provider_check.filter(
    df["hospital_id"] != provider_hospital_df["hospital_id"]
)

mismatch_count = invalid_provider_hospital_records.count()

print(
    "Number of claims with provider-hospital mismatch:",
    mismatch_count
)

print("Claims with provider-hospital mismatch:")

invalid_provider_hospital_records.show(
    10,
    truncate=False
)

add_validation_result(
    "Provider-Hospital Mismatch",
    mismatch_count
)


# --------------------------------------------------
# Validate Invalid Hospital IDs
# --------------------------------------------------

hospitals_file = "data/source/hospitals.csv"

hospitals_df = spark.read.csv(
    hospitals_file,
    header=True,
    inferSchema=True
)

invalid_hospital_records = df.join(
    hospitals_df,
    df["hospital_id"] == hospitals_df["hospital_id"],
    "left_anti"
)

invalid_hospital_count = invalid_hospital_records.count()

print(
    "Number of claims with invalid hospital IDs:",
    invalid_hospital_count
)

print("Claims with invalid hospital IDs:")

invalid_hospital_records.show(
    10,
    truncate=False
)

add_validation_result(
    "Invalid Hospital IDs",
    invalid_hospital_count
)


# --------------------------------------------------
# Validate Claim Amount
# --------------------------------------------------

invalid_claim_amount_records = df.filter(
    col("claim_amount").isNull() |
    (col("claim_amount") <= 0)
)

invalid_claim_amount_count = invalid_claim_amount_records.count()

print(
    "Number of claims with invalid claim amount:",
    invalid_claim_amount_count
)

print("Claims with invalid claim amount:")

invalid_claim_amount_records.show(
    10,
    truncate=False
)

add_validation_result(
    "Invalid Claim Amount",
    invalid_claim_amount_count
)

# --------------------------------------------------
# Validate Claim Status
# --------------------------------------------------

valid_claim_statuses = [
    "Submitted",
    "Approved",
    "Rejected",
    "Pending"
]

invalid_claim_status_records = df.filter(
    ~col("claim_status").isin(valid_claim_statuses)
)

invalid_claim_status_count = invalid_claim_status_records.count()

print(
    "Number of claims with invalid claim status:",
    invalid_claim_status_count
)

print("Claims with invalid claim status:")

invalid_claim_status_records.show(
    10,
    truncate=False
)

add_validation_result(
    "Invalid Claim Status",
    invalid_claim_status_count
)

# --------------------------------------------------
# Validate Claim Type
# --------------------------------------------------

valid_claim_types = [
    "Inpatient",
    "Outpatient",
    "Pharmacy",
    "Diagnostic",
    "Emergency"
]

invalid_claim_type_records = df.filter(
    ~col("claim_type").isin(valid_claim_types)
)

invalid_claim_type_count = invalid_claim_type_records.count()

print(
    "Number of claims with invalid claim type:",
    invalid_claim_type_count
)

print("Claims with invalid claim type:")

invalid_claim_type_records.show(
    10,
    truncate=False
)

add_validation_result(
    "Invalid Claim Type",
    invalid_claim_type_count
)


# --------------------------------------------------
# Validate Required ID Fields
# --------------------------------------------------

required_columns = [
    "claim_id",
    "patient_id",
    "provider_id",
    "hospital_id"
]

for column_name in required_columns:

    missing_records = df.filter(
        col(column_name).isNull() |
        (trim(col(column_name)) == "")
    )

    missing_count = missing_records.count()

    print(
        f"Number of claims with missing {column_name}:",
        missing_count
    )

    print(
        f"Claims with missing {column_name}:"
    )

    missing_records.show(
        10,
        truncate=False
    )

    add_validation_result(
        f"Missing {column_name}",
        missing_count
    )


    # --------------------------------------------------
# Validate Required Claim Fields
# --------------------------------------------------

required_claim_fields = [
    "claim_date",
    "claim_amount",
    "claim_status",
    "claim_type"
]

for column_name in required_claim_fields:

    missing_records = df.filter(
        col(column_name).isNull()
    )

    missing_count = missing_records.count()

    print(
        f"Number of claims with missing {column_name}:",
        missing_count
    )

    print(
        f"Claims with missing {column_name}:"
    )

    missing_records.show(
        10,
        truncate=False
    )

    add_validation_result(
        f"Missing {column_name}",
        missing_count
    ) 


    # --------------------------------------------------
# Validate Future Claim Dates
# --------------------------------------------------

future_claim_date_records = df.filter(
    col("claim_date") > current_date()
)

future_claim_date_count = future_claim_date_records.count()

print(
    "Number of claims with future claim date:",
    future_claim_date_count
)

print("Claims with future claim date:")

future_claim_date_records.show(
    10,
    truncate=False
)

add_validation_result(
    "Future Claim Date",
    future_claim_date_count
)

# --------------------------------------------------
# Validation Summary
# --------------------------------------------------

print("\n========== VALIDATION SUMMARY ==========")

for result in validation_results:
    print(
        f"{result['check_name']}: "
        f"{result['failed_records']} "
        f"-> {result['status']}"
    )

overall_status = "PASS"

for result in validation_results:
    if result["status"] == "FAIL":
        overall_status = "FAIL"
        break

print(f"\nOverall Validation Status: {overall_status}")

if overall_status == "FAIL":
    spark.stop()
    sys.exit(1)

spark.stop()