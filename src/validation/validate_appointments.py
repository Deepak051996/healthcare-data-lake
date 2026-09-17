import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date , trim


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
    .appName("HealthcareAppointmentValidation")
    .master("local[*]")
    .getOrCreate()
)


# --------------------------------------------------
# Read Appointment Data
# --------------------------------------------------

input_file = "data/simulated/appointments.csv"

df = spark.read.csv(
    input_file,
    header=True,
    inferSchema=True
)


# --------------------------------------------------
# Basic Data Inspection
# --------------------------------------------------

print("Appointment Data:")

df.show(5, truncate=False)

print("Schema:")

df.printSchema()

print("Total Records:", df.count())


# --------------------------------------------------
# Validate Duplicate Appointment IDs
# --------------------------------------------------

duplicate_appointments = (
    df.groupBy("appointment_id")
      .count()
      .filter("count > 1")
)

duplicate_count = duplicate_appointments.count()

print("Duplicate Appointment IDs:")

duplicate_appointments.show(truncate=False)

print("Number of duplicate appointment IDs:", duplicate_count)

add_validation_result(
    "Duplicate Appointment IDs",
    duplicate_count
)


# --------------------------------------------------
# Read Patient Master Data
# --------------------------------------------------

patient_file = "data/source/patients.csv"

patients_df = spark.read.csv(
    patient_file,
    header=True,
    inferSchema=True
)

print("Total Patients:", patients_df.count())


# --------------------------------------------------
# Validate Patient IDs
# --------------------------------------------------

invalid_patient_records = df.join(
    patients_df,
    df["patient_id"] == patients_df["patient_id"],
    "left_anti"
)

invalid_patient_count = invalid_patient_records.count()

print(
    "Number of appointments with invalid patient IDs:",
    invalid_patient_count
)

print("Appointments with invalid patient IDs:")

invalid_patient_records.show(10, truncate=False)

add_validation_result(
    "Invalid Patient IDs",
    invalid_patient_count
)


# --------------------------------------------------
# Read Provider Master Data
# --------------------------------------------------

provider_file = "data/source/providers.csv"

providers_df = spark.read.csv(
    provider_file,
    header=True,
    inferSchema=True
)

print("Total Providers:", providers_df.count())


# --------------------------------------------------
# Validate Provider IDs
# --------------------------------------------------

invalid_provider_records = df.join(
    providers_df,
    df["provider_id"] == providers_df["provider_id"],
    "left_anti"
)

invalid_provider_count = invalid_provider_records.count()

print(
    "Number of appointments with invalid provider IDs:",
    invalid_provider_count
)

print("Appointments with invalid provider IDs:")

invalid_provider_records.show(10, truncate=False)

add_validation_result(
    "Invalid Provider IDs",
    invalid_provider_count
)


# --------------------------------------------------
# Validate Provider-Hospital Relationship
# --------------------------------------------------

provider_hospital_df = providers_df.select(
    "provider_id",
    "hospital_id"
)

appointment_provider_check = df.join(
    provider_hospital_df,
    df["provider_id"] == provider_hospital_df["provider_id"],
    "inner"
)

invalid_provider_hospital_records = appointment_provider_check.filter(
    df["hospital_id"] != provider_hospital_df["hospital_id"]
)

mismatch_count = invalid_provider_hospital_records.count()

print(
    "Number of appointments with provider-hospital mismatch:",
    mismatch_count
)

print("Appointments with provider-hospital mismatch:")

invalid_provider_hospital_records.show(10, truncate=False)

add_validation_result(
    "Provider-Hospital Mismatch",
    mismatch_count
)


# --------------------------------------------------
# Read Hospital Master Data
# --------------------------------------------------

hospital_file = "data/source/hospitals.csv"

hospitals_df = spark.read.csv(
    hospital_file,
    header=True,
    inferSchema=True
)

print("Total Hospitals:", hospitals_df.count())


# --------------------------------------------------
# Validate Hospital IDs
# --------------------------------------------------

# --------------------------------------------------
# Validate Hospital IDs
# --------------------------------------------------

# --------------------------------------------------
# Validate Hospital IDs
# --------------------------------------------------

invalid_hospital_records = df.join(
    hospitals_df,
    df["hospital_id"] == hospitals_df["hospital_id"],
    "left_anti"
)

invalid_hospital_count = invalid_hospital_records.count()

print(
    "Number of appointments with invalid hospital IDs:",
    invalid_hospital_count
)

print("Appointments with invalid hospital IDs:")

invalid_hospital_records.show(10, truncate=False)

add_validation_result(
    "Invalid Hospital IDs",
    invalid_hospital_count
)

# --------------------------------------------------
# Validate Appointment Status
# --------------------------------------------------

valid_statuses = [
    "Scheduled",
    "Completed",
    "Cancelled",
    "No Show"
]

invalid_status_records = df.filter(
    ~col("appointment_status").isin(valid_statuses)
)

invalid_status_count = invalid_status_records.count()

print(
    "Number of appointments with invalid status:",
    invalid_status_count
)

print("Appointments with invalid status:")

invalid_status_records.show(10, truncate=False)

add_validation_result(
    "Invalid Appointment Status",
    invalid_status_count
)


# --------------------------------------------------
# Validate Missing Appointment Dates
# --------------------------------------------------

missing_appointment_date_records = df.filter(
    col("appointment_date").isNull()
)

missing_appointment_date_count = missing_appointment_date_records.count()

print(
    "Number of appointments with missing date:",
    missing_appointment_date_count
)

print("Appointments with missing date:")

missing_appointment_date_records.show(10, truncate=False)

add_validation_result(
    "Missing Appointment Date",
    missing_appointment_date_count
)


# --------------------------------------------------
# Validate Appointment Date vs Status
# --------------------------------------------------

# --------------------------------------------------
# Validation: Completed / No Show with Future Date
# --------------------------------------------------

invalid_date_status_records = df.filter(
    (col("appointment_status").isin("Completed", "No Show"))
    & (col("appointment_date") > current_date())
)

invalid_date_status_count = invalid_date_status_records.count()

print(
    "Number of completed/no-show appointments with future date:",
    invalid_date_status_count
)

print("Appointments with invalid date/status combination:")

invalid_date_status_records.show(
    10,
    truncate=False
)

add_validation_result(
    "Completed/No Show with Future Date",
    invalid_date_status_count
)


# --------------------------------------------------
# Validate Required Fields
# --------------------------------------------------


required_columns = [
    "appointment_id",
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
        f"Number of missing {column_name}: {missing_count}"
    )

    print(
        f"Appointments with missing {column_name}:"
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
# Validate Diagnosis
# --------------------------------------------------

missing_diagnosis_records = df.filter(
    col("diagnosis").isNull() |
    (trim(col("diagnosis")) == "")
)

missing_diagnosis_count = missing_diagnosis_records.count()

print(
    "Number of appointments with missing diagnosis:",
    missing_diagnosis_count
)

print("Appointments with missing diagnosis:")

missing_diagnosis_records.show(
    10,
    truncate=False
)

add_validation_result(
    "Missing Diagnosis",
    missing_diagnosis_count
)

# --------------------------------------------------
# Final Validation Summary
# --------------------------------------------------

print("\n========== VALIDATION SUMMARY ==========")

for result in validation_results:
    print(
        f"{result['check_name']}: "
        f"{result['failed_records']} "
        f"-> {result['status']}"
    )

# --------------------------------------------------
# Overall Validation Status
# --------------------------------------------------

overall_status = "PASS"

for result in validation_results:
    if result["status"] == "FAIL":
        overall_status = "FAIL"
        break

print(f"\nOverall Validation Status: {overall_status}")

# --------------------------------------------------
# Return validation status
# --------------------------------------------------

if overall_status == "FAIL":
    spark.stop()
    sys.exit(1)




# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()

