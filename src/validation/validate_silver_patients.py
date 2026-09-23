from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = (
    SparkSession.builder
    .appName("ValidateSilverPatients")
    .master("local[*]")
    .getOrCreate()
)

input_path = "data/silver/patients"

df = spark.read.parquet(input_path)

print("======================================")
print("SILVER PATIENT VALIDATION")
print("======================================")

# 1. Total records
total_records = df.count()
print("Total Records:", total_records)

# 2. Duplicate patient IDs
duplicate_patients = (
    df.groupBy("patient_id")
      .count()
      .filter(col("count") > 1)
)

duplicate_count = duplicate_patients.count()

print("Duplicate Patient IDs:", duplicate_count)

if duplicate_count > 0:
    duplicate_patients.show()

# 3. Null patient IDs
null_patient_id = df.filter(
    col("patient_id").isNull()
).count()

print("Null Patient IDs:", null_patient_id)

# 4. Null patient names
null_patient_name = df.filter(
    col("patient_name").isNull()
).count()

print("Null Patient Names:", null_patient_name)

# 5. Null date of birth
null_dob = df.filter(
    col("date_of_birth").isNull()
).count()

print("Null Date of Birth:", null_dob)

# 6. Null registration date
null_registration = df.filter(
    col("registration_date").isNull()
).count()

print("Null Registration Date:", null_registration)

# 7. Invalid gender
invalid_gender = df.filter(
    ~col("gender").isin("M", "F")
).count()

print("Invalid Gender:", invalid_gender)

# 8. Final result
if (
    duplicate_count == 0
    and null_patient_id == 0
    and null_patient_name == 0
    and null_dob == 0
    and null_registration == 0
    and invalid_gender == 0
):
    print("======================================")
    print("SILVER VALIDATION PASSED")
    print("======================================")
else:
    print("======================================")
    print("SILVER VALIDATION FAILED")
    print("======================================")

spark.stop()