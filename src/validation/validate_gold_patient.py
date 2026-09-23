from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("ValidateGoldPatient")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.parquet("data/gold/dim_patient")

print("======================================")
print("GOLD PATIENT DIMENSION VALIDATION")
print("======================================")

total_records = df.count()
print("Total Records:", total_records)

duplicate_patient_ids = (
    df.groupBy("patient_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_count = duplicate_patient_ids.count()
print("Duplicate Patient IDs:", duplicate_count)

null_patient_ids = df.filter(col("patient_id").isNull()).count()
print("Null Patient IDs:", null_patient_ids)

null_patient_names = df.filter(col("patient_name").isNull()).count()
print("Null Patient Names:", null_patient_names)

invalid_age = df.filter(
    (col("age") < 0) | (col("age") > 120)
).count()

print("Invalid Age:", invalid_age)

invalid_registration_year = df.filter(
    (col("registration_year") < 2000) |
    (col("registration_year") > 2030)
).count()

print("Invalid Registration Year:", invalid_registration_year)

print("======================================")

if (
    total_records > 0
    and duplicate_count == 0
    and null_patient_ids == 0
    and null_patient_names == 0
    and invalid_age == 0
    and invalid_registration_year == 0
):
    print("GOLD PATIENT VALIDATION PASSED")
else:
    print("GOLD PATIENT VALIDATION FAILED")

print("======================================")

spark.stop()