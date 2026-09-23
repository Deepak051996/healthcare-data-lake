from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("ValidateSilverClaims")
    .master("local[*]")
    .getOrCreate()
)

input_path = "data/silver/claims"

df = spark.read.parquet(input_path)

print("=" * 40)
print("SILVER CLAIMS VALIDATION")
print("=" * 40)

# 1. Total records
total_records = df.count()
print("Total Records:", total_records)

# 2. Duplicate Claim IDs
duplicate_claims = (
    df.groupBy("claim_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate Claim IDs:", duplicate_claims)

# 3. Null Claim IDs
null_claim_ids = df.filter(col("claim_id").isNull()).count()
print("Null Claim IDs:", null_claim_ids)

# 4. Null Patient IDs
null_patient_ids = df.filter(col("patient_id").isNull()).count()
print("Null Patient IDs:", null_patient_ids)

# 5. Null Claim Dates
null_claim_dates = df.filter(col("claim_date").isNull()).count()
print("Null Claim Dates:", null_claim_dates)

# 6. Null Claim Amounts
null_claim_amounts = df.filter(col("claim_amount").isNull()).count()
print("Null Claim Amounts:", null_claim_amounts)

# 7. Invalid Claim Amounts
invalid_amounts = df.filter(
    col("claim_amount") <= 0
).count()

print("Invalid Claim Amounts:", invalid_amounts)

# 8. Invalid Claim Status
valid_statuses = ["APPROVED", "SUBMITTED", "REJECTED", "PENDING"]

invalid_status = df.filter(
    ~col("claim_status").isin(valid_statuses)
).count()

print("Invalid Claim Status:", invalid_status)

# 9. Invalid Claim Type
valid_types = [
    "EMERGENCY",
    "PHARMACY",
    "DIAGNOSTIC",
    "INPATIENT",
    "OUTPATIENT"
]

invalid_claim_type = df.filter(
    ~col("claim_type").isin(valid_types)
).count()

print("Invalid Claim Type:", invalid_claim_type)

print("=" * 40)

# Final validation
if (
    duplicate_claims == 0
    and null_claim_ids == 0
    and null_patient_ids == 0
    and null_claim_dates == 0
    and null_claim_amounts == 0
    and invalid_amounts == 0
    and invalid_status == 0
    and invalid_claim_type == 0
):
    print("SILVER CLAIMS VALIDATION PASSED")
else:
    print("SILVER CLAIMS VALIDATION FAILED")

print("=" * 40)

spark.stop()