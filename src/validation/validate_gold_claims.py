from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("ValidateGoldClaims")
    .master("local[*]")
    .getOrCreate()
)

input_path = "data/gold/fact_claims"

df = spark.read.parquet(input_path)

print("=" * 40)
print("GOLD CLAIMS FACT VALIDATION")
print("=" * 40)

# Total records
total_records = df.count()
print("Total Records:", total_records)

# Duplicate Claim IDs
duplicate_claims = (
    df.groupBy("claim_id")
      .count()
      .filter(col("count") > 1)
      .count()
)

print("Duplicate Claim IDs:", duplicate_claims)

# Null checks
null_claim_ids = df.filter(col("claim_id").isNull()).count()
null_patient_ids = df.filter(col("patient_id").isNull()).count()
null_claim_dates = df.filter(col("claim_date").isNull()).count()
null_claim_amounts = df.filter(col("claim_amount").isNull()).count()

print("Null Claim IDs:", null_claim_ids)
print("Null Patient IDs:", null_patient_ids)
print("Null Claim Dates:", null_claim_dates)
print("Null Claim Amounts:", null_claim_amounts)

# Invalid claim amounts
invalid_amounts = (
    df.filter(col("claim_amount") < 0)
      .count()
)

print("Invalid Claim Amounts:", invalid_amounts)

# Invalid claim status
valid_statuses = [
    "APPROVED",
    "REJECTED",
    "SUBMITTED",
    "PENDING"
]

invalid_status = (
    df.filter(~col("claim_status").isin(valid_statuses))
      .count()
)

print("Invalid Claim Status:", invalid_status)

# Invalid claim type
valid_types = [
    "EMERGENCY",
    "PHARMACY",
    "DIAGNOSTIC",
    "INPATIENT",
    "OUTPATIENT"
]

invalid_type = (
    df.filter(~col("claim_type").isin(valid_types))
      .count()
)

print("Invalid Claim Type:", invalid_type)

# Invalid amount category
valid_categories = ["LOW", "MEDIUM", "HIGH"]

invalid_category = (
    df.filter(~col("claim_amount_category").isin(valid_categories))
      .count()
)

print("Invalid Claim Amount Category:", invalid_category)

# Invalid approval flag
invalid_approved_flag = (
    df.filter(~col("is_approved").isin([0, 1]))
      .count()
)

print("Invalid Approved Flag:", invalid_approved_flag)

# Invalid rejection flag
invalid_rejected_flag = (
    df.filter(~col("is_rejected").isin([0, 1]))
      .count()
)

print("Invalid Rejected Flag:", invalid_rejected_flag)

print("=" * 40)

# Final validation
if (
    total_records == 30000
    and duplicate_claims == 0
    and null_claim_ids == 0
    and null_patient_ids == 0
    and null_claim_dates == 0
    and null_claim_amounts == 0
    and invalid_amounts == 0
    and invalid_status == 0
    and invalid_type == 0
    and invalid_category == 0
    and invalid_approved_flag == 0
    and invalid_rejected_flag == 0
):
    print("GOLD CLAIMS FACT VALIDATION PASSED")
else:
    print("GOLD CLAIMS FACT VALIDATION FAILED")

print("=" * 40)

spark.stop()