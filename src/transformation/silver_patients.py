from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lower
)

# --------------------------------------------------
# 1. Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("SilverPatients")
    .master("local[*]")
    .getOrCreate()
)

# --------------------------------------------------
# 2. Paths
# --------------------------------------------------

input_path = "data/bronze/patients"
output_path = "data/silver/patients"

# --------------------------------------------------
# 3. Read Bronze Data
# --------------------------------------------------

patients_df = spark.read.parquet(input_path)

print("Bronze Records:", patients_df.count())

# --------------------------------------------------
# 4. Clean String Columns
# --------------------------------------------------

patients_df = (
    patients_df
    .withColumn("patient_id", trim(col("patient_id")))
    .withColumn("patient_name", trim(col("patient_name")))
    .withColumn("gender", upper(trim(col("gender"))))
    .withColumn("phone", trim(col("phone")))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))
)

# --------------------------------------------------
# 5. Remove Duplicate Patients
# --------------------------------------------------

patients_df = patients_df.dropDuplicates(["patient_id"])

# --------------------------------------------------
# 6. Remove Invalid Records
# --------------------------------------------------

patients_df = patients_df.filter(
    col("patient_id").isNotNull()
    & col("patient_name").isNotNull()
    & col("date_of_birth").isNotNull()
    & col("registration_date").isNotNull()
)

# --------------------------------------------------
# 7. Show Silver Data
# --------------------------------------------------

print("Silver Patients Data:")

patients_df.show(5, truncate=False)

print("Silver Schema:")

patients_df.printSchema()

print("Silver Records:", patients_df.count())

# --------------------------------------------------
# 8. Write Silver Data
# --------------------------------------------------

(
    patients_df
    .write
    .mode("overwrite")
    .parquet(output_path)
)

print("Silver data written successfully.")
print("Output path:", output_path)

# --------------------------------------------------
# 9. Stop Spark
# --------------------------------------------------

spark.stop()