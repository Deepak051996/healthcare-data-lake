from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_date,
    year,
    month,
    floor,
    months_between
)

# --------------------------------------------------
# 1. Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("GoldPatientDimension")
    .master("local[*]")
    .getOrCreate()
)

# --------------------------------------------------
# 2. Paths
# --------------------------------------------------

input_path = "data/silver/patients"
output_path = "data/gold/dim_patient"

# --------------------------------------------------
# 3. Read Silver Data
# --------------------------------------------------

patients_df = spark.read.parquet(input_path)

print("Silver Records:", patients_df.count())

# --------------------------------------------------
# 4. Calculate Patient Age
# --------------------------------------------------

patients_df = patients_df.withColumn(
    "age",
    floor(
        months_between(
            current_date(),
            col("date_of_birth")
        ) / 12
    ).cast("int")
)

# --------------------------------------------------
# 5. Add Registration Year and Month
# --------------------------------------------------

patients_df = (
    patients_df
    .withColumn(
        "registration_year",
        year(col("registration_date"))
    )
    .withColumn(
        "registration_month",
        month(col("registration_date"))
    )
)

# --------------------------------------------------
# 6. Select Business Columns
# --------------------------------------------------

gold_df = patients_df.select(
    "patient_id",
    "patient_name",
    "gender",
    "date_of_birth",
    "age",
    "phone",
    "email",
    "city",
    "state",
    "registration_date",
    "registration_year",
    "registration_month"
)

# --------------------------------------------------
# 7. Show Gold Data
# --------------------------------------------------

print("Gold Patient Dimension:")

gold_df.show(5, truncate=False)

print("Gold Schema:")

gold_df.printSchema()

print("Gold Records:", gold_df.count())

# --------------------------------------------------
# 8. Write Gold Data
# --------------------------------------------------

(
    gold_df
    .write
    .mode("overwrite")
    .partitionBy(
        "registration_year",
        "registration_month"
    )
    .parquet(output_path)
)

print("Gold data written successfully.")
print("Output path:", output_path)

# --------------------------------------------------
# 9. Stop Spark
# --------------------------------------------------

spark.stop()