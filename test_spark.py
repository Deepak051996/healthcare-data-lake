import os
import sys

from pyspark.sql import SparkSession


# Use the exact Python executable currently running this script.
python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


spark = (
    SparkSession.builder
    .appName("HealthcareDataLakeTest")
    .master("local[*]")
    .getOrCreate()
)


data = [
    (1, "John", "M"),
    (2, "Sarah", "F"),
    (3, "David", "M"),
]

columns = ["patient_id", "patient_name", "gender"]

df = spark.createDataFrame(data, columns)

df.show()

df.printSchema()

spark.stop()