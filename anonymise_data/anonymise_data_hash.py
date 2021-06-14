import argparse
import time

from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructType, StructField, StringType


def anonymise_csv(input_csv: str, output_csv: str):
    """
    Function is the Python application code for spark which transforms PII data with random data
    :param input_csv: the input csv file path
    :param output_csv: the output csv file path
    """

    spark = SparkSession \
        .builder \
        .getOrCreate()

    csv_schema = StructType([
        StructField(name="first_name", dataType=StringType(), nullable=True),
        StructField(name="last_name", dataType=StringType(), nullable=True),
        StructField(name="address", dataType=StringType(), nullable=True),
        StructField(name="date_of_birth", dataType=StringType(), nullable=True)
    ])

    input_df = spark.read.schema(schema=csv_schema).csv(input_csv, header=True)

    anonymise_df = input_df.withColumn("first_name", f.hash("first_name")) \
        .withColumn("last_name", f.hash("last_name")).withColumn("address", f.hash("address"))

    anonymise_df.write.format("csv").option("header", True) \
        .mode("overwrite").option("sep", ",") \
        .save(output_csv)
    spark.stop()


# Couldn't time it, as I am running on personal windows laptop
startTime = time.time()

# Parse application arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_csv", required=True, type=str)
parser.add_argument("--output_csv", required=True, type=str)
args = parser.parse_args()

# Run the spark code
anonymise_csv(input_csv=args.input_csv, output_csv=args.output_csv)

runTime = time.time() - startTime
print(f"{runTime}")
