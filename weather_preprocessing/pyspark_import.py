import findspark
findspark.init()

from pyspark.sql.functions import input_file_name
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os


def main():
    spark = SparkSession.builder.appName("weather_preprocessing").getOrCreate()
    sc = spark.sparkContext

    session = SparkSession(sc)

    customSchema = StructType([
        StructField("STATION", StringType(), True),
        StructField("DATE", StringType(), True),
        StructField("LATITUDE", StringType(), True),
        StructField("LONGITUDE", StringType(), True),
        StructField("ELEVATION", StringType(), True),
        StructField("NAME", StringType(), True),
        StructField("TEMP", StringType(), True),
        StructField("TEMP_ATTRIBUTES", StringType(), True),
        StructField("DEWP", StringType(), True),
        StructField("DEWP_ATTRIBUTES", StringType(), True),
        StructField("SLP", StringType(), True),
        StructField("SLP_ATTRIBUTES", StringType(), True),
        StructField("STP", StringType(), True),
        StructField("STP_ATTRIBUTES", StringType(), True),
        StructField("VISIB", StringType(), True),
        StructField("VISIB_ATTRIBUTES", StringType(), True),
        StructField("WDSP", StringType(), True),
        StructField("WDSP_ATTRIBUTES", StringType(), True),
        StructField("MXSPD", StringType(), True),
        StructField("GUST", StringType(), True),
        StructField("MAX", StringType(), True),
        StructField("MAX_ATTRIBUTES", StringType(), True),
        StructField("MIN", StringType(), True),
        StructField("MIN_ATTRIBUTES", StringType(), True),
        StructField("PRCP", StringType(), True),
        StructField("PRCP_ATTRIBUTES", StringType(), True),
        StructField("SNDP", StringType(), True),
        StructField("FRSHTT", StringType(), True),
        StructField("filename", StringType(), True),
    ])

    fullPath = "datasets_mock/????/*.csv"

    df = spark.read.format("csv") \
        .option("header", False) \
        .option("sep", ",") \
        .schema(customSchema) \
        .load(fullPath) \
        .withColumn("filename", input_file_name())

    print(df.head(25))


if __name__ == "__main__":
    main()
