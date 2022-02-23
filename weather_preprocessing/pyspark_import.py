import datetime
import time
from itertools import product

import findspark

findspark.init()

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
import pyspark.sql.functions as F
import csv


def main():
    spark = SparkSession.builder.appName("weather_preprocessing").getOrCreate()
    sc = spark.sparkContext
    start = time.time()

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
    ])

    fullPath = "weather_preprocessing/datasets/????/*.csv"

    df = spark.read.format("csv") \
        .option("header", True) \
        .option("sep", ",") \
        .schema(customSchema) \
        .load(fullPath)
    print(f"Data reading took {time.time() - start:.2f}s")
    start = time.time()

    df = clean_weather_data(df)
    print(f"Data cleaning took {time.time() - start:.2f}s")
    start = time.time()

    # Write cleaned data to a file
    print(f"Writing {df.count()} rows to file")
    df.write.mode('overwrite').parquet('clean_data/weather.parquet')
    print(f"Data writing took {time.time() - start:.2f}s")
    return


def clean_weather_data(df):
    """
    According to the following table, remove stations with out-of-range observations.
    # STATION           any
    # DATE     			any
    # TEMP     			not 9999.9
    # TEMP_ATTRIBUTES 	> 0
    # DEWP				not 9999.9
    # DEWP_ATTRIBUTES 	> 0
    # SLP				not 9999.9
    # SLP_ATTRIBUTES 		> 0
    # STP				not 9999.9
    # STP_ATTRIBUTES 		> 0
    # VISIB				not 999.9
    # VISIB_ATTRIBUTES	> 0
    # WDSP				not 999.9
    # WDSP_ATTRIBUTES	> 0
    # MXSPD				not 999.9
    # GUST				not 999.9
    # MAX				not 9999.9
    # MAX_ATTRIBUTES    any
    # MIN				not 9999.9
    # MIN_ATTRIBUTES    any
    # PRCP				not negative
    # PRCP_ATTRIBUTES	not "I"
    """
    # Remove unnamed columns
    df = df.drop(*["LATITUDE", "LONGITUDE", "ELEVATION", "NAME", "SNDP", "FRSHTT"])
    # Find the maximum number of records if a station recorded every single day
    first_date, last_date = df.agg(F.min(df.DATE), F.max(df.DATE)).head()
    max_records = (last_date - first_date).days
    print(f"Full dataset: {df.count()} rows")
    print(f"Number of stations: {df.select('STATION').distinct().count()}")
    print(f"Total number of days in range: {max_records}")
    df = df.filter(
        (df.TEMP != "9999.9") &
        (df.TEMP_ATTRIBUTES != "0") &
        (df.DEWP != "9999.9") &
        (df.DEWP_ATTRIBUTES != "0") &
        (df.SLP != "9999.9") &
        (df.SLP_ATTRIBUTES != "0") &
        (df.STP != "9999.9") &
        (df.STP_ATTRIBUTES != "0") &
        (df.VISIB != "999.9") &
        (df.VISIB_ATTRIBUTES != "0") &
        (df.WDSP != "999.9") &
        (df.WDSP_ATTRIBUTES != "0") &
        (df.MXSPD != "999.9") &
        (df.GUST != "999.9") &
        (df.MAX != "9999.9") &
        (df.MIN != "9999.9") &
        (df.PRCP_ATTRIBUTES != "I")
    )
    print(f"After filtering: {df.count()} rows")
    print(f"Number of stations: {df.select('STATION').distinct().count()}")
    count_by_station = df.groupBy('STATION').count().collect()

    # TODO: Make this faster. (possible solution: use DateType, rangeBetween, and remove dates not in range | join on)
    # Import stock dates, convert to list
    with open("date.csv", newline='') as f:
        reader = csv.reader(f)
        dates = list(reader)
        dates = dates[1:]
        # from "MM/dd/yyyy" to "yyyy-MM-dd" in string format
        stock_dates = [f"{x[6:10]}-{x[0:2]}-{x[3:5]}" for x, y in dates]

    # Keep only station data where the dates are in date.csv
    df = df.filter(df.DATE.isin(stock_dates))
    print("Rows after date filtering:", df.count())

    keep_set = {row['STATION'] for row in count_by_station if row['count'] >= len(stock_dates)}

    print(f"Keeping data for {len(keep_set)} stations")
    return df.filter(df.STATION.isin(keep_set))


if __name__ == "__main__":
    main()
