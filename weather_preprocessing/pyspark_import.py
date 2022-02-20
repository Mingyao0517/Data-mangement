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
        #.option("header", False) \
        .option("header", True) \
        .option("sep", ",") \
        .schema(customSchema) \
        .load(fullPath) \
        .withColumn("filename", input_file_name())

    print(df.head(25))

    
    df_test = df.withColumn("DATE1",(F.to_date(F.col("DATE"),"yyyy-MM-dd")))
    #print(df_test.show(3))
    min_max_timestamps = df_test.agg(F.min(df_test.DATE1),F.max(df_test.DATE1)).head()
    first_date, last_date = [ts for ts in min_max_timestamps]
    #print(min_max_timestamps)
    #print(first_date,last_date)

    station = [row.STATION for row in df_test.select("STATION").distinct().collect()]
    all_days_in_range = [first_date + datetime.timedelta(days=d)
                     for d in range((last_date - first_date).days + 1)]
    dates_by_station = spark.createDataFrame(product(station,all_days_in_range),
                                             schema=("STATION","DATE2"))
    df2 = (dates_by_station.join(df_test,
                                 (F.to_date(df_test.DATE) == dates_by_station.DATE2)
                                & (dates_by_station.STATION == df_test.STATION),
                                 how="left")
           .drop(df_test.STATION)
           )
    wind = (Window
            .partitionBy("STATION")
            .rangeBetween(Window.unboundedPreceding, -1)
            .orderBy(F.unix_timestamp("DATE2"))
            )
    df3 = df2.withColumn("LAST_TEMP",
                         F.last("TEMP", ignorenulls=True).over(wind))
    print(df3.show(3))
    df4 = df3.select(
        df3.STATION,
        F.coalesce(df3.DATE1, F.to_timestamp(df3.DATE2)).alias("DATE1"),
        F.coalesce(df3.TEMP, df3.LAST_TEMP).alias("TEMP"))
    df4.show(3)


if __name__ == "__main__":
    main()
