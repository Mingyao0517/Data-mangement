from pyspark.sql import SparkSession, Window

def main():
    spark = SparkSession.builder.appName("weather_preprocessing").getOrCreate()
    # Sanity check the written parquet file
    path = 'clean_data/weather.parquet'
    df = spark.read.parquet(path)
    print(f"Imported {df.count()} rows")


if __name__ == "__main__":
    main()
