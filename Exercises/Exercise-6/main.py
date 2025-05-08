import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, to_date
from zipfile import ZipFile


def unzip_csv_files(data_folder):
    print(">>> Unzipping .zip files in folder:", data_folder)
    for filename in os.listdir(data_folder):
        if filename.endswith(".zip"):
            filepath = os.path.join(data_folder, filename)
            with ZipFile(filepath, 'r') as zip_ref:
                zip_ref.extractall(data_folder)
                print(f">>> Extracted: {filename}")


def create_spark_session():
    print(">>> Creating SparkSession...")
    return SparkSession.builder.appName("Exercise-6").getOrCreate()


def standardize_columns(df):
    """
    Standardize column names to match expected schema.
    """
    column_mapping = {
        "ride_id": "trip_id",
        "started_at": "start_time",
        "ended_at": "end_time",
        "start_station_name": "from_station_name",
        "start_station_id": "from_station_id",
        "end_station_name": "to_station_name",
        "end_station_id": "to_station_id",
        "member_casual": "usertype",
    }
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df


def read_data(spark, data_folder):
    print(">>> Reading CSV files from folder:", data_folder)
    dfs = []
    for filename in os.listdir(data_folder):
        if filename.endswith(".csv"):
            path = os.path.join(data_folder, filename)
            print(f">>> Reading file: {filename}")
            df = spark.read.csv(path, header=True, inferSchema=True)
            df = standardize_columns(df)  # Standardize column names
            print(f">>> Schema for {filename}:")
            df.printSchema()
            dfs.append(df)
    if len(dfs) == 0:
        raise ValueError("No valid CSV files found in the folder.")
    return dfs[0].unionByName(dfs[1], allowMissingColumns=True) if len(dfs) > 1 else dfs[0]


def average_trip_duration_per_day(df, output_folder):
    print(">>> Calculating average trip duration per day...")
    df = df.withColumn("date", to_date("start_time"))
    result = df.groupBy("date").agg(avg("tripduration").alias("average_duration"))
    output = os.path.join(output_folder, "average_trip_duration_per_day.csv")
    print(">>> Writing result to:", output)
    result.coalesce(1).write.csv(output, header=True, mode="overwrite")


def trip_count_per_day(df, output_folder):
    print(">>> Counting trips per day...")
    df = df.withColumn("date", to_date("start_time"))
    result = df.groupBy("date").agg(count("*").alias("trip_count"))
    output = os.path.join(output_folder, "trip_count_per_day.csv")
    print(">>> Writing result to:", output)
    result.coalesce(1).write.csv(output, header=True, mode="overwrite")


def main():
    data_folder = "/var/tmp/app/Exercise-6/data"
    output_folder = "/var/tmp/app/Exercise-6/reports"
    os.makedirs(output_folder, exist_ok=True)

    unzip_csv_files(data_folder)
    spark = create_spark_session()
    df = read_data(spark, data_folder)

    print(">>> Number of rows in DataFrame:", df.count())
    df.printSchema()

    average_trip_duration_per_day(df, output_folder)
    trip_count_per_day(df, output_folder)

    print(">>> Reports completed.")


if __name__ == "__main__":
    main()