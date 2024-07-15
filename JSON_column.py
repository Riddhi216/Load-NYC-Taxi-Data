from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create Spark session
spark = SparkSession.builder.appName("NYC Taxi Data").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("dbfs:/mnt/taxi-data/yellow_tripdata_2020-01.csv", header=True, inferSchema=True)

# Display the DataFrame schema
df.printSchema()

# Display a sample of the data
df.show(5)

# Define the schema for the JSON column (if any)
json_schema = StructType([
    StructField("field1", StringType(), True),
    StructField("field2", DoubleType(), True)
])

# Parse and flatten the JSON column (if any)
df = df.withColumn("json_data", from_json(col("json_column"), json_schema))
df_flattened = df.select("*", "json_data.*").drop("json_column", "json_data")

# Show the flattened DataFrame
df_flattened.show(5)

# Write the DataFrame to an external Parquet table
df_flattened.write.mode("overwrite").parquet("dbfs:/mnt/taxi-data/yellow_tripdata_2020-01-flattened.parquet")

# Read back the Parquet file to confirm
parquet_df = spark.read.parquet("dbfs:/mnt/taxi-data/yellow_tripdata_2020-01-flattened.parquet")
parquet_df.show(5)
