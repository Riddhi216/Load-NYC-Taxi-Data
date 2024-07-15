from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("NYC Taxi Data").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("dbfs:/mnt/taxi-data/yellow_tripdata_2020-01.csv", header=True, inferSchema=True)

# Display the DataFrame schema
df.printSchema()

# Display a sample of the data
df.show(5)
