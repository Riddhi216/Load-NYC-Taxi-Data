from pyspark.sql.functions import col

# Add the 'Revenue' column
df = df.withColumn("Revenue", 
                   col("fare_amount") + col("extra") + col("mta_tax") +
                   col("improvement_surcharge") + col("tip_amount") +
                   col("tolls_amount") + col("total_amount"))

df.show(5)
