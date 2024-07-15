from pyspark.sql.functions import current_timestamp, expr

# Filter for the last 10 seconds
df_last_10_seconds = df.filter((current_timestamp() - col("pickup_datetime")) < expr("INTERVAL 10 SECONDS"))

# Group by pickup location and count passengers
top_pickup_locations = df_last_10_seconds.groupBy("pickup_location").sum("passenger_count").orderBy(col("sum(passenger_count)").desc())
top_pickup_locations.show()
