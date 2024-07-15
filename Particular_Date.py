from pyspark.sql.functions import to_date

# Filter by specific date
df_filtered = df.filter(to_date(df["pickup_datetime"]) == '2020-01-15')

# Group by vendor and calculate total revenue and passenger count
vendor_stats = df_filtered.groupBy("vendor_id").agg(
    {"passenger_count": "sum", "trip_distance": "sum", "Revenue": "sum"}
)

# Get the top 2 vendors by revenue
top_vendors = vendor_stats.orderBy(col("sum(Revenue)").desc()).limit(2)
top_vendors.show()
