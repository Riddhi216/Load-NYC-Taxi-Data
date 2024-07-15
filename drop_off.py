# Assuming route is defined by pickup and dropoff locations
route_passenger_count = df.groupBy("pickup_location", "dropoff_location").sum("passenger_count")
most_passengers_route = route_passenger_count.orderBy(col("sum(passenger_count)").desc()).first()
print(most_passengers_route)
