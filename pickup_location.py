# Group by area and count passengers
passenger_count_by_area = df.groupBy("pickup_location").sum("passenger_count")
passenger_count_by_area.show()
