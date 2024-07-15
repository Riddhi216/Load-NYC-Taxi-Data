# Average fare and total earnings by vendor
average_fare_by_vendor = df.groupBy("vendor_id").agg({"fare_amount": "avg", "Revenue": "avg"})
average_fare_by_vendor.show()
