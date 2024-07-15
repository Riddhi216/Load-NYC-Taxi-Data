# Count of payments by payment mode
payment_mode_count = df.groupBy("payment_type").count()
payment_mode_count.show()
