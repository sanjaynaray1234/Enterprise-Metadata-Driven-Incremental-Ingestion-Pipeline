from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, add_months, current_date

spark = SparkSession.builder.master("local[*]").appName("DatePractice").getOrCreate()

raw_data = [("TXN001", "2026-06-28"), ("TXN002", "2026-05-15"), ("TXN003", "2026-04-10"), ("TXN004", "2026-03-05")]
df = spark.createDataFrame(raw_data, ["TransactionID", "TransactionDate"])

processed_df = df.withColumn("TransactionDate", to_date(col("TransactionDate"), "yyyy-MM-dd")) \
    .withColumn("FormattedDate", date_format(col("TransactionDate"), "dd-MM-yyyy "))

processed_df.show(truncate=False)
