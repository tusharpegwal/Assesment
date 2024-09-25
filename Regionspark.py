from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder.appName("SalesDataETL").getOrCreate()


region_a_df = spark.read.csv("path/to/region_a.csv", header=True, inferSchema=True)
region_b_df = spark.read.csv("path/to/region_b.csv", header=True, inferSchema=True)

# Add region column
region_a_df = region_a_df.withColumn("region", expr("'A'"))
region_b_df = region_b_df.withColumn("region", expr("'B'"))

# Combine data from both regions
combined_df = region_a_df.union(region_b_df)

# Add total_sales column
combined_df = combined_df.withColumn("total_sales", col("QuantityOrdered") * col("ItemPrice"))

# Add net_sale column
combined_df = combined_df.withColumn("net_sale", col("total_sales") - col("PromotionDiscount"))

# Remove duplicates based on OrderId
combined_df = combined_df.dropDuplicates(["OrderId"])

# Exclude orders where total sales amount is negative or zero after applying discounts
filtered_df = combined_df.filter(col("net_sale") > 0)

# Load the transformed data into SQLite database
filtered_df.write.format("jdbc").options(
    url="jdbc:sqlite:path/to/database.db",
    driver="org.sqlite.JDBC",
    dbtable="sales_data",
    user="",
    password=""
).mode("overwrite").save()

# Stop the Spark session
spark.stop()
