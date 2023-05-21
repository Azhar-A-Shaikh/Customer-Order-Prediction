import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("customer_order_job").getOrCreate()

# Define the schema for the dataframes
holiday_event_schema = StructType([
    StructField("date", StringType(), True),
    # Add other columns from holidays_events.csv
])

oil_schema = StructType([
    StructField("date", StringType(), True),
    # Add other columns from oil.csv
])

stores_schema = StructType([
    StructField("store_nbr", IntegerType(), True),
    # Add other columns from stores.csv
])

transactions_schema = StructType([
    StructField("date", StringType(), True),
    StructField("store_nbr", IntegerType(), True),
    # Add other columns from transactions.csv
])

train_schema = StructType([
    StructField("id", StringType(), True),
    StructField("date", StringType(), True),
    # Add other columns from train.csv
])

# Read the dataframes from the CSV files
source_path_holidays = "s3://orders_data/holidays_events.csv"
df_holiday_event = spark.read.csv(source_path_holidays, header=True, schema=holiday_event_schema)

source_path_oil = "s3://orders_data/oil.csv"
df_oil = spark.read.csv(source_path_oil, header=True, schema=oil_schema)

source_path_stores = "s3://orders_data/stores.csv"
df_stores = spark.read.csv(source_path_stores, header=True, schema=stores_schema)

source_path_transactions = "s3://orders_data/transactions.csv"
df_trans = spark.read.csv(source_path_transactions, header=True, schema=transactions_schema)

source_path_train = "s3://orders_data/train.csv"
df_train = spark.read.csv(source_path_train, header=True, schema=train_schema)

# Perform joins and column renaming
df = df_train.join(df_holiday_event, "date")
df = df.join(df_oil, "date")
df = df.join(df_stores, "store_nbr")
df = df.join(df_trans, ["date", "store_nbr"])
df = df.withColumnRenamed("type", "holiday_type")
df = df.withColumnRenamed("type1", "store_type")
df = df.withColumnRenamed("family", "product_category")

# Select specific fields and fill missing values
df1 = df.select("id", "store_nbr", "date", "product_category", "onpromotion",
                "holiday_type", "locale", "transferred", "dcoilwtico", "store_type",
                "cluster", "transactions", "sales")
df1 = df1.fillna({"dcoilwtico": 64.077})


# Creating columns for year, month, day, hour, minute, and day_of_week
df2 = df1.toDF()
df2 = df2.withColumn("year", date_format("date", "yyyy")) \
    .withColumn("month", date_format("date", "MM")) \
    .withColumn("day", date_format("date", "dd")) \
    .withColumn("hour", date_format("date", "HH")) \
    .withColumn("minute", date_format("date", "mm")) \
    .withColumn("day_of_week", date_format("date", "EE"))

# Specify the destination path for writing the output
destination_path = "s3://result_order/"

# Write the dataframe to JSON format in the specified path
df2.write.json(destination_path)

# Stop the Spark session
spark.stop()


# Code 
 # ( spark-submit --master local[*] --name customer_order_job --packages "org.apache.hadoop:hadoop-aws:3.2.0" \
 #   --conf "spark.hadoop.fs.s3a.access.key=YOUR_ACCESS_KEY" \
 #  --conf "spark.hadoop.fs.s3a.secret.key=YOUR_SECRET_KEY" \
 #    script.py ) 
