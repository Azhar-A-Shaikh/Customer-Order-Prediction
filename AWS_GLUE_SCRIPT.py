import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

sc = SparkContext()
glueContext = GlueContext(sc)

job = Job(glueContext)
job.init("customer_order_job")

source_path_holidays = "s3://orders_data/holidays_events.csv"
source_path_oil = "s3://orders_data/oil.csv"
source_path_stores = "s3://orders_data/stores.csv"
source_path_transactions = "s3://orders_data/transactions.csv"
source_path_train = "s3://orders_data/train.csv"

destination_path = "s3://result_order/"

df_holiday_event = glueContext.create_dynamic_frame.from_options(
    "csv",
    {'paths': [source_path_holidays], 'header': True, 'inferSchema': True}
)

df_oil = glueContext.create_dynamic_frame.from_options(
    "csv",
    {'paths': [source_path_oil], 'header': True, 'inferSchema': True}
)

df_stores = glueContext.create_dynamic_frame.from_options(
    "csv",
    {'paths': [source_path_stores], 'header': True, 'inferSchema': True}
)

df_trans = glueContext.create_dynamic_frame.from_options(
    "csv",
    {'paths': [source_path_transactions], 'header': True, 'inferSchema': True}
)

df_train = glueContext.create_dynamic_frame.from_options(
    "csv",
    {'paths': [source_path_train], 'header': True, 'inferSchema': True}
)

# joins and column renaming
df = df_train.join(df_holiday_event, ["date"])
df = df.join(df_oil, ['date'])
df = df.join(df_stores, ['store_nbr'])
df = df.join(df_trans, ['date', 'store_nbr'])
df = df.rename_field('type', 'holiday_type')
df = df.rename_field('type1', 'store_type')
df = df.rename_field('family', 'product_category')

df1 = df.select_fields(['id', 'store_nbr', 'date', 'product_category', 'onpromotion',
                        'holiday_type', 'locale', 'transferred', 'dcoilwtico', 'store_type',
                        'cluster', 'transactions', 'sales'])

df1 = df1.na_fill({'dcoilwtico': 64.077})

# Creating columns for year, month, day, hour, minute, and day_of_week

df2 = df1.toDF()
df2 = df2.withColumn("year", date_format("date", "yyyy")) \
    .withColumn("month", date_format("date", "MM")) \
    .withColumn("day", date_format("date", "dd")) \
    .withColumn("hour", date_format("date", "HH")) \
    .withColumn("minute", date_format("date", "mm")) \
    .withColumn("day_of_week", date_format("date", "EE"))

glueContext.write_dynamic_frame.from_options(
    frame=df2,
    connection_type="s3",
    connection_options={"path": destination_path},
    format="json"
)

job.commit()
