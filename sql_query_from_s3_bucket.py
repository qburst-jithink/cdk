import boto3
from io import StringIO
from pyspark.sql import SparkSession

# create a Spark session with the PostgreSQL JDBC driver
spark = SparkSession.builder \
    .appName("Join PostgreSQL tables and write to DataFrame") \
    .master("local[*]") \
    .config("spark.jars", "/home/gopikaradhakrishnan/Desktop/spark-new/spark-3.3.2-bin-hadoop3/postgresql-42.2.6.jar") \
    .getOrCreate()

# Define connection parameters
jdbc_url = "jdbc:postgresql://database-1.cr4vhy3almjs.eu-north-1.rds.amazonaws.com:5432/postgres"
table_name1 = "employees"
table_name2 = "departments"
join_column = "dept_id"
connection_properties = {
    "user": "postgres",
    "password": "postgresglue"
}
driver = "org.postgresql.Driver"

# Get the SQL query from an S3 bucket
s3 = boto3.client('s3')
bucket_name = "your-bucket"
key = "query.sql"
obj = s3.get_object(Bucket=bucket_name, Key=key)
query_str = obj['Body'].read().decode('utf-8')

# Read the PostgreSQL tables into DataFrames
df1 = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("query", query_str) \
    .option("user", connection_properties["user"]) \
    .option("password", connection_properties["password"]) \
    .option("driver", driver) \
    .load()

df2 = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table_name2) \
    .option("user", connection_properties["user"]) \
    .option("password", connection_properties["password"]) \
    .option("driver", driver) \
    .load()

# Perform the join operation
joined_df = df1.join(df2, join_column)

# Write the joined DataFrame to a new table in the same database
table_name_new = "employees_departments"
joined_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table_name_new) \
    .option("user", connection_properties["user"]) \
    .option("password", connection_properties["password"]) \
    .option("driver", driver) \
    .mode("append") \
    .save()

