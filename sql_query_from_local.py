
from pyspark.sql import SparkSession


# create a Spark session with the PostgreSQL JDBC driver
spark = SparkSession.builder \
    .appName("PostgreSQL to DataFrame") \
    .master("local[*]") \
    .config("spark.jars", "/home/gopikaradhakrishnan/Desktop/spark-new/spark-3.3.2-bin-hadoop3/postgresql-42.2.6.jar") \
    .getOrCreate()

# Define connection parameters
jdbc_url = "jdbc:postgresql://database-1.cr4vhy3almjs.eu-north-1.rds.amazonaws.com:5432/postgres"
table_name = "employees"
connection_properties = {
    "user": "postgres",
    "password": "postgresglue"
}
driver = "org.postgresql.Driver"

# Read SQL query from file
with open("sql/sql_query.sql", "r") as f:
    query = f.read()

# Read the PostgreSQL table with the SQL query into a DataFrame
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"({query}) AS query") \
    .option("user", connection_properties["user"]) \
    .option("password", connection_properties["password"]) \
    .option("driver", driver) \
    .load()

# Write DataFrame to a new table in the same database
table_name_new = "employees_new"
df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table_name_new) \
    .option("user", connection_properties["user"]) \
    .option("password", connection_properties["password"]) \
    .option("driver", driver) \
    .mode("append") \
    .save()

