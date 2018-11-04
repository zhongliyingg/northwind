import os
import re
from pyspark.sql import SparkSession

import settings
from helper import camel_to_snake, camel_to_snake_columns


# Northwind Tables for Extract
NORTHWIND_TABLES = [
    "Categories",
    "CustomerCustomerDemo",
    "CustomerDemographics",
    "Customers",
    "EmployeeTerritories",
    "Employees",
    "Order Details",
    "Orders",
    "Products",
    "Region",
    "Shippers",
    "Suppliers",
    "Territories"
]

# Setup Spark Sessions
spark = SparkSession.builder.master("local").appName(settings.APP_NAME).getOrCreate()
spark_read_jdbc = spark.read \
                    .format("jdbc") \
                    .option("url", settings.NORTHWIND_DB_CONN_STR) \
                    .option("user", settings.NORTHWIND_DB_USER) \
                    .option("password", settings.NORTHWIND_DB_PASSWORD) \
                    .option("driver", settings.NORTHWIND_DB_DRIVER) \


def read_table_from_db(table_name):
    print("Reading Table from Northwind Database: {}".format(table_name))
    
    dbtable = table_name
    if re.search(r"\s+", dbtable):
        # Enclose in backticks for MySQL query
        dbtable = "`{}`".format(dbtable)

    df = spark_read_jdbc.option("dbtable", dbtable).load()
    return df

def format_parquet_data_path(data_name):
    return "{data_parquet_path}/{data_name}" \
                .format(data_parquet_path=settings.DATA_PARQUET_PATH, data_name=data_name)

def extract_table_from_db_to_parquet(table_name):
    print("Extracting Table from Northwind Database: {}".format(table_name))

    data_name = camel_to_snake(table_name)
    parquet_path = format_parquet_data_path(data_name)

    if os.path.exists(parquet_path):
        print("Data already extracted to {}".format(parquet_path))
    else:
        df = read_table_from_db(table_name)
        df = camel_to_snake_columns(df)
    
        print("Writing to Parquet Files: {}".format(data_name))   
        df.write.parquet(parquet_path)

    return data_name

def create_view_from_parquet(data_name):
    print("Creating Temp View from Parquet Files: {}".format(data_name))

    parquet_path = format_parquet_data_path(data_name)
    df = spark.read.parquet(parquet_path)
    df.printSchema()
    df.createOrReplaceTempView(data_name)
    return df

def create_view_from_csv(name):
    print "Creating Temp View from CSV Files: {}".format(name)

    df = spark.read.load("{data_csv_path}/northwind_{name}.csv"\
                            .format(data_csv_path=settings.DATA_CSV_PATH, name=name),
                            format="csv", sep=",", inferSchema="true", header="true")
    df = camel_to_snake_columns(df)
    df.printSchema()
    df.createOrReplaceTempView(name)
    return df

def init():
    print("Initialising Northwind Data...")

    for t in NORTHWIND_TABLES:
        print('Initialising Data: {table}'.format(table=t))
        try:
            print("Reading data from Northwind Database...")
            data_name = extract_table_from_db_to_parquet(t)
            df = create_view_from_parquet(data_name)
        except:
            print("Something wrong happened. Reading data from archived CSVs.")
            data_name = camel_to_snake(t)
            df = create_view_from_csv(data_name)

    print("Loaded Temp Views:")
    spark.sql("show tables").show()

    print("Northwind Data Load Completed.")
    print("You may query data in the Temp Views using `spark.sql`" )
