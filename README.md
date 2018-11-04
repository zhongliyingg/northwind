# About

This project reads in a sample northwind dataset from https://relational.fit.cvut.cz/dataset/Northwind using pyspark for further queries.

# Dependencies

The code connects directly to the MySQL database to extract the sample data.
The following jar is required:

mysql-connector-java-5.1.44.jar

Download: https://mvnrepository.com/artifact/mysql/mysql-connector-java/5.1.44




# Running the project

At the root of the project, start the pyspark console
```
pyspark 
```

Import the data module and run `init()` to initalise the northwind dataset in spark
```
>>> import data
>>> data.init()
```

# Querying Data

Note the table names & columns has been converted from CamelCase to snake_case for readability & ease of query.

The data has been loaded into Temp Views which will allow it to be queried using `spark.sql`.

Example:
```
>>> spark.sql("select c.category_name, count(*) from products p inner join categories c on c.category_id = p.category_id group by c.category_name").show()
+--------------+--------+
| category_name|count(1)|
+--------------+--------+
|Dairy Products|      10|
|  Meat/Poultry|       6|
|    Condiments|      12|
|     Beverages|      12|
|Grains/Cereals|       7|
|       Seafood|      12|
|   Confections|      13|
|       Produce|       5|
+--------------+--------+
```