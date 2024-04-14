```python
import findspark
findspark.init()
findspark.find()
```




    'C:\\Spark'




```python
from pyspark.sql import SparkSession
```


```python
spark = SparkSession \
 .builder \
 .appName("Python Spark SQL basic example") \
 .config("spark.some.config.option", "some-value") \
 .getOrCreate()
```


```python
from pyspark.sql import SparkSession 
  
spark = SparkSession.builder.appName("DataFrame").getOrCreate() 
  
Sale_df = spark.read.text(r"B:\mdata\Download\sales.csv.txt") 
  
Sale_df.show()
```

    +--------------------+
    |               value|
    +--------------------+
    |1,A, 2023-01-01,I...|
    |2,A, 2022-01-01,I...|
    |2,A, 2023-01-07,I...|
    |3,A, 2023-01-10,I...|
    |3,A, 2022-01-11,I...|
    |3,A, 2023-01-11,I...|
    |2,B, 2022-02-01,I...|
    |2,B, 2023-01-02,I...|
    |1,B, 2023-01-04,I...|
    |1,B, 2023-02-11,I...|
    |3,B, 2023-01-16,I...|
    |3,B, 2022-02-01,I...|
    |3,C, 2023-01-01,I...|
    |1,C, 2023-01-01,U...|
    |6,C, 2022-01-07,U...|
    |3,D, 2023-02-16,U...|
    |5,D, 2022-02-01,U...|
    |3,E, 2023-02-01,U...|
    |4,E, 2023-02-01,U...|
    |4,E, 2023-02-07,U...|
    +--------------------+
    only showing top 20 rows
    
    

**Sale DataFrame**


```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType ,DateType
```


```python
# Create a spark session using getOrCreate() function 
spark_session = SparkSession.builder.getOrCreate() 
  
# Define the structure for the data frame 
schema = StructType([ 
    StructField("Product_id",IntegerType(),True),
    StructField("customer_id",StringType(),True),
    StructField("order_date",DateType(),True),
    StructField("location",StringType(),True),
    StructField("source_order",StringType(),True),
]) 
  
# Applying custom schema to data frame 
Sale_df = spark_session.read.format( 
    "csv").schema(schema).option( 
    "header", False).load(r"B:\mdata\Download\sales.csv.txt") 
  
# Display the updated schema 
Sale_df.printSchema() 
```

    root
     |-- Product_id: integer (nullable = true)
     |-- customer_id: string (nullable = true)
     |-- order_date: date (nullable = true)
     |-- location: string (nullable = true)
     |-- source_order: string (nullable = true)
    
    


```python
display(Sale_df)
```


    DataFrame[Product_id: int, customer_id: int, order_date: int, location: int, source_order: int]



```python
Sale_df.show()
```

    +----------+-----------+-----------+--------+------------+
    |Product_id|customer_id| order_date|location|source_order|
    +----------+-----------+-----------+--------+------------+
    |         1|          A| 2023-01-01|   India|      Swiggy|
    |         2|          A| 2022-01-01|   India|      Swiggy|
    |         2|          A| 2023-01-07|   India|      Swiggy|
    |         3|          A| 2023-01-10|   India|  Restaurant|
    |         3|          A| 2022-01-11|   India|      Swiggy|
    |         3|          A| 2023-01-11|   India|  Restaurant|
    |         2|          B| 2022-02-01|   India|      Swiggy|
    |         2|          B| 2023-01-02|   India|      Swiggy|
    |         1|          B| 2023-01-04|   India|  Restaurant|
    |         1|          B| 2023-02-11|   India|      Swiggy|
    |         3|          B| 2023-01-16|   India|      zomato|
    |         3|          B| 2022-02-01|   India|      zomato|
    |         3|          C| 2023-01-01|   India|      zomato|
    |         1|          C| 2023-01-01|      UK|      Swiggy|
    |         6|          C| 2022-01-07|      UK|      zomato|
    |         3|          D| 2023-02-16|      UK|  Restaurant|
    |         5|          D| 2022-02-01|      UK|      zomato|
    |         3|          E| 2023-02-01|      UK|  Restaurant|
    |         4|          E| 2023-02-01|      UK|      Swiggy|
    |         4|          E| 2023-02-07|      UK|  Restaurant|
    +----------+-----------+-----------+--------+------------+
    only showing top 20 rows
    
    


```python
#Deriving data from year,Month,Quater

from pyspark.sql. functions import month, year, quarter
Sale_df=Sale_df.withColumn ("order_year", year (Sale_df.order_date))
display(Sale_df)
```


    DataFrame[Product_id: int, customer_id: string, order_date: date, location: string, source_order: string, order_year: int]



```python
Sale_df.show()
```

    +----------+-----------+----------+--------+------------+----------+
    |Product_id|customer_id|order_date|location|source_order|order_year|
    +----------+-----------+----------+--------+------------+----------+
    |         1|          A|2023-01-01|   India|      Swiggy|      2023|
    |         2|          A|2022-01-01|   India|      Swiggy|      2022|
    |         2|          A|2023-01-07|   India|      Swiggy|      2023|
    |         3|          A|2023-01-10|   India|  Restaurant|      2023|
    |         3|          A|2022-01-11|   India|      Swiggy|      2022|
    |         3|          A|2023-01-11|   India|  Restaurant|      2023|
    |         2|          B|2022-02-01|   India|      Swiggy|      2022|
    |         2|          B|2023-01-02|   India|      Swiggy|      2023|
    |         1|          B|2023-01-04|   India|  Restaurant|      2023|
    |         1|          B|2023-02-11|   India|      Swiggy|      2023|
    |         3|          B|2023-01-16|   India|      zomato|      2023|
    |         3|          B|2022-02-01|   India|      zomato|      2022|
    |         3|          C|2023-01-01|   India|      zomato|      2023|
    |         1|          C|2023-01-01|      UK|      Swiggy|      2023|
    |         6|          C|2022-01-07|      UK|      zomato|      2022|
    |         3|          D|2023-02-16|      UK|  Restaurant|      2023|
    |         5|          D|2022-02-01|      UK|      zomato|      2022|
    |         3|          E|2023-02-01|      UK|  Restaurant|      2023|
    |         4|          E|2023-02-01|      UK|      Swiggy|      2023|
    |         4|          E|2023-02-07|      UK|  Restaurant|      2023|
    +----------+-----------+----------+--------+------------+----------+
    only showing top 20 rows
    
    


```python
Sale_df=Sale_df.withColumn ("order_Month", month (Sale_df.order_date))
Sale_df=Sale_df.withColumn ("order_Quarter", quarter (Sale_df.order_date))
display(Sale_df)
Sale_df.show()
```


    DataFrame[Product_id: int, customer_id: string, order_date: date, location: string, source_order: string, order_year: int, order_Month: int, order_Quarter: int]


    +----------+-----------+----------+--------+------------+----------+-----------+-------------+
    |Product_id|customer_id|order_date|location|source_order|order_year|order_Month|order_Quarter|
    +----------+-----------+----------+--------+------------+----------+-----------+-------------+
    |         1|          A|2023-01-01|   India|      Swiggy|      2023|          1|            1|
    |         2|          A|2022-01-01|   India|      Swiggy|      2022|          1|            1|
    |         2|          A|2023-01-07|   India|      Swiggy|      2023|          1|            1|
    |         3|          A|2023-01-10|   India|  Restaurant|      2023|          1|            1|
    |         3|          A|2022-01-11|   India|      Swiggy|      2022|          1|            1|
    |         3|          A|2023-01-11|   India|  Restaurant|      2023|          1|            1|
    |         2|          B|2022-02-01|   India|      Swiggy|      2022|          2|            1|
    |         2|          B|2023-01-02|   India|      Swiggy|      2023|          1|            1|
    |         1|          B|2023-01-04|   India|  Restaurant|      2023|          1|            1|
    |         1|          B|2023-02-11|   India|      Swiggy|      2023|          2|            1|
    |         3|          B|2023-01-16|   India|      zomato|      2023|          1|            1|
    |         3|          B|2022-02-01|   India|      zomato|      2022|          2|            1|
    |         3|          C|2023-01-01|   India|      zomato|      2023|          1|            1|
    |         1|          C|2023-01-01|      UK|      Swiggy|      2023|          1|            1|
    |         6|          C|2022-01-07|      UK|      zomato|      2022|          1|            1|
    |         3|          D|2023-02-16|      UK|  Restaurant|      2023|          2|            1|
    |         5|          D|2022-02-01|      UK|      zomato|      2022|          2|            1|
    |         3|          E|2023-02-01|      UK|  Restaurant|      2023|          2|            1|
    |         4|          E|2023-02-01|      UK|      Swiggy|      2023|          2|            1|
    |         4|          E|2023-02-07|      UK|  Restaurant|      2023|          2|            1|
    +----------+-----------+----------+--------+------------+----------+-----------+-------------+
    only showing top 20 rows
    
    

**Menu DataFrame**


```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType ,DateType

# Create a spark session using getOrCreate() function 
spark_session = SparkSession.builder.getOrCreate() 
  
# Define the structure for the data frame 
schema = StructType([ 
    StructField("Product_id",IntegerType(),True),
    StructField("Product_Name",StringType(),True),
    StructField("Prize",StringType(),True),
]) 
  
# Applying custom schema to data frame 
menu_df = spark_session.read.format( 
    "csv").schema(schema).option( 
    "header", False).load(r"B:\mdata\Download\menu.csv.txt") 
  
# Display the updated schema 
menu_df.printSchema() 
```

    root
     |-- Product_id: integer (nullable = true)
     |-- Product_Name: string (nullable = true)
     |-- Prize: string (nullable = true)
    
    


```python
menu_df.show()
```

    +----------+------------+-----+
    |Product_id|Product_Name|Prize|
    +----------+------------+-----+
    |         1|       PIZZA|  100|
    |         2|     Chowmin|  150|
    |         3|    sandwich|  120|
    |         4|        Dosa|  110|
    |         5|     Biryani|   80|
    |         6|       Pasta|  180|
    +----------+------------+-----+
    
    

**Total Amount Spend By Each Customer**


```python
Total_amount_spent = (Sale_df.join(menu_df, 'Product_id').groupBy( 'customer_id').agg({'Prize': 'sum' }).orderBy('customer_id'))
Total_amount_spent.show()
```

    +-----------+----------+
    |customer_id|sum(Prize)|
    +-----------+----------+
    |          A|    4260.0|
    |          B|    4440.0|
    |          C|    2400.0|
    |          D|    1200.0|
    |          E|    2040.0|
    +-----------+----------+
    
    

**Total Amount Spend By Each Food Categories**


```python
Total_Spent_InFood = (Sale_df.join(menu_df, 'Product_id').groupBy( 'Product_Name').agg({'Prize': 'Sum' }).orderBy('Product_Name'))
Total_Spent_InFood.show()
```

    +------------+----------+
    |Product_Name|sum(Prize)|
    +------------+----------+
    |     Biryani|     480.0|
    |     Chowmin|    3600.0|
    |        Dosa|    1320.0|
    |       PIZZA|    2100.0|
    |       Pasta|    1080.0|
    |    sandwich|    5760.0|
    +------------+----------+
    
    

**Total Amount Of Sale Each Month**


```python
Total_Spent_InMonth = (Sale_df.join(menu_df, 'Product_id').groupBy( 'order_Month').agg({'Prize': 'Sum' }).orderBy('order_Month'))
Total_Spent_InMonth.show()
```

    +-----------+----------+
    |order_Month|sum(Prize)|
    +-----------+----------+
    |          1|    2960.0|
    |          2|    2730.0|
    |          3|     910.0|
    |          5|    2960.0|
    |          6|    2960.0|
    |          7|     910.0|
    |         11|     910.0|
    +-----------+----------+
    
    

**Yearly Sale**


```python
Total_Spent_InYear = (Sale_df.join(menu_df, 'Product_id').groupBy( 'order_year').agg({'Prize': 'Sum' }).orderBy('order_year'))
Total_Spent_InYear.show()
```

    +----------+----------+
    |order_year|sum(Prize)|
    +----------+----------+
    |      2022|    4350.0|
    |      2023|    9990.0|
    +----------+----------+
    
    

**Quaterly Sales**


```python
Total_Spent_InQuarter = (Sale_df.join(menu_df, 'Product_id').groupBy( 'order_Quarter').agg({'Prize': 'Sum' }).orderBy('order_Quarter'))
Total_Spent_InQuarter.show()
```

    +-------------+----------+
    |order_Quarter|sum(Prize)|
    +-------------+----------+
    |            1|    6600.0|
    |            2|    5920.0|
    |            3|     910.0|
    |            4|     910.0|
    +-------------+----------+
    
    


```python

```
