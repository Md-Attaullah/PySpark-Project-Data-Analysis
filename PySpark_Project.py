#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
findspark.find()


# In[2]:


from pyspark.sql import SparkSession


# In[3]:


spark = SparkSession \
 .builder \
 .appName("Python Spark SQL basic example") \
 .config("spark.some.config.option", "some-value") \
 .getOrCreate()


# In[36]:


from pyspark.sql import SparkSession 
  
spark = SparkSession.builder.appName("DataFrame").getOrCreate() 
  
Sale_df = spark.read.text(r"B:\mdata\Download\sales.csv.txt") 
  
Sale_df.show()


# **Sale DataFrame**

# In[51]:


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType ,DateType


# In[41]:


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


# In[12]:


display(Sale_df)


# In[15]:


Sale_df.show()


# In[42]:


#Deriving data from year,Month,Quater

from pyspark.sql. functions import month, year, quarter
Sale_df=Sale_df.withColumn ("order_year", year (Sale_df.order_date))
display(Sale_df)


# In[28]:


Sale_df.show()


# In[43]:


Sale_df=Sale_df.withColumn ("order_Month", month (Sale_df.order_date))
Sale_df=Sale_df.withColumn ("order_Quarter", quarter (Sale_df.order_date))
display(Sale_df)
Sale_df.show()


# **Menu DataFrame**

# In[52]:


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


# In[54]:


menu_df.show()


# **Total Amount Spend By Each Customer**

# In[59]:


Total_amount_spent = (Sale_df.join(menu_df, 'Product_id').groupBy( 'customer_id').agg({'Prize': 'sum' }).orderBy('customer_id'))
Total_amount_spent.show()


# **Total Amount Spend By Each Food Categories**

# In[64]:


Total_Spent_InFood = (Sale_df.join(menu_df, 'Product_id').groupBy( 'Product_Name').agg({'Prize': 'Sum' }).orderBy('Product_Name'))
Total_Spent_InFood.show()


# **Total Amount Of Sale Each Month**

# In[65]:


Total_Spent_InMonth = (Sale_df.join(menu_df, 'Product_id').groupBy( 'order_Month').agg({'Prize': 'Sum' }).orderBy('order_Month'))
Total_Spent_InMonth.show()


# **Yearly Sale**

# In[66]:


Total_Spent_InYear = (Sale_df.join(menu_df, 'Product_id').groupBy( 'order_year').agg({'Prize': 'Sum' }).orderBy('order_year'))
Total_Spent_InYear.show()


# **Quaterly Sales**

# In[67]:


Total_Spent_InQuarter = (Sale_df.join(menu_df, 'Product_id').groupBy( 'order_Quarter').agg({'Prize': 'Sum' }).orderBy('order_Quarter'))
Total_Spent_InQuarter.show()


# In[ ]:




