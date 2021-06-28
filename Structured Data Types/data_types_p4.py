from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession\
    .builder\
    .appName('ExampleApp')\
    .getOrCreate()

"""
Working with Dates and Timestamps
"""
dateDF = spark.range(10)\
    .withColumn('today', current_date())\
    .withColumn('now', current_timestamp())

dateDF.createOrReplaceTempView('dateTable')
dateDF.printSchema()
