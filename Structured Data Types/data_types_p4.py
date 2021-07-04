from datetime import date
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
dateDF.show()

dateDF\
    .select(
        date_sub(col('today'), 5).alias('today_sub_5'),
        date_add(col('today'), 5).alias('today_add_5'),
    ).show(1)

dateDF\
    .withColumn('week_ago', date_sub(col('today'), 7))\
    .select(
        col('week_ago'),
        datediff(col('week_ago'), col('today'))
    ).show(1)

dateDF\
    .select(
        to_date(lit('2016-01-01')).alias('start'),
        to_date(lit('2017-05-02')).alias('end'),
    )\
    .select(
        datediff(col('start'), col('end'))
    ).show(1)

dateDF\
    .select(
        to_date(lit('2016-01-01')).alias('start'),
        to_date(lit('2017-05-02')).alias('end'),
    )\
    .select(
        months_between(col('start'), col('end'))
    ).show(1)

dateDF\
    .select(
        to_date(lit('2016-20-12')).alias('invalid_date_format'),
        to_date(lit('2017-12-11')).alias('valid_date_format'),
    ).show(1)

dateFormat = 'yyyy-dd-MM'
cleanDateDF = spark.range(1).select(
    to_date(lit('2017-12-11'), dateFormat).alias('date'),
    to_date(lit('2017-20-12'), dateFormat).alias('date2')
)

cleanDateDF.createOrReplaceTempView('dateTable2')

cleanDateDF.show()

cleanDateDF.select(
    to_timestamp(lit('2020-20-08'), dateFormat)
).show()

cleanDateDF.filter(
    col('date') > lit('2017-12-12')
).show()

cleanDateDF.filter(
    col('date') < lit('2017-12-12')
).show()
