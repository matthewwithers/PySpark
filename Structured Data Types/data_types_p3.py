from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession\
    .builder\
    .appName('ExampleApp')\
    .getOrCreate()

df = spark.read.format('csv')\
    .option('header', 'true')\
    .option('inferSchema', 'true')\
    .load('../Spark-The-Definitive-Guide/data/retail-data/by-day/2010-12-01.csv')

"""
 Working with Strings
"""

df\
    .select(
        initcap(col('description')).alias('new_column'),
        col('description').alias('orginal_column')
    ).show(5)

df\
    .select(
        col('description'),
        lower(col('description')),
        upper(col('description')),
        initcap(col('description'))
    ).show(5)

# Triming functions lpad, rpad, ltrim, rtrim, trim
df\
    .select(
        ltrim(lit('         hello           ')).alias('ltrim'),
        rtrim(lit('         hello           ')).alias('rtrim'),
        trim(lit('          hello           ')).alias('trim'),
        lpad(lit('hello'), 3, ' ').alias('lpad'),
        rpad(lit('hello'), 10, ' ').alias('rpad')
    ).show(5)

# Regular Expressions
regex_string = 'BLACK|WHITE|RED|GREEN|BLUE'
df\
    .select(
        regexp_replace(col('description'), regex_string,
                       'COLOR').alias('color_clean'),
        col('description')
    ).show(5)

df\
    .select(
        translate(col('description'), 'LEET', '1337'),
        col('description')
    )\
    .show(2)
