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

extract_str = '(BLACK|WHITE|RED|GREEN|BLUE)'
df\
    .select(
        regexp_extract(col('description'), extract_str,
                       1).alias('extacted_str'),
        col('description')
    ).show(5)

# In Spark we can also check from String that contain certain substrings
containsBlack = instr(col('description'), 'BLACK') >= 1
containsWhite = instr(col('description'), 'WHITE') >= 1

df\
    .withColumn('hasSimpleColor', containsBlack | containsWhite)\
    .where('hasSimpleColor')\
    .select('description', 'hasSimpleColor')\
    .show(5, False)

simpleColors = ['black', 'white', 'red', 'green', 'blue']


def color_locator(column, color_string):
    return locate(color_string.upper(), column)\
        .cast('boolean')\
        .alias('is_' + color_string)


selectedColumns = [color_locator(df.Description, color)
                   for color in simpleColors]
selectedColumns.append(expr('*'))  # has to be a Column type


df\
    .select(*selectedColumns).show(3)

df\
    .select(*selectedColumns)\
    .where(expr('is_white OR is_red'))\
    .select('description')\
    .show(3, False)
