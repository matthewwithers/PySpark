"""
Working with Nulls in Data

As a best practive you should always use nulls to represent missing or empty data in your dataframes.
Spark can optimize workint with null values more than it can if you use empty strings or other values. The
primary way of interacting with null values at DataFrame scale is to use the .na subpackage on a DataFrame
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructField, StructType

spark = SparkSession\
    .builder\
    .appName('ExampleApp')\
    .getOrCreate()

df = spark.read.format('csv')\
    .option('inferSchema', 'true')\
    .option('header', 'true')\
    .load('../Spark-The-Definitive-Guide/data/retail-data/by-day/2010-12-01.csv')

# COALESCE
df\
    .withColumn('nonnull_col', lit('value'))\
    .select(
        coalesce(col('nonnull_col'), col('description'), col('customerId')),
    ).show(1)


# IFNULL - Allows you to select the 2nd value if the 1st is null
# NULLIF - Will return null if the two values are equal or returns the 2nd if they are not
# NVL - Returns the 2nd value if the first is null
# NVL2 - Returns the 2nd value if the first is NOT null; otherwise it will return the last specified value

df.na.drop()  # drops a row if any value in the row is null
df.na.drop('any')  # drops a row if any value in the row is null
df.na.drop('all')  # drops a row if all values in the row are null
df.na.drop('all', subset=['description', 'customerId'])

"""
Working with Complex Types

There are 3 kinds of complex types (structs, arrays, maps)
"""

# Structs
# Structs are dataframes within dataframes

df.selectExpr(
    "(Description, InvoiceNo) as complex",
    "*"
).show(5)

df.selectExpr(
    "struct(Description,InvoiceNo) as complex",
    "*"
).show(5)

complexDF = df\
    .select(
        struct('Description', 'InvoiceNo').alias('complex')
    )
complexDF.createOrReplaceTempView('complexDF')

complexDF\
    .select('complex.Description')

complexDF\
    .select(
        col('complex').getField('description')
    ).show(1)

complexDF.select(col('complex.*')).show()


# Arrays
df\
    .select(
        split(col('description'), ' ')
    ).show(2)

df\
    .select(split(col('description'), ' ').alias('array_col'))\
    .selectExpr('array_col[0]').show()

df\
    .withColumn('array_col', split(col('description'), ' '))\
    .selectExpr('array_col[0]').show()

df\
    .withColumn('array_col', split(col('description'), ' '))\
    .select(size('array_col')).show()

df\
    .withColumn('array_col', split(col('description'), ' '))\
    .select(array_contains(col('array_col'), 'WHITE'))\
    .show()

df\
    .withColumn('splitted', split(col('description'), ' '))\
    .withColumn('exploded', explode(col('splitted')))\
    .select(
        'description',
        'InvoiceNo',
        'exploded'
    ).show()

"""
Maps are created by using the map function and key-value pairs columns. You then can select them just like you might
select from an array
"""

df\
    .withColumn('complex_map', create_map(col('description'), col('InvoiceNo')))\
    .selectExpr(
        """complex_map['WHITE METAL LANTERN']""",
        """explode(complex_map)"""
    ).show()


"""
Working with JSON 
"""

jsonDF = spark.range(1).selectExpr(
    """
    '{"myJSONKey": {
            "myJSONValue":[1,2,3]
        }
     }' as jsonString
    """
)

jsonDF.select(
    get_json_object(col('jsonString'), '$.myJSONKey.myJSONValue[0]'),
    json_tuple(col('jsonString'), 'myJSONKey')
).show()
