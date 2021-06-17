from pyspark.sql import SparkSession
from pyspark.sql.functions import *

"""
Topics Covered:

Booleans
Numbers
Strings
Dates and timestamps 
Handling null 
Complex types 
User-defined functions

"""
if __name__ == '__main__':

    spark = SparkSession\
        .builder\
        .appName('ExampleApp')\
        .getOrCreate()

    df = spark.read.format('csv')\
        .option('header', 'true')\
        .option('inferSchema', 'true')\
        .load('../Spark-The-Definitive-Guide/data/retail-data/by-day/2010-12-01.csv')

    df.printSchema()
    df.createOrReplaceTempView('dfTable')

    # Working with numbers
    fabricatedQuantity = pow(col('Quantity') * col('UnitPrice'), 2) + 5
    df\
        .select(
            expr('CustomerId').alias('Customer ID'),
            fabricatedQuantity.alias('Real Quantity'),
            col('Quantity').alias('Original Qty')
        ).show()

    df\
        .selectExpr(
            'customerId',
            '(POWER((Quantity * UnitPrice),2) + 5) as real_quantity',
            'Quantity as org_qty'
        ).show(1)

    df\
        .select(
            round(lit(2.5)),
            bround(lit(2.5))
        ).show(1)

    df.describe().show()
