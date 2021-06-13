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
    df.show(5)

    # Booleans
    df\
        .where(col('InvoiceNo') != 536365)\
        .select('InvoiceNo', 'Description')\
        .show(5, False)

    df\
        .where("InvoiceNo = 536365")\
        .show(5, False)

    df\
        .where("InvoiceNo <> 536365")\
        .show(5, False)

    priceFilter = col('UnitPrice') > 600
    descriptionFilter = instr(df.Description, 'POSTAGE') >= 1
    df\
        .where(df.StockCode.isin('DOT'))\
        .where(priceFilter | descriptionFilter)\
        .show()

    DOTCodeFilter = col('StockCode') == 'DOT'
    priceFilter = col('UnitPrice') > 600
    descripFilter = instr(col('Description'), 'POSTAGE') >= 1

    df\
        .withColumn('isExpensive', DOTCodeFilter & (priceFilter | descripFilter))\
        .where('isExpensive')\
        .select('UnitPrice', 'isExpensive')\
        .show()

    df\
        .withColumn('isExpensive', expr('NOT UnitPrice <= 250'))\
        .where('isExpensive')\
        .select('Description', 'UnitPrice')\
        .show()
