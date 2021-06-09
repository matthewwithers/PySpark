from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import expr, column, col
from pyspark.sql import Row

spark = SparkSession\
    .builder\
    .appName('ExampleApp')\
    .master('local')\
    .getOrCreate()

if __name__ == '__main__':
    df = spark\
        .read\
        .format('csv')\
        .option('header', 'true')\
        .load('../Spark-The-Definitive-Guide/data/flight-data/csv/2015-summary.csv')

    df.createOrReplaceTempView('dfTable')
    df.select('DEST_COUNTRY_NAME').show(2)
    df.select('DEST_COUNTRY_NAME', 'ORIGIN_COUNTRY_NAME').show()

    df.select(
        expr('DEST_COUNTRY_NAME'),
        col('DEST_COUNTRY_NAME')
    ).show(5)

    df.select(
        expr('DEST_COUNTRY_NAME as destination')
    ).show(5)
