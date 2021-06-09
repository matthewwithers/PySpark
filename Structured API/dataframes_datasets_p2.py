from pyspark.sql import SparkSession
from pyspark.sql.types import *
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
        .load('../Spark-The-Definitive-Guide/data/flight-data/csv/2015-summary.csv')

    df.createOrReplaceTempView('dfTable')

    """
    manually create the schema for the Row below
    """
    myManualSchema = StructType([
        StructField('some', StringType(), True),
        StructField('col', StringType(), True),
        StructField('names', LongType(), False)
    ])

    myRow = Row('Hello', None, 1)
    myDF = spark\
        .createDataFrame([myRow], myManualSchema)
    myDF.show()
