from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import expr, lit
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

    df.select(
        expr('*'),
        lit(1).alias('One')
    ).show(2)

    df\
        .withColumn('numberOne', lit(1))\
        .withColumn('numberTwo', lit(2))\
        .show()

    df\
        .withColumn('withinCountry', expr('DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME'))\
        .show()

    df\
        .withColumn('destination', expr('DEST_COUNTRY_NAME'))\
        .show()

    df\
        .withColumnRenamed('DEST_COUNTRY_NAME', 'New Column Name')\
        .show()

    dfWithLongColName = df.withColumn(
        "This Long Column-Name",
        expr('ORIGIN_COUNTRY_NAME')
    )

    dfWithLongColName.show()

    dfWithLongColName.selectExpr(
        # use `` when dealing with string that have spaces and/or reserved characters
        "`This Long Column-Name`",
        "`This Long Column-Name` as `new col`"
    ).show()

    dfWithLongColName\
        .selectExpr('`This Long Column-Name`')\
        .show(2)
