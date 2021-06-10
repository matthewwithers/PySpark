from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import col, count, desc, asc, expr

spark = SparkSession\
    .builder\
    .appName('ExampleApp')\
    .master('local')\
    .getOrCreate()

# spark.conf.set('spark.sql.caseSensitive', True)

if __name__ == '__main__':
    df = spark\
        .read\
        .format('csv')\
        .option('header', 'true')\
        .load('../Spark-The-Definitive-Guide/data/flight-data/csv/2015-summary.csv')

    df.createOrReplaceTempView('dfTable')

    # with case sensitive is enabled this would fail
    df.selectExpr('dest_country_name').show(5)
    df.drop('origin_country_name', 'count').show(5)

    # casting column types
    df.withColumn(
        'count2',  # new column name
        col('count').cast('long')  # new column value
    ).show(5)

    df.where('count < 2').show()

    # single filtering
    df\
        .where('origin_country_name == \'United States\'').show()

    # multi filtering
    df\
        .where('origin_country_name == \'United States\'')\
        .where('count <= 5')\
        .where(col('dest_country_name').like('%dova%'))\
        .show()

    print(df.select('origin_country_name',
                    'dest_country_name').distinct().count())

    seed = 5
    withReplacement = False
    fraction = 0.5
    print(df.sample(withReplacement=withReplacement,
          fraction=fraction, seed=seed).count())

    schema = df.schema
    newRows = [
        Row('New Country', 'Other Country', 5),
        Row('New Country 2', 'Other Country 3', 1)
    ]
    parallizedRows = spark.sparkContext.parallelize(newRows)
    newDF = spark.createDataFrame(parallizedRows, schema)
    df.union(newDF)\
        .where('count = 1')\
        .where(col('origin_country_name') != 'United States')\
        .show()

    # sorting
    df.sort('count').show(5)
    df.orderBy('count', 'dest_country_name').show()
    df.orderBy(col('count'), col('dest_country_name'))
    df.orderBy(desc('count'), asc('dest_country_name')).show()

    df = spark\
        .read\
        .format('json')\
        .load('../Spark-The-Definitive-Guide/data/flight-data/json/*-summary.json')\
        .sortWithinPartitions('count')

    df.limit(5).show()
    df.orderBy(desc('dest_country_name')).limt(5).show()
