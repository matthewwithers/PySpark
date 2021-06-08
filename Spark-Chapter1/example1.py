from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName('Sample App') \
    .getOrCreate() 

flightData2015 = spark \
    .read \
    .option('inferSchema', 'True') \
    .option('header', 'true') \
    .csv('../Spark-The-Definitive-Guide/data/flight-data/csv/2015-summary.csv')

flightData2015.show()
# print(flightData2015.take(3))
# print(flightData2015.sort('count').explain())

# DataFrames & Sql
flightData2015.createOrReplaceTempView('flight_data_2015')

sqlWay = spark.sql(
    """
    SELECT DEST_COUNTRY_NAME, count(1)
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
    """
)

dataFrameWay = flightData2015 \
    .groupBy('DEST_COUNTRY_NAME') \
    .count()

sqlWay.show()
sqlWay.explain()

dataFrameWay.show()
dataFrameWay.explain()
