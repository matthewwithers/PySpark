from pyspark.sql import SparkSession
from pyspark.sql.functions import max, desc

spark = SparkSession\
    .builder\
    .appName('Sample App')\
    .getOrCreate() 

flightData2015 = spark\
    .read\
    .option('inferSchema', 'True')\
    .option('header', 'true')\
    .csv('../Spark-The-Definitive-Guide/data/flight-data/csv/2015-summary.csv')

flightData2015.show()
# print(flightData2015.take(3))
# print(flightData2015.sort('count').explain())

# DataFrames & Sql
flightData2015.createOrReplaceTempView('flight_data_2015')

sqlWay = spark.sql(
    """
    SELECT 
        DEST_COUNTRY_NAME, count(1)
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

# Get the max values in the DataFrame
flightData2015 \
    .select(max('count')).take(1)

maxSql = spark.sql(
    """
    SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
    ORDER BY sum(count) DESC
    """
)

maxSql.show()


flightData2015\
    .groupBy('DEST_COUNTRY_NAME')\
    .sum('count')\
    .withColumnRenamed('sum(count)','destination_total')\
    .sort(desc("destination_total"))\
    .limit(5)\
    .show()


flightData2015\
    .groupBy('DEST_COUNTRY_NAME')\
    .sum('count')\
    .withColumnRenamed('sum(count)','destination_total')\
    .sort(desc("destination_total"))\
    .limit(5)\
    .explain()


