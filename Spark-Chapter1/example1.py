from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName('Sample App') \
    .getOrCreate() 

flightData2015 = spark \
    .read \
    .option('inferSchema', 'True') \
    .option('header','true') \
    .csv('./Spark-The-Definitive-Guide/data/flight-data/csv/2015-summary.csv')

flightData2015.show()
