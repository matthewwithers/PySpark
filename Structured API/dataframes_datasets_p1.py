
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import col,column, expr

spark = SparkSession\
    .builder\
    .appName('ExmapleApp')\
    .getOrCreate()\

"""
Structed API consists of 3 core types of distributed collection APIs
    1. Datasets
    2. Dataframes
    3. SQL Tables and views

Spark is a distributed programming model in which the user specifies "TRANSFROMATIONS"
Multiple transformations build up a directed graph of instructions.
An "ACTION" begin the process ofr executing that graph of instructions, as a single job,
by breaking it down into stages and tasks to execute across the cluster. The structures
that are manipulated with transformations and actions are Dataframes and Datasets.

To create a new DataFrame or Dataset you call a transformation.
To start a computation or covert to native language types you call an action.
"""

if __name__ == '__main__':


    print(spark.range(2).collect())
    # Creates an array of type Row(): [Row(id=0), Row(id=1)]

    df = spark\
            .read\
            .format('json')\
            .load('../Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json')
    

    df.printSchema()
    """
    root
        |-- DEST_COUNTRY_NAME: string (nullable = true)
        |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
        |-- count: long (nullable = true)
    """

    print(
        spark\
            .read\
            .format('json')\
            .load('../Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json')\
            .schema
    )
    
    """
    StructType (List (StructField(DEST_COUNTRY_NAME,StringType,true),
                      StructField(ORIGIN_COUNTRY_NAME,StringType,true),
                      StructField(count,LongType,true)))
    """


    myManualSchema = StructType([
        StructField('DEST_COUNTRY_NAME',StringType(),True),
        StructField('ORIGIN_COUNTRY_NAME',StringType(),True),
        StructField('count',LongType(),False,metadata={'hello':'world'})
    ])

    df = spark\
        .read\
        .format('json')\
        .schema(myManualSchema)\
        .load('../Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json')
    
    df.show()

    """
    In Spark, columns are logical constructions that simply represent a value computed on a per-record basis by means
    of an expression. This means that to have a real value for a column, we need to have a row, we need to have a Dataframe
    object. Its not possible to manipulate an individual column outside the context of a dataframe. You must use Spark Transformations
    within a Dataframe to modify the contents of a column.

    col['SomeColumnName']
    column['someColumnName']
    """

    df[df['count'] >= 7000].show()
    print(df.columns) # ['DEST_COUNTRY_NAME', 'ORIGIN_COUNTRY_NAME', 'count']
    print(df.first()) # Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Romania', count=15)

     # Creating Rows
    myRow = Row('Hello',None,1,False)
    print(myRow)
    print(myRow[0])
 
