from pyspark.sql import SparkSession

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

