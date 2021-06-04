from pyspark.sql import SparkSession, dataframe
from pyspark.sql.functions import avg, count

spark = (
    SparkSession
    .builder
    .appName('Averge Age')
    .getOrCreate()
)

# Create a DataFrame
df = spark.createDataFrame(
    [
        ("Brooke", 20),
        ("Denny", 31),
        ("Jules", 30),
        ("TD", 35),
        ("Brooke", 25)
    ], ['name', 'age']
)

# Group the same names together, aggregate their ages, and compute the average
avg_df = df.groupBy('name').agg(avg('age'))
avg_df.show()
