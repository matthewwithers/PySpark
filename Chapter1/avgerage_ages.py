from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.types import *

# schema = StructType([
#     StructField('author', StringType(), False),
#     StructField('title', StringType(), False),
#     StructField('pages', IntegerType, False)
# ])

# schema = "author STRING, title STRING, pages INT"

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
