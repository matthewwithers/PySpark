from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Define schema for our data using DDL
schema = """
    Id INT,
    First STRING,
    Last STRING,
    Url STRING,
    Published STRING,
    Hits INT,
    Campaigns ARRAY<STRING>
"""

# Create static data
data = [
    [1, "Jules", "Damji", "https://tinyurl.1",
        "1/4/2016", 4535, ['Twitter', 'LinkedIn']],
    [2, "Brooke", "Wenig", "https://tinyurl.2",
     "1/4/2016", 4535, ['web', 'FB', 'Twitter', 'LinkedIn']],
    [3, "Denny", "Lee", "https://tinyurl.3",
     "1/4/2016", 4535, ['Twitter', 'LinkedIn']],
    [4, "Tathagata", "Das", "https://tinyurl.4",
     "1/4/2016", 4535, ['web', 'FB', 'Twitter', 'LinkedIn']],
    [5, "Matei", "Zaharia", "https://tinyurl.5",
     "1/4/2016", 4535, ['Twitter', 'LinkedIn']],
    [6, "Reynold", "Xin", "https://tinyurl.6",
     "1/4/2016", 4535, ['web', 'FB', 'Twitter', 'LinkedIn']],
]


if __name__ == "__main__":
    # Create Spark Session
    spark = (
        SparkSession
        .builder
        .appName('Example App')
        .getOrCreate()
    )

    blogs_df = spark.createDataFrame(data, schema=schema)
    blogs_df.show()
    print(blogs_df.printSchema())
