from pyspark.sql import SparkSession

# schema = StructType([
#     StructField("author", StringType(), False),
#     StructField("title", StringType(), False),
#     StructField("pages", IntegerType(), False)
# ])

# schema = "author STRING, title STRING, pages INT"

schema = """
    `Id` INT,
    `First` STRING,
    `Last` STRING,
    `Url` STRING,
    `Published` STRING,
    `Hits` INT,
    `Campaigns` ARRAY<STRING>
"""

data = [
    [1, "Jules", "Damji", "https://tinyurl.1",
        "1/4/2016", 4535, ["twitter", "Linkedin"]],
    [2, "Brooke", "Wenig", "https://tinyurl.1",
        "1/4/2016", 4535, ["twitter", "Linkedin"]],
    [3, "Denny", "Lee", "https://tinyurl.1",
        "1/4/2016", 4535, ["twitter", "Linkedin"]],
    [4, "Tathagata", "Das", "https://tinyurl.1",
        "1/4/2016", 4535, ["twitter", "Linkedin"]],
    [5, "Matei", "Zaharia", "https://tinyurl.1",
        "1/4/2016", 4535, ["twitter", "Linkedin"]],
    [6, "Reynold", "Xin", "https://tinyurl.1",
        "1/4/2016", 4535, ["twitter", "Linkedin"]],
]

if __name__ == "__main__":
    # create a spark session
    spark = (
        SparkSession
        .builder
        .appName("Example-3_6")
        .getOrCreate()
    )

    # create a DataFrame using the schema defined above
    blogs_df = spark.createDataFrame(data, schema)
    # show dataframe
    blogs_df.show()
    # print the schema used by spark to process the DataFrame
    print(blogs_df.printSchema())
