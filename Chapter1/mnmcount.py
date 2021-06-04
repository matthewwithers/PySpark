# import the necessary libraries
# since this is a python file, import the SparkSession and related functions from PySpark Module
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    # Build a SparkSession using the SparkSession API
    # If one does not exist, then create an instance
    # There can only be one SparkSession per JVM

    spark = (SparkSession
             .builder
             .appName('PythonMnMCount')
             .getOrCreate()
             )
    # Get the m&m dataset filename from the command line arguments
    mnmfile = sys.argv[1]

    # Read the file into a Spark DataFrame using the CSV
    # Format by inferring the schema and specifying that the file contains a header
    # This provides column names from comma seperated fields

    mnm_df = (spark.read.format('csv')
              .option('header', 'true')
              .option('inferSchema', 'true')
              .load(mnmfile)
              )

    # Use the Dataframe highlevel api
    # Because some of spark's functions return the same object we can chain function calls
    # 1. Select form the dataframe the fields "State", "Color", "Count"
    # 2. Since we want to group each state and its M&M color count we use groupBy()
    # 3. Aggregate counts of all colors and groupBy() State and Color
    # 4. Orderby() in decending order

    count_mnm_df = (mnm_df
                    .select("State", "Color", "Count")
                    .groupBy("State", "Color")
                    .agg(count("Count").alias("Total"))
                    .orderBy("Total", ascending=False)
                    )
    # Show the resulting aggregations for all the states and colors;
    # a total count of each color per State
    # Note show() is an action. which will trigger the above query to be executed

    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    ca_count_mnm_df = (
        mnm_df
        .select("State", "Color", "Count")
        .where(mnm_df.State == "CA")
        .groupBy("State", "Color")
        .agg(count('Count').alias('Total'))
        .orderBy('Total', ascending=False)
    )

    # Show the resulting aggregation for California
    # As above, show() is an action that will trigger the exec of the entier computation

    ca_count_mnm_df.show(n=10, truncate=False)
    spark.stop()