import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    # Build a SparkSession using the SparkSession APIs
    # If one does not exist, then create an instance.
    # There can only be one SparkSession per JVM
    spark = (SparkSession.builder.appName("PythonMnMCount").getOrCreate())

    # Get the M&M data set filename from the command-line arguments
    mnm_file = sys.argv[1]
    # Read the file into a Spark DataFrame using the CSV
    # Format by inferring the schema and specifying that the file contains a header (which provides column_names for CSVs
    mnm_df = (spark.read.format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(mnm_file))

    # Use the dataframe high level apis
    # RDDs are not use
    # Becuase of spark's functions return the same object, we chain function calls
    # 1. Select from the dataframe the fields "State", "Color", and "Count"
    # 2. Since we want to group each state and its M&M color use groupby()
    # 3. Aggregate counts of all colors and groupBy() state and color
    # 4. orderBy() in desc order

    count_mnm_df = (mnm_df
                    .select("State", "Color", "Count")
                    .groupBy("State", "Color")
                    .agg(count("Count").alias("Total"))
                    .orderBy("Total", ascending=False))

    # Show the resulting aggregations for all the states and colors
    # a total count of each per state
    # Note: Show() is an actionn, which will trigger the above query to be executed
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # while the above code aggregated and counted for all the states, what if we just want to see the data for a single state?
    # 1. Select from all rows in the DataFrame
    # 2. Filter only CA state
    # 3. groupBy() State and Color as shown above
    # 4. Aggregate the counts for each color
    # 5. orderBy() in desc order

    # Find the aggregated count for Californua by filtering
    ca_count_mnm_df = (
        mnm_df
        .select("State", "Color", "Count")
        .where(mnm_df.State == "CA")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False)
    )

    # Show the resulting aggregation for California
    # As above, show() is an action that will trigger the execution of the entire computation
    ca_count_mnm_df.show(n=10, truncate=False)

    spark.stop()
