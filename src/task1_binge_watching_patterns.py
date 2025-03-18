from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round as spark_round

def initialize_spark(app_name="Task1_Binge_Watching_Patterns"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the movie ratings data from a CSV file into a Spark DataFrame.
    """
    schema = """
        UserID INT, MovieID INT, MovieTitle STRING, Genre STRING, Rating FLOAT, ReviewCount INT, 
        WatchedYear INT, UserLocation STRING, AgeGroup STRING, StreamingPlatform STRING, 
        WatchTime INT, IsBingeWatched BOOLEAN, SubscriptionStatus STRING
    """
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def detect_binge_watching_patterns(df):
    """
    Identify the percentage of users in each age group who binge-watch movies.
    """
    # Filter users who have IsBingeWatched = True
    binge_watchers = df.filter(col("IsBingeWatched") == True)

    # Group by AgeGroup and count the number of binge-watchers
    binge_watch_count = binge_watchers.groupBy("AgeGroup").agg(count("UserID").alias("BingeWatchers"))

    # Count the total number of users in each age group
    total_users_per_age_group = df.groupBy("AgeGroup").agg(count("UserID").alias("TotalUsers"))

    # Join both dataframes to calculate percentages
    result_df = binge_watch_count.join(total_users_per_age_group, "AgeGroup") \
        .withColumn("Percentage", spark_round((col("BingeWatchers") / col("TotalUsers")) * 100, 2))

    return result_df

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a single CSV file using pandas.
    """
    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = result_df.toPandas()
    # Write to CSV
    pandas_df.to_csv(output_path, index=False)

def main():
    """
    Main function to execute Task 1.
    """
    spark = initialize_spark()

    input_file = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-saiharsha27/input/movie_ratings_data.csv"
    output_file = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-saiharsha27/Outputs/binge_watching_patterns.csv"

    df = load_data(spark, input_file)
    result_df = detect_binge_watching_patterns(df)  # Call function here
    write_output(result_df, output_file)

    spark.stop()

if __name__ == "__main__":
    main()
