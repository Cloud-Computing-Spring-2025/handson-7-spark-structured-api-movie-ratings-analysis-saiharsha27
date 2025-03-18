from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def initialize_spark(app_name="Task2_Churn_Risk_Users"):
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

def identify_churn_risk_users(df):
    """
    Identify users with canceled subscriptions and low watch time (<100 minutes).
    """
    # Filter users where SubscriptionStatus = 'Canceled' AND WatchTime < 100
    churn_risk_users = df.filter((col("SubscriptionStatus") == "Canceled") & (col("WatchTime") < 100))
    
    # Count the number of such users
    total_churn_risk_users = churn_risk_users.count()
    
    # Get total users count for comparison
    total_users = df.select("UserID").distinct().count()
    
    # Create a DataFrame with the expected format
    result_df = df.sparkSession.createDataFrame([
        ("Users with low watch time & canceled subscriptions", total_churn_risk_users)
    ], ["Churn Risk Users", "Total Users"])
    
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
    Main function to execute Task 2.
    """
    spark = initialize_spark()

    input_file = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-saiharsha27/input/movie_ratings_data.csv"
    output_file = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-saiharsha27/Outputs/churn_risk_users.csv"

    df = load_data(spark, input_file)
    result_df = identify_churn_risk_users(df)  # Call function here
    write_output(result_df, output_file)

    spark.stop()

if __name__ == "__main__":
    main()
