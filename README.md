# Movie Ratings Analysis with PySpark

This project performs various analyses on movie ratings data using PySpark's structured API. It consists of three main tasks that help analyze user behavior, identify churn risk, and track movie watching trends.

## Project Structure

```
handson-7-spark-structured-api-movie-ratings-analysis/
├── input/
│   └── movie_ratings_data.csv
├── Outputs/
│   ├── binge_watching_patterns.csv
│   ├── churn_risk_users.csv
│   └── movie_watching_trends.csv
├── src/
│   ├── task1_binge_watching_patterns.py
│   ├── task2_churn_risk_users.py
│   └── task3_movie_watching_trends.py
├── README.md
└── docker-compose.yml
```

## Dataset

The input dataset (`movie_ratings_data.csv`) contains movie ratings data with the following schema:
- UserID (INT)
- MovieID (INT)
- MovieTitle (STRING)
- Genre (STRING)
- Rating (FLOAT)
- ReviewCount (INT)
- WatchedYear (INT)
- UserLocation (STRING)
- AgeGroup (STRING)
- StreamingPlatform (STRING)
- WatchTime (INT)
- IsBingeWatched (BOOLEAN)
- SubscriptionStatus (STRING)

## Approach

### General Approach

This project uses PySpark's structured API to analyze large-scale movie ratings data efficiently. The approach follows these common steps across all tasks:

1. **Data Loading**: Each task initializes a SparkSession and loads data using a predefined schema
2. **Data Transformation**: Task-specific transformations are applied using PySpark DataFrame operations
3. **Result Generation**: Results are computed and formatted according to the requirements
4. **Output Writing**: Results are converted to pandas DataFrames and written to CSV files

### Task-Specific Approaches

#### Task 1: Detect Binge Watching Patterns

For this task, the approach involves:
- Filtering the dataset to isolate users who have binge-watched movies
- Grouping the filtered data by age groups
- Performing an aggregation to count users in each group
- Calculating the percentage of binge-watchers relative to the total users in each age group
- Rounding the percentages to two decimal places for readability

#### Task 2: Identify Churn Risk Users

The approach for identifying churn risk:
- Applying multiple filter conditions simultaneously to identify users with canceled subscriptions and low watch time
- Counting the distinct users that meet these criteria
- Presenting the results in a format that highlights the potential churn risk segment

#### Task 3: Analyze Movie Watching Trends

For trend analysis:
- Grouping data by the year movies were watched
- Counting the number of movie watches per year
- Ordering the results by year to create a chronological view of trends
- Using the resulting time series to identify patterns in movie watching behavior

## Findings

### Binge Watching Patterns

Based on the analysis, we found that:
- Different age groups display varying tendencies toward binge-watching
- The output shows the percentage of users in each age group (Teen, Adult, Senior) who engage in binge-watching
- This information can help streaming platforms tailor content recommendations and release strategies for different age segments

### Churn Risk Users

Key findings about churn risk:
- A specific segment of users exhibiting both canceled subscriptions and low watch time was identified
- These users represent a critical cohort for potential re-engagement strategies
- The count of such users provides a baseline for measuring the effectiveness of retention efforts

### Movie Watching Trends

The trend analysis revealed:
- Patterns in movie watching behavior over different years
- Years with higher movie consumption, potentially correlating with major releases or external factors
- Potential seasonal or annual trends that could inform content acquisition and marketing strategies

## Challenges Faced and Solutions

### Challenge 1: CSV Output Format
**Challenge:** By default, Spark's `write.csv()` method creates a directory with part files rather than a single CSV file, which made it difficult to produce the expected output format.

**Solution:** Modified the `write_output` function to convert Spark DataFrames to pandas DataFrames and use pandas' `to_csv()` method, which directly writes a single CSV file. This approach eliminated the need for post-processing part files.

### Challenge 2: Data Type Handling
**Challenge:** Boolean values in the input data needed special handling, particularly for the `IsBingeWatched` column.

**Solution:** Used explicit schema definition during data loading to ensure proper data type interpretation. Explicitly filtered boolean values using `col("IsBingeWatched") == True` syntax to ensure compatibility with Spark's SQL expression evaluation.

### Challenge 3: Output Format for Churn Risk Analysis
**Challenge:** The expected output format for the churn risk analysis required a specific presentation that differed from the default aggregation output.

**Solution:** Created a custom DataFrame with the desired structure using `createDataFrame()` method with explicitly defined column names and a tuple containing the descriptive text and count value. This ensured the output matched the expected format while maintaining the analytical results.

### Challenge 4: Percentage Calculation and Rounding
**Challenge:** Calculating accurate percentages of binge-watchers and ensuring proper rounding to two decimal places.

**Solution:** Used Spark's mathematical functions for division and the `spark_round` function to calculate and round percentages. Joined the DataFrames containing binge-watcher counts and total user counts before calculating percentages to ensure accurate ratio computation.

### Challenge 5: Resource Management
**Challenge:** Efficient resource utilization when processing potentially large datasets.

**Solution:** Implemented proper SparkSession management with explicit `spark.stop()` calls to release resources after job completion. Used appropriate DataFrame operations like filtering early in the processing pipeline to reduce data volume in subsequent operations.

## Implementation Details

- Each task is implemented in a separate Python file
- All tasks use PySpark's structured API for data processing
- The `write_output` function was modified to write results directly to CSV files using pandas
- Results are stored in the `Outputs` folder

## Running the Code

To run any of the tasks:

```bash
spark-submit src/task1_binge_watching_patterns.py
spark-submit src/task2_churn_risk_users.py
spark-submit src/task3_movie_watching_trends.py
```

## Dependencies

- PySpark
- pandas

## Conclusion

This project demonstrates the power of PySpark's structured API for analyzing large-scale movie ratings data. By examining binge-watching patterns, identifying churn risk users, and analyzing movie watching trends, we can gain valuable insights into user behavior and preferences. These insights can inform content recommendation systems, retention strategies, and content acquisition decisions for streaming platforms.

The modular structure of the code allows for easy extension to include additional analyses or to adapt to changes in the data schema. The use of pandas for output writing simplifies the process of generating readable CSV files from Spark DataFrames.

By overcoming the challenges described above, we've created a robust analytical solution that produces clean, useful outputs while leveraging the distributed processing capabilities of Spark for scalable data analysis.