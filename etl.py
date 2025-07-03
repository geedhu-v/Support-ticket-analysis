from functools import lru_cache
from pathlib import Path
from typing import List

from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, avg, when, max as spark_max, min as spark_min
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    TimestampType,
    DoubleType,
)

@lru_cache(maxsize=1)
def get_spark():
    sc = SparkContext(master="local[1]", appName="Support Tickets Transformer")
    spark = SparkSession(sc)
    return spark


def load_tickets(tickets_path: Path) -> DataFrame:
    """Load and parse tickets from JSONL file.

    Args:
        tickets_path: Path to the tickets JSONL file

    Returns:
        DataFrame with ticket data
    """
    schema = StructType(
        [
            StructField("ticketId", StringType(), False),
            StructField("teamId", IntegerType(), False),
            StructField("priorityId", IntegerType(), False),
            StructField("resolved", BooleanType(), False),
            StructField("createdAt", TimestampType(), False),
            StructField("resolvedAt", TimestampType(), True),
            StructField("responseTime", IntegerType(), True),
            StructField("satisfactionScore", IntegerType(), True),
        ]
    )

    return get_spark().read.json(str(tickets_path), schema=schema, multiLine=True)


def load_teams(teams_path: Path) -> DataFrame:
    """Load and parse teams from CSV file.

    Args:
        teams_path: Path to the teams CSV file

    Returns:
        DataFrame with team data
    """
    schema = StructType(
        [
            StructField("teamId", IntegerType(), False),
            StructField("teamName", StringType(), False),
        ]
    )

    return get_spark().read.csv(str(teams_path), header=True, schema=schema)


def load_priorities() -> DataFrame:
    """Return a DataFrame with priority ID to name mapping.

    Returns:
        DataFrame with priority data
    """
    # TODO: Implement this function
    # Should return a DataFrame with columns: priorityId, priorityName
    # Data should be: (1, "High"), (2, "Medium"), (3, "Low")
    data = [(1, "High"), (2, "Medium"), (3, "Low")]
    schema = StructType(
        [
            StructField("priorityId", IntegerType(), True),
            StructField("priorityName", StringType(), True),
        ]
    )
    spark = get_spark()
    df = spark.createDataFrame(data,schema)
    return df

def join_tables(tickets: DataFrame, teams: DataFrame, priorities: DataFrame) -> DataFrame:
    """Join tickets, teams, and priorities tables.

    Args:
        tickets: DataFrame with ticket data
        teams: DataFrame with team data
        priorities: DataFrame with priority data

    Returns:
        Joined DataFrame with all relevant columns
    """
    # TODO: Fix this function to accept and join all three DataFrames
    # Join tickets and teams
    joined_df = tickets.join(teams, on="teamId",how="left")
    # Join tickets and teams with priorities
    joined_df = joined_df.join(priorities, on="prorityId", how = "left")
    
    return joined_df


def filter_late_tickets(data: DataFrame, hours: int) -> DataFrame:
    """Filter out tickets that were resolved more than specified hours after creation.

    Args:
        data: DataFrame with ticket data
        hours: Maximum hours between creation and resolution

    Returns:
        Filtered DataFrame with only resolved tickets within time limit
    """
    return data.filter(col("resolved") == True).filter(
        col("responseTime") <= hours * 60
    )


def calculate_team_scores(data: DataFrame) -> DataFrame:
    """Calculate final scores for each team and priority.
    
    For each team and priority:
    - If priority is High or Medium, use max satisfaction score
    - If priority is Low, use min satisfaction score

    Args:
        data: DataFrame with ticket data

    Returns:
        DataFrame with team scores by priority
    """
    # TODO: Implement this function
    # Should return DataFrame with columns: teamId, teamName, priorityId, priorityName, finalScore
    #pass
    # Add a new column for aggregation type
    data = data.withColumn(
        "aggType", when(col("priorityName").isin("High","Medium"),"max").otherwise("min")
    )
    
    # Separate tickets based on priority
    high_medium_df = data.filter(col("aggType") == "max").groupBy(
        "teamId", "teamName", "priorityId", "priorityName"
    ).agg(
        spark_max("satisfactionScore").alias("finalScore")
    )

    low_df = data.filter(col("aggType") == "min").groupBy(
        "teamId", "teamName", "priorityId", "priorityName"
    ).agg(
        spark_min("satisfactionScore").alias("finalScore")
    )

    # Union both results
    final_df = high_medium_df.unionByName(low_df)

    return final_df


def save(data: DataFrame, output_path: Path) -> None:
    """Save DataFrame to parquet format.

    Args:
        data: DataFrame to save
        output_path: Path where to save the data
    """
    # TODO: Fix this function
    data.write.mode("overwrite").partitionBy("teamId").parquet(str(output_path))
