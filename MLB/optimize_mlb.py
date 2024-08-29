from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("optimize") \
    .getOrCreate()

#teams = ["phillies", "yankees"]
teams = []
with open("mlb_teams.txt", "r") as f:
        for line in f:
            teams.append(line.strip())

df = {}
grouped_df = {}
for team in teams:
    # Read the CSV file with options
    df[team] = spark.read.format("csv").option("delimiter", ",").option("header", True) \
            .load(team+"_stats_clean.csv")
    
    #df[team] = df[team].groupBy("YEAR").agg(F.max(F.col("HR").cast("int")).alias("HR record per year"))

    grouped_df[team] =  df[team].groupBy("YEAR").agg(F.max(F.col("HR").cast("int")).alias(team+" HR record per year"))
    grouped_df[team].sort(grouped_df[team][team+" HR record per year"].desc()).show(n=10)

joined_df = df["phillies"].join(df["yankees"], on="PLAYER", how="inner").sort(df["phillies"]["YEAR"].desc())
joined_df.show(n=100)

spark.stop()
