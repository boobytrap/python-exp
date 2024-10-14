from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# Retrieve the storage key from an environment variable
storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
storage_key = os.getenv("AZURE_STORAGE_KEY")

# Check if the key is not set
if not storage_account:
    # Prompt the user to input the key if it's not set
    storage_account = input("Please enter your Azure Storage Account Name: ")

# Check if the key is not set
if not storage_key:
    # Prompt the user to input the key if it's not set
    storage_key = input("Please enter your Azure Storage Account Access Key: ")

#spark = SparkSession.builder \
    #.appName("optimize") \
    #.config("spark.jars.packages", "org.apache.hadoop:hadoop-azure-datalake:3.2.0,org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6") \
    #.getOrCreate()
    #.config("spark.jars.ivy", "/path/to/custom/ivy/cache") \

spark = SparkSession.builder \
    .appName("optimize") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure-datalake:3.2.0,org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6") \
    .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .config("spark.executor.instances", 4) \
    .config("spark.executor.cores", 4) \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

#conf = spark.conf.set("spark.driver.memory", "4g")
#SparkContext(conf=conf)


    #.config("spark.eventLog.enabled", "true") \cd /
    #.config("spark.eventLog.dir", "file:///home/user/spark/logs") \

spark.conf.set("fs.azure.account.key."+storage_account+".blob.core.windows.net", storage_key)
spark.conf.set("spark.default.parallelism", 10)
spark.conf.set("spark.sql.shuffle.partitions", 10)

#teams = ["phillies", "yankees"]
teams = []
with open("mlb_teams.txt", "r") as f:
        for line in f:
            teams.append(line.strip())

df = {}
grouped_df = {}

### DF1 ###
for team in teams:
    container_name = "mlb"
    blob_stats = 'data/'+team+'/stats_all-time-by-season.csv'
    blob_clean = 'data/'+team+'/stats_clean.csv'

    # Read the CSV file with options
    df[team] = spark.read.format("csv").option("delimiter", ",").option("header", True) \
            .load("wasbs://"+container_name+"@"+storage_account+".blob.core.windows.net/"+blob_clean)
    
    #df[team] = df[team].groupBy("YEAR").agg(F.max(F.col("HR").cast("int")).alias("HR record per year"))

    grouped_df[team] =  df[team].groupBy("YEAR").agg(F.max(F.col("HR").cast("int")).alias(team+" HR record per year"))
    grouped_df[team].sort(grouped_df[team][team+" HR record per year"].desc())
#######################

### DF2 ###
print ("Players who played for 2 MLB franchises")
print ("***************************************")
for i in range(0, len(teams)):
     for j in range(i+1, len(teams)):
        team1, team2 = teams[i], teams[j]
        
        df[team1] = df[team1].withColumnRenamed("Year", "Year_at_"+team1)
        df[team2] = df[team2].withColumnRenamed("Year", "Year_at_"+team2)

        joined_df = df[team1].join(df[team2], on="PLAYER", how="inner").sort(df[team1]["Year_at_"+team1].desc())
        joined_df.select("Player", "Year_at_"+team1, "Year_at_"+team2)

        df_agg_dedup = joined_df.groupBy("Player") \
            .agg(
                F.collect_set("Year_at_"+team1).alias("Years_at_"+team1),
                F.collect_set("Year_at_"+team2).alias("Years_at_"+team2))

        if not df_agg_dedup.rdd.isEmpty():
             df_agg_dedup.show(truncate=False)
#######################


### DF3 ###

print ("Players who played for 3 MLB franchises")
print ("***************************************")
for i in range(0, len(teams)):
     for j in range(i+1, len(teams)):
        for k in range(j+1, len(teams)):
            team1, team2, team3 = teams[i], teams[j], teams[k]
            
            df[team1] = df[team1].withColumnRenamed("Year", "Year_at_"+team1)
            df[team2] = df[team2].withColumnRenamed("Year", "Year_at_"+team2)
            df[team3] = df[team3].withColumnRenamed("Year", "Year_at_"+team3)

            joined_df = df[team1].join(df[team2], on="PLAYER", how="inner").join(df[team3], on="PLAYER", how="inner").sort(df[team1]["Year_at_"+team1].desc())
            joined_df.select("Player", "Year_at_"+team1, "Year_at_"+team2, "Year_at_"+team3)

            df_agg_dedup = joined_df.groupBy("Player") \
                .agg(
                    F.collect_set("Year_at_"+team1).alias("Years_at_"+team1),
                    F.collect_set("Year_at_"+team2).alias("Years_at_"+team2),
                    F.collect_set("Year_at_"+team3).alias("Years_at_"+team3))

            if not df_agg_dedup.rdd.isEmpty():
                df_agg_dedup.show(truncate=False)
#######################

spark.stop()
