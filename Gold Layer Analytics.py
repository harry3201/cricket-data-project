# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,importing libraries
from pyspark.sql.functions import *


# COMMAND ----------

silver_df = spark.read.table('workspace.default.cricket_silver_matches')
display(silver_df)

# COMMAND ----------

# DBTITLE 1,gold analytics 1 : match type distribution
gold_match_type_df = silver_df.groupBy('match_type').agg(count('*').alias('Total_Matches'))
display(gold_match_type_df)

# COMMAND ----------

# DBTITLE 1,gold analytics 2 :venue type distribution
gold_venue_df=silver_df.groupBy('venue').agg(count('*').alias('Total_Matches'))
display(gold_venue_df)

# COMMAND ----------

# DBTITLE 1,gold analytics 3: team -wise match count
team1_df=silver_df.select(col("team_1").alias("team"))
team2_df=silver_df.select(col("team_2").alias("team"))
all_teams_df=team1_df.union(team2_df)
gold_team_df=all_teams_df.groupBy('team').agg(count('*').alias('Total_Matches'))
display(gold_team_df)


# COMMAND ----------

# DBTITLE 1,Cell 6
# Gold Analytics 4: Distinct Match Types
distinct_match_types = silver_df.select('match_type').distinct().count()
print(f"Total Distinct Match Types: {distinct_match_types}")

# Gold Analytics 5: Distinct Venues
distinct_venues = silver_df.select('venue').distinct().count()
print(f"Total Distinct Venues: {distinct_venues}")

# Gold Analytics 6: Match Outcome Distribution (Winner Analysis)
# Extract winner from status column (format: "Team won by X" or "Team opt to bat/bowl" for ongoing)
from pyspark.sql.functions import when, regexp_extract

gold_winner_df = silver_df.filter(col('match_ended') == True) \
    .withColumn('winner', regexp_extract(col('status'), r'^(.*?)\s+won\s+by', 1)) \
    .filter(col('winner') != '') \
    .groupBy('winner') \
    .agg(count('*').alias('Total_Wins')) \
    .orderBy(col('Total_Wins').desc())

display(gold_winner_df)