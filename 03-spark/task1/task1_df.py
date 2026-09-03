#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, concat, split
# import time  # Для подсчета CPU time

spark = SparkSession.builder \
    .appName("task1_df") \
    .master("yarn") \
    .getOrCreate()


edges = spark.read.text("/data/twitter/twitter_sample.txt") \
    .selectExpr(
        "cast(split(value, '\t')[1] as int) as src",
        "cast(split(value, '\t')[0] as int) as dst"
    ).repartition(100).cache()

start = 12
target = 34


# cpu_start = time.process_time()

# Инициализация с явным кэшированием
frontier = spark.createDataFrame([(start, [start])], ["id", "path"]).cache()
visited = spark.createDataFrame([(start,)], ["id"]).cache()

result = None

while True:

    candidates = frontier.join(edges, frontier.id == edges.src) \
        .select(
            col("dst").alias("id"),
            concat(col("path"), array(col("dst"))).alias("path")
        )
    
    # освобождаем память
    frontier.unpersist()


    new_frontier = candidates.join(visited, "id", "left_anti").cache()


    target_row = new_frontier.filter(col("id") == target).limit(1).collect()

    if target_row:
        result = target_row[0]["path"]
        break

    if new_frontier.rdd.isEmpty():
        break


    new_visited = visited.union(new_frontier.select("id")).distinct().cache()
    visited.unpersist()
    visited = new_visited

    frontier = new_frontier

# cpu_end = time.process_time()
# cpu_time = cpu_end - cpu_start

if result:
    print(",".join(map(str, result)))

# print("CPU time:", cpu_time)
