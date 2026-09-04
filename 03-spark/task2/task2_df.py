#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, lower, split, posexplode,
    concat_ws, count, log, lead, lit
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, LongType, StringType

spark = SparkSession.builder.appName("WikiCollocations").getOrCreate()

stop_words_df = spark.read.text("/data/wiki/stop_words_en-xpo6.txt") \
    .withColumnRenamed("value", "stop")

# Замечание 1
schema = StructType([
    StructField("id", LongType(), True),
    StructField("text", StringType(), True)
])

articles_df = spark.read.csv(
    "/data/wiki/en_articles_part", 
    sep="\t", 
    header=False, 
    schema=schema
)

processed_df = articles_df.withColumn(
    "words",
    split(lower(col("text")), " ")
)

exploded_df = processed_df.select(
    col("id"),
    posexplode(col("words")).alias("pos", "word")
)

exploded_df = exploded_df.withColumn(
    "word",
    regexp_replace(col("word"), r"^\W+|\W+$", "")
)

valid_words_df = exploded_df \
    .filter(col("word") != "") \
    .filter(col("word").rlike("^[a-z]+$")) \
    .join(stop_words_df, col("word") == col("stop"), "left_anti")

valid_words_df.cache()

total_words_count = valid_words_df.count()

word_counts_df = valid_words_df.groupBy("word") \
    .agg(count("*").alias("word_count"))

# Замечание 2: Кешируем датафрейм
word_counts_df.cache()

window_spec = Window.partitionBy("id").orderBy("pos")

bigrams_df = valid_words_df \
    .withColumn("next_word", lead("word").over(window_spec)) \
    .filter(col("next_word").isNotNull()) \
    .select(
        concat_ws("_", col("word"), col("next_word")).alias("bigram"),
        col("word").alias("w1"),
        col("next_word").alias("w2")
    )

bigrams_df.cache()

total_pairs_count = bigrams_df.count()

bigram_counts_df = bigrams_df.groupBy("bigram") \
    .agg(count("*").alias("count"))

filtered_bigrams_df = bigram_counts_df.filter(col("count") >= 500)

final_df = filtered_bigrams_df \
    .withColumn("w1", split(col("bigram"), "_").getItem(0)) \
    .withColumn("w2", split(col("bigram"), "_").getItem(1))

df_j1 = final_df.join(
    word_counts_df.withColumnRenamed("word", "w1_join"),
    final_df.w1 == col("w1_join"),
    "left"
).withColumnRenamed("word_count", "count_w1") \
 .drop("w1_join")

df_j2 = df_j1.join(
    word_counts_df.withColumnRenamed("word", "w2_join"),
    df_j1.w2 == col("w2_join"),
    "left"
).withColumnRenamed("word_count", "count_w2") \
 .drop("w2_join")

df_npmi = df_j2 \
    .withColumn("P_ab", col("count") / lit(total_pairs_count)) \
    .withColumn("P_a", col("count_w1") / lit(total_words_count)) \
    .withColumn("P_b", col("count_w2") / lit(total_words_count)) \
    .withColumn("PMI", log(col("P_ab") / (col("P_a") * col("P_b")))) \
    .withColumn("NPMI", -col("PMI") / log(col("P_ab")))

result = df_npmi \
    .orderBy(col("NPMI").desc(), col("bigram")) \
    .select("bigram") \
    .limit(39)

for row in result.collect():
    print(row.bigram)

spark.stop()
