#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor
import happybase

HBASE_HOST = 'mipt-node01.atp-fivt.org'
HBASE_TABLE = 'pubg_kills_bessmertnyjde'

INPUT_PATH = '/data/hobod/pubg'

BATCH_SIZE = 5000

def write_partition(rows):
    import happybase

    connection = happybase.Connection(HBASE_HOST, timeout=30000)
    table = connection.table(HBASE_TABLE)
    batch = table.batch(batch_size=BATCH_SIZE)
    cnt = 0

    for row in rows:
        try:
            match_id = str(row['match_id'])
            weapon = str(row['killed_by'])

            if weapon is None or weapon == "None":
                continue

            time_bucket = int(row['time_bucket'])
            kills = int(row['kills'])

            row_key = "{}_{}".format(match_id, time_bucket).encode('utf-8')
            column = "weapons:{}".format(weapon).encode('utf-8')
            value = str(kills).encode('utf-8')

            batch.put(row_key, {column: value})
            cnt += 1

            if cnt % BATCH_SIZE == 0:
                batch.send()
                batch = table.batch(batch_size=BATCH_SIZE)

        except Exception:
            pass

    batch.send()
    connection.close()

def main():
    spark = SparkSession.builder \
        .appName("PUBG_Writer_bessmertnyjde") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(INPUT_PATH)

    prepared = df.select(
        col("match_id"),
        col("killed_by"),
        col("time").cast("int")
    ).filter(
        col("match_id").isNotNull()
    ).filter(
        col("killed_by").isNotNull()
    ).filter(
        col("time").isNotNull()
    )

    prepared = prepared.withColumn(
        "time_bucket",
        (floor(col("time") / 100) * 100).cast("int")
    )

    aggregated = prepared.groupBy(
        "match_id",
        "time_bucket",
        "killed_by"
    ).count().withColumnRenamed("count", "kills")

    aggregated = aggregated.repartition(32)

    connection = happybase.Connection(HBASE_HOST, timeout=30000)
    connection.open()

    if HBASE_TABLE.encode('utf-8') not in connection.tables():
        connection.create_table(
            HBASE_TABLE,
            {
                'weapons': {
                    'compression': 'SNAPPY',
                    'bloom_filter_type': 'ROW'
                }
            }
        )

    connection.close()
    aggregated.foreachPartition(write_partition)
    spark.stop()

if __name__ == "__main__":
    main()
