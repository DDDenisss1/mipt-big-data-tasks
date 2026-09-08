#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import hyperloglog

sc = SparkContext(appName="KafkaStreamingTask5")
ssc = StreamingContext(sc, 10)

KAFKA_BROKER = "mipt-node08.atp-fivt.org:9092"
TOPIC = "bessmertnyjde-topic"

kafka_params = {
    "metadata.broker.list": KAFKA_BROKER
}

stream = KafkaUtils.createDirectStream(
    ssc,
    [TOPIC],
    kafka_params
)

SEGMENTS = ["seg_windows", "seg_firefox", "seg_iphone"]

hll_state = {
    seg: hyperloglog.HyperLogLog(0.01)
    for seg in SEGMENTS
}

def classify(ua):
    ua = ua.lower()
    res = []

    if "windows" in ua:
        res.append("seg_windows")

    if "firefox" in ua:
        res.append("seg_firefox")

    if "iphone" in ua:
        res.append("seg_iphone")

    return res

def process_rdd(rdd):

    if rdd.isEmpty():
        return

    def update_partition(partition):

        local = {seg: hyperloglog.HyperLogLog(0.01) for seg in SEGMENTS}

        for _, value in partition:
            parts = value.split("\t")

            if len(parts) != 2:
                continue

            uid = parts[0]
            ua = parts[1]

            for seg in classify(ua):
                local[seg].add(uid)

        yield local

    partials = rdd.mapPartitions(update_partition).collect()

    for part in partials:
        for seg in SEGMENTS:
            hll_state[seg].update(part[seg])

stream.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTerminationOrTimeout(120)

results = [(seg, len(hll_state[seg])) for seg in SEGMENTS]
results.sort(key=lambda x: (-x[1], x[0]))

for seg, cnt in results:
    print(seg, cnt)
