#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import hyperloglog
import os
from hdfs import Config
import subprocess
import sys

client = Config().get_client()
nn_address = subprocess.check_output(
    'hdfs getconf -confKey dfs.namenode.http-address',
    shell=True
).strip().decode("utf-8")

sc = SparkContext(master='yarn-client')

DATA_PATH = "/data/course4/uid_ua_100k_splitted_by_5k"

batches = [
    sc.textFile(os.path.join(*[nn_address, DATA_PATH, path]))
    for path in client.list(DATA_PATH)[:30]
]

BATCH_TIMEOUT = 2
ssc = StreamingContext(sc, BATCH_TIMEOUT)

dstream = ssc.queueStream(rdds=batches)

SEGMENTS = ["seg_windows", "seg_firefox", "seg_iphone"]

hll_state = {
    seg: hyperloglog.HyperLogLog(0.01)
    for seg in SEGMENTS
}

def classify(ua):
    ua_lower = ua.lower()
    res = []

    if "windows" in ua_lower:
        res.append("seg_windows")

    if "firefox" in ua_lower:
        res.append("seg_firefox")

    if "iphone" in ua_lower:
        res.append("seg_iphone")

    return res


def process_rdd(rdd):
    if rdd.isEmpty():
        return

    def process_partition(partition):
        local_hll = {
            seg: hyperloglog.HyperLogLog(0.01)
            for seg in SEGMENTS
        }

        for line in partition:
            parts = line.split("\t")
            if len(parts) < 2:
                continue

            uid = parts[0].strip()
            ua = parts[1].strip()

            for seg in classify(ua):
                local_hll[seg].add(uid)

        yield local_hll

    partials = rdd.mapPartitions(process_partition).collect()

    for part in partials:
        for seg in SEGMENTS:
            hll_state[seg].update(part[seg])


dstream.foreachRDD(process_rdd)

ssc.start()

ssc.awaitTerminationOrTimeout(60)

results = [(seg, len(hll_state[seg])) for seg in SEGMENTS]
results.sort(key=lambda x: (-x[1], x[0]))

for seg, cnt in results:
    print("{} {}".format(seg, cnt))

sys.stdout.flush()

ssc.stop(stopSparkContext=True, stopGraceFully=True)
