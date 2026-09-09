#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import sys
import argparse
import hyperloglog

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

def parse_args():
    parser = argparse.ArgumentParser(description="Spark Streaming Kafka Task 6")
    parser.add_argument("--partition", type=int, help="Kafka partition number (optional)")
    parser.add_argument("--offset-from", type=int, help="Start offset (inclusive, optional)")
    parser.add_argument("--offset-to", type=int, help="End offset (exclusive, optional)")
    return parser.parse_args()

def main():
    args = parse_args()

    sc = SparkContext(appName="KafkaStreamingTask6")
    ssc = StreamingContext(sc, 10)

    KAFKA_BROKER = "mipt-node08.atp-fivt.org:9092"
    TOPIC = "bessmertnyjde-topic"
    kafka_params = {"metadata.broker.list": KAFKA_BROKER}

    from_offsets = None

    if args.partition is not None:
        tp = TopicAndPartition(TOPIC, args.partition)
        start_offset = args.offset_from if args.offset_from is not None else 0
        from_offsets = {tp: start_offset}
        print("[INFO] Reading partition=%s, from_offset=%s" % (args.partition, start_offset), file=sys.stderr)
    else:
        if args.offset_from is not None:
            print("[WARN] --offset-from ignored without --partition (0-8 API limitation)", file=sys.stderr)
        if args.offset_to is not None:
            print("[WARN] --offset-to is approximate in streaming mode (batch-level filtering)", file=sys.stderr)

    if from_offsets:
        stream = KafkaUtils.createDirectStream(
            ssc, [TOPIC], kafka_params, fromOffsets=from_offsets
        )
    else:
        stream = KafkaUtils.createDirectStream(ssc, [TOPIC], kafka_params)

    SEGMENTS = ["seg_windows", "seg_firefox", "seg_iphone"]
    hll_state = {seg: hyperloglog.HyperLogLog(0.01) for seg in SEGMENTS}

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

        if args.offset_to is not None and args.partition is not None:
            for o in rdd.offsetRanges():
                if o.partition == args.partition:
                    if o.fromOffset >= args.offset_to:
                        print("[SKIP] Batch offset %s >= limit %s, skipping" % (o.fromOffset, args.offset_to), file=sys.stderr)
                        return
                    
                    # Если батч частично пересекает лимит — обрабатываем целиком
        def update_partition(partition_iter):
            local_hll = {seg: hyperloglog.HyperLogLog(0.01) for seg in SEGMENTS}

            for record in partition_iter:
                key, value = record
                if value is None:
                    continue

                parts = value.split("\t")
                if len(parts) != 2:
                    continue

                uid, ua = parts[0], parts[1]
                for seg in classify(ua):
                    local_hll[seg].add(uid)

            yield local_hll

        partials = rdd.mapPartitions(update_partition).collect()

        for part in partials:
            for seg in SEGMENTS:
                hll_state[seg].update(part[seg])

    stream.foreachRDD(process_rdd)
    ssc.start()
    print("[INFO] Streaming started, waiting 120 seconds...", file=sys.stderr)
    ssc.awaitTerminationOrTimeout(120)
    ssc.stop(stopSparkContext=False, stopGraceFully=True)

    results = [(seg, len(hll_state[seg])) for seg in SEGMENTS]
    results.sort(key=lambda x: (-x[1], x[0]))

    print("\n=== RESULTS ===", file=sys.stderr)
    for seg, cnt in results:
        print("%s %s" % (seg, cnt))

if __name__ == "__main__":
    main()
