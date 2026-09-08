#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from kafka import KafkaProducer
from hdfs import Config
import os
import time

# Kafka settings
KAFKA_BROKER = "mipt-node08.atp-fivt.org:9092"
TOPIC = "bessmertnyjde-topic"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda x: x.encode("utf-8")
)

# HDFS settings
DATA_PATH = "/data/course4/uid_ua_100k_splitted_by_5k"

client = Config().get_client()

# Чтение бачей из HDFS
files = sorted(client.list(DATA_PATH))

print("Total batches:", len(files))

for filename in files:

    hdfs_file = os.path.join(DATA_PATH, filename)

    print("Sending batch:", hdfs_file)

    with client.read(hdfs_file, encoding="utf-8") as reader:

        for line in reader:

            line = line.strip()

            if not line:
                continue

            producer.send(TOPIC, line)

    producer.flush()

    print("Batch sent:", filename)

    time.sleep(10)

producer.close()

print("All batches were sent successfully")
