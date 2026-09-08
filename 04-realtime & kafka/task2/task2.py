#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import sys
import subprocess

if len(sys.argv) != 2:
    sys.exit(1)

topic = sys.argv[1]
kafka_bin = "/opt/cloudera/parcels/KAFKA-3.0.0-1.3.0.0.p0.40/lib/kafka/bin"
zk = "mipt-master.atp-fivt.org:2181,mipt-node01.atp-fivt.org:2181,mipt-node02.atp-fivt.org:2181"

try:
    cmd = [
        kafka_bin + "/kafka-console-consumer.sh",
        "--zookeeper", zk,
        "--topic", topic,
        "--from-beginning",
        "--timeout-ms", "60000"
    ]
    
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1
    )
    
    count = 0
    for line in proc.stdout:
        if line.strip():
            count += 1
            print("[LOG]", line.strip(), file=sys.stderr)
    
    proc.wait()

    print(count)
    
except Exception as e:
    print("[ERROR]", str(e), file=sys.stderr)
    print(0)
