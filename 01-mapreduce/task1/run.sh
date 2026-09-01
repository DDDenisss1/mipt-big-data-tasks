#!/usr/bin/env bash

OUT_DIR="dbessmertnyi_wordcount_outdir"
NUM_REDUCERS=8

hdfs dfs -rm -r -skipTrash ${OUT_DIR}.tmp 1>&2 2>/dev/null
hdfs dfs -rm -r -skipTrash ${OUT_DIR} 1>&2 2>/dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
	-D mapreduce.job.name="dbessmertnyi_job1" \
	-D mapreduce.job.reduces=${NUM_REDUCERS} \
	-files mapper1.py,reducer1.py \
	-mapper mapper1.py \
	-reducer reducer1.py \
	-input /data/wiki/en_articles \
	-output ${OUT_DIR}.tmp 1>&2 2>&1

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
	-D mapreduce.job.name="dbessmertnyi_job2" \
	-D mapreduce.job.reduces=1 \
	-files mapper2.py,reducer2.py \
	-mapper mapper2.py \
	-reducer reducer2.py \
	-input ${OUT_DIR}.tmp \
	-output ${OUT_DIR} 1>&2 2>&1

hdfs dfs -cat ${OUT_DIR}/part-* 2>/dev/null | head
