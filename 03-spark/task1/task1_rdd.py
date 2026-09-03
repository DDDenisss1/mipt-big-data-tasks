#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark import SparkContext, SparkConf
# import time  # Для подсчета CPU time

conf = SparkConf().setAppName("task1_rdd").setMaster("yarn")
sc = SparkContext(conf=conf)

def parse_edge(line):
    user, follower = line.split("\t")
    return int(follower), int(user)

n = 100

edges = sc.textFile("/data/twitter/twitter_sample.txt") \
    .map(parse_edge) \
    .partitionBy(n) \
    .persist()

start = 12
target = 34

if start == target:
    print(start)
    sc.stop()
    exit(0)

# cpu_start = time.process_time()

frontier = sc.parallelize([(start, [start])]).partitionBy(n).persist()

visited = sc.parallelize([(start, 1)]).partitionBy(n).persist()

result = None

while True:

    candidates = frontier.join(edges, n) \
        .map(lambda x: (x[1][1], x[1][0] + [x[1][1]])).partitionBy(n)

    new_frontier = candidates.leftOuterJoin(visited, n) \
        .filter(lambda x: x[1][1] is None) \
        .map(lambda x: (x[0], x[1][0])) \
        .persist()

    found_vertex = new_frontier.filter(
        lambda x: x[0] == target
    ).take(1)

    if found_vertex:
        result = found_vertex[0][1]

        frontier.unpersist()
        new_frontier.unpersist()

        break

    if new_frontier.isEmpty():

        frontier.unpersist()
        new_frontier.unpersist()

        break

    old_visited = visited

    visited = visited.union(
        new_frontier.map(lambda x: (x[0], 1))
    ).partitionBy(n).persist()

    old_visited.unpersist()
    frontier.unpersist()

    frontier = new_frontier

# cpu_end = time.process_time()
# cpu_time = cpu_end - cpu_start

if result:
    print(",".join(map(str, result)))
else:
    print("Path not found")

# print("CPU time:", cpu_time)

sc.stop()
