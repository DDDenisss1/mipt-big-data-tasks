#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

results = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    try:
        parts = line.split('\t')
        if len(parts) != 3:
            continue
        user = parts[0]
        sessions = int(parts[1])
        deaths = int(parts[2])

        if sessions == 0:
            continue

        avg = round(deaths / float(sessions), 2)
        
        results.append((user, avg, sessions))
    except (ValueError, IndexError):
        continue

results.sort(key=lambda x: (-x[1], x[0]))

for user, avg, sessions in results[:10]:
    print("{}\t{}\t{}".format(user, avg, sessions))
