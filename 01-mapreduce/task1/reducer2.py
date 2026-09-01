#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

results = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    try:
        word, count = line.split('\t')
        results.append((word, int(count)))
    except (ValueError, IndexError):
        continue

results.sort(key=lambda x: (-x[1], x[0]))

for word, count in results[:10]:
    print("{}\t{}".format(word, count))
