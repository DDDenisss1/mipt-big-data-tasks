#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

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
        
        print("{}\t{}\t{}".format(user, sessions, deaths))
    except (ValueError, IndexError):
        continue
