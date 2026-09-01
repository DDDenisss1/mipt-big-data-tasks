#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

current_user = None
sessions = 0
deaths = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    try:
        parts = line.split('\t')
        if len(parts) != 3:
            continue
        user = parts[0]
        event_type = parts[1]
        count = int(parts[2])
    except (ValueError, IndexError):
        continue
    
    if current_user != user:
        if current_user is not None:
            print("{}\t{}\t{}".format(current_user, sessions, deaths))
        current_user = user
        sessions = 0
        deaths = 0
    
    if event_type == 'SESSION':
        sessions += count
    elif event_type == 'DEATH':
        deaths += count

if current_user is not None:
    print("{}\t{}\t{}".format(current_user, sessions, deaths))
