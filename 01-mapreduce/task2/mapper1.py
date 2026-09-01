#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import re

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    match_start = re.search(r'UUID of player (\S+) is', line)
    if match_start:
        user = match_start.group(1)
        if user and not user.startswith('(/') and ':' not in user:
            print("{}\tSESSION\t1".format(user))
        continue

    match_death = re.search(r'([a-zA-Z0-9_]+) died', line)
    if match_death:
        user = match_death.group(1)
        if user and not user.startswith('(/') and ':' not in user:
            print("{}\tDEATH\t1".format(user))
