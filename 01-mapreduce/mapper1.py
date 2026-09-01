#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import re

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    parts = line.split('\t', 1)
    if len(parts) != 2:
        continue
    
    text = parts[1]
    words = re.findall(r'[a-zA-Z]+', text)
    
    for word in words:
        if 6 <= len(word) <= 9:
            if word[0].isupper() and word[1:].islower():
                print("{}\tP\t1".format(word.lower()))
            elif word.islower():
                print("{}\tL\t1".format(word.lower()))
