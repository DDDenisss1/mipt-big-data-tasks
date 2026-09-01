#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

current_word = None
proper_count = 0
has_lowercase = False

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    try:
        parts = line.split('\t')
        if len(parts) != 3:
            continue
        word = parts[0]
        tag = parts[1]
        count = int(parts[2])
    except (ValueError, IndexError):
        continue
    
    if current_word != word:
        if current_word is not None and proper_count > 0 and not has_lowercase:
            print("{}\t{}".format(current_word, proper_count))
        current_word = word
        proper_count = 0
        has_lowercase = False
    
    if tag == 'P':
        proper_count += count
    elif tag == 'L':
        has_lowercase = True

if current_word is not None and proper_count > 0 and not has_lowercase:
    print("{}\t{}".format(current_word, proper_count))
