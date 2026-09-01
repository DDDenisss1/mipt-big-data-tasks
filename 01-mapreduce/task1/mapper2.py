#!/usr/bin/env python
# -*- coding: utf-8 -*-              

import sys                                                    

for line in sys.stdin:                                      
    line = line.strip()                                         
    if not line:                                      
        continue                                    
    
    try:                                                           
        parts = line.split('\t')                              
        if len(parts) != 2:                               
            continue                                
        word = parts[0]           
        count = parts[1]                               

        print("{}\t{}".format(word, count))               
    except (ValueError, IndexError):               
        continue            
