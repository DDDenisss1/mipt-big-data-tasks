#!/usr/bin/env bash

PYSPARK_PYTHON=/usr/bin/python3 \
PYSPARK_DRIVER_PYTHON=/usr/bin/python3 \
spark2-submit writer.py #> writer_output.txt 2>&1 #- в файл писал лог - потом дебажил
