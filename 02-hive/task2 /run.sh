#!/usr/bin/env python

hive -f 00_task2-create-table-text.sql 1>&2 2>&1

hive -f 01_task2-create-table-orc.sql 1>&2 2>&1

hive -f 02_task2-create-table-parquet.sql 1>&2 2>&1

hive -f 04_task2-select-orc.sql 2>/dev/null
