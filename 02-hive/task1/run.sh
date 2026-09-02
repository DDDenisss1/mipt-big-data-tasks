#!/usr/bin/env bash

hive -f 00_task1-create-table-serde.sql 1>&2 2>&1

hive -f 01_task1-select-50.sql 2>/dev/null
