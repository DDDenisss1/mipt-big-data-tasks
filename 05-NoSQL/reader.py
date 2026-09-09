#!/usr/bin/env python
# -*- coding: utf-8 -*-

import happybase
import argparse
from collections import defaultdict

HBASE_HOST = 'mipt-node01.atp-fivt.org'
HBASE_TABLE = 'pubg_kills_bessmertnyjde'

def extract_match_and_bucket(row_key):
    row_key = row_key.decode('utf-8')
    parts = row_key.rsplit("_", 1)

    if len(parts) != 2:
        return None, None

    match_id = parts[0]

    try:
        bucket = int(parts[1])
    except Exception:
        return None, None

    return match_id, bucket

def print_top(match_id, stats, top_n):

    print("")
    print("MATCH:", match_id)

    sorted_weapons = sorted(
        stats.items(),
        key=lambda x: (-x[1], x[0])
    )

    for weapon, kills in sorted_weapons[:top_n]:
        print("{}\t{}".format(weapon, kills))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--match',
        required=False,
        help='match_id'
    )

    parser.add_argument(
        '--start',
        type=int,
        required=True,
        help='start time'
    )

    parser.add_argument(
        '--end',
        type=int,
        required=True,
        help='end time'
    )

    parser.add_argument(
        '--top',
        type=int,
        default=10,
        help='top N weapons'
    )

    args = parser.parse_args()

    match_filter = args.match
    start = args.start
    end = args.end
    top_n = args.top

    if start > end:
        print("ERROR: start > end")
        return

    start_bucket = (start // 100) * 100
    end_bucket = (end // 100) * 100

    connection = happybase.Connection(
        HBASE_HOST,
        timeout=30000
    )

    table = connection.table(HBASE_TABLE)
    match_stats = defaultdict(lambda: defaultdict(int))
    scanned = 0

    # если нужно вывести один матч
    if match_filter:
        for bucket in range(start_bucket, end_bucket + 1, 100):
            row_key = "{}_{}".format(
                match_filter,
                bucket
            ).encode('utf-8')

            row = table.row(row_key)

            if not row:
                continue

            for column, value in row.items():
                try:
                    column = column.decode('utf-8')
                    value = int(value.decode('utf-8'))

                    weapon = column.split(":", 1)[1]
                    match_stats[match_filter][weapon] += value

                except Exception:
                    pass

    # если нужно вывести все матчи
    else:
        for row_key, data in table.scan():
            scanned += 1
            if scanned % 10000 == 0:
                print("scanned:", scanned)

            match_id, bucket = extract_match_and_bucket(row_key)

            if match_id is None:
                continue
            if bucket < start_bucket or bucket > end_bucket:
                continue
            for column, value in data.items():
                try:
                    column = column.decode('utf-8')
                    value = int(value.decode('utf-8'))
                    weapon = column.split(":", 1)[1]
                    match_stats[match_id][weapon] += value

                except Exception:
                    pass

    connection.close()

    print("\nRESULT [{} - {}]\n".format(start, end))

    for match_id in sorted(match_stats.keys()):
        print_top(
            match_id,
            match_stats[match_id],
            top_n
        )

    print("")

if __name__ == "__main__":
    main()
