#! /usr/bin/env python

import sys

total_count = 0
old_key = None

for line in sys.stdin:
    data = line.strip().split("\t")
    if len(data) != 2:
        # Something has gone wrong. Skip this line.
        continue

    this_key, this_count = data
    if old_key and old_key != this_key:
        print old_key, "\t", total_count
        old_key = this_key
        total_count = 0

    old_key = this_key
    total_count += float(this_count)

# emit the last line
if old_key != None:
    print old_key, "\t", total_count