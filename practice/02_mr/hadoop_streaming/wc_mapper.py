#! /usr/bin/env python

import sys

# read standard input line by line
for line in sys.stdin:
    # split on tab
    data = line.lower().strip().split("\t")

    try:
        review_id, review_text = data

        for word in review_text.split():
            # Now print out the data that will be passed to the reducer
            print "{0}\t{1}".format(word, 1)
    except StandardError as err:
        continue