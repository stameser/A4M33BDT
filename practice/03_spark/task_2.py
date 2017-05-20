"""
For each word find number of songs in which word has been used
"""


def inv_index(row):
    import csv
    try:
        idx, song, year, artist, genre, lyrics = csv.reader([row]).next()
        words = set(lyrics.lower().split())

        return [(word, artist + ':' + song) for word in words]
    except StandardError as err:
        return [(None, str(err))]


rdd = sc.textFile('lyrics_data/')
# repartition data to speed-up process a bit
word_artist_rdd = rdd.repartition(20).flatMap(inv_index).filter(lambda x: x[0] is not None)


def seqOp(acc, value):
    acc.append(value)
    return acc


def comOp(acc1, acc2):
    return acc1 + acc2


inverted_index_rdd = word_artist_rdd.aggregateByKey([], seqOp, comOp, numPartitions=100)

print inverted_index_rdd.orderBy().take(10)
