"""
For each song compute number of words

"""


def parser(row):
    import csv
    try:
        # use csv module for correct csv parsing
        idx, song, year, artist, genre, lyrics = csv.reader([row]).next()

        words = set(lyrics.lower().split())
        number_of_words = len(words)

        return artist + ':' + song, number_of_words
    except StandardError as err:
        return None, str(err)


rdd = sc.textFile('lyrics_data/')
word_lyrics = rdd.map(parser).filter(lambda x: x[0] is not None and x[0] > 0)\

# sort by number of words, descending
sorted_lyrics = word_lyrics.sortBy(lambda x: x[1], ascending=False)

print sorted_lyrics.take(10)
