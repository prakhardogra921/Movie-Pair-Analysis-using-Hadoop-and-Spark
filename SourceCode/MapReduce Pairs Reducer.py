#!/usr/bin/env python

import sys
threshold = int(sys.argv[1])
pair_count = {}

movie_names = {}

f = open("movies.csv", "r")
skip_first = True

for line in f.readlines():
    if skip_first:
        skip_first = False
        continue
    movie_names[int(line.split(",")[0])] = line.split(",")[1]
f.close()

for line in sys.stdin:
    line = line.strip()
    pair, count = line.split("\t", 1)
    try:
        count = int(count)
    except ValueError:
        continue

    try:
        pair_count[pair] = pair_count[pair] + count
    except:
        pair_count[pair] = count

count = 0
for pair in pair_count.keys():
    if count == 20:
        break
    if pair_count[pair] > threshold:
        movie1 = int(pair.split("_")[0])
        movie2 = int(pair.split("_")[1])
        name1 = movie_names[movie1]
        name2 = movie_names[movie2]
        print ("%s\t%s\t%s" % (name1, name2, pair_count[pair]))
        count += 1