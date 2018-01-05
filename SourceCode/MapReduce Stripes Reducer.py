#!/usr/bin/env python

import sys
import json

threshold = int(sys.argv[1])

movie_names = {}
movie_dict = {}

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
    movie1, all_movies = line.split("\t", 1)
    movie1 = int(movie1)
    all_movies = json.loads(all_movies)
    stripe = {}
    for movie in all_movies:
        stripe[int(movie)] = int(all_movies[movie])

    if movie1 in movie_dict:
        for movie in stripe:
            if movie in movie_dict[movie1]:
                movie_dict[movie1][movie] += stripe[movie]
            else:
                movie_dict[movie1][movie] = stripe[movie]
    else:
        movie_dict[movie1] = stripe

count = 0
for movie1 in movie_dict:
    for movie in movie_dict[movie1]:
        if count == 20:
            break
        if movie_dict[movie1][movie] > threshold:
            print ("%s\t%s\t%s" % (movie_names[int(movie1)], movie_names[int(movie)], movie_dict[movie1][movie]))
            count += 1

