#!/usr/bin/env python

import sys
import json

movie_dict = {}
skip_first = True
count = 0
user = 0
movie_list = []
all_movies = ""

for line in sys.stdin:
    if skip_first:
        skip_first = False
        continue
    info = line.split(",")

    if float(info[2]) >= 4.0:
        if user != int(info[0]):
            movie_list = sorted(movie_list)
            l = len(movie_list)
            if l > 1:
                for i in range(l-1):
                    for j in range(i+1, l):
                        if movie_list[j] in movie_dict:
                            movie_dict[movie_list[j]] += 1
                        else:
                            movie_dict[movie_list[j]] = 1
                    print (str(movie_list[i]) + "\t" + json.dumps(movie_dict))
                    movie_dict.clear()
            movie_list[:] = []

            user = int(info[0])
        movie_list.append(int(info[1]))

movie_list = sorted(movie_list)
l = len(movie_list)

if l > 1:
    for i in range(l-2):
        for j in range(i+1, l-1):
            if movie_list[j] in movie_dict:
                movie_dict[movie_list[j]] += 1
            else:
                movie_dict[movie_list[j]] = 1
        print (str(movie_list[i]) + "\t" + json.dumps(movie_dict))