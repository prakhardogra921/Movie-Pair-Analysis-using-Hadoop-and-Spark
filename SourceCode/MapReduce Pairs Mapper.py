#!/usr/bin/env python

import sys

skip_first = True
count = 0
user = 0
movie_list = []

for line in sys.stdin:
    if skip_first:
        skip_first = False
        continue

    info = line.split(",")
    if float(info[2]) >= 4.0:
        if user != int(info[0]):                            #for every new user
            movie_list = sorted(movie_list)
            l = len(movie_list)
            for i in range(l-1):
                for j in range(i+1, l):
                    print ("%s\t%s" % (str(movie_list[i]) +"_"+ str(movie_list[j]), "1"))
            movie_list[:] = []
            user = int(info[0])
        movie_list.append(int(info[1]))

#for the last user
movie_list = sorted(movie_list)
l = len(movie_list)
for i in range(l-1):
    for j in range(i+1, l):
        print ("%s\t%s" % (str(movie_list[i]) +"_"+ str(movie_list[j]), "1"))
