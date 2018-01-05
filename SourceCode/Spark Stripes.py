from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    skip_first = True
    with open("ml-latest/movies.csv") as f:
        for line in f:
            if skip_first:
                skip_first = False
                continue
            fields = line.split(",")
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames

def map1(movie_list):
    list = []
    movie_dict = {}
    l = len(movie_list)
    movie_list = map(int, movie_list)
    movie_list = sorted(movie_list)
    for i in range(l - 2):
        for j in range(i + 1, l - 1):
            if movie_list[j] in movie_dict:
                movie_dict[movie_list[j]] += 1
            else:
                movie_dict[movie_list[j]] = 1
        list.append([movie_list[i], movie_dict])
        movie_dict = {}
    return list

def reduce1(movie_dic, data):

    all_movies = data
    movie_dict = movie_dic
    for movie in all_movies:
        if movie in movie_dict:
            movie_dict[movie] += all_movies[movie]
        else:
            movie_dict[movie] = all_movies[movie]
    return movie_dict

def map2(data):
    movie1, movie_dict = data
    list = []
    for movie in movie_dict:
        if movie_dict[movie] > 50:
            list.append(movieNameDictionary[movie1] +","+ movieNameDictionary[movie] +","+ str(movie_dict[movie]))
    return list

conf = SparkConf().setMaster("local[*]").setAppName("Frequent_Stripes")
sc = SparkContext(conf=conf)

text_file = sc.textFile("input/ratings.csv")
movieNameDictionary = loadMovieNames()
out1 = text_file.map(lambda line: line.strip().split(",")).zipWithIndex().filter(lambda tup: tup[1] > 1).map(lambda x:x[0])
out2 = out1.filter(lambda a: float(a[2]) >= 4.0)
out3 = out2.map(lambda a: (a[0], a[1])).reduceByKey(lambda x, y: x + ',' + y).map(lambda x: x[1])
out4 = out3.map(lambda line: line.strip().split(",")).flatMap(map1)
out5 = out4.reduceByKey(reduce1, numPartitions=16)
out6 = out5.flatMap(map2)
out6.sample(False, 20).saveAsTextFile("spark_output/stripes/100p")