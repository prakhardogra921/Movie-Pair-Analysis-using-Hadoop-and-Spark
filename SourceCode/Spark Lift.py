from pyspark import SparkConf, SparkContext

def load_movie_names():
    movieNames = {}
    skip_first = True
    with open("ml-latest-small/movies.csv") as f:
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
    for i in range(l - 1):
        for j in range(i + 1, l):
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

def reduce2(movie_dic, data):
    movie1, all_movies = data
    movief, movie_dict = movie_dic
    m1 = 0
    mf = 0

    for movie in all_movies:
        m1 += all_movies[movie]

    for movie in movie_dict:
        mf += movie_dict[movie]

    movief = str(movief) +"_" +str(mf)+  "," + str(movie1) + "_" +str(m1)
    return movief, {}

def map2(data):
    movie1, movie_dict = data
    list = []
    for movie in movie_dict:
        if movie in glob_movie_dict and movie1 in glob_movie_dict:
            lift = (float(movie_dict[movie]) * float(total))/(float(glob_movie_dict[movie1]) * float(glob_movie_dict[movie]))
            if lift > 1.6:
                list.append(str(movieNameDictionary[movie1]) +","+ str(movieNameDictionary[movie]) +","+ str(lift))
    return list

def create_global_movie_dict():
    glob_movie_dict = {}
    movie_string, _ = out6
    for movie_count in movie_string.split(","):
        movie = int(movie_count.split("_")[0])
        count = int(movie_count.split("_")[1])
        if movie in glob_movie_dict:
            glob_movie_dict[movie] += count
        else:
            glob_movie_dict[movie] = count
    return glob_movie_dict

conf = SparkConf().setMaster("local[*]").setAppName("Frequent_Stripes")
sc = SparkContext(conf=conf)

text_file = sc.textFile("ratings.csv")
movieNameDictionary = load_movie_names()
out1 = text_file.map(lambda line: line.strip().split(",")).zipWithIndex().filter(lambda tup: tup[1] > 1).map(lambda x:x[0])
out2 = out1.filter(lambda a: float(a[2]) >= 4.0)
out3 = out2.map(lambda a: (a[0], a[1])).reduceByKey(lambda x, y: x + ',' + y).map(lambda x: x[1])
total = out3.count()
out4 = out3.map(lambda line: line.strip().split(",")).flatMap(map1)
out5 = out4.reduceByKey(reduce1, numPartitions=8)
out5.collect()
out6 = out5.reduce(reduce2)
glob_movie_dict = create_global_movie_dict()
out7 = out5.flatMap(map2)
out7.sample(False,0.1).saveAsTextFile("spark_output/lift/100p")