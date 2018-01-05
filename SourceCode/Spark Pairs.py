from pyspark import SparkConf, SparkContext

def load_movie_names():
    movieNames = {}
    skip_first = True
    with open("movies.csv") as f:
        for line in f:
            if skip_first:
                skip_first = False
                continue
            fields = line.split(",")
            movieNames[fields[0]] = fields[1].decode('ascii', 'ignore')
    return movieNames

def map1(data):
    return '\t'.join(data)

def reduce1(data):
    list = []
    line=data.split('\t')
    for i in range(len(line) - 1):
        for j in range(i+1, len(line)):
            list.append((line[i]+ "," + line[j], 1))
    return list

def reduce2(line):
    data = map(int, line.split(","))
    list = []
    for i in range(len(data) - 1):
        for j in range(i+1, len(data)):
            list.append(((str(data[i])+","+str(data[j])), 1))
    return list

def listing(data):
    return data

conf = SparkConf().setMaster("local[*]").setAppName("Frequent_Pairs")
sc = SparkContext(conf=conf)
movieNameDictionary = load_movie_names()
text_file = sc.textFile("ratings.csv")
out1 = text_file.map(lambda line: line.strip().split(",")).zipWithIndex().filter(lambda tup: tup[1] > 1).map(lambda x:x[0])
out2 = out1.filter(lambda a: float(a[2])>=4.0)
out3 = out2.map(lambda a: (a[0],a[1]))
out4 = out3.reduceByKey(lambda x,y : x + ',' + y).map(lambda x: x[1])
out5 = out4.flatMap(reduce2)
out6 = out5.reduceByKey(lambda x, y : x + y, numPartitions=16)
out7 = out6.filter(lambda a: a[1] > 1000)
out8 = out7.map(lambda a: movieNameDictionary[a[0].split(",")[0]] +"\t"+ movieNameDictionary[a[0].split(",")[1]] + "\t" + str(a[1]))
out8.sample(False, 20).saveAsTextFile("spark_output/pairs/100p")