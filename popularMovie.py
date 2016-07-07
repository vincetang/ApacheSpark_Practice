from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Most Popular Movies")
sc = SparkContext(conf = conf)

def loadMovieNames():
	movieNames = {}
	with open("/Users/Vince/Developer/SparkCourse/ml-100k/u.item") as f:
		for line in f:
			fields = line.split('|')
			movieNames[int(fields[0])] = fields[1]
	return movieNames

def parseLines(dataLine):
	fields = dataLine.split()
	return int(fields[1])

nameDict = sc.broadcast(loadMovieNames())

dataLine = sc.textFile("file:///Users/Vince/Developer/SparkCourse/ml-100k/u.data")
movieIDs = dataLine.map(parseLines)
movieCounters = movieIDs.map(lambda x : (x, 1))
movieCounts = movieCounters.reduceByKey(lambda x, y : x + y)
flipped = movieCounts.map(lambda (x, y) : (y, x))
sortedMovies = flipped.sortByKey()
sortedMoviesWithNames = sortedMovies.map(lambda (count, movie) : (nameDict.value[movie], count))

results = sortedMoviesWithNames.collect()

for result in results:
	print '{} : {}'.format(result[0], result[1])