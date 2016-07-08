from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Popular Marvel Heroes")
sc = SparkContext(conf = conf)

def countCoOccurences(line):
	elements = line.split()
	return (int(elements[0]), len(elements) -1)

def parseNames(line):
	fields = line.split('\"')
	# return (int id, String name)
	return (int(fields[0]), fields[1].encode("utf8"))

names = sc.textFile("file:///Users/Vince/Developer/SparkCourse/marvel-names.txt")
namesRdd = names.map(parseNames)

lines = sc.textFile("file:///Users/Vince/Developer/SparkCourse/marvel-graph.txt")

pairings = lines.map(countCoOccurences)
totalFriendsByCharacter = pairings.reduceByKey(lambda countX, countY: countX + countY)
flipped = totalFriendsByCharacter.map(lambda (heroID, cooccurences) : (cooccurences, heroID))

mostPopular = flipped.max() # (cooccurences, heroID)

mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print "The most popular super hero is " + mostPopularName + " with " + str(mostPopular[0]) + " occurences."