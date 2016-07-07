import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalizeWords(text):
	return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile("file:///Users/Vince/Developer/SparkCourse/Book.txt")
words = input.flatMap(normalizeWords)

# Convert every word into (word, 1), then add up all the words
wordCounts = words.map(lambda x:(x,1)).reduceByKey(lambda x, y: x + y)

# flip key and values so we can sort by the occurances
wordCountsSorted = wordCounts.map(lambda (x,y):(y,x)).sortByKey()
results = wordCountsSorted.collect()

for result in results:
	count = str(result[0])
	word = result[1].encode('ascii', 'ignore')
	if (word):
		print word + ":\t\t" + count