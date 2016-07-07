from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Customer Total Spent")
sc = SparkContext(conf = conf)

def parseLines(line):
	fields = line.split(',')
	custID = int(fields[0])
	amountSpent = float(fields[2])
	return (custID, amountSpent)

lines = sc.textFile("file:///Users/Vince/Developer/SparkCourse/customer-orders.csv")
custSpent = lines.map(parseLines)
aggregatedSpent = custSpent.reduceByKey(lambda x, y: x + y)
sortedSpenders = aggregatedSpent.map(lambda (x, y): (y, x)).sortByKey()
results = sortedSpenders.collect()

for result in results:
	print '{:2d} spent {:.2f}'.format(result[1], result[0])