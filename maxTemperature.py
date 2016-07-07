from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemps")
sc = SparkContext(conf = conf)

def parseLine(line):
	fields = line.split(',')
	stationID = fields[0]
	entryType = fields[2]
	temperature = float(fields[3])
	return (stationID, entryType, temperature)

lines = sc.textFile("file:///Users/Vince/Developer/SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: max(x, y))
results = minTemps.collect();

for result in results:
	print result[0] + "\t{:.2f}C".format(result[1])
