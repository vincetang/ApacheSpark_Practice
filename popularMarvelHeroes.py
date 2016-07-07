from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Popular Marvel Heroes")
sc = SparkContext(conf = conf)

