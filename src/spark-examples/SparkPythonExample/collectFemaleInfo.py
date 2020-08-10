# -*- coding: utf-8 -*

import sys

from pyspark import SparkConf, SparkContext

def contains(str, substr):
	if substr in str:
		return True
	return False

if __name__ == "__main__":
	if len(sys.argv) < 2:
		print "Usage: CollectFemaleInfo <file>"
		exit(-1)
			
	# Create SparkContext and set AppName.
	sc = SparkContext(appName = "CollectFemaleInfo")
	
	"""
	The following programs are used to implement the following functions: 
	1. Read data. This code indicates the data path that the input parameter argv[1] specifies. - textFile
	2. Filter data about the time that female netizens spend online. - filter
	3. Aggregate the total time that each female netizen spends online. - map/map/reduceByKey
	4. Filter information about female netizens who spend more than 2 hours online. - filter
	"""
	inputPath = sys.argv[1]
	result = sc.textFile(name = inputPath, use_unicode = False) \
		.filter(lambda line: contains(line, "female")) \
		.map(lambda line: line.split(',')) \
		.map(lambda dataArr: (dataArr[0], int(dataArr[2]))) \
		.reduceByKey(lambda v1, v2: v1 + v2) \
		.filter(lambda tupleVal: tupleVal[1] > 120) \
		.collect()
	for (k, v) in result:
		print k + "," + str(v)
	
	# Stop SparkContext
	sc.stop()