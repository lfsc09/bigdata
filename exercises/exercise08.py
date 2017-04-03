# EXEC: spark-submit --master yarn --deploy-mode client --executor-memory 1g --name exercise08 --conf "spark.app.id=exercise08" exercise08.py file1 file2
#
#	Get first 1500 common elements between two books.
#
import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
 	conf = SparkConf().setAppName("Exercise 08")
 	sc = SparkContext(conf=conf)
	file1 = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split()).map(lambda word: (word, 1)).distinct()
	file2 = sc.textFile(sys.argv[2]).flatMap(lambda line: line.split()).map(lambda word: (word, 1)).distinct()
	result = file1.fullOuterJoin(file2).flatMap(lambda val: [val[0]] if val[1][0] != None and val[1][1] != None else []).take(1500)
	sc.parallelize(result).saveAsTextFile("exercise08-out")
