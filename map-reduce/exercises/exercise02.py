#spark-submit --master yarn --deploy-mode client --executor-memory 1g --name exercise02 --conf "spark.app.id=exercise02" exercise02.py 'dir_name|filename'
#
#	Count words by file.
#
import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
 	conf = SparkConf().setAppName("Exercise 02")
 	sc = SparkContext(conf=conf)
	files = sc.wholeTextFiles(sys.argv[1]).map(lambda (filename, content): filename).collect()
	words = sc.textFile(files[0]).flatMap(lambda line: line.split()).map(lambda _: (0, 1)).reduceByKey(lambda _1,_2: _1+_2).map(lambda _: (files[0], _[1]))
	for filename in files[1:]:
		words_file = sc.textFile(filename).flatMap(lambda line: line.split()).map(lambda _: (0, 1)).reduceByKey(lambda _1,_2: _1+_2).map(lambda _: (filename, _[1]))
		words = words.union(words_file)
	words.saveAsTextFile("exercise02-out")
