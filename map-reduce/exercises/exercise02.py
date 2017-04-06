#spark-submit --master yarn --deploy-mode client --executor-memory 1g --name exercise02 --conf "spark.app.id=exercise02" exercise02.py 'dir_name|filename'
#
#	Count words by file.
#
import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
 	conf = SparkConf().setAppName("Exercise 02")
 	sc = SparkContext(conf=conf)
	rdd = sc.wholeTextFiles(sys.argv[1]).cache()
	files = rdd.collect()
	words = sc.parallelize(files[0][1].split()).map(lambda _: (0, 1)).reduceByKey(lambda _1,_2: _1+_2).map(lambda _: (files[0][0], _[1]))
	for (filename, content) in files[1:]:
		words_file = sc.parallelize(content.split()).map(lambda _: (0, 1)).reduceByKey(lambda _1,_2: _1+_2).map(lambda _: (filename, _[1]))
		words = words.union(words_file)
	words.saveAsTextFile("exercise02-out")
