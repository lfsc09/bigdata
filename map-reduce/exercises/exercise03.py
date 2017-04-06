#spark-submit --master yarn --deploy-mode client --executor-memory 1g --name exercise03 --conf "spark.app.id=exercise03" exercise03.py 'dir_name|filename' 'word'
#
#	Given a word, check which files contain it.
#
import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
 	conf = SparkConf().setAppName("Exercise 03")
 	sc = SparkContext(conf=conf)
	given_word = sys.argv[2]
	rdd = sc.wholeTextFiles(sys.argv[1]).cache()
	files = rdd.collect()
	places_found = []
	for (filename, content) in files:
		places_found.append(sc.parallelize(content.split()).map(lambda word: (word, filename)).reduceByKey(lambda _1,_2: _1).sortByKey().lookup(given_word))
	sc.parallelize(places_found).flatMap(lambda _: _).saveAsTextFile("exercise03-out")
