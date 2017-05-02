#spark-submit --master yarn --deploy-mode client --executor-memory 1g --name exercise04 --conf "spark.app.id=exercise04" exercise04.py 'dir_name|filename' 'word'
#
#	Given a word, check which files contain it and the number of occurrences.
#
import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
 	conf = SparkConf().setAppName("Exercise 04")
 	sc = SparkContext(conf=conf)
	given_word = sys.argv[2]
	files = sc.wholeTextFiles(sys.argv[1]).map(lambda (filename, content): filename).collect()
	places_found = []
	for filename in files:
		places_found.append(sc.textFile(filename).flatMap(lambda line: line.split()).map(lambda word: (word, (filename, 1))).reduceByKey(lambda _1,_2: (_1[0], _1[1]+_2[1])).sortByKey().lookup(given_word))
	sc.parallelize(places_found).flatMap(lambda _: _).saveAsTextFile("exercise04-out")
