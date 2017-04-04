#spark-submit --master yarn --deploy-mode client --executor-memory 1g --name exercise05 --conf "spark.app.id=exercise05" exercise05.py 'dir_name|filename'
#
#	Get 1500 most used words.
#
import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
 	conf = SparkConf().setAppName("Exercise 05")
 	sc = SparkContext(conf=conf)
	glob = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda _1,_2: _1+_2)
	sc.parallelize(glob.sortBy(lambda _: -_[1]).take(1500)).saveAsTextFile("exercise05-out")
