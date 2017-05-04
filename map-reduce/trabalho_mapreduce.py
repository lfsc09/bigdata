#spark-submit --master yarn --deploy-mode client --executor-memory 1g --name trabalho --conf "spark.app.id=trabalho" trabalho_mapreduce.py 'dir_name|filename'
#
#	Show the url links that are repeated in files (Mostra as urls que se repetem em arquivos)
#
import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
 	conf = SparkConf().setAppName("Trabalho")
 	sc = SparkContext(conf=conf)
	url_tests = ["http", "www", ".com"]
	rdd_all = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split()).map(lambda word: (word, 1) if any(x in word for x in url_tests) else (0, 0)).reduceByKey(lambda _1,_2: _1+_2).filter(lambda _: _[1] >= 2).coalesce(2).saveAsTextFile("trabalho_mapreduce-out")
