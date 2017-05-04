#spark-submit --master yarn --deploy-mode client --executor-memory 1g --name trabalho --conf "spark.app.id=trabalho" trabalho_mapreduce.py 'dir_name|filename' 'url_number'
#
#	Show the files that have more than 'url_number' repeated url links (Mostra os arquivos que tem mais de 'url_number' urls repetidas)
#
import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
 	conf = SparkConf().setAppName("Trabalho")
 	sc = SparkContext(conf=conf)
	url_number = int(sys.argv[2])
	url_tests = ["http", "www", ".com", ".br", ".org", "gov"]
	rdd_files = sc.wholeTextFiles(sys.argv[1]).cache()
	files = rdd_files.map(lambda (filename, content): filename).collect()
	places_found = []
	for filename in files:
		rdd = sc.textFile(filename).flatMap(lambda line: line.split()).map(lambda word: (word, (filename, 1)) if any(x in word for x in url_tests) else (0, (0,0))).reduceByKey(lambda _1,_2: (_1[0], _1[1]+_2[1])).filter(lambda _: _[1][1] > url_number).collect()
		if len(rdd) > 0:
			places_found.append(rdd[0][1][0])
	sc.parallelize(places_found).saveAsTextFile("trabalho_mapreduce-out")
