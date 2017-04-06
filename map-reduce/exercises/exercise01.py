#spark-submit --master yarn --deploy-mode client --executor-memory 1g --name exercise01 --conf "spark.app.id=exercise01" exercise01.py 'dir_name|filename'
#
#	Count words excluding prepositions.
#
import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
 	conf = SparkConf().setAppName("Exercise 01")
 	sc = SparkContext(conf=conf)
	prepositions = sc.parallelize(["aboard","about","above","across","after","against","along","amid","among","anti","around","as","at","before","behind","below","beneath","beside","besides","between","beyond","but","by","concerning","considering","despite","down","during","except","excepting","excluding","following","for","from","in","inside","into","like","minus","near","of","off","on","onto","opposite","outside","over","past","per","plus","regarding","round","save","since","than","through","to","toward","towards","under","underneath","unlike","until","up","upon","versus","via","with","within","without"])
	glob = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda _1,_2: _1+_2).leftOuterJoin(prepositions.map(lambda word: (word, 1)))
	glob.flatMap(lambda _: [(_[0], _[1][0])] if _[1][1] == None else []).saveAsTextFile("exercise01-out")
