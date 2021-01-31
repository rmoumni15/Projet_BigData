import json

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from movie import *

es_write_conf = {"es.nodes": 'localhost',
                 "es.port": '9200',
                 "es.resource": 'movies-index/movie'
                 }

conf = SparkConf().setAppName("PythonStreamingDirectKafkaWordCount") \
    .set("es.nodes", "localhost:9200") \
    .set("es.index.auto.create", "true")

sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount", conf=conf)
sc.addPyFile("/home/rida/Projet_BigData/Spark/movie.py")
ssc = StreamingContext(sc, 5)
# brokers, topic = sys.argv[1:]
kvs = KafkaUtils.createDirectStream(ssc, ["movies"], {"metadata.broker.list": "localhost:9092"})

x = kvs.map(lambda row: row[1]) \
    .map(lambda row: row.split("||")) \
    .map(lambda row: Movie(row[0], float(row[1]), row[2], float(row[3]), float(row[4]), row[5], row[6],
                           row[7], row[8], row[9], row[10], row[11])) \
    .map(lambda obj: clean_movie(obj)) \
    .map(lambda obj: obj.__dict__) \
    .map(lambda obj: (None, obj)) \
    .foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(
                                path='-',
                                outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                                keyClass="org.apache.hadoop.io.NullWritable",
                                valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                                conf=es_write_conf))

ssc.start()
ssc.awaitTermination()
