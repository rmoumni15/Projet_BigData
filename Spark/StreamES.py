import lit as lit
import schema

from pyspark.sql import functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, current_timestamp, schema_of_json
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from movie import *

conf = SparkConf().setAppName('trump-sentimental')
spark = SparkSession.builder.appName('trump-sentimental').getOrCreate()
kafka_df = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'movies') \
    .option('failOnDataLoss', False) \
    .load()

parsed_df = kafka_df \
    .select(F.col('value')) \
    .withColumn('timestamp', lit(current_timestamp()))
mdf = parsed_df.select('parsed_value.*', 'timestamp')

kafka_df.writeStream.outputMode('append') \
    .format('org.elasticsearch.spark.sql') \
    .option('es.nodes', 'localhost') \
    .option('es.port', 9200) \
    .option('checkpointLocation', '/checkpoint') \
    .option('es.spark.sql.streaming.sink.log.enabled', False) \
    .start('trump-sentimental') \
    .awaitTermination()

""""
x = kvs.map(lambda row: row[1]) \
    .map(lambda row: row.split("||")) \
    .map(lambda row: Movie(row[0], row[1], row[2], row[3], row[4], row[5], row[6],
                           row[7], row[8], row[9], row[10], row[11])) \
    .map(lambda obj: clean_movie(obj)) \
    .map(lambda obj: obj.__dict__) \
    .foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(
                                path='-',
                                outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                                keyClass="org.apache.hadoop.io.NullWritable",
                                valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",

                                # critically, we must specify our `es_write_conf`
                                conf=es_write_conf))

"""
