
sudo virtualenv -p python3.7 BigData_kt
pip install -r requirements.txt

sudo docker-compose -f docker-compose.yml up -d

sleep 30

sudo curl -X POST localhost:5601/api/saved_objects/_import -H "kbn-xsrf: true" --form file=@export.ndjson

python3 main.py

sleep 20

spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.3,org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.3,org.elasticsearch:elasticsearch-hadoop:7.10.0 Spark/SparkStream.py

