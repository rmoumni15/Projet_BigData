echo STEP 1/2 : Initializing Containers ..........

sudo docker-compose -f dashboard/docker-compose.yml up -d

echo Initialization done....

echo Clearing elasticsearch space ....... Probelme rencontré lors de la demonstration......
sleep 1
curl -XPUT -H "Content-Type: application/json" http://localhost:9200/_cluster/settings -d '{ "transient": { "cluster.routing.allocation.disk.threshold_enabled": false } }'
curl -XPUT -H "Content-Type: application/json" http://localhost:9200/_all/_settings -d '{"index.blocks.read_only_allow_delete": null}'


echo STEP 2/2 : Importing Dashboard....

cd dashboard

until sudo curl -X POST localhost:5601/api/saved_objects/_import -H "kbn-xsrf: true" --form file=@export.ndjson
do
  echo "Kibana not ready yet... retrying.....0"
  sleep 30
done

cd ..

echo Done...


