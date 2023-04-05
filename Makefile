run-test-cassandra:
	-sudo docker stop one-node-cassandra
	-sudo docker rm one-node-cassandra
	sudo docker run -p 9042:9042 -p 9142:9142 --rm --name one-node-cassandra -d cassandra:4.0
	#cat ./schemas/cassandra.cql | sudo docker exec -t one-node-cassandra cqlsh -u cassandra -p cassandra


run-test-opensearch:
	-sudo docker stop opensearch-node
	-sudo docker rm opensearch-node
	sudo docker run -p 9200:9200 --rm -e "discovery.type=single-node" --name opensearch-node -d opensearchproject/opensearch:latest
	sleep 5
	curl -H"Content-type: application/json" -XPUT "https://localhost:9200/goshenite?pretty" -ku admin:admin --data "@./schemas/opensearch-mapping.json"


.PHONY:run-test-opensearch run-test-cassandra
