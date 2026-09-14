docker exec kafka kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --topic order-events --partitions 3 --replication-factor 1 --config retention.ms=3600000

docker exec kafka kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic order-events