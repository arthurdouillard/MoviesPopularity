MAX		=	100
ADDR	=	"localhost:9092"
TOPIC	=	"movies"

all: kafka fetch launch

kafka:
	echo "Starting Zookeeper and Kafka..."
	zkserver start
	kafka-server-start /usr/local/etc/kafka/server.properties &

kafka-stop:
	echo "Stopping Kafka and Zookeeper..."
	kafka-server-stop
	zkserver stop

fetch:
	echo "Fetching data..."
	./dataFetching/imdbFetcher.py --max ${MAX} --kafka ${ADDR} --topic ${TOPIC}

launch:
	echo "Launching..."
	# sbt run  ???
