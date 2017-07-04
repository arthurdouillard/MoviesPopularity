MAX		=	100
ADDR	=	"localhost:9092"
T_RAW	= 	"raw"
T_SENT	= 	"sent"

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
	./dataFetching/imdbFetcher.py --max ${MAX} --kafka ${ADDR} --topic ${T_RAW}

sentiment:
	echo "Analysing sentiments..."
	./sentimentAnalysis/imdbFetcher.py --kafka ${ADDR} --src ${T_RAW}\
		--dst ${T_SENT} --clf sentimentAnalysis/classifier.pkl


launch:
	echo "Launching..."
	# sbt run  ???
