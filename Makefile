ADDR	=	"localhost:9092"
T_RAW	= 	"raw"
T_SENT	= 	"sent"

all: fetch processing

kafka:
	zkserver start
	kafka-server-start /usr/local/etc/kafka/server.properties &

kafka-stop:
	kafka-server-stop

zookeeper-stop:
	zkserver stop

fetch:
	./dataFetching/imdbFetcher.py --max ${MAX} --kafka ${ADDR} --topic ${T_RAW} --verbose

processing:
	cd sentimentAnalysis && sbt "run ${ADDR} ${T_RAW}"
