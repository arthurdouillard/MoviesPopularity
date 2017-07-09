ADDR	=	"localhost:9092"

T_RAW	= 	"raw"
T_SAVE  = 	"save"
SAVE_PATH= "file:///tmp/save"

all: fetch processing

kafka:
	zkserver start
	kafka-server-start /usr/local/etc/kafka/server.properties &

kafka-stop:
	kafka-server-stop

zookeeper-stop:
	zkserver stop

fetch:
	./dataFetching/imdbFetcher.py --kafka ${ADDR} --topic ${T_RAW} --verbose

processing:
	cd sentimentAnalysis && sbt "run ${ADDR} ${T_RAW} ${T_SAVE}" 2>/dev/null

save:
	cd dataSaver && sbt "run ${ADDR} ${T_SAVE} ${SAVE_PATH}"
