# Movies Popularity
## Scala Project

The goal of this project is to use Apache Spark to analyze data about movies.

Setup:

- Start Zookerper with *zkserver start*
- Start Kafka server with:
 *kafka-server-start.sh /usr/local/etc/kafka/server.properties*
- Launch the data fetching or the data loader which writes into kafka
- Launch the program with the following command: *sbt "run **<brokers_list>** **<topics_list>**"*

4 steps


1.  Fetch movies data
2.  Analyse and process that data
3.  Persistence of the data
4.  Graphical visualization.

