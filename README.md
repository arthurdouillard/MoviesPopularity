# Movies Popularity
## Scala Project

The goal of this project is to use Apache Spark to analyze data about movies.

## Steps

### Init servers

```shell
> zkserver start
> kafka-server-start /usr/local/etc/kafka/server.properties &
```

### Fetch movies data

```shell
> ./dataFetching/imdbFetcher.py --max {NUMBER_OF_MOVIES} --kafka {BROKER_ADDR} --topic {TOPIC_1} --verbose
```

### Analyse sentiments

```shell
> ./sentimentAnalysis/sentimentAnalyser.py --src {TOPIC_1} --dst {TOPIC_2} --clf {PRE_TRAIN_CLF} --kafka {BROKER} --verbose
```

### Process ML data

```shell
> sbt "run {brokers_list} {topics_list} {hdfs_path}" 
```
