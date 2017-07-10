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
> make fetch
```

### Analyze ML data 

```shell
> make processing
```

### Save ML data
```shell
> make save
```
