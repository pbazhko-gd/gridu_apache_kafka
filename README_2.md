## Execution environment
#### Init the whole environment
```bash
docker-compose --project-directory ./_docker up -d
```
#### Destroy the whole environment
```bash
docker-compose --project-directory ./_docker down
```
#### Recreate one container in docker environment
```bash
docker-compose --project-directory ./_docker up -d --no-deps --build kafka-connect
```
## Kafka topics
### Source topics
#### github-accounts
```bash
docker exec -it kafka1 kafka-topics \
--bootstrap-server :9092 \
--create \
--topic github-accounts \
--partitions 3 \
--replication-factor 2
```
#### github-commits
```bash
docker exec -it kafka1 kafka-topics \
--bootstrap-server :9092 \
--create \
--topic github-commits \
--partitions 3 \
--replication-factor 2
```
---
### Topics for metrics
*Topics for metrics have only one partition due to the nature of metric events:
they have a single key, and distribution across multiple partitions is unnecessary.*
#### github-commits-total-count
```bash
docker exec -it kafka1 kafka-topics \
--bootstrap-server :9092 \
--create \
--topic github-commits-total-count \
--partitions 1 \
--replication-factor 2
```
#### github-commits-per-author
```bash
docker exec -it kafka1 kafka-topics \
--bootstrap-server :9092 \
--create \
--topic github-commits-per-author \
--partitions 1 \
--replication-factor 2
```
#### top-committers
```bash
docker exec -it kafka1 kafka-topics \
--bootstrap-server :9092 \
--create \
--topic top-committers \
--partitions 1 \
--replication-factor 2
```
#### github-committers-total-count
```bash
docker exec -it kafka1 kafka-topics \
--bootstrap-server :9092 \
--create \
--topic github-committers-total-count \
--partitions 1 \
--replication-factor 2
```
#### github-commits-per-language
```bash
docker exec -it kafka1 kafka-topics \
--bootstrap-server :9092 \
--create \
--topic github-commits-per-language \
--partitions 1 \
--replication-factor 2
```
#### top-languages
```bash
docker exec -it kafka1 kafka-topics \
--bootstrap-server :9092 \
--create \
--topic top-languages \
--partitions 1 \
--replication-factor 2
```
---
## Listen to Kafka topics
```bash
docker exec -it kafka1 kafka-console-consumer \
--bootstrap-server :9092 \
--topic github-accounts \
#--from-beginning \
--property print.headers=false \
--property print.timestamp=false \
--property print.partition=false \
--property print.offset=true \
--property print.key=true \
--property print.value=true 
```
---
## Schema registry
#### Get existing schemas list in schema-registry
```bash
curl http://localhost:8081/subjects
```
#### Get schema versions list in schema-registry
```bash
curl http://localhost:8081/subjects/github-accounts-value/versions
```
```bash
curl http://localhost:8081/subjects/github-commits-value/versions
```
#### Get schema by its version in schema-registry
```bash
curl http://localhost:8081/subjects/github-accounts-value/versions/8
```
```bash
curl http://localhost:8081/subjects/github-commits-value/versions/8
```
#### Change schema compatibility in schema-registry
```bash
curl -X PUT \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "NONE"}' \
  http://localhost:8081/config/github-accounts-value
```
#### Upload new schema in schema-registry
```bash
curl -X POST http://localhost:8081/subjects/github-accounts-value/versions \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
--data '{"schema": '"$(jq -Rs . < ./common/src/main/resources/github-account-schema.json)"', "schemaType": "JSON"}' 
```
```bash
curl -X POST http://localhost:8081/subjects/github-commits-value/versions \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
--data '{"schema": '"$(jq -Rs . < ./common/src/main/resources/github-commit-schema.json)"', "schemaType": "JSON"}' 
```
---
## How to install connector
1. Execute **mvn clean package** in a target maven module
2. Move jar file to **_docker/kafka_connect_plugins**
3. Restart all kafka-connect containers
---
#### Get available connector plugins list
```bash
curl http://localhost:18083/connector-plugins
```

#### Get active connectors list
```bash
curl http://localhost:18083/connectors
```
---
## GitHub accounts source connector
#### Get source connector info
```bash
curl http://localhost:18083/connectors/github-accounts-source-connector
```

#### Get source connector status
```bash
curl http://localhost:18083/connectors/github-accounts-source-connector/status
```

#### Delete source connector
```bash
curl -X DELETE http://localhost:18083/connectors/github-accounts-source-connector
```

#### Register and configure source connector
```bash
curl -X POST http://localhost:18083/connectors \
-H "Content-Type: application/json" \
-d '{
    "name": "github-accounts-source-connector",
    "config": {
        "connector.class": "com.griddynamics.gridu.pbazhko.connector.GitHubAccountsSourceConnector",
        "tasks.max": "1",

        "github.accounts.file.path": "/input/github-accounts.txt",
        "github.accounts.topic": "github-accounts",

        "key.converter": "org.apache.kafka.connect.storage.StringConverter",

        "value.converter.schemas.enable": "true",
        "value.converter": "io.confluent.connect.json.JsonSchemaConverter",
        "value.converter.auto.register.schemas": "true",
        "value.converter.schema.registry.url": "http://schema-registry:8081"
    }
}'
```
---
## GitHub commits metrics sink connectors
#### Get sink connector info
```bash
curl http://localhost:18083/connectors/github-commits-metrics-sink-connector
```
#### Get sink connector status
```bash
curl http://localhost:18083/connectors/github-commits-metrics-sink-connector/status
```
#### Delete sink connector
```bash
curl -X DELETE http://localhost:18083/connectors/github-commits-metrics-sink-connector
```
#### Register and configure sink connector
```bash
curl -X POST http://localhost:18083/connectors \
-H "Content-Type: application/json" \
-d '{
    "name": "github-commits-metrics-sink-connector",
    "config": {
        "connector.class": "com.griddynamics.gridu.pbazhko.connector.GitHubCommitsMetricsSinkConnector",
        "tasks.max": "1",
        "topics": "top-committers,top-languages,github-commits-per-author,github-commits-per-language,github-commits-total-count,github-committers-total-count",
        "consumer.override.auto.offset.reset": "earliest",

        "target.file.path": "/output/top-committers.json",

        "top.committers.topic.name": "top-committers",
        "top.languages.topic.name": "top-languages",
        "commits.per.author.topic.name": "github-commits-per-author",
        "commits.per.language.topic.name": "github-commits-per-language",
        "total.commits.count.topic.name": "github-commits-total-count",
        "total.committers.count.topic.name": "github-committers-total-count",

        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.storage.StringConverter"
    }
}'
```
