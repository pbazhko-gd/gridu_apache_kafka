### How to install connector
1. Execute **mvn clean package**
2. Move jar file to **_docker/kafka_connect_plugins**
3. Restart all kafka-connect containers
4. Use the command listed below to operate with the connector
5. GitHub accounts list is located in **_docker/github-accounts.txt** file (check docker-compose.yml)

### Get existing schemas list in schema-registry
```bash
curl http://localhost:8081/subjects
```

### Get schema versions list in schema-registry
```bash
curl http://localhost:8081/subjects/github-accounts-value/versions
```

### Get schema by its version in schema-registry
```bash
curl http://localhost:8081/subjects/github-accounts-value/versions/7
```

### Change schema compatibility in schema-registry
```bash
curl -X PUT \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "NONE"}' \
  http://localhost:8081/config/github-accounts-value
```

### Upload new schema in schema-registry
```bash
curl -X POST http://localhost:8081/subjects/github-accounts-value/versions \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
--data '{"schema": '"$(jq -Rs . < ./common/src/main/resources/github-account-schema.json)"', "schemaType": "JSON"}' 
```

### Listen to Kafka topic
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

### Get available connector plugins list
```bash
curl http://localhost:18083/connector-plugins
```

### Get active connectors list
```bash
curl http://localhost:18083/connectors
```

### Get connector info
```bash
curl http://localhost:18083/connectors/github-accounts-source-connector
```

### Get connector status
```bash
curl http://localhost:18083/connectors/github-accounts-source-connector/status
```

### Delete connector
```bash
curl -X DELETE http://localhost:18083/connectors/github-accounts-source-connector
```

### Register and configure connector
```bash
curl -X POST http://localhost:18083/connectors \
-H "Content-Type: application/json" \
-d '{
    "name": "github-accounts-source-connector",
    "config": {
        "connector.class": "com.griddynamics.gridu.pbazhko.connector.GitHubAccountsSourceConnector",
        "tasks.max": "1",

        "github.accounts.file.path": "/data/github-accounts.txt",
        "github.accounts.topic": "github-accounts",

        "key.converter": "org.apache.kafka.connect.storage.StringConverter",

        "value.converter.schemas.enable": "true",
        "value.converter": "io.confluent.connect.json.JsonSchemaConverter",
        "value.converter.auto.register.schemas": "true",
        "value.converter.schema.registry.url": "http://schema-registry:8081"
    }
}'
```
