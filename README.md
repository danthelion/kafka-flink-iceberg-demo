# Kafka, Flink & Iceberg

## Build Flink Application

```
./gradlew clean shadowJar
```

## Deploy infrastructure

```
docker-compose up
```

# Deploy Flink Application


Navigate to [http://localhost:8888](http://localhost:8888) and create a new `Python3` notebook.

Run the following to create an empty database.

```sql
%%sql

CREATE DATABASE test
```

Go to the Flink UI at [http://localhost:8081/#/submit](http://localhost:8081/#/submit) and upload the shadow
jar located at `build/libs/flink-iceberg-2-0.0.1.jar`.

Submit the Flink application and provide the parameters.

```
--database "test" --table "clickstream" --branch "main"
```

Validate

```sql
%%sql

SELECT * FROM test.clickstream LIMIT 10;
```
