# Kafka, Flink & Iceberg

## Build Flink Application

```
./gradlew clean shadowJar
```

## Deploy infrastructure

```
docker-compose up
```

## Start data generator

TODO: Move to Docker

```python
python datagen/datagen.py
```

## Deploy Flink Application


Navigate to [http://localhost:8888](http://localhost:8888) and create a new `Python3` notebook. Using the `%%sql` magic, create an `test` database.
```sql
%%sql

CREATE DATABASE test
```

Navigate to the Flink UI at [http://localhost:8081/#/submit](http://localhost:8081/#/submit) and upload the shadow jar located at `build/libs/flink-iceberg-2-0.0.1.jar`.

Submit the Flink application and provide the parameters. (The table will be created if it does not exist).
```
--database "test" --table "clickstream" --branch "main"
```

Once the Flink application starts, data will begin streaming into the `test.clickstream` table.
You can then run a spark query in the notebook to see the results!
```sql
%%sql

SELECT * FROM test.clickstream LIMIT 10
```

Additional optional Flink arguments:
- `--checkpoint` - Set a checkpoint interval in milliseconds (default: 10000)
- `--event_interval` -  Set a time in milliseconds to sleep between each randomly generated record (default: 5000)
