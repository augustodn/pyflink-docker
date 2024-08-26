# Step-by-Step Guide to Run the Streaming Pipeline
## Step 1: Launch the multi-container application

Launch the containers by running docker-compose. I preferred to do it without detached mode to see the logs while the containers are spinning up and then running.

```bash
docker compose up
```

Check for the logs to see if the services are running properly.

## Step 2: Create the Kafka topics
Next, we’re going to create the topics to receive data from the IoT sensors and store the alerts filtered by the Flink application.

```bash
docker compose exec kafka kafka-topics --create --topic sensors --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
Followed by

```bash
docker compose exec kafka kafka-topics --create --topic alerts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

To check if the topics were created correctly you can execute the following command

```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```
## Step 3: Create Postgres table
Login to the postgres console

```bash
psql -h localhost -U flinkuser -d flinkdb
```

Enter the password `flinkpassword` to log into the posgres console, remember this is a local configuration so default access have been configured in the `docker-compose.yml`. Then create the table

```sql
CREATE TABLE raw_sensors_data (
	message_id VARCHAR(255) PRIMARY KEY,
	sensor_id INT NOT NULL,
	message TEXT NOT NULL,
	timestamp TIMESTAMPTZ NOT NULL
);
```

You can check if the table is properly created by doing the following

```
flinkdb=# \d raw_sensors_data
```
This will show you a result similar to the following one:

```
                      Table "public.raw_sensors_data"
   Column   |            Type             | Collation | Nullable | Default
------------+-----------------------------+-----------+----------+---------
 message_id | character varying(255)      |           | not null |
 sensor_id  | integer                     |           | not null |
 message    | text                        |           | not null |
 timestamp  | timestamp without time zone |           | not null |
Indexes:
    "raw_sensors_data_pkey" PRIMARY KEY, btree (message_id)

flinkdb=#
```
## Step 4: Launching the Kafka producer
Create a local environment and install python kafka package:

```bash
pip install kafka-python
```
Then execute the kafka producer, which mimics IoT sensor messages and publishes messages to the `sensors` topic

```bash
python pyflink/usr_jobs/kafka_producer.py
```

Leave it running for the rest of the tutorial.

## Step 5: Initializing the Flink task
We’re going to launch the Flink application from within the container, so you can monitor it from the web UI through `localhost:8081`. Run the following command from the repository root:

```bash
docker compose exec flink-jobmanager flink run -py /opt/flink/usr_jobs/postgres_sink.py
```

You’ll see some logging information, additionally alerts will also be displayed in the `flink-jobmanager` container logs. However, we’ll check the messages using Postgres table and reading the alerts topic, which were created on this purpose.

## Step 6: Read Alerts in Kafka Topic
To read data in the alerts topic, you can execute the following command:

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic alerts --from-beginning
```

That will bring all the messages that the topic have received so far.

## Step 7: Read raw data from Postgres table

Additionally you can query the raw messages from the IoT sensor and even parse the JSON data in PostgreSQL:

```sql
SELECT
    *,
    (message::json->>'temperature')::numeric as temperature
FROM raw_sensors_data
LIMIT 10;
```
