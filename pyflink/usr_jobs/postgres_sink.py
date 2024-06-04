import json

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import JdbcSink
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
)
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.common import Row
from datetime import datetime

def parse_data(data: str) -> Row:
    data = json.loads(data)
    message_id = data["message_id"]
    sensor_id = int(data["sensor_id"])
    message = json.dumps(data["message"])
    timestamp = datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%S.%f+00:00")
    return Row(message_id, sensor_id, message, timestamp)


# Define the Stream Execution Environment
env = StreamExecutionEnvironment.get_execution_environment()

# Define the data type
type_info = Types.ROW(
    [
        Types.STRING(),  # message_id
        Types.INT(),  # sensor_id
        Types.STRING(),  # message
        Types.SQL_TIMESTAMP(),  # timestamp
    ]
)


# Get current directory
current_dir_list = __file__.split("/")[:-1]
current_dir = "/".join(current_dir_list)

# Adding the jar to the flink streaming environment
env.add_jars(
    f"file://{current_dir}/flink-connector-jdbc-3.1.2-1.18.jar",
    f"file://{current_dir}/postgresql-42.7.3.jar",
    f"file://{current_dir}/flink-sql-connector-kafka-3.1.0-1.18.jar",
)

properties = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "iot-sensors",
}

earliest = False
offset = (
    KafkaOffsetsInitializer.earliest() if earliest else KafkaOffsetsInitializer.latest()
)

# Create a Kafka Source
# NOTE: FlinkKafkaConsumer class is deprecated
kafka_source = (
    KafkaSource.builder()
    .set_topics("sensors")
    .set_properties(properties)
    .set_starting_offsets(offset)
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

# Create a DataStream from the Kafka source and assign watermarks
data_stream = env.from_source(
    kafka_source, WatermarkStrategy.no_watermarks(), "Kafka sensors topic"
)

# Print line for readablity in the console
print("start reading data from kafka")


transformed_data = data_stream.map(parse_data, output_type=type_info)

# Define the JDBC Sink
jdbc_sink = JdbcSink.sink(
    "INSERT INTO raw_sensors_data (message_id, sensor_id, message, timestamp) VALUES (?, ?, ?, ?)",
    type_info,
    JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
    .with_url("jdbc:postgresql://localhost:5432/flinkdb")
    .with_driver_name("org.postgresql.Driver")
    .with_user_name("flinkuser")
    .with_password("flinkpassword")
    .build(),
    JdbcExecutionOptions.builder()
    .with_batch_interval_ms(1000)
    .with_batch_size(200)
    .with_max_retries(5)
    .build(),
)


# Add the JDBC sink to the data stream
transformed_data.print()
transformed_data.add_sink(jdbc_sink)
print("start writing data to postgresql")


# Execute the Flink job
env.execute("Flink PostgreSQL Sink Example")
