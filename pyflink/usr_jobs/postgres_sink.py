import json
from datetime import datetime

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import JdbcSink
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
)
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)


def parse_data(data: str) -> Row:
    data = json.loads(data)
    message_id = data["message_id"]
    sensor_id = int(data["sensor_id"])
    message = json.dumps(data["message"])
    timestamp = datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%S.%f+00:00")
    return Row(message_id, sensor_id, message, timestamp)


def filter_temperatures(value: str) -> str | None:
    TEMP_THRESHOLD = 30.0
    data = json.loads(value)
    message_id = data["message_id"]
    sensor_id = int(data["sensor_id"])
    temperature = float(data["message"]["temperature"])
    timestamp = data["timestamp"]
    if temperature > TEMP_THRESHOLD:  # Change 30.0 to your threshold
        alert_message = {
            "message_id": message_id,
            "sensor_id": sensor_id,
            "temperature": temperature,
            "alert": "High temperature detected",
            "timestamp": timestamp,
        }
        return json.dumps(alert_message)
    return None


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

# Filter events with temperature above threshold
alarms_data = (
    data_stream
        .map(filter_temperatures, output_type=Types.STRING())
        .filter(lambda x: x is not None)
)

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

kafka_sink = (
    KafkaSink.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("alerts")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
)

# Add the JDBC sink to the data stream
print("start sinking data")
alarms_data.print()
alarms_data.sink_to(kafka_sink)
transformed_data.add_sink(jdbc_sink)

# Execute the Flink job
env.execute("Flink PostgreSQL Sink Example")
