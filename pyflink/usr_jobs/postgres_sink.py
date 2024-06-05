import json
import logging
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

KAFKA_HOST = "kafka:19092"
POSTGRES_HOST = "postgres:5432"

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


def initialize_env() -> StreamExecutionEnvironment:
    """Makes stream execution environment initialization"""
    env = StreamExecutionEnvironment.get_execution_environment()

    # Get current directory
    root_dir_list = __file__.split("/")[:-2]
    root_dir = "/".join(root_dir_list)

    # Adding the jar to the flink streaming environment
    env.add_jars(
        f"file://{root_dir}/lib/flink-connector-jdbc-3.1.2-1.18.jar",
        f"file://{root_dir}/lib/postgresql-42.7.3.jar",
        f"file://{root_dir}/lib/flink-sql-connector-kafka-3.1.0-1.18.jar",
    )
    return env


def configure_source(server: str, earliest: bool = False) -> KafkaSource:
    """Makes kafka source initialization"""
    properties = {
        "bootstrap.servers": server,
        "group.id": "iot-sensors",
    }

    offset = KafkaOffsetsInitializer.latest()
    if earliest:
        offset = KafkaOffsetsInitializer.earliest()

    kafka_source = (
        KafkaSource.builder()
        .set_topics("sensors")
        .set_properties(properties)
        .set_starting_offsets(offset)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return kafka_source


def configure_postgre_sink(sql_dml: str, type_info: Types) -> JdbcSink:
    """Makes postgres sink initialization. Config params are set in this function."""
    return JdbcSink.sink(
        sql_dml,
        type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(f"jdbc:postgresql://{POSTGRES_HOST}/flinkdb")
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


def configure_kafka_sink(server: str, topic_name: str) -> KafkaSink:

    return (
        KafkaSink.builder()
        .set_bootstrap_servers(server)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic_name)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def main() -> None:
    """Main flow controller"""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    # Initialize environment
    logger.info("Initializing environment")
    env = initialize_env()

    # Define source and sinks
    logger.info("Configuring source and sinks")
    kafka_source = configure_source(KAFKA_HOST)
    sql_dml = (
        "INSERT INTO raw_sensors_data (message_id, sensor_id, message, timestamp) "
        "VALUES (?, ?, ?, ?)"
    )
    TYPE_INFO = Types.ROW(
        [
            Types.STRING(),  # message_id
            Types.INT(),  # sensor_id
            Types.STRING(),  # message
            Types.SQL_TIMESTAMP(),  # timestamp
        ]
    )
    jdbc_sink = configure_postgre_sink(sql_dml, TYPE_INFO)
    kafka_sink = configure_kafka_sink(KAFKA_HOST, "alerts")
    logger.info("Source and sinks initialized")

    # Create a DataStream from the Kafka source and assign watermarks
    data_stream = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka sensors topic"
    )

    # Make transformations to the data stream
    transformed_data = data_stream.map(parse_data, output_type=TYPE_INFO)
    alarms_data = data_stream.map(
        filter_temperatures, output_type=Types.STRING()
    ).filter(lambda x: x is not None)
    logger.info("Defined transformations to data stream")

    logger.info("Ready to sink data")
    alarms_data.print()
    alarms_data.sink_to(kafka_sink)
    transformed_data.add_sink(jdbc_sink)

    # Execute the Flink job
    env.execute("Flink PostgreSQL and Kafka Sink")


if __name__ == "__main__":
    main()
