from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types
from pyflink.datastream.connectors import JdbcSink
from pyflink.datastream.connectors.jdbc import JdbcConnectionOptions, JdbcExecutionOptions

# Define the Stream Execution Environment
env = StreamExecutionEnvironment.get_execution_environment()

# Define the data type
type_info = Types.ROW([Types.INT(), Types.STRING(), Types.STRING(), Types.INT()])

# Create a DataStream from a collection
data_stream = env.from_collection(
    [
        (101, "Stream Processing with Apache Flink", "Fabian Hueske, Vasiliki Kalavri", 2019),
        (102, "Streaming Systems", "Tyler Akidau, Slava Chernyak, Reuven Lax", 2018),
        (103, "Designing Data-Intensive Applications", "Martin Kleppmann", 2017),
        (104, "Kafka: The Definitive Guide", "Gwen Shapira, Neha Narkhede, Todd Palino", 2017)
    ],
    type_info=type_info
)


# Get current directory
current_dir_list = __file__.split("/")[:-1]
current_dir = "/".join(current_dir_list)

# Adding the jar to the flink streaming environment
env.add_jars(
    f"file://{current_dir}/flink-connector-jdbc-3.1.2-1.18.jar",
    f"file://{current_dir}/postgresql-42.7.3.jar"
)



# Define the JDBC Sink
jdbc_sink = JdbcSink.sink(
    "INSERT INTO books (id, title, authors, year) VALUES (?, ?, ?, ?)",
	type_info,
    JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url('jdbc:postgresql://localhost:5432/flinkdb')
        .with_driver_name('org.postgresql.Driver')
        .with_user_name('flinkuser')
        .with_password('flinkpassword')
        .build(),
    JdbcExecutionOptions.builder()
        .with_batch_interval_ms(1000)
        .with_batch_size(200)
        .with_max_retries(5)
        .build()
)

# Add the JDBC sink to the data stream
data_stream.add_sink(jdbc_sink)

# Execute the Flink job
env.execute("Flink PostgreSQL Sink Example")
