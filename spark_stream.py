import logging
import os

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

SPARK_VERSION = os.getenv('SPARK_VERSION', '3.4.1')
SCALA_VERSION = os.getenv('SCALA_VERSION', '2.12')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'localhost')
CHECKPOINT_LOCATION = os.path.abspath(os.getenv('CHECKPOINT_LOCATION', 'tmp/checkpoint'))
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'spark_streams')
TABLE = os.getenv('CASSANDRA_TABLE', 'created_users')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """ % KEYSPACE)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS %s.%s (
        id TEXT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """ % (KEYSPACE, TABLE))

    print("Table created successfully!")

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config(
                'spark.jars.packages',
                f"com.datastax.spark:spark-cassandra-connector_{SCALA_VERSION}:{SPARK_VERSION},"
                f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"
            ) \
            .config('spark.cassandra.connection.host', CASSANDRA_HOST) \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS) \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster([CASSANDRA_HOST])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is None:
        raise SystemExit(1)

    # connect to kafka with spark connection
    spark_df = connect_to_kafka(spark_conn)
    if spark_df is None:
        raise SystemExit(1)

    selection_df = create_selection_df_from_kafka(spark_df)
    session = create_cassandra_connection()

    if session is None:
        raise SystemExit(1)

    create_keyspace(session)
    create_table(session)

    logging.info("Streaming is being started...")

    streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                       .option('checkpointLocation', CHECKPOINT_LOCATION)
                       .option('keyspace', KEYSPACE)
                       .option('table', TABLE)
                       .outputMode('append')
                       .start())

    streaming_query.awaitTermination()
