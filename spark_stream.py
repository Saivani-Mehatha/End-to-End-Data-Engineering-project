import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Create keyspace if not exists
def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        logging.info("Keyspace created successfully!")
    except Exception as e:
        logging.error(f"Error creating keyspace: {e}")

# Create table if not exists
def create_table(session):
    try:
        session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
        """)
        logging.info("Table created successfully!")
    except Exception as e:
        logging.error(f"Error creating table: {e}")

# Insert data into Cassandra table
def insert_data(session, **kwargs):
    logging.info(f"Inserting data for {kwargs.get('first_name')} {kwargs.get('last_name')}")

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (kwargs.get('id'), kwargs.get('first_name'), kwargs.get('last_name'), kwargs.get('gender'),
              kwargs.get('address'), kwargs.get('post_code'), kwargs.get('email'), kwargs.get('username'),
              kwargs.get('dob'), kwargs.get('registered_date'), kwargs.get('phone'), kwargs.get('picture')))
        logging.info(f"Data inserted for {kwargs.get('first_name')} {kwargs.get('last_name')}")
    except Exception as e:
        logging.error(f"Could not insert data due to {e}")

# Create Spark session
def create_spark_connection():
    try:
        spark_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1," 
                                          "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return spark_conn
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        return None

# Connect to Kafka and read stream
def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
        return spark_df
    except Exception as e:
        logging.error(f"Could not create Kafka dataframe: {e}")
        return None

# Create Cassandra connection
def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        logging.info("Cassandra connection established successfully")
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection: {e}")
        return None

# Transform Kafka stream data
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
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    try:
        sel = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')) \
            .select("data.*")
        logging.info("Data transformed successfully")
        return sel
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        return None

# Main function to start Spark Streaming
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Create Spark session
    spark_conn = create_spark_connection()

    if spark_conn:
        # Connect to Kafka and consume data
        spark_df = connect_to_kafka(spark_conn)

        if spark_df:
            selection_df = create_selection_df_from_kafka(spark_df)
            session = create_cassandra_connection()

            if session:
                create_keyspace(session)
                create_table(session)

                logging.info("Starting Spark Streaming...")

                # Streaming query to insert data into Cassandra
                streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                                   .option('checkpointLocation', '/tmp/checkpoint')
                                   .option('keyspace', 'spark_streams')
                                   .option('table', 'created_users')
                                   .start())

                streaming_query.awaitTermination()
            else:
                logging.error("Cassandra connection failed")
        else:
            logging.error("Kafka connection failed")
    else:
        logging.error("Spark session creation failed")
