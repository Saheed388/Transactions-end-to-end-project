from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
import psycopg2

def spark_AllcleanData_cassandra_to_psql():
    spark = SparkSession.builder \
        .appName("CassandraToPostgres") \
        .config("spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,org.postgresql:postgresql:42.2.5") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .getOrCreate()

    try:
        # Read data from Cassandra
        df = spark.read.format("org.apache.spark.sql.cassandra") \
            .options(table="transactions", keyspace="bank_transactions") \
            .load()

        # Clean numeric columns
        numeric_cols = ["balance_amount", "deposit_amount", "withdrawal_amount"]
        for column in numeric_cols:
            df = df.withColumn(column, regexp_replace(col(column), "[^0-9.]", "").cast("double"))

        # Clean account_number (remove non-numeric characters)
        df = df.withColumn("account_number", regexp_replace(col("account_number"), "[^0-9]", ""))

        # Print schema and show data
        print("Schema:")
        df.printSchema()
        print("Data Sample:")
        df.show(truncate=False)

        # PostgreSQL Configuration
        POSTGRES_URL = "jdbc:postgresql://host.docker.internal:5432/postgres"
        POSTGRES_USER = "postgres"
        POSTGRES_PASSWORD = "postgres"
        POSTGRES_TABLE = "transactions"

        # Connect to PostgreSQL and create table
        conn = psycopg2.connect(
            dbname="postgres",
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host="host.docker.internal",  # Adjust host if needed
            port="5432"
        )
        cursor = conn.cursor()

        # Define PostgreSQL schema mapping
        spark_to_pg_types = {
            "string": "TEXT",
            "int": "INTEGER",
            "bigint": "BIGINT",
            "double": "DOUBLE PRECISION",
            "float": "REAL",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP"
        }

        # Extract schema from Spark DataFrame
        schema = df.dtypes  # List of (column_name, column_type)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
            {", ".join([f"{col} {spark_to_pg_types.get(dtype, 'TEXT')}" for col, dtype in schema])},
            PRIMARY KEY (transaction_id)  -- Using transaction_id as the primary key
        );
        """

        print("Executing SQL Query:", create_table_query)
        cursor.execute(create_table_query)
        conn.commit()
        print("Table created successfully.")

        cursor.close()
        conn.close()

        # Write data to PostgreSQL
        df.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", POSTGRES_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print("Data written to PostgreSQL successfully.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        spark.stop()

if __name__ == "__main__":
    spark_AllcleanData_cassandra_to_psql()
