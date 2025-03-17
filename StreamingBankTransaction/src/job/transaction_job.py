from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_transaction_table_sink_to_postgres(t_env):
    """
    Create a PostgreSQL sink table for transactions.
    """
    table_name = 'transactions_event'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            account_no STRING,
            transaction_date TIMESTAMP,
            transaction_details STRING,
            chq_no STRING,
            value_date TIMESTAMP,
            withdrawal_amt DOUBLE,
            deposit_amt DOUBLE,
            balance_amt DOUBLE
        )
        WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',  -- Replace with your PostgreSQL username
            'password' = 'postgres',  -- Replace with your PostgreSQL password
            'driver' = 'org.postgresql.Driver'
        )
    """
    t_env.execute_sql(sink_ddl)
    print(f"PostgreSQL sink table '{table_name}' created.")
    return table_name

def create_event_source_kafka(t_env):
    """
    Create a Kafka source table for transactions.
    """
    table_name = "transactions_event_source"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            `Account No` STRING,
            `DATE` BIGINT,
            `TRANSACTION DETAILS` STRING,
            `CHQ.NO.` STRING,
            `VALUE DATE` BIGINT,
            `WITHDRAWAL AMT` DOUBLE,
            `DEPOSIT AMT` DOUBLE,
            `BALANCE AMT` DOUBLE,
            `event_watermark` AS TO_TIMESTAMP_LTZ(`DATE`, 3),  -- Convert BIGINT to TIMESTAMP
            WATERMARK FOR `event_watermark` AS `event_watermark` - INTERVAL '15' SECOND
        )
        WITH (
            'connector' = 'kafka',
            'topic' = 'transaction-topic',  -- Replace with your Kafka topic
            'properties.bootstrap.servers' = 'redpanda-1:29092',  -- Replace with your Kafka broker
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """
    t_env.execute_sql(source_ddl)
    print(f"Kafka source table '{table_name}' created.")
    return table_name

def log_processing():
    """
    Main function to process data from Kafka and write to PostgreSQL.
    """
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    env.enable_checkpointing(10 * 1000)  # Enable checkpointing every 10 seconds

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka source table
        source_table = create_event_source_kafka(t_env)
        # Create PostgreSQL sink table
        sink_table = create_transaction_table_sink_to_postgres(t_env)

        # Write data from Kafka to PostgreSQL
        insert_sql = f"""
            INSERT INTO {sink_table}
            SELECT
                TRIM(`Account No`) AS account_no,
                TO_TIMESTAMP_LTZ(`DATE`, 3) AS transaction_date,  -- Convert BIGINT to TIMESTAMP
                `TRANSACTION DETAILS` AS transaction_details,
                `CHQ.NO.` AS chq_no,
                TO_TIMESTAMP_LTZ(`VALUE DATE`, 3) AS value_date,  -- Convert BIGINT to TIMESTAMP
                COALESCE(`WITHDRAWAL AMT`, 0.0) AS withdrawal_amt,
                COALESCE(`DEPOSIT AMT`, 0.0) AS deposit_amt,
                COALESCE(`BALANCE AMT`, 0.0) AS balance_amt
            FROM {source_table}
        """
        t_env.execute_sql(insert_sql).wait()
        print("Data successfully written from Kafka to PostgreSQL.")

    except Exception as e:
        print("Error processing data:", str(e))

if __name__ == '__main__':
    log_processing()
