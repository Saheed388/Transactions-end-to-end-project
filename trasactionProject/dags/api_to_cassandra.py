import os
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import uuid
import logging
from kaggle.api.kaggle_api_extended import KaggleApi
import tempfile
from datetime import datetime

def process_bank_transactions():
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Set the Kaggle config directory to ~/.config/kaggle
    os.environ["KAGGLE_CONFIG_DIR"] = os.path.expanduser("~/.config/kaggle")
    logger.info("Kaggle credentials successfully set.")

    # Authenticate Kaggle API
    try:
        logger.info("Initializing Kaggle API...")
        api = KaggleApi()
        api.authenticate()
        logger.info("Kaggle API authenticated successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize Kaggle API: {e}")
        raise Exception("Failed to authenticate Kaggle API")

    # Connect to Cassandra
    try:
        logger.info("Connecting to Cassandra...")
        cluster = Cluster(['cassandra'], port=9042)
        session = cluster.connect()
        session.default_timeout = 600  # Increase timeout from default (10s)

        logger.info("Connected to Cassandra successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {e}")
        raise Exception("Cassandra connection failed")

    # Create Keyspace and Table
    try:
        logger.info("Creating keyspace and table...")
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS bank_transactions
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
        session.set_keyspace('bank_transactions')

        session.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id UUID PRIMARY KEY,
                account_number TEXT,
                transaction_date TIMESTAMP,
                transaction_details TEXT,
                cheque_number TEXT,
                withdrawal_amount DOUBLE,
                deposit_amount DOUBLE,
                balance_amount DOUBLE
            )
        """)
        logger.info("Keyspace and table created successfully.")
    except Exception as e:
        logger.error(f"Failed to create keyspace or table: {e}")
        cluster.shutdown()
        raise Exception("Failed to create keyspace or table")

    # Check if the cheque_number column exists in the table
    column_exists = False
    try:
        rows = session.execute("SELECT column_name FROM system_schema.columns WHERE keyspace_name = 'bank_transactions' AND table_name = 'transactions';")
        for row in rows:
            if row.column_name == 'cheque_number':
                column_exists = True
                break
    except Exception as e:
        logger.error(f"Failed to check column existence: {e}")
        cluster.shutdown()
        exit(1)

    # Download and process dataset from Kaggle
    try:
        logger.info("Downloading dataset from Kaggle...")
        with tempfile.TemporaryDirectory() as temp_dir:
            api.dataset_download_files('apoorvwatsky/bank-transaction-data', path=temp_dir, unzip=True)
            xlsx_file = next((file for file in os.listdir(temp_dir) if file.endswith('.xlsx')), None)

            if not xlsx_file:
                logger.error("No XLSX file found in the downloaded dataset.")
                raise Exception("No XLSX file found in the downloaded dataset")

            df = pd.read_excel(os.path.join(temp_dir, xlsx_file))

        df.rename(columns={
            'Account No': 'account_number',
            'DATE': 'transaction_date',
            'TRANSACTION DETAILS': 'transaction_details',
            'CHQ.NO.': 'cheque_number',
            'WITHDRAWAL AMT': 'withdrawal_amount',
            'DEPOSIT AMT': 'deposit_amount',
            'BALANCE AMT': 'balance_amount'
        }, inplace=True)

        # Handle potential data type issues
        try:
            df['transaction_details'] = df['transaction_details'].astype(str)
        except Exception as e:
            logger.warning(f"Failed to convert transaction_details to string: {e}")

        try:
            df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
            df['transaction_date'].fillna(pd.Timestamp('1970-01-01 00:00:00'), inplace=True)
            df['transaction_date'] = df['transaction_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logger.warning(f"Failed to process transaction_date: {e}")

        # Fill NaN values with appropriate defaults
        df.fillna({
            'cheque_number': 'N/A',
            'withdrawal_amount': 0.0,
            'deposit_amount': 0.0,
            'balance_amount': 0.0
        }, inplace=True)

        # Insert data into Cassandra
        logger.info("Inserting data into Cassandra...")
        batch_size = 100
        total_rows = len(df)
        inserted_rows = 0

        if column_exists:
            insert_query = """
                INSERT INTO transactions (transaction_id, account_number, transaction_date, transaction_details, cheque_number, withdrawal_amount, deposit_amount, balance_amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
        else:
            insert_query = """
                INSERT INTO transactions (transaction_id, account_number, transaction_date, transaction_details, withdrawal_amount, deposit_amount, balance_amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

        while inserted_rows < total_rows:
            batch = BatchStatement()
            for _, row in df[inserted_rows:inserted_rows + batch_size].iterrows():
                try:
                    if column_exists:
                        batch.add(insert_query, (
                            uuid.uuid4(),
                            row['account_number'],
                            row['transaction_date'],
                            str(row['transaction_details']),  # Ensure it's a string
                            str(row['cheque_number']),
                            float(row['withdrawal_amount']),
                            float(row['deposit_amount']),
                            float(row['balance_amount'])
                        ))
                    else:
                        batch.add(insert_query, (
                            uuid.uuid4(),
                            row['account_number'],
                            row['transaction_date'],
                            str(row['transaction_details']),  # Ensure it's a string
                            float(row['withdrawal_amount']),
                            float(row['deposit_amount']),
                            float(row['balance_amount'])
                        ))
                except Exception as e:
                    logger.warning(f"Skipping row due to error: {e}")

            try:
                session.execute(batch)
                inserted_rows += batch_size
                logger.info(f"Inserted {inserted_rows} of {total_rows} rows...")
            except Exception as e:
                logger.error(f"Failed to execute batch insert: {e}")

        logger.info("Data inserted into Cassandra successfully.")

    except Exception as e:
        logger.error(f"Failed to process or insert data: {e}")
    finally:
        session.shutdown()
        cluster.shutdown()
        logger.info("Cassandra connection closed.")

# Call the function to execute the script
process_bank_transactions()