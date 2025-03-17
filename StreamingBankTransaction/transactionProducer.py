import os
import pandas as pd
import json
import time
from kafka import KafkaProducer
import uuid
import logging
from kaggle.api.kaggle_api_extended import KaggleApi
import tempfile

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
server = 'localhost:9092'
topic_name = 'transactions-topic'

# JSON serializer for Kafka messages
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def main():
    # Initialize Kafka producer
    try:
        logger.info("Initializing Kafka producer...")
        producer = KafkaProducer(
            bootstrap_servers=[server],
            value_serializer=json_serializer
        )
        logger.info("Kafka producer initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize Kafka producer: {e}")
        exit(1)

    # Initialize Kaggle API
    try:
        logger.info("Initializing Kaggle API...")
        api = KaggleApi()
        api.authenticate()
        logger.info("Kaggle API authenticated successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize Kaggle API: {e}")
        exit(1)

    # Download and process dataset directly from Kaggle
    try:
        logger.info("Downloading dataset from Kaggle...")

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            api.dataset_download_files('apoorvwatsky/bank-transaction-data', path=temp_dir, unzip=True)

            # Find the first Excel file in the extracted files

            xlsx_file = next((file for file in os.listdir(temp_dir) if file.endswith('.xlsx')), None)
            if not xlsx_file:
                logger.error("No XLSX file found in the downloaded dataset.")
                exit(1)

            # Read the Excel file into a pandas DataFrame
            df = pd.read_excel(os.path.join(temp_dir, xlsx_file))
            logger.info(f"Dataset loaded successfully with {len(df)} rows.")

            # Process and send each row to Kafka
            for index, row in df.iterrows():
                # Convert row to dictionary
                transaction = row.to_dict()


                # Send the transaction to Kafka
                try:
                    producer.send(topic_name, value=transaction)
                    logger.info(f"Sent transaction {transaction['transaction_id']} to Kafka.")
                except Exception as e:
                    logger.error(f"Failed to send transaction {transaction['transaction_id']} to Kafka: {e}")

            # Flush and close the producer
            producer.flush()
            producer.close()
            logger.info("All transactions sent to Kafka. Producer closed.")

    except Exception as e:
        logger.error(f"Failed to process dataset: {e}")
        exit(1)

if __name__ == "__main__":
    main()