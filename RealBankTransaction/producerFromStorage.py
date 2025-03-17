import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

def main():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],  # Ensure Flink uses same broker
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(3, 0, 0),
            request_timeout_ms=10000
        )

        transactions_json = 'data/transactions.json'

        with open(transactions_json, 'r', encoding='utf-8') as file:
            data = json.load(file)

        if not isinstance(data, list):
            print("Error: JSON file must contain a list of objects.")
            return

        total_rows = len(data)

        for index, record in enumerate(data, start=1):
            # Convert JSON keys to match Flink expected schema
            formatted_record = {
                "Account No": record.get("account_no", ""),
                "DATE": record.get("transaction_date", 0),
                "TRANSACTION DETAILS": record.get("transaction_details", ""),
                "CHQ.NO.": record.get("chq_no", ""),
                "VALUE DATE": record.get("value_date", 0),
                "WITHDRAWAL AMT": record.get("withdrawal_amt", 0.0),
                "DEPOSIT AMT": record.get("deposit_amt", 0.0),
                "BALANCE AMT": record.get("balance_amt", 0.0),
            }

            future = producer.send('transaction-topic', value=formatted_record)
            
            try:
                future.get(timeout=5)
                print(f"Sent {index}/{total_rows}: {json.dumps(formatted_record, indent=2)}")
            except KafkaError as e:
                print(f"Failed to send record {index}: {e}")

        producer.flush()
        producer.close()

    except FileNotFoundError:
        print("Error: transactions.json file not found!")
    except json.JSONDecodeError:
        print("Error: Invalid JSON format!")
    except Exception as e:
        print(f"Unexpected Error: {e}")

if __name__ == "__main__":
    main()
