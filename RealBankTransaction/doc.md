CREATE TABLE flink_keyspace.transactions_event (
    transaction_date TEXT,
    account_number TEXT,
    transaction_details TEXT,
    cheque_number TEXT,
    withdrawal_amount DOUBLE,
    deposit_amount DOUBLE,
    balance_amount DOUBLE,
    PRIMARY KEY (transaction_date, account_number)
);

CREATE TABLE test_cassandra_sink (
    id STRING,
    value DOUBLE
) WITH (
    'connector' = 'cassandra',
    'host' = '127.0.0.1',
    'port' = '9042',
    'keyspace' = 'flink_keyspace',
    'table-name' = 'test_table'
);

INSERT INTO test_cassandra_sink VALUES ('id1', 123.45);



CREATE TABLE transactions (
    id STRING,
    amount DOUBLE,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'cassandra',
    'host' = 'cassandra',
    'keyspace' = 'flink_keyspace',
    'table-name' = 'transactions_event'
);


CREATE TABLE transaction_table (
    `Account No` STRING,
    `DATE` BIGINT,
    `TRANSACTION DETAILS` STRING,
    `CHQ.NO.` STRING,
    `VALUE DATE` BIGINT,
    `WITHDRAWAL AMT` DOUBLE,
    `DEPOSIT AMT` DOUBLE,
    `BALANCE AMT` DOUBLE,
    `.` STRING
) WITH (
    'connector' = 'kafka', 
    'topic' = 'transactions-topic', 
    'properties.bootstrap.servers' = 'localhost:9092', 
    'format' = 'json'  -- If your data is in JSON format
);


docker compose exec jobmanager ./bin/flink run -py /opt/src/job/transaction_job.py --pyFiles /opt/src -d

docker compose exec flink-jobmanager ./bin/flink run -py /opt/src/job/transaction_job.py --pyFiles /opt/src -d

docker compose exec jobmanager bash

bin/sql-client.sh embedded



flink-connector-cassandra-1.16.0.jar

Place it in Flink’s lib/ folder:

cp flink-connector-cassandra-1.16.0.jar $FLINK_HOME/lib/

docker compose exec jobmanager ls /opt/flink/job/

chmod 644 /opt/flink/lib/*.jar

./bin/stop-cluster.sh
./bin/start-cluster.sh
tail -n 100 /opt/flink/log/jobmanager.log

cqlsh 127.0.0.1 9042

ls -l /opt/flink/lib/flink-connector-cassandra_2.12-1.16.0.jar





    docker exec -it postgres psql -U postgres -d postgres

    CREATE TABLE processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
);



CREATE TABLE transactions_event (
    account_no TEXT,
    transaction_date BIGINT,
    transaction_details TEXT,
    chq_no TEXT,
    value_date BIGINT,
    withdrawal_amt DOUBLE PRECISION,
    deposit_amt DOUBLE PRECISION,
    balance_amt DOUBLE PRECISION
);


docker exec -it redpanda-1 rpk topic create transaction-topic --brokers=localhost:9092
