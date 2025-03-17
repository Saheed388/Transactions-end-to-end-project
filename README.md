# Data Processing Pipeline

USE THE LINK FOR INSTALLATION GUIDE 
https://docs.google.com/document/d/1N9HuppvZlJ7EMVoWMQrnYV6wzOPqjTYDvRV1odF7wOg/edit?usp=sharing

## Overview
This project is a **Big Data Processing Pipeline** that integrates batch and streaming data processing using **Apache Kafka, Apache Flink, Apache Spark, Cassandra, PostgreSQL (PSQL), and Airflow**. The processed data is then visualized using **Power BI**.

## Architecture

### **1. Data Source**
- The data originates from an **KAGGLE API** and is ingested into the system using a **Python Producer App**.
- The producer pushes data to either:
  - **Apache Kafka** for streaming processing.
  - **Apache Airflow** for batch processing.

### **2. Streaming Data Processing**
- **Apache Kafka** serves as the message broker, handling real-time data streams.
- **Apache Flink** processes streaming data and sends it to **PostgreSQL (PSQL)**.
- **PostgreSQL** stores the cleaned and structured streaming data.

### **3. Batch Data Processing**
- **Apache Airflow** orchestrates batch workflows.
- **Python** extracts and pre-processes batch data before sending it to **Cassandra**.
- **Cassandra** stores raw transactional data.
- **Apache Spark** cleans and transforms data from Cassandra.
- The transformed data is then moved to **PostgreSQL** for further analysis.

### **4. Visualization & Reporting**
- Processed data in **PostgreSQL** is used to generate visual reports using **Power BI**.

### **5. Notifications**
- **Email Notifications** are sent through **Gmail** to alert users about pipeline status and data availability.

## Technologies Used
- **Python**: Data extraction and processing.
- **Apache Kafka**: Real-time data streaming.
- **Apache Flink**: Stream processing.
- **Apache Spark**: Batch data processing.
- **Apache Airflow**: Workflow orchestration.
- **Cassandra**: NoSQL database for scalable storage.
- **PostgreSQL (PSQL)**: Structured data storage.
- **Power BI**: Data visualization.
- **Docker**: Containerization and deployment.
- **Gmail SMTP**: Email notifications.

## Setup & Installation
### Prerequisites
- **Docker** installed on your system.
- **Apache Kafka, Flink, Spark, Cassandra, PostgreSQL, Airflow** setup using Docker.
- **Power BI** installed for visualization.


### Steps to Run the Pipeline
1. **Clone the repository**:
   ```bash
   git clone https://github.com/Saheed388/Transactions-end-to-end-project
   cd data-pipeline
   ```
2. **Start Services using Docker**:
   ```bash
   docker-compose up -d
   ```
3. **Run the Python producer to push data to Kafka**:
   ```bash
   python producer.py
   ```
4. **Trigger Apache Airflow DAG for batch processing**:
   ```bash
   airflow dags trigger batch_pipeline
   ```
5. **Monitor Spark Jobs**:
   ```bash
   spark-submit --master local batch_processing.py
   ```
6. **Access PostgreSQL Database**:
   ```bash
   psql -h localhost -U postgres -d transactions_db
   ```
7. **Connect Power BI to PostgreSQL for visualization.**

## Future Enhancements
- Implement **Apache Iceberg** for better table management.
- Introduce **Grafana** for real-time monitoring.
- Automate alerts with **Slack notifications**.
- Use **dbt (Data Build Tool)** for SQL-based transformations.

## Contributors
- **Your Name** - Lead Developer
- **Other Contributors** - Data Engineers, Analysts, DevOps

## License
This project is licensed under the **MIT License**.

## Contact
For queries, reach out via Saheedjimoh338@gmail.com or open an issue on the repository.

---

This README provides a clear and structured explanation of your project for anyone visiting the repository. Let me know if you'd like any modifications!

