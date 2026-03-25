# CM2606-DATA_Engineering_CW

## 📌 Project Overview
This project implements a cloud-based ETL pipeline for retail sales analytics. The pipeline extracts data from Amazon S3, processes it using a Python ETL pipeline on an EC2 instance, and loads the transformed data into a PostgreSQL data warehouse structured using a star schema.

---

## 📁 Project Structure

### 🔹 src/
Contains all ETL pipeline scripts:

- **main.py**  
  Orchestrates the pipeline execution (ingestion → transformation → validation → loading)

- **ingest.py**  
  Reads raw data from Amazon S3

- **transform.py**  
  Performs data cleaning and transformation (duplicates removal, type conversion, formatting)

- **load.py**  
  Loads processed data into PostgreSQL and uploads to S3 processed layer

- **logger.py**  
  Handles logging of pipeline execution

---

### 🔹 data_lake/
- Stores raw and processed datasets (used for local testing)

---

### 🔹 Notebooks/
- Used for initial data exploration and profiling

---

### 🔹 requirements.txt
- Lists all required Python libraries (pandas, boto3, sqlalchemy, etc.)

---

### 🔹 pipeline.log
- Contains logs generated during pipeline execution

---

## ⚙️ Technologies Used
- Python
- Pandas
- Boto3 (AWS SDK)
- PostgreSQL
- Amazon S3 (Data Lake)
- Amazon EC2 (Compute)
- IAM Roles (Security)

---

## ▶️ How to Run the Pipeline

```bash
python src/main.py