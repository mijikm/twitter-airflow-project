# Twitter data pipeline using Airflow 

### Purpose
The purpose of this project is to build a data pipeline using Airflow and Python. This can be divided into three steps. First, Twitter data is extracted using Twitter API and transformed with Python. Second, the code is deployed on Airflow/Amazon EC2. Third, triggering a newly created DAG runs the code and saves the final result, a csv file, on Amazon S3.

### Tools
- Airflow
- Python
- Amazon EC2
- Amazon S3

### Version
23 Nov 2022

### Code
- `twitter_etl`: this contains the `run_twitter_etl` function which creates connection between the code and Twitter API and extracts data from it. The file also contains some steps and commands used to launch the airflow server, deploy the code, and triggers DAG.
- `twitter_dag`: this creates DAG and triggers it using PythonOperator.

### Data sources
- Twitter

