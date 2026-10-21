
FROM apache/airflow:3.3.2-python3.12


RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    "dbt-core>=1.12.5,<2.0" \
    "dbt-databricks>=1.12.7,<2.0" \
    "apache-airflow-providers-databricks==7.22.0" \
    "apache-airflow-providers-standard==1.20.0"