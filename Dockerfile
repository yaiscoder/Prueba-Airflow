FROM apache/airflow:3.1.5-python3.12

USER airflow

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.5/constraints-3.12.txt" \
    pandas \
    "psycopg[binary]" \
    python-dotenv \
    apache-airflow-providers-fab \
    apache-airflow-providers-postgres
