import os
import json
from pathlib import Path
from airflow.models import Connection, Variable
from airflow.utils.session import create_session


def configure_warehouse_connection() -> None:
    connection = Connection(
        conn_id="warehouse_postgres",
        conn_type="postgres",
        host="postgres",
        schema=os.environ["POSTGRES_DB"],
        login=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        port=5432,
    )

    with create_session() as session:
        existing = (session.query(Connection).filter(Connection.conn_id == "warehouse_postgres").one_or_none())

        if existing is None:
            session.add(connection)
        else:
            existing.conn_type = connection.conn_type
            existing.host = connection.host
            existing.schema = connection.schema
            existing.login = connection.login
            existing.password = connection.password
            existing.port = connection.port


def configure_variables() -> None:
    Variable.set("etl_input_path", "/opt/airflow/project/data/data.csv",)
    Variable.set("etl_batch_size", os.getenv("ETL_BATCH_SIZE", "5000"))
    Variable.set("etl_source_timezone", os.getenv("ETL_SOURCE_TIMEZONE", "UTC"),)


def configure_admin_password() -> None:
    username = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
    password = os.environ["AIRFLOW_ADMIN_PASSWORD"]

    password_file = Path(
        "/opt/airflow/config/simple_auth_manager_passwords.json"
    )

    password_file.parent.mkdir(parents=True, exist_ok=True)

    passwords = {}

    if password_file.exists():
        with password_file.open("r", encoding="utf-8") as file:
            passwords = json.load(file)

    passwords[username] = password

    with password_file.open("w", encoding="utf-8") as file:
        json.dump(passwords, file)

    print(f"Custom password set for {username}")



if __name__ == "__main__":
    configure_admin_password()
    configure_warehouse_connection()
    configure_variables()
    print("Airflow Connection and Variables configured")












    