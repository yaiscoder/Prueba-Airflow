from sqlalchemy import text

from src.datamart.config.database import engine


def test_database_connection() -> None:
    with engine.connect() as connection:
        result = connection.execute(
            text("SELECT current_database(), current_user")
        )
        database, user = result.one()

        assert database == "datamart"
        assert user == "datamart_user"
