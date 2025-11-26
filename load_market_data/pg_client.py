import pandas as pd
from sqlalchemy import create_engine

class PostgresClient:
    def __init__(self, host, port, user, password, database):
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(conn_str)

    def save_market_data(self, df: pd.DataFrame, table: str):
        """Запись данных в PostgreSQL"""
        df.to_sql(
            table,
            self.engine,
            if_exists="append",
            index=False,
            schema="test"
        )
