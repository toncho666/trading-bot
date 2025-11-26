import pandas as pd
from sqlalchemy import create_engine

class PostgresClient:
    def __init__(self, host, port, user, password, database):
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(conn_str, pool_pre_ping=True)

    def save_market_data(self, df: pd.DataFrame):
        df.to_sql(
            "market_data",
            self.engine,
            if_exists="append",
            index=False,
            schema="test"
        )
