import pandas as pd
from sqlalchemy import create_engine

class PostgresClient:
    def __init__(self, host, port, user, password, database, tbl_nm):
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(conn_str, pool_pre_ping=True)
        self.tbl_nm = tbl_nm

    def save_market_data(self, df: pd.DataFrame):
        df.to_sql(
            name=self.tbl_nm,
            schema="test",
            self.engine,
            if_exists="append",
            index=False
        )
