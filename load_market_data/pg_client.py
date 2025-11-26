# import pandas as pd
# from sqlalchemy import create_engine

# class PostgresClient:
#     def __init__(self, host, port, user, password, database):
#         conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
#         self.engine = create_engine(conn_str)

#     def save_market_data(self, df: pd.DataFrame, table: str):
#         """Запись данных в PostgreSQL"""
#         df.to_sql(
#             table,
#             self.engine,
#             if_exists="append",
#             index=False,
#             schema="test"
#         )


import psycopg2
import psycopg2.extras
import pandas as pd
import logging


class PostgresClient:
    def __init__(self, host, port, user, password, database):
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=database
        )
        self.conn.autocommit = True

        self._ensure_schema()
        self._ensure_table()
        self._ensure_index()

    # --------------------------------------------------------
    # 1. Создание схемы
    # --------------------------------------------------------
    def _ensure_schema(self):
        query = "CREATE SCHEMA IF NOT EXISTS test;"
        with self.conn.cursor() as cur:
            cur.execute(query)
        logging.info("Schema test checked/created.")

    # --------------------------------------------------------
    # 2. Создание таблицы
    # --------------------------------------------------------
    def _ensure_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS test.btc_usd_t (
            timestamp   timestamptz NOT NULL,
            Open        numeric,
            High        numeric,
            Low         numeric,
            Close       numeric,
            Volume      numeric,
            symbol      text NOT NULL,
            timeframe   text NOT NULL
        );
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
        logging.info("Table test.market_data checked/created.")

    # --------------------------------------------------------
    # 3. Создание уникального индекса
    # --------------------------------------------------------
    def _ensure_index(self):
        query = """
        CREATE UNIQUE INDEX IF NOT EXISTS market_data_idx
            ON test.market_data (timestamp, symbol, timeframe);
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
        logging.info("Unique index checked/created.")

    # --------------------------------------------------------
    # 4. Сохранение данных UPSERT
    # --------------------------------------------------------
    def save_market_data(self, df: pd.DataFrame, table: str = "btc_usd_t"):
        """Вставляет только новые строки (UPSERT DO NOTHING)."""

        if df.empty:
            logging.info("DataFrame is empty — nothing to insert.")
            return

        columns = [
            "timestamp", "Open", "High", "Low", "Close", "Volume", "symbol", "timeframe"
        ]

        insert_query = f"""
        INSERT INTO test.{table} ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (timestamp, symbol, timeframe) DO NOTHING;
        """

        values = [
            (
                row["timestamp"],
                row["Open"],
                row["High"],
                row["Low"],
                row["Close"],
                row["Volume"],
                row["symbol"],
                row["timeframe"],
            )
            for _, row in df[columns].iterrows()
        ]

        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                insert_query,
                values,
                page_size=500
            )

        logging.info(f"Inserted new rows: {len(values)} (duplicates skipped automatically).")
