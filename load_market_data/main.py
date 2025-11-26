from concurrent.futures import ThreadPoolExecutor, as_completed
from market_data_fetcher import MarketDataFetcher
from pg_client import PostgresClient
import os
import logging

# ------------ настройки логов ------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ------------ функция для одного символа ------------
def fetch_symbol_data(symbol, timeframe, fetcher):
    logging.info(f"Fetching data for {symbol}")
    try:
        df = fetcher.fetch_ohlcv(symbol, timeframe)
        logging.info(f"Fetched {len(df)} rows for {symbol}")
        return symbol, df, None
    except Exception as e:
        logging.error(f"Error fetching {symbol}: {e}")
        return symbol, None, e

# ------------ MAIN PROCESS ------------
def main():
    symbols = [
        "BTC/USDT",
        "ETH/USDT",
        "BNB/USDT",
        "SOL/USDT",
        "XRP/USDT"
    ]

    timeframe = "1h"

    # 1. Инициализация клиентов
    fetcher = MarketDataFetcher("binance")
    pg = PostgresClient(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        database=os.getenv("PG_DB"),
    )

    # 2. Параллельный сбор данных (до 5 потоков)
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_symbol_data, s, timeframe, fetcher): s for s in symbols}

        for future in as_completed(futures):
            symbol = futures[future]
            sym, df, error = future.result()
            if df is not None:
                results.append(df)

    # 3. Запись в БД
    for df in results:
        pg.save_market_data(df)

    logging.info("All data saved successfully.")


if name == "__main__":
    main()
