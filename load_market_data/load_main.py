from market_data_fetcher import MarketDataFetcher
from pg_client import PostgresClient
import os
import logging

def main():
    symbol = "BTC/USDT"
    timeframe = "1h"

    # 1. Получение данных
    fetcher = MarketDataFetcher("binance")
    df = fetcher.fetch_ohlcv(symbol, timeframe)

    print('_________________df_________________')
    print('df', df)
    print('_________________df.info()_________________')
    print('df.info()', df.info())
    
    # Проверка на пустой DataFrame
    if df.empty:
        logging.error("DataFrame is empty. Exiting.")
        return
    # 2. Подключение к БД
    try:
        client = PostgresClient(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASS", ""),
            database=os.getenv("DB_NAME", "postgres")
        )
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        return

    # 3. Запись
    try:
        client.save_market_data(df, "btc_usd_t")
        logging.info("Data saved successfully")
    except Exception as e:
        logging.error(f"Failed to save data: {e}")
        raise

if __name__ == "__main__":
    main()
