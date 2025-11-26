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
    print('df', df.head())
    print('_________________df.info()_________________')
    print('df.info()', df.info())
    
    # 2. Подключение к БД
    client = PostgresClient(
        host = os.getenv("DB_HOST"),
        port = '5432',
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASS"),
        database = os.getenv("DB_NAME")
    )

    # 3. Запись
    client.save_market_data(df, "btc_usd_t")


if __name__ == "__main__":
    main()
