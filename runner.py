import os
import importlib.util
import ccxt
import psycopg2
from datetime import datetime
import pandas as pd

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

print("Environment variables:")
print(f"DB_HOST: {os.getenv('DB_HOST')}")
print(f"DB_NAME: {os.getenv('DB_NAME')}")
print(f"DB_USER: {os.getenv('DB_USER')}")
print(f"DB_PASS: {os.getenv('DB_PASS')}")

# Создаём клиент биржи (Bybit через ccxt)
exchange = ccxt.okx() #binanceus, okx, bybit

# Подключение к Postgres
conn = psycopg2.connect(
    dbname=DB_NAME, 
    user=DB_USER, 
    password=DB_PASS, 
    host=DB_HOST, 
    port=5432
)
cur = conn.cursor()

def fetch_data(symbol="BTC/USDT", timeframe="1h", limit=100):
    """Получение исторических данных OHLCV с Binance"""
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    # Преобразуем в список словарей
    df = pd.DataFrame(
        ohlcv,
        columns=["timestamp", "Open", "High", "Low", "Close", "Volume"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df.set_index("timestamp", inplace=True)
    return df


# Папка со стратегиями
strategies_folder = "strategies"

def run_strategy(file):
    spec = importlib.util.spec_from_file_location("strategy", file)
    strategy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(strategy)

    # Загружаем данные
    data = fetch_data("BTC/USDT", "1h")

    # Стратегия возвращает DataFrame
    signal_df = strategy.trading_strategy(data)

    print('signal_df is')
    print(signal_df)

    if signal_df is not None and not signal_df.empty:
        last_row = signal_df.iloc[-1]   # берём последнюю строку

        # Проверяем наличие сигнала
        if "signal" in signal_df.columns and last_row["signal"] != 0:
            print('Сигнал присутствует')
            signal_dict = {
                "symbol": symbol,
                "timeframe": timeframe,
                "side": "buy" if last_row["signal"] == 1 else "sell",
                "volume": 10,
                "open_price": float(data["open"].iloc[-1]),
                "close_price": float(data["close"].iloc[-1]),
            }

            cur.execute(
                """
                INSERT INTO test.signals (strategy_name, symbol, timeframe, side, volume, open_price, close_price, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    os.path.basename(file),
                    signal_dict["symbol"],
                    signal_dict["timeframe"],
                    signal_dict["side"],
                    signal_dict["volume"],
                    signal_dict["open_price"],
                    signal_dict["close_price"],
                    datetime.utcnow()
                )
            )
            conn.commit()
            print(f"[INFO] Сигнал добавлен: {signal_dict}")
        else:
            print('Сигнал отсутствует')

# Запуск всех стратегий
for f in os.listdir(strategies_folder):
    if f.endswith(".py"):
        run_strategy(os.path.join(strategies_folder, f))

cur.close()
conn.close()
