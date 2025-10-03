import os
import importlib.util
import ccxt
import psycopg2
from datetime import datetime

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

print("Environment variables:")
print(f"DB_HOST: {os.getenv('DB_HOST')}")
print(f"DB_NAME: {os.getenv('DB_NAME')}")
print(f"DB_USER: {os.getenv('DB_USER')}")
print(f"DB_PASS: {os.getenv('DB_PASS')}")



# Создаём клиент биржи (Binance через ccxt)
exchange = ccxt.binance()

# Подключение к Postgres
conn = psycopg2.connect(
    dbname=DB_NAME, 
    user=DB_USER, 
    password=DB_PASS, 
    host=DB_HOST, 
    port=5432
)
cur = conn.cursor()

# Папка со стратегиями
strategies_folder = "strategies"

def run_strategy(file):
    spec = importlib.util.spec_from_file_location("strategy", file)
    strategy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(strategy)
    
    if hasattr(strategy, "trading_strategy"):
        signal = strategy.trading_strategy(exchange)
        if signal:
            cur.execute(
                """
                INSERT INTO test.signals (strategy_name, symbol, timeframe, side, volume, open_price, close_price, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    os.path.basename(file),
                    signal.get("symbol", "BTC/USDT"),
                    signal.get("timeframe", "1h"),
                    signal["side"],
                    signal["volume"],
                    signal["open_price"],
                    signal["close_price"],
                    datetime.utcnow()
                )
            )
            conn.commit()
            print(f"[INFO] Сигнал добавлен: {signal}")

# Запуск всех стратегий
for f in os.listdir(strategies_folder):
    if f.endswith(".py"):
        run_strategy(os.path.join(strategies_folder, f))

cur.close()
conn.close()
