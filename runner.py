from hist_data import fetch_data
from tg_notification import send_telegram_message

import os
import importlib.util
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Подключение к Postgres
conn = psycopg2.connect(
    dbname=DB_NAME, 
    user=DB_USER, 
    password=DB_PASS, 
    host=DB_HOST, 
    port=5432
)
cur = conn.cursor()

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}")

# Папка со стратегиями
strategies_folder = "strategies"

def run_strategy(file):
    spec = importlib.util.spec_from_file_location("strategy", file)
    strategy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(strategy)

    symbol = "BTC/USDT"
    timeframe = "1h"

    # Загружаем данные от биржи ToDO - переписать чтобы забирали данные из БД по любому таймфрейму
    data = fetch_data(symbol, timeframe)

    print('data is:')
    print(data)

    # Стратегия возвращает DataFrame с сигналами по стратегии
    signal_df = strategy.trading_strategy(data)

    print('signal_df is:')
    print(signal_df)

    if signal_df is not None and not signal_df.empty:
        # сохраняем весь датафрейм в отдельную таблицу
        strategy_name = os.path.splitext(os.path.basename(file))[0]
        table_name = f"signal_df_{strategy_name}"

        signal_df.to_sql(table_name, engine, if_exists="replace", index=True)
        print(f"DataFrame сохранён в таблицу {table_name}")
        
        # берём последнюю строку
        last_row = signal_df.iloc[-1]

        # Проверяем наличие сигнала
        if last_row["signal"] in ["1", 1, "-1", -1]:
            print('Сигнал присутствует')
            signal_dict = {
                "symbol": symbol,
                "timeframe": timeframe,
                "side": "buy" if last_row["signal"] in ["1", 1] else "sell" if last_row["signal"] in ["-1", -1] else None,
                "volume": 10,
                "open_price": float(last_row["Open"]),
                "close_price": float(last_row["Close"]),
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

            # отправляем уведомление в Telegram
            msg = (
                f"📢 Новый сигнал!\n"
                f"Стратегия: {os.path.basename(file)}\n"
                f"Инструмент: {signal_dict['symbol']}\n"
                f"Таймфрейм: {signal_dict['timeframe']}\n"
                f"Сторона: {signal_dict['side'].upper()}\n"
                f"Объём: {signal_dict['volume']}\n"
                f"Цена открытия: {signal_dict['open_price']}\n"
                f"Цена закрытия: {signal_dict['close_price']}\n"
                f"Время: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )
            send_telegram_message(tg_token = TELEGRAM_TOKEN
                                  ,tg_chat_id = TELEGRAM_CHAT_ID 
                                  ,message = msg)
        else:
            print('Сигнал отсутствует')
    else:
        print('Пустой результат от стратегии')

# Запуск всех стратегий
for f in os.listdir(strategies_folder):
    if f.endswith(".py"):
        run_strategy(os.path.join(strategies_folder, f))

cur.close()
conn.close()
