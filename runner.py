from hist_data import fetch_data
from tg_notification import send_telegram_message

import os
import importlib.util
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pytz
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

        signal_df.to_sql(name=table_name
                        ,schema='test'
                        ,con=engine
                        ,if_exists="replace"
                        ,index=True)
        print(f"DataFrame сохранён в таблицу {table_name}")
        
        # берём последнюю строку
        # Текущее время в Московском часовом поясе
        moscow_tz = pytz.timezone('Europe/Moscow')
        # current_time = datetime.now(moscow_tz)
        current_time = datetime.now()

        # Время последнего закрытого часа (предыдущий час)
        last_closed_hour = current_time.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

        print(f"Текущее время: {current_time}")
        print(f"Последний закрытый час: {last_closed_hour}")
        
        # Ищем запись за последний закрытый час
        last_closed_row = signal_df[signal_df.index == last_closed_hour].iloc[-1]
        print('last_closed_row')
        print(last_closed_row)

        # last_row = signal_df.iloc[-1]

        # Проверяем наличие сигнала
        if last_closed_row["signal"] in ["1", 1, "-1", -1]:
            print('Сигнал присутствует')
            signal_dict = {
                "symbol": symbol,
                "timestamp": last_closed_row["timestamp"],
                "timeframe": timeframe,
                "side": "buy" if last_closed_row["signal"] in ["1", 1] else "sell" if last_closed_row["signal"] in ["-1", -1] else None,
                "volume": 10,
                "open_price": float(last_closed_row["Open"]),
                "close_price": float(last_closed_row["Close"]),
            }

            cur.execute(
                """
                INSERT INTO test.signals (strategy_name, symbol, timeframe, side, volume, open_price, close_price, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    os.path.basename(file),
                    signal_dict["symbol"],
                    signal_dict["timestamp"],
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

            # формируем уведомление с визуальными маркерами
            side_emoji = "🟢 BUY 📈" if signal_dict["side"].lower() == "buy" else "🔴 SELL 📉"
            strategy_name = os.path.basename(file).replace(".py", "")

            # отправляем уведомление в Telegram
            msg = (
                f"🚀 *НОВЫЙ СИГНАЛ!*\n\n"
                f"🎯 *Стратегия:* `{strategy_name}`\n"
                f"💹 *Инструмент:* {signal_dict['symbol']}\n"
                f"💹 *Дата и время свечи:* {signal_dict['timestamp']}\n"
                f"⏱ *Таймфрейм:* {signal_dict['timeframe']}\n\n"
                f"{side_emoji}\n"
                f"📦 *Объём:* {signal_dict['volume']}\n"
                f"💰 *Цена открытия:* {signal_dict['open_price']}\n"
                f"💸 *Цена закрытия:* {signal_dict['close_price']}\n\n"
                f"🕒 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )

            send_telegram_message(tg_token = TELEGRAM_TOKEN
                                 ,tg_chat_id = TELEGRAM_CHAT_ID 
                                 ,message = msg
                                 ,parse_mode="Markdown")
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
