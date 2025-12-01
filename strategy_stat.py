
import os
import importlib.util
import psycopg2
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import pytz
import pandas as pd
import re
from tg_notification import send_telegram_message

# ============================================================
# 1. Конфигурация окружения
# ============================================================
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Папка со стратегиями
STRATEGIES_FOLDER = "strategies"
TABLE_MD = "test.btc_usd_t"   # таблица с рыночными данными

# ============================================================
# 2. Подключение к БД Postgres
# ============================================================
# engine = create_engine(
#     f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"
# )

# conn = psycopg2.connect(
#     dbname=DB_NAME,
#     user=DB_USER,
#     password=DB_PASS,
#     host=DB_HOST,
#     port=5432
# )
# conn.autocommit = True
# cur = conn.cursor()

# ============================================================
# 4. Получение последних данных OHLCV из БД
# ============================================================
def fetch_market_data(tbl:str) -> pd.DataFrame:
    query = text(f"""
        SELECT *
        FROM {tbl}
        ORDER BY timestamp ASC
    """)

    df = pd.read_sql(query, engine)

    if df.empty:
        raise RuntimeError("❌ Нет данных OHLCV в БД для стратегии!")

    df.set_index("timestamp", inplace=True)
    return df



def backtest_strategy(
    df: pd.DataFrame,
    stop_loss_pct: float,
    take_profit_pct: float,
    initial_balance: float = 10000.0,
    trade_size: float = 1.0
) -> dict:
    """
    Бэктест стратегии с возвратом метрик:
    - total_return
    - win_rate
    - total_trades
    - avg_trade
    - sharpe_ratio
    - trades_df
    """

    df = df.copy()

    # Список сделок
    trades = []

    current_position = 0          # -1, 0, 1
    entry_price = None
    stop_loss_price = None
    take_profit_price = None

    for i in range(1, len(df)):
        signal = df['signal'].iloc[i]
        o = df['open'].iloc[i]
        h = df['high'].iloc[i]
        l = df['low'].iloc[i]
        c = df['close'].iloc[i]

        # ---------- ЕСЛИ ЕСТЬ ОТКРЫТАЯ ПОЗИЦИЯ ----------
        if current_position != 0:

            # LONG
            if current_position == 1:
                # SL
                if l <= stop_loss_price:
                    exit_price = stop_loss_price
                    pnl = (exit_price - entry_price) / entry_price * 100 * trade_size

                    trades.append({
                        'entry_price': entry_price,
                        'exit_price': exit_price,
                        'signal': 1,
                        'pnl_pct': pnl,
                        'stop_loss': True,
                        'take_profit': False
                    })

                    current_position = 0
                    entry_price = None
                    continue  # к следующей свечке

                # TP
                elif h >= take_profit_price:
                    exit_price = take_profit_price
                    pnl = (exit_price - entry_price) / entry_price * 100 * trade_size

                    trades.append({
                        'entry_price': entry_price,
                        'exit_price': exit_price,
                        'signal': 1,
                        'pnl_pct': pnl,
                        'stop_loss': False,
                        'take_profit': True
                    })

                    current_position = 0
                    entry_price = None
                    continue

            # SHORT
            elif current_position == -1:
                # SL
                if h >= stop_loss_price:
                    exit_price = stop_loss_price
                    pnl = (entry_price - exit_price) / entry_price * 100 * trade_size

                    trades.append({
                        'entry_price': entry_price,
                        'exit_price': exit_price,
                        'signal': -1,
                        'pnl_pct': pnl,
                        'stop_loss': True,
                        'take_profit': False
                    })

                    current_position = 0
                    entry_price = None
                    continue

                # TP
                elif l <= take_profit_price:
                    exit_price = take_profit_price
                    pnl = (entry_price - exit_price) / entry_price * 100 * trade_size

                    trades.append({
                        'entry_price': entry_price,
                        'exit_price': exit_price,
                        'signal': -1,
                        'pnl_pct': pnl,
                        'stop_loss': False,
                        'take_profit': True
                    })

                    current_position = 0
                    entry_price = None
                    continue

            # Если приходит противоположный сигнал — закрываем и переворачиваемся
            if signal != 0 and signal != current_position:
                exit_price = c

                if current_position == 1:
                    pnl = (exit_price - entry_price) / entry_price * 100 * trade_size
                else:
                    pnl = (entry_price - exit_price) / entry_price * 100 * trade_size

                trades.append({
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'signal': current_position,
                    'pnl_pct': pnl,
                    'stop_loss': False,
                    'take_profit': False
                })

                # Открываем новую позицию
                current_position = signal
                entry_price = o

                if current_position == 1:
                    stop_loss_price = entry_price * (1 - stop_loss_pct / 100)
                    take_profit_price = entry_price * (1 + take_profit_pct / 100)
                else:
                    stop_loss_price = entry_price * (1 + stop_loss_pct / 100)
                    take_profit_price = entry_price * (1 - take_profit_pct / 100)

                continue

        # ---------- ОТКРЫТИЕ НОВОЙ ПОЗИЦИИ ----------
        if current_position == 0 and signal != 0:
            current_position = signal
            entry_price = o

            if signal == 1:
                stop_loss_price = entry_price * (1 - stop_loss_pct / 100)
                take_profit_price = entry_price * (1 + take_profit_pct / 100)
            else:
                stop_loss_price = entry_price * (1 + stop_loss_pct / 100)
                take_profit_price = entry_price * (1 - take_profit_pct / 100)

    # ---------- ЗАКРЫВАЕМ ПОСЛЕДНЮЮ ПОЗИЦИЮ ----------
    if current_position != 0:
        last_price = df.iloc[-1]['close']

        if current_position == 1:
            pnl = (last_price - entry_price) / entry_price * 100 * trade_size
        else:
            pnl = (entry_price - last_price) / entry_price * 100 * trade_size

        trades.append({
            'entry_price': entry_price,
            'exit_price': last_price,
            'signal': current_position,
            'pnl_pct': pnl,
            'stop_loss': False,
            'take_profit': False
        })

    # ---------- РАСЧЁТ МЕТРИК ----------
    if len(trades) == 0:
        return {
            'total_return': 0,
            'win_rate': 0,
            'total_trades': 0,
            'avg_trade': 0,
            'sharpe_ratio': 0,
            'trades': pd.DataFrame()
        }

    trades_df = pd.DataFrame(trades)

    total_return = trades_df['pnl_pct'].sum()
    win_rate = (trades_df['pnl_pct'] > 0).mean() * 100
    avg_trade = trades_df['pnl_pct'].mean()
    std = trades_df['pnl_pct'].std()
    sharpe = avg_trade / std if std != 0 else 0

    return {
        'total_return': total_return,
        'win_rate': win_rate,
        'total_trades': len(trades_df),
        'avg_trade': avg_trade,
        'sharpe_ratio': sharpe
        # 'trades': trades_df
    }


strategies = {
        "close_open_1pct": {"sl": 0.01,  "tp": 0.025},
        "close_open_engulfing": {"sl": 0.01,  "tp": 0.02},
        "macd": {"sl": 0.01,  "tp": 0.025},
    }


def run_strategy_tester(file):
    
    spec = importlib.util.spec_from_file_location("strategy", file)
    strategy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(strategy)

    # Загружаем данные от биржи ToDO - переписать чтобы забирали данные из БД по любому таймфрейму
    data = fetch_market_data(TABLE_MD)

    # Стратегия возвращает DataFrame с сигналами по стратегии
    signal_df = strategy.trading_strategy(data)

    print('signal_df', signal_df)
    
    strategy_nm = os.path.basename(file).replace(".py", "")
    
    result = backtest_strategy(
            df=signal_df,
            # stop_loss_pct=0.5,   # 0.5% стоп-лосс
            stop_loss_pct=strategies[strategy_nm]['sl'] * 100,   # 0.5% стоп-лосс
            # take_profit_pct=1.5, # 1.5% тейк-профит
            take_profit_pct=strategies[strategy_nm]['tp'] * 100, # 1.5% тейк-профит
            initial_balance=10000.0,
            trade_size=0.5       # 50% капитала на сделку
        )

    print(f'----------------{strategy_nm}-----------------')
    print('result')
    print(result)
    for key in result:
        print(f'{key}: {result[key]} ')
    print(f'----------------strategy {strategy} end-----------------')

    # start_date = datetime(2024, 1, 1)   # пример — подставьте свою дату начала
    # end_date = datetime.utcnow()
    # days = (end_date - start_date).days

    # msg = (
    #     "📊 *{strategy_nm}*\n\n"
    #     f"📅 *Дата начала:* `{start_date.strftime('%Y-%m-%d')}`\n"
    #     f"📅 *Дата конца:* `{end_date.strftime('%Y-%m-%d')}`\n"
    #     f"⏳ *Период теста:* `{days} дней`\n\n"
    #     f"💰 *Общая доходность:* `{result['total_return']:.2f}%`\n\n"
    #     f"🎯 *Win-rate:* `{result['win_rate']:.1f}%`\n"
    #     f"🔄 *Всего сделок:* `{result['total_trades']}`\n"
    #     f"📈 *Средняя прибыль на сделку:* `{result['avg_trade']:.3f}%`\n"
    #     f"⚖️ *Коэффициент Шарпа:* `{result['sharpe_ratio']:.3f}`\n\n"
    #     f"🕒 Отчёт сформирован: {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')}"
    # )





engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"
)

conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=5432
)
conn.autocommit = True
cur = conn.cursor()

# Запуск всех стратегий
for f in os.listdir(STRATEGIES_FOLDER):
    if f.endswith(".py"):
        run_strategy_tester(os.path.join(STRATEGIES_FOLDER, f))

cur.close()
conn.close()
