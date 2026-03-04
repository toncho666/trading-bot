
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
    initial_balance: float = 10_000.0,
    trade_size: float = 1.0,
    commission_pct: float = 0.1,          # 0.1 % за вход и 0.1 % за выход
) -> dict:
    """
    Бэктест простой стратегии, где в столбце ``df['signal']`` записан
    торговый сигнал:  1 – открыть LONG,  -1 – открыть SHORT, 0 – ничего.

    Параметры
    ----------
    df : pd.DataFrame
        Должен содержать столбцы: ``open, high, low, close, signal``.
    stop_loss_pct : float
        Размер стоп‑лосса в процентах (например, 2 → 2 %).
    take_profit_pct : float
        Размер тейк‑приба в процентах.
    initial_balance : float, optional
        Начальный капитал (полезно, если захотите добавить маржинальное
        управление – сейчас не используется, но оставлен для совместимости).
    trade_size : float, optional
        Кол‑во единиц инструмента в каждой сделке (по умолчанию 1).
    commission_pct : float, optional
        Комиссия за каждую сторону сделки, в процентах от цены сделки
        (по умолчанию 0.1 % = 0.001).

    Returns
    -------
    dict
        {
            'total_return' : float   # суммарный PnL % за весь период,
            'win_rate'     : float   # % прибыльных сделок,
            'total_trades' : int,
            'avg_trade'    : float   # средний PnL %,
            'sharpe_ratio' : float,
            'trades'       : pd.DataFrame   # детализация каждой сделки
        }
    """
    # ------------------------------------------------------------------
    #  Внутренняя функция – вычисление чистого PnL в процентах
    # ------------------------------------------------------------------
    def calc_pnl(entry_price: float, exit_price: float, side: int) -> float:
        """
        side = 1  → LONG
        side = -1 → SHORT
        """
        # 1) «Грубый» профит (без комиссии)
        if side == 1:      # LONG
            gross = (exit_price - entry_price)
        else:              # SHORT
            gross = (entry_price - exit_price)

        # 2) Комиссия за вход + выход
        commission = commission_pct / 100.0
        commission_cost = commission * (entry_price + exit_price)

        # 3) Чистый профит в денежном выражении (size учитывается)
        net_profit = (gross - commission_cost) * trade_size

        # 4) PnL в процентах от стоимости входа
        pnl_pct = net_profit / (entry_price * trade_size) * 100.0
        return pnl_pct

    # ------------------------------------------------------------------
    #  Подготовка
    # ------------------------------------------------------------------
    df = df.copy().reset_index(drop=True)
    trades = []                     # список словарей с данными по каждой сделке
    current_position = 0           # -1 = SHORT, 0 = без позиции, 1 = LONG
    entry_price = None
    stop_loss_price = None
    take_profit_price = None

    # ------------------------------------------------------------------
    #  Основной цикл по барам
    # ------------------------------------------------------------------
    for i in range(1, len(df)):
        signal = df.at[i, "signal"]
        o = df.at[i, "open"]
        h = df.at[i, "high"]
        l = df.at[i, "low"]
        c = df.at[i, "close"]

        # --------------------------------------------------------------
        #  Позиция уже открыта
        # --------------------------------------------------------------
        if current_position != 0:
            # ---------- LONG ----------
            if current_position == 1:
                # Стоп‑лосс
                if l <= stop_loss_price:
                    exit_price = stop_loss_price
                    pnl = calc_pnl(entry_price, exit_price, 1)
                    trades.append({
                        "entry_price": entry_price,
                        "exit_price":  exit_price,
                        "signal":      1,
                        "pnl_pct":     pnl,
                        "stop_loss":   True,
                        "take_profit": False,
                    })
                    current_position = 0
                    entry_price = None
                    continue

                # Тейк‑профит
                if h >= take_profit_price:
                    exit_price = take_profit_price
                    pnl = calc_pnl(entry_price, exit_price, 1)
                    trades.append({
                        "entry_price": entry_price,
                        "exit_price":  exit_price,
                        "signal":      1,
                        "pnl_pct":     pnl,
                        "stop_loss":   False,
                        "take_profit": True,
                    })
                    current_position = 0
                    entry_price = None
                    continue

            # ---------- SHORT ----------
            else:  # current_position == -1
                # Стоп‑лосс
                if h >= stop_loss_price:
                    exit_price = stop_loss_price
                    pnl = calc_pnl(entry_price, exit_price, -1)
                    trades.append({
                        "entry_price": entry_price,
                        "exit_price":  exit_price,
                        "signal":     -1,
                        "pnl_pct":    pnl,
                        "stop_loss":   True,
                        "take_profit": False,
                    })
                    current_position = 0
                    entry_price = None
                    continue

                # Тейк‑профит
                if l <= take_profit_price:
                    exit_price = take_profit_price
                    pnl = calc_pnl(entry_price, exit_price, -1)
                    trades.append({
                        "entry_price": entry_price,
                        "exit_price":  exit_price,
                        "signal":     -1,
                        "pnl_pct":    pnl,
                        "stop_loss":   False,
                        "take_profit": True,
                    })
                    current_position = 0
                    entry_price = None
                    continue

            # ---------------------------------------------------------
            #  Противоположный сигнал — закрываем и сразу открываем новую
            # ---------------------------------------------------------
            if signal != 0 and signal != current_position:
                # закрываем текущую по цене закрытия текущего бара
                exit_price = c
                pnl = calc_pnl(entry_price, exit_price, current_position)
                trades.append({
                    "entry_price": entry_price,
                    "exit_price":  exit_price,
                    "signal":      current_position,
                    "pnl_pct":     pnl,
                    "stop_loss":   False,
                    "take_profit": False,
                })

                # открываем позицию в направлении нового сигнала
                current_position = signal
                entry_price = o
                if current_position == 1:          # LONG
                    stop_loss_price   = entry_price * (1 - stop_loss_pct / 100.0)
                    take_profit_price = entry_price * (1 + take_profit_pct / 100.0)
                else:                               # SHORT
                    stop_loss_price   = entry_price * (1 + stop_loss_pct / 100.0)
                    take_profit_price = entry_price * (1 - take_profit_pct / 100.0)
                continue

        # --------------------------------------------------------------
        #  Нет открытой позиции – открываем, если сигнал != 0
        # --------------------------------------------------------------
        if current_position == 0 and signal != 0:
            current_position = signal
            entry_price = o
            if signal == 1:                     # LONG
                stop_loss_price   = entry_price * (1 - stop_loss_pct / 100.0)
                take_profit_price = entry_price * (1 + take_profit_pct / 100.0)
            else:                               # SHORT
                stop_loss_price   = entry_price * (1 + stop_loss_pct / 100.0)
                take_profit_price = entry_price * (1 - take_profit_pct / 100.0)
            # комиссия за вход уже учитывается в calc_pnl() при закрытии

    # ------------------------------------------------------------------
    #  Закрываем открывшуюся позицию в конце тестового периода
    # ------------------------------------------------------------------
    if current_position != 0:
        last_price = df.iloc[-1]["close"]
        pnl = calc_pnl(entry_price, last_price, current_position)
        trades.append({
            "entry_price": entry_price,
            "exit_price":  last_price,
            "signal":      current_position,
            "pnl_pct":     pnl,
            "stop_loss":   False,
            "take_profit": False,
        })

    # ------------------------------------------------------------------
    #  Метрики
    # ------------------------------------------------------------------
    if not trades:
        # никаких сделок – возвращаем нулевые показатели
        return {
            "total_return": 0.0,
            "win_rate":     0.0,
            "total_trades": 0,
            "avg_trade":    0.0,
            "sharpe_ratio": 0.0,
            "trades":       pd.DataFrame(),
        }

    trades_df = pd.DataFrame(trades)

    total_return = trades_df["pnl_pct"].sum()
    win_rate = (trades_df["pnl_pct"] > 0).mean() * 100.0
    avg_trade = trades_df["pnl_pct"].mean()
    std = trades_df["pnl_pct"].std(ddof=0)          # population std, можно менять
    sharpe = avg_trade / std if std != 0 else 0.0

    return {
        "total_return": total_return,
        "win_rate":     win_rate,
        "total_trades": len(trades_df),
        "avg_trade":    avg_trade,
        "sharpe_ratio": sharpe,
        # "trades":       trades_df,
    }


strategies = {
        "close_open_1pct": {"sl": 0.006,  "tp": 0.035},
        "close_open_engulfing": {"sl": 0.011,  "tp": 0.035},
        "macd_hist": {"sl": 0.008,  "tp": 0.035},
        "candles": {"sl": 0.008,  "tp": 0.04},
        "fractal": {"sl": 0.068,  "tp": 0.05},
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
    print('min index', signal_df.index.min())
    print('max index', signal_df.index.max())
    
    strategy_nm = os.path.basename(file).replace(".py", "")
    
    result = backtest_strategy(
            df=signal_df,
            stop_loss_pct=strategies[strategy_nm]['sl'] * 100,   # 0.5% стоп-лосс
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

    start_date = signal_df.index.min()
    end_date = signal_df.index.max()
    days = (end_date - start_date).days

    msg = (
        f"📊 *{strategy_nm}*\n\n"
        f"📅 *Дата начала:* `{start_date.strftime('%Y-%m-%d')}`\n"
        f"📅 *Дата конца:* `{end_date.strftime('%Y-%m-%d')}`\n"
        f"⏳ *Период теста:* `{days} дней`\n\n"
        f"💰 *Общая доходность:* `{result['total_return']:.2f}%`\n\n"
        f"🎯 *Win-rate:* `{result['win_rate']:.1f}%`\n"
        f"🔄 *Всего сделок:* `{result['total_trades']}`\n"
        f"📈 *Средняя прибыль на сделку:* `{result['avg_trade']:.3f}%`\n"
        f"⚖️ *Коэффициент Шарпа:* `{result['sharpe_ratio']:.3f}`\n\n"
        f"🕒 Отчёт сформирован: {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )

    send_telegram_message(tg_token = TELEGRAM_TOKEN
                         ,tg_chat_id = TELEGRAM_CHAT_ID 
                         ,message = msg
                         ,parse_mode="Markdown")

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
