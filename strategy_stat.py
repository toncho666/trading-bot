
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
    trade_size: float = 1.0,
    commission_pct: float = 0.1,
    slippage_pct: float = 0.005,
):
    """
    Исправленная версия бэктеста с корректной обработкой стопов и сигналов.
    """
    
    def calc_pnl_fixed(entry_price: float, exit_price: float, side: int) -> float:
        """Расчет PnL с учетом объема, комиссий и проскальзывания."""
        # Проскальзывание при входе и выходе
        if side == 1:  # LONG
            entry_with_slippage = entry_price * (1 + slippage_pct / 100.0)
            exit_with_slippage = exit_price * (1 - slippage_pct / 100.0)
        else:  # SHORT
            entry_with_slippage = entry_price * (1 - slippage_pct / 100.0)
            exit_with_slippage = exit_price * (1 + slippage_pct / 100.0)
        
        # Комиссия (в процентах от оборота)
        commission_rate = commission_pct / 100.0
        commission_entry = commission_rate * entry_with_slippage * trade_size
        commission_exit = commission_rate * exit_with_slippage * trade_size
        total_commission = commission_entry + commission_exit
        
        # Валовая прибыль
        if side == 1:  # LONG
            gross_profit = (exit_with_slippage - entry_with_slippage) * trade_size
        else:  # SHORT
            gross_profit = (entry_with_slippage - exit_with_slippage) * trade_size
        
        # Чистая прибыль
        net_profit = gross_profit - total_commission
        
        # PnL в процентах
        entry_cost = entry_with_slippage * trade_size
        pnl_pct = (net_profit / entry_cost) * 100.0 if entry_cost != 0 else 0.0
        
        return pnl_pct
    
    # Подготовка данных
    df = df.copy().reset_index(drop=True)
    
    # Добавляем колонки для отслеживания
    df['position'] = 0
    df['entry_price'] = np.nan
    df['stop_price'] = np.nan
    df['take_profit_price'] = np.nan
    
    trades = []
    current_position = 0
    entry_idx = None
    entry_price_val = None
    stop_price = None
    tp_price = None
    
    # Основной цикл
    for i in range(len(df)):
        # Данные текущего бара
        o = df.at[i, "open"]
        h = df.at[i, "high"]
        l = df.at[i, "low"]
        c = df.at[i, "close"]
        
        # Сигнал с предыдущего бара (исполняем на открытии текущего)
        # Берем сигнал с предыдущей строки, если есть
        if i > 0:
            signal = df.at[i-1, "signal"]
        else:
            signal = 0
        
        # ========== 1. ПРОВЕРКА ЗАКРЫТИЯ ПО СТОПАМ ==========
        if current_position != 0:
            exit_reason = None
            exit_price = None
            
            if current_position == 1:  # LONG позиция
                # Стоп-лосс сработал
                if l <= stop_price:
                    exit_reason = "stop_loss"
                    # Исполняем по стопу (или хуже - max, так как стоп ниже entry)
                    exit_price = max(stop_price, o)
                
                # Тейк-профит сработал
                elif h >= tp_price:
                    exit_reason = "take_profit"
                    # Исполняем по тейку (или лучше - max, так как тейк выше entry)
                    exit_price = max(tp_price, o)
            
            else:  # SHORT позиция
                # Стоп-лосс сработал
                if h >= stop_price:
                    exit_reason = "stop_loss"
                    # Исполняем по стопу (или хуже - min, так как стоп выше entry)
                    exit_price = min(stop_price, o)
                
                # Тейк-профит сработал
                elif l <= tp_price:
                    exit_reason = "take_profit"
                    # Исполняем по тейку (или лучше - min, так как тейк ниже entry)
                    exit_price = min(tp_price, o)
            
            # Закрываем позицию если сработал стоп/тейк
            if exit_reason:
                pnl = calc_pnl_fixed(entry_price_val, exit_price, current_position)
                trades.append({
                    "entry_idx": entry_idx,
                    "exit_idx": i,
                    "entry_price": entry_price_val,
                    "exit_price": exit_price,
                    "side": current_position,
                    "pnl_pct": pnl,
                    "reason": exit_reason,
                    "duration_bars": i - entry_idx,
                })
                
                # Сбрасываем состояние
                current_position = 0
                entry_idx = None
                entry_price_val = None
                stop_price = None
                tp_price = None
        
        # ========== 2. ЗАКРЫТИЕ ПО СИГНАЛУ ==========
        # Проверяем, нужно ли закрыть текущую позицию по противоположному сигналу
        if current_position != 0 and signal != 0 and signal != current_position:
            exit_price = o  # закрываем по open текущего бара
            
            pnl = calc_pnl_fixed(entry_price_val, exit_price, current_position)
            trades.append({
                "entry_idx": entry_idx,
                "exit_idx": i,
                "entry_price": entry_price_val,
                "exit_price": exit_price,
                "side": current_position,
                "pnl_pct": pnl,
                "reason": "signal_reversal",
                "duration_bars": i - entry_idx,
            })
            
            # Сбрасываем
            current_position = 0
            entry_idx = None
            entry_price_val = None
            stop_price = None
            tp_price = None
        
        # ========== 3. ОТКРЫТИЕ НОВОЙ ПОЗИЦИИ ==========
        # Открываем только если нет позиции И есть сигнал
        if current_position == 0 and signal != 0:
            current_position = signal
            entry_idx = i
            entry_price_val = o  # открываем по open текущего бара
            
            # Устанавливаем стопы и тейки
            if signal == 1:  # LONG
                stop_price = entry_price_val * (1 - stop_loss_pct / 100.0)
                tp_price = entry_price_val * (1 + take_profit_pct / 100.0)
            else:  # SHORT
                stop_price = entry_price_val * (1 + stop_loss_pct / 100.0)
                tp_price = entry_price_val * (1 - take_profit_pct / 100.0)
        
        # Записываем состояние
        df.at[i, 'position'] = current_position
        if current_position != 0:
            df.at[i, 'entry_price'] = entry_price_val
            df.at[i, 'stop_price'] = stop_price
            df.at[i, 'take_profit_price'] = tp_price
    
    # ========== 4. ЗАКРЫТИЕ В КОНЦЕ ПЕРИОДА ==========
    if current_position != 0:
        last_price = df.iloc[-1]["close"]
        pnl = calc_pnl_fixed(entry_price_val, last_price, current_position)
        trades.append({
            "entry_idx": entry_idx,
            "exit_idx": len(df) - 1,
            "entry_price": entry_price_val,
            "exit_price": last_price,
            "side": current_position,
            "pnl_pct": pnl,
            "reason": "end_of_data",
            "duration_bars": len(df) - 1 - entry_idx,
        })
    
    # ========== РАСЧЕТ МЕТРИК ==========
    if not trades:
        return {
            "total_return": 0.0,
            "win_rate": 0.0,
            "total_trades": 0,
            "avg_trade": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
            "profit_factor": 0.0,
            "trades_df": pd.DataFrame(),
            "equity_curve": pd.Series([initial_balance]),
        }
    
    trades_df = pd.DataFrame(trades)
    
    # 1. Общая доходность
    total_return = trades_df["pnl_pct"].sum()
    
    # 2. Win rate
    win_rate = (trades_df["pnl_pct"] > 0).mean() * 100.0
    
    # 3. Средняя сделка
    avg_trade = trades_df["pnl_pct"].mean()
    
    # 4. Profit Factor
    gross_profits = trades_df[trades_df["pnl_pct"] > 0]["pnl_pct"].sum()
    gross_losses = abs(trades_df[trades_df["pnl_pct"] < 0]["pnl_pct"].sum())
    profit_factor = gross_profits / gross_losses if gross_losses != 0 else np.inf
    
    # 5. Кривая капитала и просадка
    equity = initial_balance
    equity_curve = [equity]
    
    for pnl in trades_df["pnl_pct"]:
        equity *= (1 + pnl / 100.0)
        equity_curve.append(equity)
    
    equity_series = pd.Series(equity_curve)
    rolling_max = equity_series.expanding().max()
    drawdowns = (equity_series - rolling_max) / rolling_max * 100
    max_drawdown = drawdowns.min()
    
    # 6. Sharpe Ratio (упрощенный)
    if len(trades_df) > 1:
        # Предполагаем, что сделки распределены равномерно
        returns_mean = trades_df["pnl_pct"].mean()
        returns_std = trades_df["pnl_pct"].std()
        sharpe = (returns_mean / returns_std) * np.sqrt(252) if returns_std != 0 else 0
    else:
        sharpe = 0.0
    
    return {
        "total_return": round(total_return, 2),
        "win_rate": round(win_rate, 1),
        "total_trades": len(trades_df),
        "avg_trade": round(avg_trade, 2),
        "sharpe_ratio": round(sharpe, 2),
        "max_drawdown": round(max_drawdown, 2),
        "profit_factor": round(profit_factor, 2) if profit_factor != np.inf else float('inf'),
        # "trades_df": trades_df,
        # "equity_curve": equity_series,
    }


strategies = {
        "close_open_1pct": {"sl": 0.006,  "tp": 0.035},
        "close_open_engulfing": {"sl": 0.011,  "tp": 0.035},
        "macd_hist": {"sl": 0.008,  "tp": 0.035},
        "candles": {"sl": 0.008,  "tp": 0.04},
        "fractal": {"sl": 0.004,  "tp": 0.05},
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
