
import os
import importlib.util
import psycopg2
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import pytz
import pandas as pd

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
def fetch_market_data(symbol: str, timeframe: str) -> pd.DataFrame:
    query = text(f"""
        SELECT *
        FROM {TABLE_MD}
        ORDER BY timestamp ASC
    """)

    df = pd.read_sql(query, engine)

    if df.empty:
        raise RuntimeError("❌ Нет данных OHLCV в БД для стратегии!")

    df.set_index("timestamp", inplace=True)
    return df



def backtest_strategy(df: pd.DataFrame, stop_loss: float, take_profit: float) -> dict:
    df = df.copy()

    # --- Create position series (signal forward-fill) ---
    signals = df['signal'].values
    positions = np.zeros_like(signals)
    current_pos = 0

    for i in range(len(signals)):
        if signals[i] != 0:
            current_pos = signals[i]
        positions[i] = current_pos

    df['position'] = positions

    entry_price = 0
    entry_signal = 0
    in_position = False
    trades = []

    for i, row in df.iterrows():
        price = row['close']
        pos = row['position']

        # Open new position
        if not in_position and pos != 0:
            in_position = True
            entry_price = price
            entry_signal = pos
            continue

        if in_position:

            # Calculate PnL
            if entry_signal == 1:      # Long
                pnl = (price - entry_price) / entry_price * 100
            else:                      # Short
                pnl = (entry_price - price) / entry_price * 100

            # Stop loss / Take profit
            stop_hit = pnl <= -stop_loss
            tp_hit = pnl >= take_profit

            if stop_hit or tp_hit:
                trades.append({
                    'entry_price': entry_price,
                    'exit_price': price,
                    'signal': entry_signal,
                    'pnl_pct': pnl,
                    'stop_loss': stop_hit,
                    'take_profit': tp_hit
                })
                in_position = False
                continue

            # Signal change → exit + optional new entry
            if pos != 0 and pos != entry_signal:
                trades.append({
                    'entry_price': entry_price,
                    'exit_price': price,
                    'signal': entry_signal,
                    'pnl_pct': pnl,
                    'stop_loss': False,
                    'take_profit': False
                })
                in_position = False

                # Open new position
                entry_price = price
                entry_signal = pos
                in_position = True

    # Close last position at end
    if in_position:
        last_price = df.iloc[-1]['close']
        pnl = (last_price - entry_price) / entry_price * 100 if entry_signal == 1 else (entry_price - last_price) / entry_price * 100
        
        trades.append({
            'entry_price': entry_price,
            'exit_price': last_price,
            'signal': entry_signal,
            'pnl_pct': pnl,
            'stop_loss': False,
            'take_profit': False
        })

    # --- Metrics ---
    if not trades:
        return {
            'total_return': 0,
            'win_rate': 0,
            'total_trades': 0,
            'avg_trade': 0,
            'sharpe_ratio': 0
        }

    trades_df = pd.DataFrame(trades)
    total_return = trades_df['pnl_pct'].sum()
    win_rate = (trades_df['pnl_pct'] > 0).mean() * 100
    avg_trade = trades_df['pnl_pct'].mean()
    sharpe = avg_trade / trades_df['pnl_pct'].std() if trades_df['pnl_pct'].std() != 0 else 0

    return {
        'total_return': total_return,
        'win_rate': win_rate,
        'total_trades': len(trades_df),
        'avg_trade': avg_trade,
        'sharpe_ratio': sharpe,
        'trades': trades_df
    }


def run_strategy_tester(file):
    spec = importlib.util.spec_from_file_location("strategy", file)
    strategy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(strategy)
    
    # Загружаем данные от биржи ToDO - переписать чтобы забирали данные из БД по любому таймфрейму
    data = fetch_market_data(SYMBOL, TIMEFRAME)


    print(f'------------------{file}----------------------')
    print('data is:')
    print(data)
    
    # Стратегия возвращает DataFrame с сигналами по стратегии
    signal_df = strategy.trading_strategy(data)
    
    print('signal_df is:')
    print(signal_df)
    
    result = backtest_strategy(
            df=signal_df,
            stop_loss_pct=0.5,   # 0.5% стоп-лосс
            take_profit_pct=1.5, # 1.5% тейк-профит
            initial_balance=10000.0,
            trade_size=0.5       # 50% капитала на сделку
        )
    
    print('result')
    print(result)
    print('---------------------------------')








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
