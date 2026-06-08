import pandas as pd
import numpy as np
from datetime import datetime, date
from sqlalchemy import text

def aggregate_daily_stats(trades_df: pd.DataFrame, strategy_name: str, initial_balance: float = 10000.0) -> pd.DataFrame:
    """
    Агрегирует статистику по дням из списка сделок.
    Возвращает DataFrame для вставки в test.strategy_name_on_date
    """
    if trades_df.empty:
        return pd.DataFrame()
    
    # Извлекаем дату из timestamp сделки
    trades_df['trade_date'] = pd.to_datetime(trades_df['dt']).dt.date
    
    # Группируем по дням
    daily_stats = []
    
    for trade_date, group in trades_df.groupby('trade_date'):
        # Базовые метрики дня
        trade_count = len(group)
        profitable = group[group['pnl_pct'] > 0]
        losing = group[group['pnl_pct'] < 0]
        
        win_rate = (len(profitable) / trade_count * 100) if trade_count > 0 else 0
        
        # Медианные значения
        avg_profitable = profitable['pnl_pct'].median() if not profitable.empty else 0
        avg_losing = losing['pnl_pct'].median() if not losing.empty else 0
        
        # Доходность дня (%)
        daily_return_pct = group['pnl_pct'].sum()
        
        # Профит в деньгах
        total_profit_today = initial_balance * (daily_return_pct / 100)
        
        # Годовая доходность (runrate): (1 + daily_return_pct/100)^252 - 1
        if daily_return_pct > -100:
            annual_profitability = ((1 + daily_return_pct / 100) ** 252 - 1) * 100
        else:
            annual_profitability = -100
        
        # Sharpe Ratio (дневной, аннуализированный)
        if len(group) >= 2:
            daily_sharpe = (group['pnl_pct'].mean() / group['pnl_pct'].std()) * np.sqrt(252)
        else:
            daily_sharpe = 0
        
        # Максимальная просадка за день (по внутридневным точкам)
        # Для упрощения используем просадку по сделкам дня
        if not group.empty:
            group = group.sort_values('exit_idx')
            equity = initial_balance
            peak = initial_balance
            max_drawdown = 0
            for _, trade in group.iterrows():
                equity *= (1 + trade['pnl_pct'] / 100)
                peak = max(peak, equity)
                dd = (equity - peak) / peak * 100
                max_drawdown = min(max_drawdown, dd)
        else:
            max_drawdown = 0
        
        daily_stats.append({
            'strategy_name': strategy_name,
            'date': trade_date,
            'total_profit_today': round(total_profit_today, 2),
            'trade_count': trade_count,
            'win_rate': round(win_rate, 1),
            'sharpe_ratio': round(daily_sharpe, 2),
            'max_drawdown': round(max_drawdown, 2),
            'avg_profitable_trade': round(avg_profitable, 2),
            'avg_losing_trade': round(avg_losing, 2),
            'profitability_dynamic': round(daily_return_pct, 2),
            'annual_profitability': round(annual_profitability, 1),
            '_loaded_at': datetime.now()
        })
    
    return pd.DataFrame(daily_stats)


def save_daily_stats_to_postgres(stats_df: pd.DataFrame, schema: str = 'test', table: str = 'strategy_name_on_date'):
    """
    Сохраняет дневную статистику в PostgreSQL.
    Обновляет существующие записи (upsert).
    """
    if stats_df.empty:
        print("⚠️ Нет данных для сохранения")
        return
    
    from sqlalchemy import create_engine, text
    
    # Создаем временную таблицу
    stats_df.to_sql('temp_daily_stats', engine, if_exists='replace', index=False)
    
    # Upsert: обновляем существующие, вставляем новые
    with engine.connect() as conn:
        upsert_query = text(f"""
            INSERT INTO {schema}.{table} 
            (strategy_name, date, total_profit_today, trade_count, win_rate, 
             sharpe_ratio, max_drawdown, avg_profitable_trade, avg_losing_trade,
             profitability_dynamic, annual_profitability, _loaded_at)
            SELECT 
                strategy_name, date, total_profit_today, trade_count, win_rate,
                sharpe_ratio, max_drawdown, avg_profitable_trade, avg_losing_trade,
                profitability_dynamic, annual_profitability, _loaded_at
            FROM temp_daily_stats
            ON CONFLICT (strategy_name, date) DO UPDATE SET
                total_profit_today = EXCLUDED.total_profit_today,
                trade_count = EXCLUDED.trade_count,
                win_rate = EXCLUDED.win_rate,
                sharpe_ratio = EXCLUDED.sharpe_ratio,
                max_drawdown = EXCLUDED.max_drawdown,
                avg_profitable_trade = EXCLUDED.avg_profitable_trade,
                avg_losing_trade = EXCLUDED.avg_losing_trade,
                profitability_dynamic = EXCLUDED.profitability_dynamic,
                annual_profitability = EXCLUDED.annual_profitability,
                _loaded_at = EXCLUDED._loaded_at
        """)
        conn.execute(upsert_query)
        conn.commit()
    
    # Удаляем временную таблицу
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE temp_daily_stats"))
        conn.commit()
    
    print(f"✅ Сохранено {len(stats_df)} записей в {schema}.{table}")


# ========== ИНТЕГРАЦИЯ В ВАШ ФЛОУ ==========

def run_strategy_with_daily_stats(
    strategy_func,  # функция, которая добавляет колонку 'signal' в DataFrame
    strategy_name: str,
    table_ohlcv: str,
    stop_loss_pct: float,
    take_profit_pct: float,
    initial_balance: float = 10000.0
):
    """
    Полный пайплайн: fetch data → apply signals → backtest → aggregate daily stats → save to DB
    """
    # 1. Загружаем данные
    df = fetch_market_data(table_ohlcv)
    
    # 2. Применяем торговую стратегию (добавляет колонку 'signal')
    df = strategy_func(df)
    
    # 3. Запускаем бэктест
    results = backtest_strategy(
        strategy_name=strategy_name,
        df=df,
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=take_profit_pct,
        initial_balance=initial_balance
    )
    
    # 4. Агрегируем дневную статистику
    daily_stats_df = aggregate_daily_stats(
        trades_df=results['trades_df'],
        strategy_name=strategy_name,
        initial_balance=initial_balance
    )
    
    # 5. Сохраняем в PostgreSQL
    if not daily_stats_df.empty:
        save_daily_stats_to_postgres(daily_stats_df)
    
    return results, daily_stats_df


# ========== ПРИМЕР ИСПОЛЬЗОВАНИЯ ==========

# Пример стратегии (добавляет сигналы)
def my_strategy(df: pd.DataFrame) -> pd.DataFrame:
    df['signal'] = 0
    df.loc[df['close'] > df['open'] * 1.01, 'signal'] = 1
    df.loc[df['close'] < df['open'] * 0.99, 'signal'] = -1
    return df

# Запуск
# results, daily_stats = run_strategy_with_daily_stats(
#     strategy_func=my_strategy,
#     strategy_name='MyTrendStrategy',
#     table_ohlcv='public.binance_btcusdt_1h',
#     stop_loss_pct=2.0,
#     take_profit_pct=4.0
# )
