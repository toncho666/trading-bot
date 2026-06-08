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
