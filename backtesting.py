# --- Бэктестинг ---
import pandas as pd
import importlib.util

def backtest_strategy(strategy_path: str, df: pd.DataFrame) -> pd.DataFrame:
  """
  Загружает стратегию, прогоняет её по df, возвращает df с колонкой 'signal'.
  signal: 1 = buy, -1 = sell, 0 = no signal
  """
  # Загружаем стратегию из файла
  spec = importlib.util.spec_from_file_location("strategy", strategy_path)
  strategy = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(strategy)
  
  # Получаем сигналы от стратегии
  df_with_signals = strategy.trading_strategy(df)
  
  return df_with_signals
