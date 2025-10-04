# --- Бэктестинг ---
import pandas as pd

def backtest_strategy(strategy_path: str, df: pd.DataFrame):
  import importlib.util
  
  spec = importlib.util.spec_from_file_location("strategy", strategy_path)
  strategy = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(strategy)

  signals = strategy.trading_strategy(df)

  if signals.empty:
      return {"signals": signals, "pnl": 0}

  # --- примерный расчёт PnL ---
  pnl = 0
  for _, row in signals.iterrows():
      if row["side"] == "buy":
          pnl += row["close_price"] - row["open_price"]
      elif row["side"] == "sell":
          pnl += row["open_price"] - row["close_price"]

  return {"signals": signals, "pnl": pnl}
