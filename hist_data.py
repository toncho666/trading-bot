# --- Получение исторических данных ---
def fetch_data(symbol="BTC/USDT", timeframe="1h", limit=200):
  import pandas as pd
  
  exchange = ccxt.okx()  # можно поменять на bybit / binanceus
  ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)

  df = pd.DataFrame(
      ohlcv, columns=["timestamp", "Open", "High", "Low", "Close", "Volume"]
  )
  df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
  df.set_index("timestamp", inplace=True)
  return df
