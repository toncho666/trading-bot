import ccxt
import pandas as pd

class MarketDataFetcher:
    def __init__(self, exchange_name="binance"):
        self.exchange = getattr(ccxt, exchange_name)()

    def fetch_ohlcv(self, symbol: str, timeframe: str = "1h", limit: int = 500) -> pd.DataFrame:
        raw = self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["symbol"] = symbol
        df["timeframe"] = timeframe
        return df
