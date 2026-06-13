import ccxt
import pandas as pd
from datetime import datetime

class MarketDataFetcher:
    def __init__(self, exchange_name="binance"):
        # self.exchange = getattr(ccxt, exchange_name)()
        self.exchange = ccxt.okx()

    def fetch_ohlcv(self, symbol: str, timeframe: str = "1h", limit: int = 500) -> pd.DataFrame:
        """Запрос OHLCV данных."""
        raw = self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        
        print('raw -> ', raw)
        
        df = pd.DataFrame(
            raw,
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )

        # Постобработка
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["symbol"] = symbol
        df["timeframe"] = timeframe
        
        
        #df = df.iloc[:-1]  # используем iloc вместо df[:-1]
        if len(df) > 1:
            df = df.iloc[:-1]
        else:
            print(f"ВНИМАНИЕ: DataFrame содержит {len(df)} строк, удаление не выполнено")
            return df
            
        return df
