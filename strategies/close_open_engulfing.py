import pandas as pd

def trading_strategy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vectorized trading strategy:
    - Sell: previous close > previous open * 1.005 AND current close < current open
    - Buy:  previous open > previous close * 1.005 AND current close > current open
    """

    df = df.copy()

    # Previous candle values
    prev_close = df['Close'].shift(1)
    prev_open  = df['Open'].shift(1)

    curr_close = df['Close']
    curr_open  = df['Open']

    # Conditions
    sell_cond = (prev_close > prev_open * 1.005) & (curr_close < curr_open)
    buy_cond  = (prev_open  > prev_close * 1.005) & (curr_close > curr_open)

    # Initialize signal column
    df['signal'] = 0

    df.loc[sell_cond, 'signal'] = -1
    df.loc[buy_cond,  'signal'] = 1

    return df
