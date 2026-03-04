import pandas as pd
import numpy as np

def trading_strategy(df: pd.DataFrame) -> pd.DataFrame:    
    df = df.copy()

    # --- EMA ---
    df['ema25'] = df['close'].ewm(span=25, adjust=False).mean()
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()

    # --- Фракталы ---
    df['fractal_high'] = (
        (df['high'] > df['high'].shift(1)) &
        (df['high'] > df['high'].shift(2)) &
        (df['high'] > df['high'].shift(-1)) &
        (df['high'] > df['high'].shift(-2))
    )

    df['fractal_low'] = (
        (df['low'] < df['low'].shift(1)) &
        (df['low'] < df['low'].shift(2)) &
        (df['low'] < df['low'].shift(-1)) &
        (df['low'] < df['low'].shift(-2))
    )

    # --- Сигнал ---
    df['signal'] = 0

    for i in range(5, len(df)):
        # BUY
        if (
            df.loc[i, 'ema25'] > df.loc[i, 'ema50']
            and df.loc[i-2, 'fractal_low']
            and df.loc[i, 'close'] > df.loc[i-2, 'high']
        ):
            df.loc[i, 'signal'] = 1

        # SELL
        elif (
            df.loc[i, 'ema25'] < df.loc[i, 'ema50']
            and df.loc[i-2, 'fractal_high']
            and df.loc[i, 'close'] < df.loc[i-2, 'low']
        ):
            df.loc[i, 'signal'] = -1

    return df
