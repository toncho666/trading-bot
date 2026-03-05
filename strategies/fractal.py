import pandas as pd
import numpy as np

def trading_strategy(df):
    df = df.copy()
    df['ema25'] = df['close'].ewm(span=25, adjust=False).mean()
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()

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

    # смещения на 2 бара назад
    ema25_up   = df['ema25'] > df['ema50']
    ema25_down = df['ema25'] < df['ema50']
    low_fract  = df['fractal_low'].shift(2)
    high_fract = df['fractal_high'].shift(2)

    buy_cond  = ema25_up & low_fract & (df['close'] > df['high'].shift(2))
    sell_cond = ema25_down & high_fract & (df['close'] < df['low'].shift(2))

    df['signal'] = 0
    df.loc[buy_cond,  'signal'] = 1
    df.loc[sell_cond, 'signal'] = -1
    return df
