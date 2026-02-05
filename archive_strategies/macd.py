import pandas as pd
import numpy as np

def trading_strategy(df, fast=11, slow=24):
    '''
    ✔ Логика 
    Обозначим:
    fast_ma — быстрая скользящая средняя
    slow_ma — медленная скользящая средняя
    diff = fast_ma - slow_ma
    
    Тогда:
    Сигнал продажи (-1), если:
    fast_ma > slow_ma
    Разница сокращается: diff[i] < diff[i-1]
    До этого росла: diff[i-1] > diff[i-2]
    
    Сигнал покупки (1), если:
    fast_ma < slow_ma
    Разница сокращается: diff[i] < diff[i-1]
    До этого росла: diff[i-1] > diff[i-2]
    '''
    df = df.copy()

    # Скользящие средние
    df['fast_ma'] = df['close'].rolling(fast).mean()
    df['slow_ma'] = df['close'].rolling(slow).mean()

    # Разница
    df['diff'] = df['fast_ma'] - df['slow_ma']

    # Разница сокращается сейчас
    df['diff_down'] = df['diff'] < df['diff'].shift(1)
    # Разница росла до этого
    df['diff_was_up'] = df['diff'].shift(1) > df['diff'].shift(2)

    # Условия
    sell_cond = (
        (df['fast_ma'] > df['slow_ma']) &
        df['diff_down'] &
        df['diff_was_up']
    )

    buy_cond = (
        (df['fast_ma'] < df['slow_ma']) &
        df['diff_down'] &
        df['diff_was_up']
    )

    # Итоговый сигнал
    df['signal'] = 0
    df.loc[sell_cond, 'signal'] = -1
    df.loc[buy_cond, 'signal'] = 1

    # Удаляем служебные колонки при необходимости
    df.drop(columns=['fast_ma', 'slow_ma', 'diff', 'diff_down', 'diff_was_up'],
            inplace=True)

    return df
