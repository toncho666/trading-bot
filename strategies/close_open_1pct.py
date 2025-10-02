import pandas as pd

def trading_strategy(df: pd.DataFrame) -> pd.DataFrame:
    # Создаем DataFrame для хранения сигналов
    signals = pd.DataFrame(index=df.index)
    signals['side'] = None
    signals['open_price'] = None
    signals['close_price'] = None

    # Вычисляем процентное изменение между закрытием и открытием
    price_change = (df['Close'] - df['Open']) / df['Open']

    # Генерируем сигналы на покупку
    buy_signals = price_change > 0.01
    signals.loc[buy_signals, 'side'] = 'buy'
    signals.loc[buy_signals, 'open_price'] = df['Open']
    signals.loc[buy_signals, 'close_price'] = df['Close']

    # Генерируем сигналы на продажу
    sell_signals = price_change < -0.01
    signals.loc[sell_signals, 'side'] = 'sell'
    signals.loc[sell_signals, 'open_price'] = df['Open']
    signals.loc[sell_signals, 'close_price'] = df['Close']

    # Удаляем строки без сигналов
    signals.dropna(inplace=True)

    return signals
