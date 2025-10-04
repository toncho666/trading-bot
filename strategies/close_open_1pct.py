import pandas as pd

def trading_strategy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Добавляет колонку 'signal' в DataFrame.
    
    - 1 если Close > Open на threshold (по умолчанию 0.5%)
    - -1 если Close < Open на threshold
    - 0 иначе
    """
    df = df.copy()

    # рассчитываем процентное изменение
    change = (df["Close"] - df["Open"]) / df["Open"]

    # логика сигналов
    df["signal"] = 0
    df.loc[change > 0.005, "signal"] = 1   # buy
    df.loc[change < -0.005, "signal"] = -1 # sell

    return df
