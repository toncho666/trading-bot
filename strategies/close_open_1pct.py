import pandas as pd

def trading_strategy(df: pd.DataFrame) -> pd.DataFrame:
    """
    change_perc = 1.2
    stop_loss = 1.0
    take_profit = 3.0
    Добавляет колонку 'signal' в DataFrame.
    
    - 1 если Close > Open на threshold (по умолчанию 0.5%)
    - -1 если Close < Open на threshold
    - 0 иначе
    """
    df = df.copy()

    # рассчитываем процентное изменение
    change = (df["close"] - df["open"]) / df["open"]

    # логика сигналов
    df["signal"] = 0
    df.loc[change > 0.012, "signal"] = 1   # buy
    df.loc[change < -0.012, "signal"] = -1 # sell

    return df
