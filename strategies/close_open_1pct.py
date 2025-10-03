import pandas as pd

def trading_strategy(df: pd.DataFrame) -> pd.DataFrame:
    # Берем только последнюю строку
    last_row = df.iloc[-1]

    # Считаем процентное изменение
    price_change = (last_row['Close'] - last_row['Open']) / last_row['Open']

    signal = None
    if price_change >= 0.005:   # рост больше 0.5%
        signal = {
            "side": "buy",
            "open_price": last_row['Open'],
            "close_price": last_row['Close']
        }
    elif price_change <= -0.005:  # падение больше 0.5%
        signal = {
            "side": "sell",
            "open_price": last_row['Open'],
            "close_price": last_row['Close']
        }

    # Возвращаем DataFrame (как и раньше, только с одной строкой)
    if signal:
        return pd.DataFrame([signal])
    else:
        return pd.DataFrame(columns=["side", "open_price", "close_price"])
