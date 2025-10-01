Для реализации данной стратегии, мы будем использовать библиотеку pandas для обработки данных. Стратегия заключается в том, чтобы генерировать сигналы на покупку, если цена закрытия выше цены открытия на 1%, и сигналы на продажу, если цена закрытия ниже цены открытия на 1%. 

Вот как можно реализовать эту стратегию в функции `trading_strategy`:

```python
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

# Пример использования
# df = pd.read_csv('your_data.csv')  # Загрузите ваш DataFrame с данными
# signals = trading_strategy(df)
# print(signals)
```

### Пояснения:
- Мы создаем новый DataFrame `signals` с индексом, совпадающим с индексом входного DataFrame `df`.
- Вычисляем процентное изменение между ценой закрытия и открытия.
- Генерируем сигналы на покупку, если процентное изменение больше 1%.
- Генерируем сигналы на продажу, если процентное изменение меньше -1%.
- Удаляем строки без сигналов, чтобы оставить только те, где были сгенерированы сигналы на покупку или продажу.