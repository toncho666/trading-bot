import pandas as pd
import numpy as np

def trading_strategy(df: pd.DataFrame, 
                                   use_volume: bool = False,
                                   min_body_ratio: float = 2.0) -> pd.DataFrame:
    """
    Рассчитывает торговые сигналы на основе свечных паттернов.
    
    Параметры:
    -----------
    df : pd.DataFrame
        DataFrame с колонками: timestamp, open, high, low, close, volume
    use_volume : bool
        Использовать ли объем для подтверждения сигналов
    min_body_ratio : float
        Минимальное соотношение тела свечи для определения сильных паттернов
        
    Возвращает:
    -----------
    pd.DataFrame с добавленными колонками:
        - signal: 1 (покупка), -1 (продажа), 0 (нет сигнала)
        - pattern: название распознанного паттерна
        - дополнительные технические колонки
    """
    
    
    # Создаем копию датафрейма чтобы не модифицировать оригинал
    df = df.copy()
    
    # 1. Базовые расчеты для свечного анализа
    df['body'] = df['close'] - df['open']
    df['body_abs'] = np.abs(df['body'])
    df['high_low_range'] = df['high'] - df['low']
    
    # Избегаем деления на ноль
    df['body_ratio'] = np.where(df['high_low_range'] > 0, 
                               df['body_abs'] / df['high_low_range'], 
                               0)
    
    # Верхняя и нижняя тень
    df['upper_shadow'] = np.where(df['body'] >= 0,
                                 df['high'] - df['close'],
                                 df['high'] - df['open'])
    df['lower_shadow'] = np.where(df['body'] >= 0,
                                 df['open'] - df['low'],
                                 df['close'] - df['low'])
    
    # Тип свечи: 1 = бычья (зеленая), -1 = медвежья (красная)
    df['candle_type'] = np.where(df['body'] >= 0, 1, -1)
    
    # 2. Простые паттерны из одной свечи
    
    # Молот (Hammer) - сигнал покупки
    hammer_condition = (
        (df['lower_shadow'] > df['body_abs'] * min_body_ratio) &  # длинная нижняя тень
        (df['upper_shadow'] < df['body_abs'] * 0.3) &            # маленькая верхняя тень
        (df['high_low_range'] > df['high_low_range'].rolling(20).mean() * 0.5)  # свеча не слишком маленькая
    )
    
    # Повешенный (Hanging Man) - сигнал продажи
    hanging_man_condition = (
        (df['lower_shadow'] > df['body_abs'] * min_body_ratio) &  # длинная нижняя тень
        (df['upper_shadow'] < df['body_abs'] * 0.3) &            # маленькая верхняя тень
        (df['high_low_range'] > df['high_low_range'].rolling(20).mean() * 0.5) &
        (df['candle_type'] == -1)  # медвежья свеча
    )
    
    # Падающая звезда (Shooting Star) - сигнал продажи
    shooting_star_condition = (
        (df['upper_shadow'] > df['body_abs'] * min_body_ratio) &  # длинная верхняя тень
        (df['lower_shadow'] < df['body_abs'] * 0.3) &            # маленькая нижняя тень
        (df['high_low_range'] > df['high_low_range'].rolling(20).mean() * 0.5) &
        (df['candle_type'] == -1)  # обычно медвежья, но может быть и бычья
    )
    
    # Доджи (Doji) - неопределенность
    doji_condition = (
        (df['body_abs'] / df['high_low_range'] < 0.1) &  # очень маленькое тело
        (df['high_low_range'] > df['high_low_range'].rolling(20).mean() * 0.3)
    )
    
    # Марабозу (Marubozu) - сильная свеча
    bull_marubozu = (
        (df['body_ratio'] > 0.9) &  # почти нет теней
        (df['candle_type'] == 1)    # бычья
    )
    
    bear_marubozu = (
        (df['body_ratio'] > 0.9) &  # почти нет теней
        (df['candle_type'] == -1)   # медвежья
    )
    
    # 3. Паттерны из двух свечей
    
    # Поглощение (Engulfing)
    bull_engulfing = (
        (df['candle_type'] == 1) &                    # текущая бычья
        (df['candle_type'].shift(1) == -1) &          # предыдущая медвежья
        (df['open'] < df['close'].shift(1)) &         # открытие ниже закрытия предыдущей
        (df['close'] > df['open'].shift(1)) &         # закрытие выше открытия предыдущей
        (df['body_abs'] > df['body_abs'].shift(1) * 0.8)  # тело больше предыдущего
    )
    
    bear_engulfing = (
        (df['candle_type'] == -1) &                   # текущая медвежья
        (df['candle_type'].shift(1) == 1) &           # предыдущая бычья
        (df['open'] > df['close'].shift(1)) &         # открытие выше закрытия предыдущей
        (df['close'] < df['open'].shift(1)) &         # закрытие ниже открытия предыдущей
        (df['body_abs'] > df['body_abs'].shift(1) * 0.8)  # тело больше предыдущего
    )
    
    # Утренняя звезда (Morning Star) - 3 свечи, упрощенный вариант
    morning_star = (
        (df['candle_type'].shift(2) == -1) &          # первая медвежья
        (df['body_abs'].shift(1) < df['body_abs'].shift(2) * 0.5) &  # вторая маленькая
        (df['candle_type'] == 1) &                    # третья бычья
        (df['close'] > df['open'].shift(2))           # закрытие выше середины первой
    )
    
    # Вечерняя звезда (Evening Star) - 3 свечи, упрощенный вариант
    evening_star = (
        (df['candle_type'].shift(2) == 1) &           # первая бычья
        (df['body_abs'].shift(1) < df['body_abs'].shift(2) * 0.5) &  # вторая маленькая
        (df['candle_type'] == -1) &                   # третья медвежья
        (df['close'] < df['open'].shift(2))           # закрытие ниже середины первой
    )
    
    # 4. Подтверждение объемом
    if use_volume and 'volume' in df.columns:
        # Средний объем за последние 20 свечей
        avg_volume = df['volume'].rolling(20).mean()
        
        # Сильный сигнал если объем выше среднего
        volume_confirmation_bull = df['volume'] > avg_volume * 1.2
        volume_confirmation_bear = df['volume'] > avg_volume * 1.2
    else:
        volume_confirmation_bull = True
        volume_confirmation_bear = True
    
    # 5. Генерация сигналов
    df['signal'] = 0
    df['pattern'] = ''
    
    # Бычьи сигналы (покупка = 1)
    buy_signals = [
        ('Hammer', hammer_condition & volume_confirmation_bull),
        ('Bull_Engulfing', bull_engulfing & volume_confirmation_bull),
        ('Morning_Star', morning_star & volume_confirmation_bull),
        ('Bull_Marubozu', bull_marubozu & volume_confirmation_bull)
    ]
    
    # Медвежьи сигналы (продажа = -1)
    sell_signals = [
        ('Hanging_Man', hanging_man_condition & volume_confirmation_bear),
        ('Shooting_Star', shooting_star_condition & volume_confirmation_bear),
        ('Bear_Engulfing', bear_engulfing & volume_confirmation_bear),
        ('Evening_Star', evening_star & volume_confirmation_bear),
        ('Bear_Marubozu', bear_marubozu & volume_confirmation_bear)
    ]
    
    # Применяем бычьи сигналы
    for pattern_name, condition in buy_signals:
        df.loc[condition, 'signal'] = 1
        df.loc[condition, 'pattern'] = pattern_name
    
    # Применяем медвежьи сигналы (перезаписываем если конфликт)
    for pattern_name, condition in sell_signals:
        df.loc[condition, 'signal'] = -1
        df.loc[condition, 'pattern'] = pattern_name
    
    # 6. Фильтрация слишком частых сигналов
    # (не генерируем новый сигнал пока не закрыта предыдущая позиция)
    df['temp_position'] = df['signal'].replace(0, pd.NA).ffill().fillna(0)
    df['signal_filtered'] = 0
    
    # Сигнал только при смене направления
    change_mask = df['temp_position'] != df['temp_position'].shift(1)
    df.loc[change_mask, 'signal_filtered'] = df.loc[change_mask, 'signal']
    
    # Заменяем исходный сигнал отфильтрованным
    df['signal'] = df['signal_filtered'].astype(int)
    
    # Удаляем временные колонки
    df.drop(['temp_position', 'signal_filtered'], axis=1, inplace=True, errors='ignore')
    
    # 7. Дополнительные технические колонки (опционально)
    # Простое скользящее среднее
    df['sma_20'] = df['close'].rolling(window=20).mean()
    df['sma_50'] = df['close'].rolling(window=50).mean()
    
    # Тренд по SMA
    df['trend'] = np.where(df['sma_20'] > df['sma_50'], 1, -1)
    
    # Фильтруем сигналы против тренда (опционально)
    # df.loc[(df['signal'] == 1) & (df['trend'] == -1), 'signal'] = 0
    # df.loc[(df['signal'] == -1) & (df['trend'] == 1), 'signal'] = 0
    
    # 8. Визуализация паттернов (можно добавить в отдельную функцию)
    df['is_pattern'] = df['signal'].abs()  # 1 если есть любой сигнал
    
    return df
