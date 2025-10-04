import pandas as pd
import numpy as np
import talib

def trading_strategy(df: pd.DataFrame,
                     ema_fast: int = 12,
                     ema_slow: int = 26, 
                     rsi_period: int = 14,
                     rsi_oversold: int = 30,
                     rsi_overbought: int = 70,
                     adx_threshold: int = 25) -> pd.DataFrame:
    """
    Генерирует торговые сигналы buy/sell на основе технических индикаторов
    
    Parameters:
    -----------
    df : pd.DataFrame
        Датафрейм с колонками: ['Open', 'High', 'Low', 'Close', 'Volume'] 
        и timestamp в индексе
    ema_fast : int
        Период быстрой EMA (по умолчанию 12)
    ema_slow : int
        Период медленной EMA (по умолчанию 26)
    rsi_period : int
        Период RSI (по умолчанию 14)
    rsi_oversold : int
        Уровень перепроданности RSI (по умолчанию 30)
    rsi_overbought : int
        Уровень перекупленности RSI (по умолчанию 70)
    adx_threshold : int
        Порог силы тренда ADX (по умолчанию 25)
    
    Returns:
    --------
    pd.DataFrame
        Исходный датафрейм с добавленными колонками:
        - Все рассчитанные индикаторы
        - 'signal': 1 (BUY), -1 (SELL), 0 (HOLD)
        - 'signal_strength': сила сигнала от 0 до 1
    """
    
    # Создаем копию датафрейма чтобы не изменять оригинал
    result_df = df.copy()
    
    # Убеждаемся, что данные отсортированы по времени
    result_df = result_df.sort_index()
    
    # 1. Расчет трендовых индикаторов
    result_df['ema_fast'] = talib.EMA(result_df['Close'], timeperiod=ema_fast)
    result_df['ema_slow'] = talib.EMA(result_df['Close'], timeperiod=ema_slow)
    result_df['sma_20'] = talib.SMA(result_df['Close'], timeperiod=20)
    
    # 2. Расчет осцилляторов
    result_df['rsi'] = talib.RSI(result_df['Close'], timeperiod=rsi_period)
    result_df['macd'], result_df['macd_signal'], result_df['macd_hist'] = talib.MACD(
        result_df['Close'], fastperiod=12, slowperiod=26, signalperiod=9
    )
    
    # 3. Расчет индикаторов волатильности
    result_df['bb_upper'], result_df['bb_middle'], result_df['bb_lower'] = talib.BBANDS(
        result_df['Close'], timeperiod=20, nbdevup=2, nbdevdn=2
    )
    
    # 4. Расчет силы тренда
    result_df['adx'] = talib.ADX(result_df['High'], result_df['Low'], result_df['Close'], timeperiod=14)
    
    # 5. Генерация сигналов
    signals = []
    signal_strengths = []
    
    for i in range(len(result_df)):
        if i == 0:
            # Для первой строки нет сигнала
            signals.append(0)
            signal_strengths.append(0.0)
            continue
            
        current = result_df.iloc[i]
        prev = result_df.iloc[i-1]
        
        score = 0
        bullish_conditions = 0
        total_conditions = 0
        
        # Условие 1: Пересечение EMA (самое важное)
        if not pd.isna(current['ema_fast']) and not pd.isna(current['ema_slow']):
            total_conditions += 1
            if current['ema_fast'] > current['ema_slow']:
                score += 3
                bullish_conditions += 1
            else:
                score -= 3
        
        # Условие 2: RSI
        if not pd.isna(current['rsi']):
            total_conditions += 1
            if current['rsi'] < rsi_oversold:
                score += 2
                bullish_conditions += 1
            elif current['rsi'] > rsi_overbought:
                score -= 2
        
        # Условие 3: MACD
        if not pd.isna(current['macd']) and not pd.isna(current['macd_signal']):
            total_conditions += 1
            if current['macd'] > current['macd_signal'] and prev['macd'] <= prev['macd_signal']:
                score += 2
                bullish_conditions += 1
            elif current['macd'] < current['macd_signal'] and prev['macd'] >= prev['macd_signal']:
                score -= 2
        
        # Условие 4: Боллинджер Баны
        if not pd.isna(current['bb_lower']) and not pd.isna(current['bb_upper']):
            total_conditions += 1
            bb_position = (current['Close'] - current['bb_lower']) / (current['bb_upper'] - current['bb_lower'])
            if bb_position < 0.2:  # Около нижней границы
                score += 1
                bullish_conditions += 1
            elif bb_position > 0.8:  # Около верхней границы
                score -= 1
        
        # Условие 5: ADX - сила тренда
        if not pd.isna(current['adx']):
            total_conditions += 1
            if current['adx'] > adx_threshold:
                # Усиливаем сигнал если есть тренд
                if bullish_conditions > total_conditions / 2:
                    score += 1
                elif bullish_conditions < total_conditions / 2:
                    score -= 1
        
        # Определение финального сигнала
        if score >= 4:
            signal = 1  # BUY
            strength = min(1.0, score / 8.0)  # Нормализуем силу сигнала
        elif score <= -4:
            signal = -1  # SELL
            strength = min(1.0, abs(score) / 8.0)
        else:
            signal = 0  # HOLD
            strength = 0.0
        
        signals.append(signal)
        signal_strengths.append(strength)
    
    # Добавляем сигналы в датафрейм
    result_df['signal'] = signals
    result_df['signal_strength'] = signal_strengths
    
    # Добавляем текстовое представление сигнала
    result_df['signal_text'] = result_df['signal'].map({1: 'BUY', -1: 'SELL', 0: 'HOLD'})
    
    return result_df
