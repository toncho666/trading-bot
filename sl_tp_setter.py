from typing import Literal, Tuple

def get_sl_tp_val(strategy_name: str
                 ,side: Literal["buy", "sell", "long", "short"]
                 ,deal_price: float):
    """
    Возвращает абсолютные цены Stop‑Loss и Take‑Profit для выбранной стратегии.

    Параметры
    ----------
    strategy_name : str
        Наименование стратегии (например, ``"close_open_1pct"``).  
        Регистронезависимое сравнение.

    side : {"buy", "sell", "long", "short"}
        Направление позиции.
        * **buy / long**  – цена покупки, SL ниже цены, TP выше цены;
        * **sell / short** – цена продажи, SL выше цены, TP ниже цены.

    deal_price : float
        Цена, от которой считается отклонение (цена входа в сделку).

    Возвращаемое значение
    ----------------------
    Tuple[float, float]
        (price_stoploss, price_takeprofit)

    Исключения
    ----------
    ValueError
        * Если стратегии с таким именем нет;
        * Если в ``side`` передано недопустимое значение;
        * Если ``deal_price`` ≤ 0.
    """
    # нормализуем название стратегии и направление
    name = strategy_name.strip().lower()
    side_norm = side.strip().lower()
    
    # поддерживаем короткую запись
    if side_norm in ("long", "buy"):
        side_norm = "buy"
    elif side_norm in ("short", "sell"):
        side_norm = "sell"

    # -------------------- 1. Словарь с параметрами стратегий --------------------
    strategies = {
        "close_open_1pct": {"sl": 0.006,  "tp": 0.035},
        "close_open_engulfing": {"sl": 0.011,  "tp": 0.035},
        # "macd": {"sl": 0.01,  "tp": 0.025},
        "candles": {"sl": 0.008,  "tp": 0.04},
        # ← здесь можно добавить новые стратегии
    }

    sl_perc = strategies[name]["sl"]   # 0.01 → 1 %
    tp_perc = strategies[name]["tp"]   # 0.025 → 2,5 %

    # -------------------- 2. Расчёт абсолютных цен --------------------
    if side_norm == "buy":
        price_sl = deal_price * (1 - sl_perc)   # ниже цены входа
        price_tp = deal_price * (1 + tp_perc)   # выше цены входа
    else:  # sell
        price_sl = deal_price * (1 + sl_perc)   # выше цены входа
        price_tp = deal_price * (1 - tp_perc)   # ниже цены входа

    return price_sl, price_tp
