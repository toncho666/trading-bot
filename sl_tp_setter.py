def get_sl_tp_val(strategy_name: str):
    """
    Возвращает списки значений SL/TP для стратегии — удобно для оптимизации.
    """
    strategies = {
        "close_open_1pct": {
            "sl": 0.01,
            "tp": 0.025
        },
        "close_open_engulfing": {
            "sl": 0.01,
            "tp": 0.025
        },
        "macd": {
            "sl": 0.01,
            "tp": 0.025
        }
    }

    name = strategy_name.lower()

    if name not in strategies:
        raise ValueError(f"Стратегия '{strategy_name}' не найдена.")

    return strategies[name]["sl"], strategies[name]["tp"]
