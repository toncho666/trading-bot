"""
Вспомогательные функции: расчёт размера позиции, округление цены,
получение свободного баланса и пр.
"""

from decimal import Decimal, ROUND_DOWN
import math

def round_price(price: float, tick_size: float) -> float:
    """
    Округляет цену к ближайшему шагу tick_size.
    """
    return float(Decimal(price).quantize(Decimal(str(tick_size)), rounding=ROUND_DOWN))

def calc_qty_from_percent(balance: float, percent: float, price: float, lot_size: float) -> float:
    """
    Рассчитывает количество лотов (quantity) из процента от баланса.
    - balance – свободный баланс в базовой валюте (USDT).
    - percent – желаемый процент от баланса (0‑100).
    - price   – текущая цена актива (BTC/USDT).
    - lot_size – минимальный размер лота (обычно 0.001 для BTC).
    """
    usd_to_risk = balance * percent / 100.0
    raw_qty = usd_to_risk / price
    # Округляем вниз к шагу lot_size
    qty = math.floor(raw_qty / lot_size) * lot_size
    return qty
