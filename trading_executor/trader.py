"""
Основной класс, который инкапсулирует работу с Bybit.
"""

import logging
from typing import Optional

from pybybit import Bybit   # клиент v5
from .config import API_KEY, API_SECRET, BASE_URL, DEFAULT_LEVERAGE, DEFAULT_ORDER_TYPE, DEFAULT_TIME_IN_FORCE
from .utils import round_price, calc_qty_from_percent

# ----------------------------------------------------------------------
# Настройка логирования (по желанию)
# ----------------------------------------------------------------------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger("BybitTrader")


class BybitTrader:
    def __init__(self, api_key: str = API_KEY, api_secret: str = API_SECRET,
                 base_url: str = BASE_URL):
        """
        Инициализация клиента.
        """
        self.client = Bybit(
            api_key=api_key,
            api_secret=api_secret,
            endpoint=base_url,
        )
        log.info("Bybit client initialized (endpoint=%s)", base_url)

    # ------------------------------------------------------------------
    # 1️⃣ Получаем параметры рынка (lot size, tick size и пр.)
    # ------------------------------------------------------------------
    def _get_symbol_info(self, symbol: str) -> dict:
        """Запрос информации о символе ( lot_size, tick_size, etc )."""
        resp = self.client.market.symbol_info(symbol=symbol)
        if resp["retCode"] != 0:
            raise RuntimeError(f"Ошибка получения symbol info: {resp['retMsg']}")
        return resp["result"][0]

    # ------------------------------------------------------------------
    # 2️⃣ Текущий свободный баланс в USDT
    # ------------------------------------------------------------------
    def _get_usdt_balance(self) -> float:
        """Возвращает свободный (available) баланс USDT."""
        resp = self.client.account.wallet_balance(coin="USDT")
        if resp["retCode"] != 0:
            raise RuntimeError(f"Ошибка получения баланса: {resp['retMsg']}")
        # В ответе содержится {'totalEquity', 'availableBalance', ...}
        return float(resp["result"]["USDT"]["availableBalance"])

    # ------------------------------------------------------------------
    # 3️⃣ Установка плеча (leverage) – делаем один раз для символа
    # ------------------------------------------------------------------
    def set_leverage(self, symbol: str, leverage: int = DEFAULT_LEVERAGE):
        """Устанавливает плечо для выбранного символа."""
        resp = self.client.account.set_leverage(symbol=symbol, buy_leverage=leverage,
                                                sell_leverage=leverage)
        if resp["retCode"] != 0:
            raise RuntimeError(f"Не удалось установить leverage: {resp['retMsg']}")
        log.info("Leverage for %s set to %sx", symbol, leverage)

    # ------------------------------------------------------------------
    # 4️⃣ Основная функция – исполнение сигнала
    # ------------------------------------------------------------------
    def execute_signal(self,
                       signal: int,
                       symbol: str,
                       stoploss: float,
                       takeprofit: float,
                       percent_of_balance: float,
                       order_type: str = DEFAULT_ORDER_TYPE,
                       time_in_force: str = DEFAULT_TIME_IN_FORCE,
                       leverage: Optional[int] = None):
        """
        Параметры:
        • signal – 1 (buy) или -1 (sell)
        • symbol – например "BTCUSDT"
        • stoploss / takeprofit – цены (не %)
        • percent_of_balance – какая часть USDT‑баланса будет задействована
        • order_type – "Market" или "Limit"
        • time_in_force – "GoodTillCancel", "ImmediateOrCancel" и т.д.
        • leverage – при необходимости переопределить (по умолчанию DEFAULT_LEVERAGE)
        """
        if signal not in (1, -1):
            raise ValueError("Signal must be 1 (buy) or -1 (sell)")

        # 1️⃣ Получаем рыночные параметры
        info = self._get_symbol_info(symbol)
        tick_size = float(info["priceFilter"]["tickSize"])
        lot_size = float(info["lotSizeFilter"]["qtyStep"])

        # 2️⃣ Текущий баланс и размер ордера
        balance = self._get_usdt_balance()
        # Текущая цена нужен только для расчёта количества,
        # но лучше взять лучшую цену из книги (bid/ask) – это уже ниже задержки.
        order_book = self.client.market.orderbook_legacy(symbol=symbol, limit=1)   # столбцы: bidPrice/askPrice
        if order_book["retCode"] != 0:
            raise RuntimeError(f"Не удалось получить Orderbook: {order_book['retMsg']}")

        best_price = float(order_book["result"]["b"][0]["price"]) if signal == 1 else \
                     float(order_book["result"]["a"][0]["price"])

        qty = calc_qty_from_percent(balance, percent_of_balance, best_price, lot_size)
        if qty <= 0:
            raise RuntimeError("Расчётный размер позиции оказался нулевым – проверьте баланс/процент/lotSize.")

        log.info("Calculated qty: %.6f (balance %.2f USDT, %.2f%%, price %.2f)",
                 qty, balance, percent_of_balance, best_price)

        # 3️⃣ При необходимости выставляем плечо
        if leverage:
            self.set_leverage(symbol, leverage)

        # 4️⃣ Формируем параметры ордера
        side = "Buy" if signal == 1 else "Sell"

        # Если Limit‑ордер, то price = best_price (можно добавить небольшую поправку)
        price = round_price(best_price, tick_size) if order_type == "Limit" else None

        # Параметры Stop‑Loss и Take‑Profit – отдельные ордера (OCO)
        sl_price = round_price(stoploss, tick_size)
        tp_price = round_price(takeprofit, tick_size)

        # ------------------------------------------------------------------
        # 5️⃣ Открываем позицию (Limit/Market)
        # ------------------------------------------------------------------
        entry_params = {
            "symbol": symbol,
            "orderType": order_type,
            "side": side,
            "qty": qty,
            "timeInForce": time_in_force,
        }
        if price:
            entry_params["price"] = price

        entry_resp = self.client.order.create(**entry_params)
        if entry_resp["retCode"] != 0:
            raise RuntimeError(f"Не удалось открыть позицию: {entry_resp['retMsg']}")
        log.info("Entry order placed: %s", entry_resp["result"])

        # ------------------------------------------------------------------
        # 6️⃣ Устанавливаем SL и TP через conditional orders (OCO)
        # ------------------------------------------------------------------
        # После создания позиции нам понадобится orderId, чтобы привязать
        # стоп‑лимитные ордера к ней. Для простоты сделаем две отдельные ордера:
        #   • Stop‑Loss  – условный ордер типа "Stop"
        #   • Take‑Profit – ордер типа "TakeProfit"
        # При этом обе они будут «reduceOnly», т.е. только закрывают.

        # Получаем актуальный order ID / позицию
        position_info = self.client.position.list(symbol=symbol)
        if position_info["retCode"] != 0:
            raise RuntimeError("Не удалось получить информацию о позиции")
        # Выбираем позицию текущего направления
        cur_pos = [p for p in position_info["result"] if float(p["size"]) > 0 and p["side"] == side.lower()]
        if not cur_pos:
            raise RuntimeError("Позиция не найдена после входа")
        entry_price = float(cur_pos[0]["avgEntryPrice"])
        log.info("Opened %s %s @ %.2f", side, symbol, entry_price)

        # Стоп‑лосс
        sl_params = {
            "symbol": symbol,
            "orderType": "Stop",
            "side": "Sell" if signal == 1 else "Buy",
            "qty": qty,
            "stopPx": sl_price,
            "basePrice": entry_price,
            "timeInForce": "GoodTillCancel",
            "reduceOnly": True,
            "closeOnTrigger": True,
        }
        sl_resp = self.client.order.create(**sl_params)
        if sl_resp["retCode"] != 0:
            raise RuntimeError(f"Не удалось установить StopLoss: {sl_resp['retMsg']}")
        log.info("StopLoss set at %.2f", sl_price)

        # Тейк‑профит
        tp_params = {
            "symbol": symbol,
            "orderType": "TakeProfit",
            "side": "Sell" if signal == 1 else "Buy",
            "qty": qty,
            "price": tp_price,
            "timeInForce": "GoodTillCancel",
            "reduceOnly": True,
            "closeOnTrigger": True,
        }
        tp_resp = self.client.order.create(**tp_params)
        if tp_resp["retCode"] != 0:
            raise RuntimeError(f"Не удалось установить TakeProfit: {tp_resp['retMsg']}")
        log.info("TakeProfit set at %.2f", tp_price)

        log.info("Все ордера успешно размещены.")
        return {
            "entry": entry_resp["result"],
            "stoploss": sl_resp["result"],
            "takeprofit": tp_resp["result"],
        }
