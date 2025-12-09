"""
Основной класс, который инкапсулирует работу с Bybit.
"""

import logging
import uuid
from typing import Optional

from pybybit import Bybit   # клиент v5
from .config import (
    API_KEY,
    API_SECRET,
    BASE_URL,
    DEFAULT_LEVERAGE,
    DEFAULT_ORDER_TYPE,
    DEFAULT_TIME_IN_FORCE,
)
from .utils import round_price, calc_qty_from_percent

# ----------------------------------------------------------------------
# Настройка логирования (по желанию)
# ----------------------------------------------------------------------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger("BybitTrader")


class BybitTrader:
    def __init__(
        self,
        api_key: str = API_KEY,
        api_secret: str = API_SECRET,
        base_url: str = BASE_URL,
    ):
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
    # 4️⃣ Основная функция – исполнение сигнала с OCO отдерами SL/TP
    # ------------------------------------------------------------------
    def execute_signal(
        self,
        signal: int,
        symbol: str,
        stoploss: float,
        takeprofit: float,
        percent_of_balance: float,
        order_type: str = DEFAULT_ORDER_TYPE,
        time_in_force: str = DEFAULT_TIME_IN_FORCE,
        leverage: Optional[int] = None,
    ) -> dict:
        """
        Параметры:
            signal               – 1 (BUY) / -1 (SELL)
            symbol               – тикер, например "BTCUSDT"
            stoploss, takeprofit – цены (не %)
            percent_of_balance   – % от свободного USDT‑баланса
            order_type           – "Market" или "Limit"
            leverage (опц.)      – если нужен отдельный уровень плеча
        """
        if signal not in (1, -1):
            raise ValueError("signal must be 1 (buy) or -1 (sell)")

        # ------------------------------------------------------------------
        # 1️⃣ Получаем параметры символа (tickSize, lotSize)
        # ------------------------------------------------------------------
        info = self._get_symbol_info(symbol)
        tick_size = float(info["priceFilter"]["tickSize"])
        lot_size = float(info["lotSizeFilter"]["qtyStep"])

        # ------------------------------------------------------------------
        # 2️⃣ Расчёт объёма позиции в лотах
        # ------------------------------------------------------------------
        balance = self._get_usdt_balance()
        # берём лучшую цену из книги, чтобы не «переплатить» при лимитном входе
        order_book = self.client.market.orderbook_legacy(symbol=symbol, limit=1)
        if order_book["retCode"] != 0:
            raise RuntimeError(f"orderbook error: {order_book['retMsg']}")

        best_price = (
            float(order_book["result"]["a"][0]["price"])
            if signal == -1
            else float(order_book["result"]["b"][0]["price"])
        )
        qty = calc_qty_from_percent(balance, percent_of_balance, best_price, lot_size)

        if qty <= 0:
            raise RuntimeError(
                "Calculated quantity is zero – check balance, percent or lotSize"
            )
        log.info(
            "Qty %.6f (balance %.2f USDT, %.2f%%, price %.2f)",
            qty,
            balance,
            percent_of_balance,
            best_price,
        )

        # ------------------------------------------------------------------
        # 3️⃣ Плечо (если передано)
        # ------------------------------------------------------------------
        if leverage:
            self.set_leverage(symbol, leverage)

        # ------------------------------------------------------------------
        # 4️⃣ Открывающий ордер (Market/Limit)
        # ------------------------------------------------------------------
        side = "Buy" if signal == 1 else "Sell"
        entry_params = {
            "symbol": symbol,
            "orderType": order_type,
            "side": side,
            "qty": qty,
            "timeInForce": time_in_force,
        }
        if order_type == "Limit":
            entry_params["price"] = round_price(best_price, tick_size)

        entry_resp = self.client.order.create(**entry_params)
        if entry_resp["retCode"] != 0:
            raise RuntimeError(f"Entry order error: {entry_resp['retMsg']}")
        log.info("Entry order placed: %s", entry_resp["result"])

        # ------------------------------------------------------------------
        # 5️⃣ Получаем текущую позицию (нужен avgEntryPrice для SL)
        # ------------------------------------------------------------------
        pos_resp = self.client.position.list(symbol=symbol)
        if pos_resp["retCode"] != 0:
            raise RuntimeError(f"Position list error: {pos_resp['retMsg']}")

        # Фильтруем позицию в нужном направлении
        cur_pos = [
            p
            for p in pos_resp["result"]
            if float(p["size"]) > 0 and p["side"] == side.lower()
        ]
        if not cur_pos:
            raise RuntimeError("Opened position not found – maybe entry not filled yet")
        entry_price = float(cur_pos[0]["avgEntryPrice"])
        log.info("Opened %s %s @ %.2f", side, symbol, entry_price)

        # ------------------------------------------------------------------
        # 6️⃣ Формируем пакет OCO (StopLoss + TakeProfit)
        # ------------------------------------------------------------------
        # Уникальный идентификатор, связывающий два ордера.
        oco_link_id = str(uuid.uuid4())

        # Округляем цены в соответствии с tickSize
        sl_price = round_price(stoploss, tick_size)
        tp_price = round_price(takeprofit, tick_size)

        # 6.1 Stop‑Loss (type = "Stop")
        sl_order = {
            "symbol": symbol,
            "orderType": "Stop",
            "side": "Sell" if signal == 1 else "Buy",
            "qty": qty,
            "stopPx": sl_price,
            "basePrice": entry_price,
            "timeInForce": "GoodTillCancel",
            "reduceOnly": True,
            "closeOnTrigger": True,
            "orderLinkId": oco_link_id,  # <-- связываем
        }

        # 6.2 Take‑Profit (type = "TakeProfit")
        tp_order = {
            "symbol": symbol,
            "orderType": "TakeProfit",
            "side": "Sell" if signal == 1 else "Buy",
            "qty": qty,
            "price": tp_price,
            "timeInForce": "GoodTillCancel",
            "reduceOnly": True,
            "closeOnTrigger": True,
            "orderLinkId": oco_link_id,
        }

        # ------------------------------------------------------------------
        # 7️⃣ Отправляем пакет
        # ------------------------------------------------------------------
        batch_resp = self.client.order.batchCreate(orders=[sl_order, tp_order])
        if batch_resp["retCode"] != 0:
            # Если пакет не прошёл, лучше отменить входящий ордер (см. выше)
            raise RuntimeError(f"OCO batch error: {batch_resp['retMsg']}")
        log.info("OCO placed – linkId=%s", oco_link_id)

        # batchCreate возвращает список ордеров в поле "result"
        oco_results = batch_resp["result"]

        # Возврат удобной структуры
        return {
            "entry": entry_resp["result"],
            "oco": {
                "linkId": oco_link_id,
                "stopLoss": oco_results[0],
                "takeProfit": oco_results[1],
            },
        }
