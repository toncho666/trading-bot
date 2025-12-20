################################################################################################################################
#                                                             Первая версия                                                    #
################################################################################################################################
# from pybit.unified_trading import HTTP
# from typing import Literal
# 
# class TradeExecutor:
#     def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
#         self.session = HTTP(
#             testnet=testnet,
#             api_key=api_key,
#             api_secret=api_secret,
#             recv_window=20000,
#             adjust_for_time_skew=True,
#         )
# 
#     def open_market_position(
#         self,
#         symbol: str,
#         side: Literal["Buy", "Sell"],
#         qty: float,
#         stop_loss: float,
#         take_profit: float,
#     ) -> dict:
#         """
#         Futures (linear) + One-Way
#         """
# 
#         # One-Way режим → всегда 0
#         position_idx = 0
# 
#         # 1️⃣ Рыночный вход
#         entry = self.session.place_order(
#             category="linear",
#             symbol=symbol,
#             side=side,
#             orderType="Market",
#             qty=str(qty),
#             positionIdx=position_idx,
#             timeInForce="IOC",
#         )
# 
#         if entry.get("retCode") != 0:
#             raise RuntimeError(f"Entry order failed: {entry}")
# 
#         # 2️⃣ Установка SL / TP
#         sltp = self.session.set_trading_stop(
#             category="linear",
#             symbol=symbol,
#             stopLoss=str(stop_loss),
#             takeProfit=str(take_profit),
#             positionIdx=position_idx,
#         )
# 
#         if sltp.get("retCode") != 0:
#             raise RuntimeError(f"SL/TP failed: {sltp}")
# 
#         return {
#             "entry": entry,
#             "sltp": sltp,
#         }
# 
# Пример использования
# executor = TradeExecutor(API_KEY, API_SECRET)
# 
# executor.open_market_position(
#     symbol="BTCUSDT",
#     side="Buy",
#     qty=0.001,
#     stop_loss=xxx,
#     take_profit=yyy,
# )




################################################################################################################################
#                                                             Вторая версия                                                    #
################################################################################################################################
# TradeExecutor:
# принимает готовые параметры от стратегии
# НЕ рассчитывает SL/TP
# НЕ меняет объём
# проверяет, что средств хватает
# поддерживает Market и Limit
# выставляет SL / TP
# содержит блок повторного входа
# не открывает позицию повторно, если она уже существует
import time
from typing import Literal
from pybit.unified_trading import HTTP


class TradeExecutor:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.session = HTTP(
            testnet=testnet,
            api_key=api_key,
            api_secret=api_secret,
            recv_window=20000,
            adjust_for_time_skew=True,
        )

    # ──────────────────────────────────────────────
    # BALANCE
    # ──────────────────────────────────────────────
    def _get_available_balance(self) -> float:
        resp = self.session.get_wallet_balance(
            accountType="UNIFIED",
            coin="USDT",
        )

        if resp["retCode"] != 0:
            raise RuntimeError(f"Balance error: {resp}")

        return float(resp["result"]["list"][0]["coin"][0]["availableToWithdraw"])

    # ──────────────────────────────────────────────
    # MARKET PRICE
    # ──────────────────────────────────────────────
    def _get_market_price(self, symbol: str) -> float:
        resp = self.session.get_tickers(
            category="linear",
            symbol=symbol,
        )

        if resp["retCode"] != 0:
            raise RuntimeError(f"Ticker error: {resp}")

        return float(resp["result"]["list"][0]["lastPrice"])

    # ──────────────────────────────────────────────
    # POSITION CHECK (ANTI DOUBLE-ENTRY)
    # ──────────────────────────────────────────────
    def _position_exists(self, symbol: str) -> bool:
        resp = self.session.get_positions(
            category="linear",
            symbol=symbol,
        )

        if resp["retCode"] != 0:
            raise RuntimeError(f"Position check error: {resp}")

        size = float(resp["result"]["list"][0]["size"])
        return size != 0

    # ──────────────────────────────────────────────
    # RISK CHECK
    # ──────────────────────────────────────────────
    def _check_margin(self, symbol: str, price: float, qty: float):
        balance = self._get_available_balance()

        resp = self.session.get_positions(
            category="linear",
            symbol=symbol,
        )

        leverage = float(resp["result"]["list"][0]["leverage"])
        required_margin = (price * qty) / leverage

        if required_margin > balance:
            raise RuntimeError(
                f"Insufficient balance: "
                f"required={required_margin:.2f}, "
                f"available={balance:.2f}"
            )

    # ──────────────────────────────────────────────
    # MAIN EXECUTION (WITH RE-ENTRY)
    # ──────────────────────────────────────────────
    def execute_trade(
        self,
        symbol: str,
        side: Literal["Buy", "Sell"],
        order_type: Literal["Market", "Limit"],
        qty: float,
        stop_loss: float,
        take_profit: float,
        limit_price: float | None = None,
        max_retries: int = 3,
        retry_delay_sec: float = 1.5,
    ) -> dict:

        position_idx = 0
        last_error = None

        for attempt in range(1, max_retries + 1):
            try:
                # 🔒 Защита от повторного входа
                if self._position_exists(symbol):
                    raise RuntimeError("Position already exists")

                # 1️⃣ Цена для risk-check
                if order_type == "Market":
                    price = self._get_market_price(symbol)
                else:
                    if limit_price is None:
                        raise ValueError("limit_price is required for Limit order")
                    price = limit_price

                # 2️⃣ Проверка маржи
                self._check_margin(symbol, price, qty)

                # 3️⃣ Отправка ордера
                order = self.session.place_order(
                    category="linear",
                    symbol=symbol,
                    side=side,
                    orderType=order_type,
                    qty=str(qty),
                    price=str(limit_price) if order_type == "Limit" else None,
                    timeInForce="GTC" if order_type == "Limit" else "IOC",
                    positionIdx=position_idx,
                )

                if order["retCode"] != 0:
                    raise RuntimeError(f"Order rejected: {order}")

                # 4️⃣ Установка SL / TP
                sltp = self.session.set_trading_stop(
                    category="linear",
                    symbol=symbol,
                    stopLoss=str(stop_loss),
                    takeProfit=str(take_profit),
                    stopLossTriggerBy="LastPrice",
                    takeProfitTriggerBy="LastPrice",
                    positionIdx=position_idx,
                    reduceOnly=True,
                    closeOnTrigger=True,
                )

                if sltp["retCode"] != 0:
                    raise RuntimeError(f"SL/TP failed: {sltp}")

                # ✅ SUCCESS
                return {
                    "symbol": symbol,
                    "side": side,
                    "order_type": order_type,
                    "qty": qty,
                    "attempt": attempt,
                    "order": order,
                    "sltp": sltp,
                }

            except Exception as e:
                last_error = e
                print(f"[Attempt {attempt}] Execution failed: {e}")

                if attempt < max_retries:
                    time.sleep(retry_delay_sec)

        raise RuntimeError(
            f"Trade execution failed after {max_retries} attempts"
        ) from last_error

# ▶️ Пример использования (Market)
# executor = TradeExecutor(
#     api_key="API_KEY",
#     api_secret="API_SECRET",
#     testnet=True,
# )

# result = executor.execute_trade(
#     symbol="BTCUSDT",
#     side="Buy",
#     order_type="Market",
#     qty=0.001,
#     stop_loss=62000,
#     take_profit=68000,
# )

# print(result)
















