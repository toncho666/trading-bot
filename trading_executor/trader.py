from pybit.unified_trading import HTTP
from typing import Literal

class TradeExecutor:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.session = HTTP(
            testnet=testnet,
            api_key=api_key,
            api_secret=api_secret,
            recv_window=20000,
            adjust_for_time_skew=True,
        )

    def open_market_position(
        self,
        symbol: str,
        side: Literal["Buy", "Sell"],
        qty: float,
        stop_loss: float,
        take_profit: float,
    ) -> dict:
        """
        Futures (linear) + One-Way
        """

        # One-Way режим → всегда 0
        position_idx = 0

        # 1️⃣ Рыночный вход
        entry = self.session.place_order(
            category="linear",
            symbol=symbol,
            side=side,
            orderType="Market",
            qty=str(qty),
            positionIdx=position_idx,
            timeInForce="IOC",
        )

        if entry.get("retCode") != 0:
            raise RuntimeError(f"Entry order failed: {entry}")

        # 2️⃣ Установка SL / TP
        sltp = self.session.set_trading_stop(
            category="linear",
            symbol=symbol,
            stopLoss=str(stop_loss),
            takeProfit=str(take_profit),
            positionIdx=position_idx,
        )

        if sltp.get("retCode") != 0:
            raise RuntimeError(f"SL/TP failed: {sltp}")

        return {
            "entry": entry,
            "sltp": sltp,
        }


# Пример использования
# executor = TradeExecutor(API_KEY, API_SECRET)

# executor.open_market_position(
#     symbol="BTCUSDT",
#     side="Buy",
#     qty=0.001,
#     stop_loss=xxx,
#     take_profit=yyy,
# )
