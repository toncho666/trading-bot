from pybit.unified_trading import HTTP

class BybitClient:
    def __init__(self, api_key, api_secret):
        self.client = HTTP(
            testnet=False,
            api_key=api_key,
            api_secret=api_secret
        )

    def place_order(self, symbol, side, order_type, qty, price=None):
        params = {
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            "qty": qty,
            "timeInForce": "PostOnly" if order_type == "Limit" else "IOC"
        }
        if order_type == "Limit":
            params["price"] = price

        return self.client.place_order(**params)

    def set_sl_tp(self, symbol, position_idx, stop_loss, take_profit):
        return self.client.set_trading_stop(
            category="linear",
            symbol=symbol,
            positionIdx=position_idx,
            stopLoss=str(stop_loss),
            takeProfit=str(take_profit)
        )
