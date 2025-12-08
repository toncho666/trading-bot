from bybit_client import BybitClient
from config import API_KEY, API_SECRET

class OrderManager:
    def __init__(self):
        self.api = BybitClient(API_KEY, API_SECRET)

    def close_opposite_positions(self, symbol, new_signal):
        # Получаем текущую позицию
        pos = self.api.client.get_positions(category="linear", symbol=symbol)["result"]["list"]
        if not pos:
            return
        
        size = float(pos[0]["size"])
        side = pos[0]["side"]   # Buy или Sell

        if size > 0:
            if new_signal == 1 and side == "Sell":
                # закрываем short
                self.api.place_order(symbol, "Buy", "Market", size)
            elif new_signal == -1 and side == "Buy":
                # закрываем long
                self.api.place_order(symbol, "Sell", "Market", size)

    def open_position(self, symbol, side, entry_type, entry_price, qty, sl, tp):
        order_type = "Market" if entry_type == "market" else "Limit"

        # 1) Открытие позиции
        response = self.api.place_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            qty=qty,
            price=entry_price
        )
        print("Entry order:", response)

        # 2) Установка SL/TP
        position_idx = 1 if side == "Buy" else 2
        sltp = self.api.set_sl_tp(symbol, position_idx, sl, tp)
        print("SL/TP:", sltp)

        return response
