from order_manager import OrderManager

def execute_signal(signal_data):
    """signal_data — словарь от первого модуля"""
    manager = OrderManager()

    signal = signal_data["signal"]
    if signal == 0:
        print(">> No order")
        return

    side = "Buy" if signal == 1 else "Sell"
    symbol = signal_data["symbol"]

    # Закрыть старую позицию если нужно
    manager.close_opposite_positions(symbol, signal)

    # Создать новую
    order = manager.open_position(
        symbol=symbol,
        side=side,
        entry_type=signal_data["entry_type"],
        entry_price=signal_data.get("entry_price"),
        qty=signal_data["qty"],
        sl=signal_data["stop_loss"],
        tp=signal_data["take_profit"],
    )

    return order
