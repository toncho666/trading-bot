import requests

def send_telegram_message(tg_token, tg_chat_id, message: str, parse_mode: str = "Markdown"):
    """Отправка сообщения в Telegram"""
    if not tg_token or not tg_chat_id:
        print("[WARN] TELEGRAM_TOKEN или TELEGRAM_CHAT_ID не заданы")
        return
    url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
    payload = {"chat_id": tg_chat_id
              ,"text": message
              ,"parse_mode": parse_mode}
    try:
        response = requests.post(url, data=payload)
        if response.status_code != 200:
            print(f"[ERROR] Telegram API error: {response.text}")
    except Exception as e:
        print(f"[ERROR] Ошибка отправки в Telegram: {e}")
