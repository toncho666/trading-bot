# /home/appuser/trading-bot/run_trading_bot.sh
#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/home/appuser/trading-bot"

# --- Подгружаем переменные из .env ---
if [[ -f "${PROJECT_DIR}/.env" ]]; then
export $(grep -v '^#' "${PROJECT_DIR}/.env" | xargs)
fi

# --- Переходим в каталог проекта, теперь относительные пути работают ---
cd "${PROJECT_DIR}"

# --- Активируем виртуальные окружение ---
source "${PROJECT_DIR}/venv/bin/activate"

# --- Запускаем скрипт ---
exec python "${PROJECT_DIR}/runner.py"
