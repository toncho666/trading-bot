#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/home/appuser/trading-bot"

# --- Подгружаем переменные из .env ---
if [[ -f "${PROJECT_DIR}/.env" ]]; then
export $(grep -v '^#' "${PROJECT_DIR}/.env" | xargs)
fi

# --- Переходим в каталог проекта ---
cd "${PROJECT_DIR}"

# --- Активируем виртуальное окружение  ---
source "${PROJECT_DIR}/venv/bin/activate"

# --- Запускаем скрипт расчета статистики по стратегиям ---
exec python "${PROJECT_DIR}/strategy_stat.py"
