#!/bin/bash

# Переходим в директорию проекта
cd ~/Telegram_to_YaDisk || { echo "❌ Директория не найдена"; exit 1; }

# Активируем виртуальное окружение
echo "🔄 Активация виртуального окружения..."
source venv/bin/activate || { echo "❌ Ошибка активации виртуального окружения"; exit 1; }

# Завершаем старые процессы бота
echo "🛑 Завершение старых процессов бота..."
pkill -f "python.*telegram_bot.py"
sleep 1

# Запускаем бота (exec заменяет процесс, поэтому deactivate не нужен)
echo "🚀 Запуск telegram_bot.py..."
exec python telegram_bot.py
