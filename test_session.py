#!/usr/bin/env python3
import os
import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession

# Загружаем переменные
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
STRING_SESSION = os.getenv("STRING_SESSION", "")

print(f"=== ДИАГНОСТИКА ===")
print(f"API_ID: {API_ID}")
print(f"API_HASH длина: {len(API_HASH)}")
print(f"STRING_SESSION длина: {len(STRING_SESSION)}")
print(f"STRING_SESSION первые 20 символов: {STRING_SESSION[:20]}...")

if not STRING_SESSION:
    print("❌ STRING_SESSION не найдена в окружении!")
    exit(1)

async def test():
    print("\n🔄 Подключаюсь к Telegram...")
    client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
    
    try:
        await client.connect()
        print("✅ Соединение установлено")
        
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"✅ УСПЕХ! Пользователь: {me.first_name} (@{me.username})")
            print(f"   ID: {me.id}")
            print(f"   Телефон: {me.phone}")
            return True
        else:
            print("❌ Сессия недействительна - требуется авторизация")
            return False
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False
    finally:
        await client.disconnect()

if __name__ == "__main__":
    success = asyncio.run(test())
    if success:
        print("\n✅ Сессия рабочая! Main.py будет работать без запроса кода.")
    else:
        print("\n❌ Сессия не работает. Нужно создать новую через qr_login.py")
