#!/usr/bin/env python3
"""
Простой скрипт для проверки авторизации
Запустите: python test_auth.py
"""

import os
import asyncio
from telethon import TelegramClient

async def test_auth():
    # Берем переменные из окружения
    api_id = int(os.getenv("API_ID", 0))
    api_hash = os.getenv("API_HASH", "")
    phone = os.getenv("PHONE_NUMBER", "")
    
    print(f"API_ID: {api_id}")
    print(f"API_HASH: {api_hash[:5]}...")
    print(f"PHONE_NUMBER: {phone}")
    
    if not all([api_id, api_hash, phone]):
        print("❌ Не все переменные установлены!")
        return
    
    client = TelegramClient('test_session', api_id, api_hash)
    
    try:
        await client.connect()
        
        if await client.is_user_authorized():
            print("✅ Уже авторизован!")
            me = await client.get_me()
            print(f"   Пользователь: {me.first_name} (@{me.username})")
        else:
            print("📱 Отправляю код...")
            await client.send_code_request(phone)
            code = input("Введите код из Telegram: ")
            await client.sign_in(phone, code)
            print("✅ Успешная авторизация!")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(test_auth())
