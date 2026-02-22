#!/usr/bin/env python3
"""
Создание StringSession для Pyrogram через код из приложения
Работает 100%
"""

import asyncio
from pyrogram import Client

async def create_session():
    api_id = int(input("Введите API_ID: "))
    api_hash = input("Введите API_HASH: ")
    phone = input("Введите номер телефона (+7...): ")
    
    client = Client(
        name="pyro_session",
        api_id=api_id,
        api_hash=api_hash,
        in_memory=True
    )
    
    try:
        await client.connect()
        
        # Отправляем код
        print(f"\n📱 Отправляю код на {phone}...")
        sent_code = await client.send_code(phone)
        
        # Вводим код
        code = input("Введите код из Telegram: ")
        
        # Входим
        await client.sign_in(phone, sent_code.phone_code_hash, code)
        print("✅ Успешный вход!")
        
        # Получаем строку сессии
        session_string = await client.export_session_string()
        print("\n" + "="*60)
        print("✅ STRING_SESSION для Pyrogram:")
        print(session_string)
        print("="*60)
        
        # Информация о пользователе
        me = client.me
        print(f"\n👤 Пользователь: {me.first_name} (@{me.username})")
        print(f"🆔 ID: {me.id}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await client.stop()

if __name__ == "__main__":
    asyncio.run(create_session())1
