#!/usr/bin/env python3
"""
Создание StringSession для Pyrogram
Запустите локально или в Codespaces
"""

import asyncio
from pyrogram import Client

async def create_session():
    api_id = int(input("Введите API_ID: "))
    api_hash = input("Введите API_HASH: ")
    phone = input("Введите номер телефона (+79123456789): ")
    
    # Создаем клиента с пустой строкой сессии
    client = Client(
        name="pyro_session_maker",
        api_id=api_id,
        api_hash=api_hash,
        in_memory=True
    )
    
    try:
        await client.start()
        
        # Отправляем код
        await client.send_code(phone)
        code = input("Введите код из Telegram: ")
        
        # Входим с кодом
        await client.sign_in(phone, code)
        
        # Получаем строку сессии
        session_string = await client.export_session_string()
        print("\n" + "="*50)
        print("✅ Новая STRING_SESSION для Pyrogram:")
        print(session_string)
        print("="*50)
        
        # Информация о пользователе
        me = client.me
        print(f"Пользователь: {me.first_name} (@{me.username})")
        print(f"ID: {me.id}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await client.stop()

if __name__ == "__main__":
    asyncio.run(create_session())
