#!/usr/bin/env python3
"""
Диагностика получения чата в Pyrogram
"""

import os
import asyncio
import logging
from pyrogram import Client

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def debug_chat():
    # Берем те же переменные, что и в основном скрипте
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    STRING_SESSION = os.getenv("STRING_SESSION", None)
    TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID", "0")
    
    print(f"API_ID: {API_ID}")
    print(f"TARGET_CHAT_ID: {TARGET_CHAT_ID}")
    
    client = Client(
        name="debug_session",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=STRING_SESSION,
        in_memory=True
    )
    
    try:
        await client.start()
        print("✅ Подключено к Telegram")
        
        me = client.me
        print(f"👤 Пользователь: {me.first_name} (@{me.username})")
        
        # Пробуем разные способы получить чат
        print("\n🔍 Способ 1: как число")
        try:
            chat = await client.get_chat(int(TARGET_CHAT_ID))
            print(f"✅ Успех! Чат: {chat.title}")
        except Exception as e:
            print(f"❌ Ошибка: {e}")
        
        print("\n🔍 Способ 2: как строка")
        try:
            chat = await client.get_chat(TARGET_CHAT_ID)
            print(f"✅ Успех! Чат: {chat.title}")
        except Exception as e:
            print(f"❌ Ошибка: {e}")
        
        print("\n🔍 Способ 3: через диалоги")
        async for dialog in client.get_dialogs():
            if str(dialog.chat.id) == TARGET_CHAT_ID:
                print(f"✅ Найден в диалогах: {dialog.chat.title}")
                break
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
    finally:
        await client.stop()

if __name__ == "__main__":
    asyncio.run(debug_chat())
