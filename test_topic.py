#!/usr/bin/env python3
"""
Тестовый скрипт для проверки получения тем
"""

import os
import asyncio
import logging
from pyrogram import Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_topics():
    # Берем переменные из окружения
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    STRING_SESSION = os.getenv("STRING_SESSION", None)
    TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", 0))
    
    print(f"API_ID: {API_ID}")
    print(f"TARGET_CHAT_ID: {TARGET_CHAT_ID}")
    
    client = Client(
        name="test_session",
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
        
        # Получаем чат
        print(f"\n🔍 Получаю чат {TARGET_CHAT_ID}...")
        chat = await client.get_chat(TARGET_CHAT_ID)
        print(f"✅ Чат: {chat.title}")
        print(f"   ID: {chat.id}")
        print(f"   Тип: {chat.type}")
        print(f"   Форум: {getattr(chat, 'is_forum', False)}")
        
        # Получаем последние 5 сообщений
        print(f"\n📨 Последние 5 сообщений:")
        messages = []
        async for msg in client.get_chat_history(chat.id, limit=5):
            messages.append(msg)
            print(f"\nСообщение {msg.id}:")
            print(f"  Дата: {msg.date}")
            print(f"  Текст: {msg.text[:50] if msg.text else 'Нет текста'}")
            print(f"  Есть медиа: {bool(msg.media)}")
            print(f"  reply_to_top_id: {getattr(msg, 'reply_to_top_id', None)}")
            print(f"  message_thread_id: {getattr(msg, 'message_thread_id', None)}")
            
            if msg.reply_to:
                print(f"  reply_to: {msg.reply_to}")
                if hasattr(msg.reply_to, 'reply_to_top_id'):
                    print(f"  reply_to.reply_to_top_id: {msg.reply_to.reply_to_top_id}")
            
            if msg.media:
                print(f"  Тип медиа: {type(msg.media)}")
        
        # Проверяем, есть ли вообще темы в чате
        print(f"\n🔍 Проверяю наличие тем в чате...")
        has_topics = False
        async for msg in client.get_chat_history(chat.id, limit=100):
            if hasattr(msg, 'reply_to_top_id') and msg.reply_to_top_id:
                has_topics = True
                print(f"✅ Найдена тема {msg.reply_to_top_id} в сообщении {msg.id}")
                break
        
        if not has_topics:
            print("❌ В последних 100 сообщениях нет тем")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.stop()

if __name__ == "__main__":
    asyncio.run(test_topics())
