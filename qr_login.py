#!/usr/bin/env python3
"""
QR-логин для Telegram
Исправленная версия
"""

import asyncio
import os
import sys

# Проверяем наличие библиотек
try:
    import qrcode
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except ImportError as e:
    print(f"❌ Ошибка: {e}")
    print("Установите зависимости:")
    print("pip install qrcode[pil] telethon")
    sys.exit(1)

# Ваши данные из .env или вставьте вручную
API_ID = int(os.getenv("API_ID", 38713310))
API_HASH = os.getenv("API_HASH", "d5bf6a1b7f35634207ab71e4b1c91a47")  # ваш hash

async def qr_login():
    print("🚀 Запуск QR-логина...")
    print(f"API_ID: {API_ID}")
    
    # Создаем клиент с временной сессией
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    
    try:
        await client.connect()
        
        if await client.is_user_authorized():
            print("✅ Уже авторизован!")
            me = await client.get_me()
            print(f"   Пользователь: {me.first_name} (@{me.username}")
            return
        
        print("\n📱 Запускаю QR-логин...")
        print("1️⃣ Открой Telegram на телефоне")
        print("2️⃣ Перейди в Настройки → Устройства")
        print("3️⃣ Нажми 'Сканировать QR-код'")
        print("4️⃣ Отсканируй QR-код ниже\n")
        
        # Получаем QR-логин объект
        qr_login = await client.qr_login()
        
        try:
            # Показываем QR-код
            qr = qrcode.QRCode(box_size=2, border=1)
            qr.add_data(qr_login.url)
            qr.print_ascii(invert=True)
            
            print(f"\n🔗 Или открой ссылку: {qr_login.url}")
            print("\n⏳ Ожидание сканирования (60 секунд)...")
            
            # Ждем сканирования
            await qr_login.wait(60)
            
            print("\n✅ QR-код отсканирован! Авторизация успешна!")
            
            # Получаем информацию о пользователе
            me = await client.get_me()
            print(f"   Пользователь: {me.first_name} (@{me.username}")
            print(f"   ID: {me.id}")
            
            # Сохраняем строку сессии
            session_string = client.session.save()
            print(f"\n🔐 СОХРАНИ ЭТУ СТРОКУ В SECRETS:")
            print(f"STRING_SESSION={session_string}")
            
        except asyncio.TimeoutError:
            print("\n❌ Таймаут. Попробуй еще раз")
            
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(qr_login())
