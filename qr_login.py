#!/usr/bin/env python3
"""
QR-логин для Telegram с поддержкой двухфакторной аутентификации
"""

import asyncio
import os
import sys
import getpass

try:
    import qrcode
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.errors import SessionPasswordNeededError
except ImportError as e:
    print(f"❌ Ошибка: {e}")
    print("Установите зависимости:")
    print("pip install qrcode[pil] telethon")
    sys.exit(1)

# Ваши данные
API_ID = int(os.getenv("API_ID", 38713310))
API_HASH = os.getenv("API_HASH", "d5bf6522bb54a8165991b066d898f90c")

async def qr_login():
    print("🚀 Запуск QR-логина...")
    print(f"API_ID: {API_ID}")
    
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    
    try:
        await client.connect()
        
        if await client.is_user_authorized():
            print("✅ Уже авторизован!")
            me = await client.get_me()
            print(f"   Пользователь: {me.first_name} (@{me.username})")
            return
        
        print("\n📱 Запускаю QR-логин...")
        print("1️⃣ Открой Telegram на телефоне")
        print("2️⃣ Перейди в Настройки → Устройства")
        print("3️⃣ Нажми 'Сканировать QR-код'")
        print("4️⃣ Отсканируй QR-код ниже\n")
        
        # Получаем QR-логин объект
        qr_login = await client.qr_login()
        
        # Показываем QR-код
        qr = qrcode.QRCode(box_size=2, border=1)
        qr.add_data(qr_login.url)
        qr.print_ascii(invert=True)
        
        print(f"\n🔗 Или открой ссылку: {qr_login.url}")
        print("\n⏳ Ожидание сканирования (60 секунд)...")
        
        try:
            # Ждем сканирования
            user = await qr_login.wait(60)
            
            print("\n✅ QR-код отсканирован!")
            
            # Проверяем, не нужен ли пароль
            if not await client.is_user_authorized():
                print("\n🔐 Требуется пароль двухфакторной аутентификации")
                
                # Запрашиваем пароль (безопасно, не отображается при вводе)
                password = getpass.getpass("Введите ваш пароль Telegram: ")
                
                # Входим с паролем
                await client.sign_in(password=password)
                print("✅ Пароль принят!")
            
            # Получаем информацию о пользователе
            me = await client.get_me()
            print(f"\n✅ Успешная авторизация!")
            print(f"   Пользователь: {me.first_name} (@{me.username})")
            print(f"   ID: {me.id}")
            print(f"   Телефон: {me.phone}")
            
            # Сохраняем строку сессии
            session_string = client.session.save()
            print(f"\n🔐 СОХРАНИ ЭТУ СТРОКУ В SECRETS:")
            print(f"STRING_SESSION={session_string}")
            
        except asyncio.TimeoutError:
            print("\n❌ Таймаут. Попробуй еще раз")
        except SessionPasswordNeededError:
            print("\n❌ Ошибка: нужен пароль, но мы его уже обработали")
            
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(qr_login())
