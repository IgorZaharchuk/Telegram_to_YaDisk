#!/usr/bin/env python3
"""
Создание StringSession для Pyrogram через QR-код
Исправленная версия
"""

import asyncio
import qrcode
from pyrogram import Client

async def create_session():
    api_id = int(input("Введите API_ID: "))
    api_hash = input("Введите API_HASH: ")
    
    print("\n📱 Запускаю QR-логин для Pyrogram...")
    print("1️⃣ Открой Telegram на телефоне")
    print("2️⃣ Перейди в Настройки → Устройства")
    print("3️⃣ Нажми 'Сканировать QR-код'")
    print("4️⃣ Отсканируй QR-код ниже\n")
    
    # Создаем клиента с пустой строкой сессии
    client = Client(
        name="pyro_qr_maker",
        api_id=api_id,
        api_hash=api_hash,
        in_memory=True
    )
    
    try:
        # Запускаем клиент
        await client.start()
        
        # Получаем QR-код для входа
        qr_login = await client.qr_login()
        
        # Показываем QR-код
        qr = qrcode.QRCode(box_size=2, border=1)
        qr.add_data(qr_login.url)
        qr.print_ascii(invert=True)
        
        print(f"\n🔗 Или открой ссылку: {qr_login.url}")
        print("\n⏳ Ожидание сканирования (60 секунд)...")
        
        try:
            # Ждем сканирования
            await qr_login.wait(60)
            print("\n✅ QR-код отсканирован!")
            
            # Если запросит пароль 2FA
            if not client.me:
                password = input("Введите пароль двухфакторной аутентификации: ")
                await client.check_password(password)
            
            print("\n✅ Авторизация успешна!")
            
            # Получаем строку сессии
            session_string = await client.export_session_string()
            print("\n" + "="*60)
            print("✅ НОВАЯ STRING_SESSION для Pyrogram:")
            print(session_string)
            print("="*60)
            
            # Информация о пользователе
            me = client.me
            print(f"\n👤 Пользователь: {me.first_name} (@{me.username})")
            print(f"📱 Телефон: +{me.phone_number}")
            print(f"🆔 ID: {me.id}")
            
        except asyncio.TimeoutError:
            print("\n❌ Таймаут. Попробуйте еще раз")
            
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.stop()

if __name__ == "__main__":
    asyncio.run(create_session())
