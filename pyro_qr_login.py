#!/usr/bin/env python3
"""
Создание StringSession для Pyrogram через QR-код
Правильная версия с использованием on_qr_login
"""

import asyncio
import qrcode
from pyrogram import Client

async def create_session():
    api_id = int(input("Введите API_ID: "))
    api_hash = input("Введите API_HASH: ")
    
    # Создаем клиента с пустой строкой сессии
    client = Client(
        name="pyro_qr_session",
        api_id=api_id,
        api_hash=api_hash,
        in_memory=True
    )
    
    # Флаг для отслеживания успешного входа
    login_success = False
    qr_url = None
    
    @client.on_qr_login()
    async def qr_login(client, qr_code):
        nonlocal qr_url
        qr_url = qr_code.url
        
        print("\n📱 QR-код для входа в Telegram:")
        print("1️⃣ Открой Telegram на телефоне")
        print("2️⃣ Перейди в Настройки → Устройства")
        print("3️⃣ Нажми 'Сканировать QR-код'")
        print("4️⃣ Отсканируй QR-код ниже\n")
        
        # Показываем QR-код
        qr = qrcode.QRCode(box_size=2, border=1)
        qr.add_data(qr_url)
        qr.print_ascii(invert=True)
        print(f"\n🔗 Или открой ссылку: {qr_url}")
    
    try:
        print("\n🚀 Запускаю Pyrogram и жду QR-код...")
        
        # Запускаем клиент
        await client.start()
        
        # Если дошли сюда, значит вход уже был выполнен через код
        if client.me:
            print("\n✅ Уже авторизован через код!")
            login_success = True
        else:
            # Ждем сканирования QR-кода
            print("\n⏳ Ожидание сканирования QR-кода (60 секунд)...")
            await asyncio.sleep(60)
            
            if client.me:
                login_success = True
                print("\n✅ QR-код отсканирован! Авторизация успешна!")
        
        if login_success or client.me:
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
        else:
            print("\n❌ Таймаут. QR-код не был отсканирован")
            
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.stop()

if __name__ == "__main__":
    asyncio.run(create_session())
