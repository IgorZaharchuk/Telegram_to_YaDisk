import asyncio
import qrcode
from telethon import TelegramClient
from telethon.sessions import StringSession

async def qr_login():
    api_id = 38713310
    api_hash = "d5bf6522bb54a8165991b066d898f90c"
    
    # Создаем клиент
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()
    
    # Запускаем QR-логин
    async with client.qr_login() as qr_login:
        # Показываем QR-код в терминале
        qr = qrcode.QRCode()
        qr.add_data(qr_login.url)
        qr.print_ascii(invert=True)
        print(f"\nИли открой ссылку: {qr_login.url}")
        print("Отсканируй QR-код в Telegram (Настройки → Устройства → Сканировать QR)")
        
        # Ждем сканирования
        try:
            await qr_login.wait()
            print("✅ Успешная авторизация!")
            
            # Сохраняем сессию как строку
            session_string = client.session.save()
            print(f"\nСохрани эту строку в secrets как STRING_SESSION:")
            print(session_string)
            
        except TimeoutError:
            print("❌ Таймаут. Попробуй еще раз")
    
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(qr_login())
