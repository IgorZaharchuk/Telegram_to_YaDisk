#!/usr/bin/env python3
"""
Создание StringSession для Pyrogram
Работает 100%
Поддерживает: обычный вход, 2FA, повторная отправка кода
"""

import asyncio
import sys
from pyrogram import Client
from pyrogram.errors import (
    PhoneNumberInvalid, PhoneCodeInvalid, PhoneCodeExpired,
    SessionPasswordNeeded, FloodWait
)

async def create_session():
    print("\n" + "="*60)
    print("🔐 Создание STRING_SESSION для Pyrogram")
    print("="*60 + "\n")
    
    # Ввод API данных
    api_id = input("Введите API_ID: ").strip()
    if not api_id.isdigit():
        print("❌ API_ID должен быть числом")
        return
    
    api_id = int(api_id)
    api_hash = input("Введите API_HASH: ").strip()
    if not api_hash:
        print("❌ API_HASH не может быть пустым")
        return
    
    phone = input("Введите номер телефона (в формате +7...): ").strip()
    if not phone:
        print("❌ Номер телефона не может быть пустым")
        return
    
    client = Client(
        name="pyro_session_maker",
        api_id=api_id,
        api_hash=api_hash,
        in_memory=True,
        workdir="./sessions"  # Сохраняем временные файлы в отдельную папку
    )
    
    try:
        await client.connect()
        print(f"\n📱 Отправляю код на {phone}...")
        
        # Отправляем код с обработкой FloodWait
        try:
            sent_code = await client.send_code(phone)
        except FloodWait as e:
            print(f"⚠️ Слишком много попыток. Подождите {e.value} секунд")
            return
        except PhoneNumberInvalid:
            print("❌ Неверный формат номера телефона")
            return
        
        # Цикл ввода кода (с возможностью повторной отправки)
        code_attempts = 0
        while code_attempts < 3:
            code = input("📨 Введите код из Telegram (или 'resend' для повторной отправки): ").strip()
            
            if code.lower() == 'resend':
                print("📱 Отправляю код повторно...")
                try:
                    sent_code = await client.resend_code(phone, sent_code.phone_code_hash)
                    continue
                except Exception as e:
                    print(f"❌ Ошибка при повторной отправке: {e}")
                    return
            
            try:
                # Пытаемся войти с кодом
                await client.sign_in(phone, sent_code.phone_code_hash, code)
                break  # Успешный вход
                
            except SessionPasswordNeeded:
                # Если включена двухфакторка
                print("🔐 Требуется пароль двухфакторной аутентификации")
                password = input("Введите пароль: ").strip()
                try:
                    await client.check_password(password)
                    break
                except Exception as e:
                    print(f"❌ Неверный пароль: {e}")
                    return
                    
            except PhoneCodeInvalid:
                print("❌ Неверный код. Попробуйте ещё раз")
                code_attempts += 1
                
            except PhoneCodeExpired:
                print("❌ Код истёк. Запросите новый")
                print("📱 Отправляю код повторно...")
                sent_code = await client.resend_code(phone, sent_code.phone_code_hash)
                code_attempts = 0  # Сбрасываем счётчик попыток
                continue
                
            except FloodWait as e:
                print(f"⚠️ Слишком много попыток. Подождите {e.value} секунд")
                return
        
        if code_attempts >= 3:
            print("❌ Слишком много неверных попыток")
            return
        
        print("✅ Успешный вход!\n")
        
        # Получаем строку сессии
        session_string = await client.export_session_string()
        
        print("="*60)
        print("✅ STRING_SESSION для Pyrogram:")
        print("\n" + session_string + "\n")
        print("="*60)
        
        # Информация о пользователе
        me = client.me
        print(f"\n👤 Пользователь: {me.first_name or ''} {me.last_name or ''}".strip())
        if me.username:
            print(f"📧 Username: @{me.username}")
        print(f"🆔 ID: {me.id}")
        print(f"📱 Номер: {phone}")
        
        # Спрашиваем, сохранить ли в файл
        save = input("\n💾 Сохранить сессию в файл session.txt? (y/N): ").strip().lower()
        if save == 'y':
            with open('session.txt', 'w') as f:
                f.write(session_string)
            print("✅ Сессия сохранена в session.txt")
        
    except KeyboardInterrupt:
        print("\n\n👋 Работа прервана пользователем")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
    finally:
        await client.stop()
        print("\n🔒 Соединение закрыто")

def main():
    """Точка входа с обработкой ошибок"""
    try:
        asyncio.run(create_session())
    except KeyboardInterrupt:
        print("\n\n👋 Пока!")
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
