"""
Telegram клиент на базе Telethon
Исправленная версия с улучшенной диагностикой
"""

import os
import sys
from telethon import TelegramClient, functions
import logging

logger = logging.getLogger(__name__)

class TelegramDownloader:
    def __init__(self, api_id: int, api_hash: str, phone: str, session_file: str = "user_session"):
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone = phone
        self.session_file = session_file
        self.client = None
    
    async def connect(self):
        """Подключение к Telegram с улучшенной обработкой ошибок"""
        try:
            # Проверяем наличие API_ID и API_HASH
            if not self.api_id or not self.api_hash:
                raise ValueError("API_ID и API_HASH должны быть заданы")
            
            if not self.phone:
                raise ValueError("PHONE_NUMBER должен быть задан")
            
            logger.info(f"🔑 Подключение с API_ID: {self.api_id}, телефон: {self.phone}")
            
            self.client = TelegramClient(self.session_file, self.api_id, self.api_hash)
            
            # Пытаемся подключиться
            await self.client.connect()
            
            # Проверяем, авторизованы ли уже
            if await self.client.is_user_authorized():
                logger.info("✅ Уже авторизован, использую существующую сессию")
                return self
            
            # Если не авторизованы, начинаем процесс входа
            logger.info("📱 Требуется авторизация. Отправляю код на телефон...")
            
            await self.client.send_code_request(self.phone)
            
            logger.info("📨 Код отправлен! Проверьте:")
            logger.info("   - Telegram на телефоне (чат 'Telegram')")
            logger.info("   - SMS (если не нашли в приложении)")
            logger.info("   - Через минуту будет доступен звонок")
            
            # Ждем код от пользователя
            code = input("Please enter the code you received: ")
            
            if not code:
                raise ValueError("Код не может быть пустым")
            
            # Пробуем войти с кодом
            await self.client.sign_in(phone=self.phone, code=code.strip())
            
            logger.info("✅ Подключено к Telegram")
            return self
            
        except Exception as e:
            logger.error(f"❌ Ошибка подключения: {e}")
            if "PHONE_NUMBER_INVALID" in str(e):
                logger.error("❌ Неправильный формат номера телефона. Должно быть +79123456789")
            elif "PHONE_CODE_INVALID" in str(e):
                logger.error("❌ Неправильный код подтверждения")
            elif "PHONE_NUMBER_FLOOD" in str(e):
                logger.error("❌ Слишком много попыток. Подождите 10-15 минут")
            raise
    
    async def disconnect(self):
        """Отключение от Telegram"""
        if self.client:
            await self.client.disconnect()
            logger.info("🔒 Отключено от Telegram")
    
    async def get_chat(self, chat_id: int):
        """Получение информации о чате"""
        try:
            return await self.client.get_entity(chat_id)
        except Exception as e:
            logger.error(f"❌ Не удалось получить чат {chat_id}: {e}")
            raise
    
    async def get_topic_name(self, chat_id: int, topic_id: int) -> str | None:
        """Получение человеческого названия темы"""
        try:
            channel = await self.client.get_input_entity(chat_id)
            result = await self.client(functions.channels.GetForumTopicsByIDRequest(
                channel=channel,
                topics=[topic_id]
            ))
            if result and result.topics and len(result.topics) > 0:
                return result.topics[0].title
        except Exception as e:
            logger.debug(f"Не удалось получить название темы {topic_id}: {e}")
        return None
    
    async def get_messages(self, chat_id: int, min_id: int = 0, reverse: bool = True):
        """Получение сообщений из чата"""
        chat = await self.get_chat(chat_id)
        return self.client.iter_messages(chat, min_id=min_id, reverse=reverse)
    
    async def download_media(self, message, path: str) -> str:
        """Скачивание медиафайла"""
        logger.info(f"📥 Скачивание: {path}")
        return await self.client.download_media(message, path)
