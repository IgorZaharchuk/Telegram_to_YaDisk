"""
Telegram клиент на базе Telethon с поддержкой StringSession
"""

import os
from telethon import TelegramClient, functions
from telethon.sessions import StringSession
import logging

logger = logging.getLogger(__name__)

class TelegramDownloader:
    def __init__(self, api_id: int, api_hash: str, session_string: str = None, session_file: str = "user_session"):
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_string = session_string
        self.session_file = session_file
        self.client = None
    
    async def connect(self):
        """Подключение к Telegram с использованием StringSession если есть"""
        
        # Если есть строка сессии, используем её
        if self.session_string:
            logger.info("🔑 Использую StringSession из переменной окружения")
            self.client = TelegramClient(StringSession(self.session_string), self.api_id, self.api_hash)
        else:
            # Иначе используем файловую сессию
            logger.info("📁 Использую файловую сессию")
            self.client = TelegramClient(self.session_file, self.api_id, self.api_hash)
        
        await self.client.connect()
        
        # Проверяем, авторизован ли
        if not await self.client.is_user_authorized():
            raise Exception("❌ Сессия недействительна")
        
        logger.info("✅ Подключено к Telegram")
        return self
    
    async def disconnect(self):
        """Отключение от Telegram"""
        if self.client:
            await self.client.disconnect()
            logger.info("🔒 Отключено от Telegram")
    
    async def get_chat(self, chat_id: int):
        """Получение информации о чате"""
        return await self.client.get_entity(chat_id)
    
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
