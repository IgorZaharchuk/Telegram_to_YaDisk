"""
Telegram клиент на базе Telethon
Исправленная версия с правильными типами для GetForumTopicsByIDRequest
"""

from telethon import TelegramClient
from telethon.tl.functions.channels import GetForumTopicsByIDRequest
from telethon.tl.types import InputChannel
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
        """Подключение к Telegram"""
        self.client = TelegramClient(self.session_file, self.api_id, self.api_hash)
        await self.client.start(phone=self.phone)
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
        """
        Получение человеческого названия темы
        Согласно документации: https://tl.telethon.dev/methods/channels/get_forum_topics_by_id.html
        """
        try:
            # Получаем InputChannel (правильный тип для API)
            chat = await self.client.get_input_entity(chat_id)
            
            # topics должен быть списком int
            result = await self.client(GetForumTopicsByIDRequest(
                channel=chat,
                topics=[topic_id]  # список целых чисел
            ))
            
            if result.topics and len(result.topics) > 0:
                # Название темы в поле title [citation:4]
                return result.topics[0].title
                
        except Exception as e:
            logger.debug(f"Не удалось получить название темы {topic_id}: {e}")
        return None
    
    async def get_messages(self, chat_id: int, min_id: int = 0, reverse: bool = True):
        """
        Получение сообщений из чата
        Возвращает асинхронный итератор
        """
        chat = await self.get_chat(chat_id)
        # iter_messages возвращает AsyncIterator, можно использовать async for
        return self.client.iter_messages(chat, min_id=min_id, reverse=reverse)
    
    async def download_media(self, message, path: str) -> str:
        """Скачивание медиафайла из сообщения"""
        logger.info(f"📥 Скачивание: {path}")
        return await self.client.download_media(message, path)
