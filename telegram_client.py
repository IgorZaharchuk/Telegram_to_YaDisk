"""
Telegram клиент на базе Telethon
Исправленная версия с правильным импортом GetForumTopicsByIDRequest
"""

from telethon import TelegramClient
from telethon.tl.functions.channels import GetForumTopicsByIDRequest  # 👈 ПРАВИЛЬНЫЙ ИМПОРТ
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
        self.client = TelegramClient(self.session_file, self.api_id, self.api_hash)
        await self.client.start(phone=self.phone)
        logger.info("✅ Подключено к Telegram")
        return self
    
    async def disconnect(self):
        if self.client:
            await self.client.disconnect()
            logger.info("🔒 Отключено от Telegram")
    
    async def get_chat(self, chat_id: int):
        return await self.client.get_entity(chat_id)
    
    async def get_topic_name(self, chat_id: int, topic_id: int) -> str | None:
        """Получение человеческого названия темы"""
        try:
            chat = await self.client.get_input_entity(chat_id)
            result = await self.client(GetForumTopicsByIDRequest(
                channel=chat,
                topics=[topic_id]
            ))
            if result.topics and len(result.topics) > 0:
                return result.topics[0].title
        except Exception as e:
            logger.debug(f"Не удалось получить название темы {topic_id}: {e}")
        return None
    
    async def get_messages(self, chat_id: int, min_id: int = 0, reverse: bool = True):
        chat = await self.get_chat(chat_id)
        return self.client.iter_messages(chat, min_id=min_id, reverse=reverse)
    
    async def download_media(self, message, path: str) -> str:
        logger.info(f"📥 Скачивание: {path}")
        return await self.client.download_media(message, path)
