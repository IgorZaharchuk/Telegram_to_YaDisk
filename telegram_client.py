"""
Telegram клиент на базе Telethon
Готовое решение для скачивания файлов и получения названий тем
"""

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.channels import GetForumTopicsByIDRequest
import logging

logger = logging.getLogger(__name__)

class TelegramDownloader:
    def __init__(self, api_id: int, api_hash: str, phone: str, session_file: str = "user_session"):
        """
        Инициализация клиента Telegram
        :param api_id: API ID с my.telegram.org
        :param api_hash: API Hash с my.telegram.org
        :param phone: Номер телефона в формате +79123456789
        :param session_file: Файл для сохранения сессии
        """
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
        :param chat_id: ID чата
        :param topic_id: ID темы
        :return: Название темы или None
        """
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
        """
        Получение сообщений из чата
        :param chat_id: ID чата
        :param min_id: Минимальный ID сообщения
        :param reverse: Сортировка от старых к новым
        :return: AsyncIterator сообщений
        """
        chat = await self.get_chat(chat_id)
        return self.client.iter_messages(chat, min_id=min_id, reverse=reverse)
    
    async def download_media(self, message, path: str) -> str:
        """
        Скачивание медиафайла из сообщения
        :param message: Сообщение из Telegram
        :param path: Путь для сохранения
        :return: Путь к скачанному файлу
        """
        logger.info(f"📥 Скачивание: {path}")
        return await self.client.download_media(message, path)
