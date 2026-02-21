"""
Telegram клиент на базе Telethon
ОСНОВАНО НА ОФИЦИАЛЬНОЙ ДОКУМЕНТАЦИИ:
- https://tl.telethon.dev/methods/channels/get_forum_topics_by_id.html
- https://stackoverflow.com/questions/79157818/
"""

from telethon import TelegramClient, functions  # Правильный импорт!
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
        ИСТОЧНИК: https://tl.telethon.dev/methods/channels/get_forum_topics_by_id.html
        и https://stackoverflow.com/a/79158221
        """
        try:
            # Получаем InputChannel (как в документации)
            channel = await self.client.get_input_entity(chat_id)
            
            # ТОЧНО КАК В ДОКУМЕНТАЦИИ: client(functions.channels.GetForumTopicsByIDRequest(...))
            result = await self.client(functions.channels.GetForumTopicsByIDRequest(
                channel=channel,
                topics=[topic_id]  # список целых чисел
            ))
            
            # Извлекаем название из первого топика (поле title)
            if result and result.topics and len(result.topics) > 0:
                topic_title = result.topics[0].title
                logger.info(f"📁 Получено название темы: {topic_title}")
                return topic_title
                
        except Exception as e:
            logger.debug(f"Не удалось получить название темы {topic_id}: {e}")
        return None
    
    async def get_messages(self, chat_id: int, min_id: int = 0, reverse: bool = True):
        """Получение сообщений из чата (асинхронный итератор)"""
        chat = await self.get_chat(chat_id)
        return self.client.iter_messages(chat, min_id=min_id, reverse=reverse)
    
    async def download_media(self, message, path: str) -> str:
        """Скачивание медиафайла"""
        logger.info(f"📥 Скачивание: {path}")
        return await self.client.download_media(message, path)
