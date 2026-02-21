"""
Telegram клиент на базе Telethon
Исправленная версия с правильным методом для тем
"""

from telethon import TelegramClient, functions, types
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
        """Подключение к Telegram"""
        if self.session_string:
            logger.info("🔑 Использую StringSession")
            self.client = TelegramClient(StringSession(self.session_string), self.api_id, self.api_hash)
        else:
            logger.info("📁 Использую файловую сессию")
            self.client = TelegramClient(self.session_file, self.api_id, self.api_hash)
        
        await self.client.connect()
        
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
    
    def get_topic_id_from_message(self, message):
        """
        Получение ID темы из сообщения
        Источник: https://stackoverflow.com/a/79158221
        """
        if not message.reply_to:
            return None
        
        # Проверяем, является ли сообщение частью форума
        if hasattr(message.reply_to, 'forum_topic') and message.reply_to.forum_topic:
            # Приоритет: reply_to_top_id (если это ответ), иначе reply_to_msg_id
            if hasattr(message.reply_to, 'reply_to_top_id') and message.reply_to.reply_to_top_id:
                return message.reply_to.reply_to_top_id
            elif hasattr(message.reply_to, 'reply_to_msg_id'):
                return message.reply_to.reply_to_msg_id
        
        return None
    
    async def get_topic_name(self, chat_id: int, topic_id: int) -> str | None:
        """
        Получение названия темы по ID
        Альтернативный подход через GetForumTopicsRequest
        """
        try:
            logger.debug(f"get_topic_name: chat_id={chat_id}, topic_id={topic_id}")
            
            # Получаем входной объект канала
            channel = await self.client.get_input_entity(chat_id)
            
            # Используем GetForumTopicsRequest для получения ВСЕХ тем
            # и потом фильтруем по ID
            result = await self.client(functions.channels.GetForumTopicsRequest(
                channel=channel,
                offset_date=None,
                offset_id=0,
                offset_topic=0,
                limit=100
            ))
            
            # Ищем нужную тему по ID
            if hasattr(result, 'topics') and result.topics:
                for topic in result.topics:
                    if hasattr(topic, 'id') and topic.id == topic_id:
                        if hasattr(topic, 'title'):
                            topic_title = topic.title
                            logger.info(f"✅ Найдено название темы: {topic_title}")
                            return topic_title
                        else:
                            logger.warning(f"⚠️ Тема найдена, но нет атрибута title")
                            # Пробуем другие атрибуты
                            logger.debug(f"Topic attributes: {dir(topic)}")
            
            logger.warning(f"⚠️ Тема с ID {topic_id} не найдена среди {len(result.topics)} тем")
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения названия темы {topic_id}: {e}", exc_info=True)
        
        return None
    
    async def get_all_topics(self, chat_id: int):
        """
        Получение ВСЕХ тем чата
        """
        try:
            channel = await self.client.get_input_entity(chat_id)
            topics = []
            offset_date = None
            offset_id = 0
            offset_topic = 0
            
            while True:
                result = await self.client(functions.channels.GetForumTopicsRequest(
                    channel=channel,
                    offset_date=offset_date,
                    offset_id=offset_id,
                    offset_topic=offset_topic,
                    limit=100
                ))
                
                if not result.topics:
                    break
                
                topics.extend(result.topics)
                
                last = result.topics[-1]
                offset_topic = last.id
                offset_id = last.top_message
                
                # Находим дату последнего сообщения
                for msg in result.messages:
                    if msg.id == offset_id:
                        offset_date = msg.date
                        break
                        
            return topics
            
        except Exception as e:
            logger.error(f"Ошибка получения всех тем: {e}")
            return []
    
    async def get_messages(self, chat_id: int, min_id: int = 0, reverse: bool = True):
        """Получение сообщений из чата"""
        chat = await self.get_chat(chat_id)
        return self.client.iter_messages(chat, min_id=min_id, reverse=reverse)
    
    async def download_media(self, message, path: str) -> str:
        """Скачивание медиафайла"""
        logger.info(f"📥 Скачивание: {path}")
        return await self.client.download_media(message, path)
