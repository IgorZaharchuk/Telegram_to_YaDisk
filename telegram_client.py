"""
Telegram клиент на базе Telethon с улучшенным определением тем
"""

import os
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
        if self.client:
            await self.client.disconnect()
            logger.info("🔒 Отключено от Telegram")
    
    async def get_chat(self, chat_id: int):
        return await self.client.get_entity(chat_id)
    
    async def get_topic_name(self, chat_id: int, topic_id: int) -> str | None:
        """Получение человеческого названия темы по ID"""
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
    
    async def get_topic_id_from_message(self, message) -> int | None:
        """
        Получение ID темы из сообщения
        Поддерживает разные форматы:
        - Сообщения в темах (reply_to.reply_to_top_id)
        - Ответы на сообщения в темах
        - Служебные сообщения о создании темы
        """
        try:
            # Если сообщение имеет reply_to
            if message.reply_to:
                # Для сообщений в темах (не General)
                if hasattr(message.reply_to, 'reply_to_top_id') and message.reply_to.reply_to_top_id:
                    return message.reply_to.reply_to_top_id
                
                # Для ответов на сообщения
                if hasattr(message.reply_to, 'reply_to_msg_id') and message.reply_to.reply_to_msg_id:
                    # Проверяем, является ли parent сообщением в теме
                    try:
                        parent = await message.get_reply_message()
                        if parent and parent.reply_to:
                            if hasattr(parent.reply_to, 'reply_to_top_id') and parent.reply_to.reply_to_top_id:
                                return parent.reply_to.reply_to_top_id
                    except:
                        pass
            
            # Если сообщение о создании темы
            if hasattr(message, 'action') and hasattr(message.action, 'title'):
                # Это служебное сообщение о создании темы
                return message.id
            
        except Exception as e:
            logger.debug(f"Ошибка при определении темы: {e}")
        
        return None
    
    async def get_messages(self, chat_id: int, min_id: int = 0, reverse: bool = True):
        chat = await self.get_chat(chat_id)
        return self.client.iter_messages(chat, min_id=min_id, reverse=reverse)
    
    async def download_media(self, message, path: str) -> str:
        logger.info(f"📥 Скачивание: {path}")
        return await self.client.download_media(message, path)
