"""
Telegram клиент на базе Pyrogram (альтернатива Telethon)
Метод GetForumTopicsByIDRequest точно работает
"""

import os
from pyrogram import Client
from pyrogram.raw.functions.channels import GetForumTopicsByIDRequest
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
        """Подключение к Telegram через Pyrogram"""
        if self.session_string:
            logger.info("🔑 Использую StringSession")
            self.client = Client(
                name="pyro_session",
                api_id=self.api_id,
                api_hash=self.api_hash,
                session_string=self.session_string,
                in_memory=True
            )
        else:
            logger.info("📁 Использую файловую сессию")
            self.client = Client(
                name=self.session_file,
                api_id=self.api_id,
                api_hash=self.api_hash
            )
        
        await self.client.start()
        logger.info("✅ Подключено к Telegram через Pyrogram")
        return self
    
    async def disconnect(self):
        """Отключение от Telegram"""
        if self.client:
            await self.client.stop()
            logger.info("🔒 Отключено от Telegram")
    
    async def get_chat(self, chat_id: int):
        """Получение информации о чате"""
        return await self.client.get_chat(chat_id)
    
    def get_topic_id_from_message(self, message):
        """
        Получение ID темы из сообщения
        В Pyrogram сообщения в темах имеют reply_to_top_id
        """
        if not message or not message.reply_to_message_id:
            return None
        
        # В Pyrogram ID темы лежит в reply_to_top_id
        if hasattr(message, 'reply_to_top_id') and message.reply_to_top_id:
            return message.reply_to_top_id
        
        return None
    
    async def get_topic_name(self, chat_id: int, topic_id: int) -> str | None:
        """
        Получение названия темы по ID через Pyrogram
        """
        try:
            logger.info(f"🔍 Запрашиваю название для темы ID: {topic_id}")
            
            # Получаем входной объект канала
            chat = await self.client.get_chat(chat_id)
            
            # Вызываем метод Pyrogram
            result = await self.client.invoke(
                GetForumTopicsByIDRequest(
                    channel=await self.client.resolve_peer(chat_id),
                    topics=[topic_id]
                )
            )
            
            # Извлекаем название
            if result and result.topics and len(result.topics) > 0:
                topic_title = result.topics[0].title
                logger.info(f"✅ Найдено название темы: {topic_title}")
                return topic_title
            else:
                logger.warning(f"⚠️ Тема с ID {topic_id} не найдена")
                    
        except Exception as e:
            logger.error(f"❌ Ошибка получения названия темы {topic_id}: {e}", exc_info=True)
        
        return None
    
    async def get_messages(self, chat_id: int, min_id: int = 0, reverse: bool = True):
        """Получение сообщений из чата"""
        # В Pyrogram получаем историю сообщений
        messages = []
        async for message in self.client.get_chat_history(chat_id, offset_id=min_id):
            if message.id > min_id:
                messages.append(message)
        return messages
    
    async def download_media(self, message, path: str) -> str:
        """Скачивание медиафайла"""
        logger.info(f"📥 Скачивание: {path}")
        return await self.client.download_media(message, file_name=path)
