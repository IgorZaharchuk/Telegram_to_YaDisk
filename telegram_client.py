"""
Telegram клиент на базе Pyrogram
Исправленная версия с правильной обработкой ID
"""

import os
from pyrogram import Client
from pyrogram.raw.functions.channels import GetForumTopicsByID
from pyrogram.enums import ChatType
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
        try:
            if self.session_string:
                logger.info("🔑 Использую StringSession для Pyrogram")
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
            
            # Показываем информацию о пользователе
            me = self.client.me
            logger.info(f"👤 Пользователь: {me.first_name} (@{me.username})")
            
            return self
            
        except Exception as e:
            logger.error(f"❌ Ошибка подключения: {e}")
            raise
    
    async def disconnect(self):
        """Отключение от Telegram"""
        if self.client and self.client.is_connected:
            await self.client.stop()
            logger.info("🔒 Отключено от Telegram")
    
    async def get_chat(self, chat_id):
        """
        Получение информации о чате
        Поддерживает разные форматы ID
        """
        try:
            # Преобразуем ID в целое число, если это строка
            if isinstance(chat_id, str):
                chat_id = int(chat_id)
            
            logger.info(f"🔍 Получаю информацию о чате с ID: {chat_id}")
            chat = await self.client.get_chat(chat_id)
            
            # Логируем информацию о чате
            chat_type = "группа" if chat.type in [ChatType.GROUP, ChatType.SUPERGROUP] else "канал" if chat.type == ChatType.CHANNEL else "личный"
            logger.info(f"✅ Чат: {chat.title if hasattr(chat, 'title') else 'Личный чат'} ({chat_type})")
            logger.info(f"   ID: {chat.id}")
            
            return chat
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения чата {chat_id}: {e}")
            raise
    
    def get_topic_id_from_message(self, message):
        """
        Получение ID темы из сообщения
        """
        if not message:
            return None
        
        # В Pyrogram ID темы лежит в message.reply_to_top_id
        if hasattr(message, 'reply_to_top_id') and message.reply_to_top_id:
            return message.reply_to_top_id
        
        return None
    
    async def get_topic_name(self, chat_id, topic_id: int) -> str | None:
        """
        Получение названия темы по ID через Pyrogram
        """
        try:
            logger.info(f"🔍 Запрашиваю название для темы ID: {topic_id}")
            
            # Получаем InputChannel
            channel = await self.client.resolve_peer(chat_id)
            
            # Вызываем метод Pyrogram
            result = await self.client.invoke(
                GetForumTopicsByID(
                    channel=channel,
                    topics=[topic_id]
                )
            )
            
            # Проверяем результат
            if result and hasattr(result, 'topics') and result.topics:
                topic = result.topics[0]
                if hasattr(topic, 'title'):
                    topic_title = topic.title
                    logger.info(f"✅ Найдено название темы: {topic_title}")
                    return topic_title
                else:
                    logger.warning(f"⚠️ Тема не имеет атрибута title")
                    
        except Exception as e:
            logger.error(f"❌ Ошибка получения названия темы {topic_id}: {e}", exc_info=True)
        
        return None
    
    async def get_messages(self, chat_id, min_id: int = 0, reverse: bool = True):
        """Получение сообщений из чата"""
        try:
            # Преобразуем ID в целое число, если это строка
            if isinstance(chat_id, str):
                chat_id = int(chat_id)
            
            messages = []
            async for message in self.client.get_chat_history(chat_id):
                if message.id > min_id:
                    messages.append(message)
            
            if reverse:
                messages.sort(key=lambda x: x.id)
            
            logger.info(f"📨 Получено {len(messages)} сообщений с ID > {min_id}")
            return messages
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения сообщений: {e}")
            raise
    
    async def download_media(self, message, path: str) -> str:
        """Скачивание медиафайла"""
        logger.info(f"📥 Скачивание: {path}")
        return await self.client.download_media(message, file_name=path)
