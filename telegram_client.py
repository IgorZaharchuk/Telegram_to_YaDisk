"""
Telegram клиент на базе Pyrogram (форк с поддержкой тем)
Установка в workflow: pip install git+https://github.com/KurimuzonAkuma/pyrogram.git@master
"""

import os
from pyrogram import Client
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
        self._me = None
    
    async def connect(self):
        """Подключение к Telegram"""
        try:
            if self.session_string:
                logger.info("🔑 Использую StringSession для форка Pyrogram")
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
            logger.info("✅ Подключено к Telegram через форк Pyrogram")
            
            self._me = self.client.me
            logger.info(f"👤 Пользователь: {self._me.first_name} (@{self._me.username})")
            
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
        """Получение информации о чате через диалоги"""
        try:
            original_id = str(chat_id)
            logger.info(f"🔍 Ищу чат: {original_id}")
            
            async for dialog in self.client.get_dialogs(limit=200):
                if str(dialog.chat.id) == original_id:
                    chat = dialog.chat
                    chat_title = getattr(chat, 'title', 'Личный чат')
                    logger.info(f"✅ Чат найден: {chat_title}")
                    logger.info(f"   Форум: {getattr(chat, 'is_forum', False)}")
                    return chat
            
            raise ValueError(f"Чат {original_id} не найден")
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения чата: {e}")
            raise
    
    def get_topic_info(self, message):
        """
        Получение информации о теме из сообщения
        В форке Pyrogram у сообщения есть поле topic
        """
        if not message:
            return None, None
        
        # В форке KurimuzonAkuma добавлено поле topic
        if hasattr(message, 'topic') and message.topic:
            return message.topic.id, message.topic.name
        
        # Стандартные поля (могут не работать в официальной версии)
        if hasattr(message, 'reply_to_top_message_id') and message.reply_to_top_message_id:
            return message.reply_to_top_message_id, None
        
        return None, None
    
    async def get_messages(self, chat_id, min_id: int = 0, limit: int = 100):
        """Получение сообщений из чата"""
        try:
            messages = []
            async for msg in self.client.get_chat_history(chat_id, limit=limit):
                if msg.id > min_id:
                    messages.append(msg)
                    
                    # Логируем наличие тем для отладки
                    if hasattr(msg, 'topic') and msg.topic:
                        logger.debug(f"📌 Сообщение {msg.id} в теме: {msg.topic.name} (ID: {msg.topic.id})")
            
            messages.sort(key=lambda x: x.id)
            logger.info(f"📨 Получено {len(messages)} сообщений")
            return messages
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения сообщений: {e}")
            raise
    
    async def download_media(self, message, path: str) -> str:
        """Скачивание медиафайла"""
        logger.info(f"📥 Скачивание: {path}")
        return await self.client.download_media(message, file_name=path)
