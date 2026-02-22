"""
Telegram клиент на базе Pyrogram
Исправленная версия для правильного определения тем
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
        self.dialog_cache = {}  # Кэш для диалогов
    
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
        """Получение информации о чате"""
        try:
            if isinstance(chat_id, str):
                try:
                    chat_id = int(chat_id)
                except ValueError:
                    pass
            
            logger.info(f"🔍 Получаю информацию о чате: {chat_id}")
            chat = await self.client.get_chat(chat_id)
            
            chat_type = "группа" if chat.type in [ChatType.GROUP, ChatType.SUPERGROUP] else "канал" if chat.type == ChatType.CHANNEL else "личный"
            logger.info(f"✅ Чат: {getattr(chat, 'title', 'Личный чат')} ({chat_type})")
            return chat
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения чата: {e}")
            raise
    
    async def refresh_dialogs(self):
        """Обновление кэша диалогов"""
        self.dialog_cache = {}
        async for dialog in self.client.get_dialogs():
            self.dialog_cache[str(dialog.chat.id)] = dialog.chat
            if dialog.chat.username:
                self.dialog_cache[dialog.chat.username] = dialog.chat
        logger.info(f"🔄 Загружено {len(self.dialog_cache)} диалогов в кэш")
    
    def get_topic_id_from_message(self, message):
        """
        Получение ID темы из сообщения
        Основано на официальной документации Pyrogram и реальных проектах
        """
        if not message:
            return None
        
        # Способ 1: Прямое поле reply_to_top_id (основной для Pyrogram)
        if hasattr(message, 'reply_to_top_id') and message.reply_to_top_id:
            logger.debug(f"✅ Найден reply_to_top_id: {message.reply_to_top_id}")
            return message.reply_to_top_id
        
        # Способ 2: Проверяем, есть ли message_thread_id
        if hasattr(message, 'message_thread_id') and message.message_thread_id:
            logger.debug(f"📌 Найден message_thread_id: {message.message_thread_id}")
            return message.message_thread_id
        
        # Способ 3: Проверяем атрибуты reply_to
        if hasattr(message, 'reply_to') and message.reply_to:
            logger.debug(f"📎 Есть reply_to: {message.reply_to}")
        
        return None
    
    async def get_topic_name(self, chat_id, topic_id: int) -> str | None:
        """Получение названия темы по ID"""
        try:
            logger.info(f"🔍 Запрашиваю название для темы ID: {topic_id}")
            
            # Сначала проверяем, есть ли тема в истории сообщений
            # TODO: Implement proper topic name fetching
            
            # Пока возвращаем None, будем использовать ID
            return None
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения названия темы {topic_id}: {e}")
            return None
    
    async def get_messages(self, chat_id, min_id: int = 0, reverse: bool = True):
        """Получение сообщений из чата"""
        try:
            if isinstance(chat_id, str):
                try:
                    chat_id = int(chat_id)
                except ValueError:
                    pass
            
            messages = []
            async for message in self.client.get_chat_history(chat_id):
                if message.id > min_id:
                    messages.append(message)
                    # Логируем информацию о теме для отладки
                    if hasattr(message, 'reply_to_top_id') and message.reply_to_top_id:
                        logger.debug(f"📌 Сообщение {message.id} в теме {message.reply_to_top_id}")
            
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
