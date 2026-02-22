"""
Telegram клиент на базе официальной версии Pyrogram
Получение тем через raw API (GetForumTopics)
"""

import os
from pyrogram import Client
from pyrogram.enums import ChatType
from pyrogram.raw.functions.channels import GetForumTopics
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
        self.topics_cache = {}  # Кэш для тем {topic_id: topic_name}
    
    async def connect(self):
        """Подключение к Telegram"""
        try:
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
            logger.info("✅ Подключено к Telegram")
            
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
        """
        Получение информации о чате через диалоги
        """
        try:
            original_id = str(chat_id)
            logger.info(f"🔍 Ищу чат: {original_id}")
            
            async for dialog in self.client.get_dialogs(limit=200):
                if str(dialog.chat.id) == original_id:
                    chat = dialog.chat
                    chat_title = getattr(chat, 'title', 'Личный чат')
                    logger.info(f"✅ Чат найден: {chat_title}")
                    logger.info(f"   ID: {chat.id}")
                    logger.info(f"   is_forum: {getattr(chat, 'is_forum', False)}")
                    return chat
            
            raise ValueError(f"Чат {original_id} не найден")
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения чата: {e}")
            raise
    
    async def load_all_topics(self, chat_id):
        """
        Загрузка всех тем чата через raw API GetForumTopics
        Возвращает словарь {topic_id: topic_name} с ключами-строками
        """
        try:
            logger.info(f"📚 Загружаю все темы чата через raw API...")
            
            # Получаем InputChannel для API запроса
            channel = await self.client.resolve_peer(chat_id)
            
            # Прямой запрос к API для получения тем
            result = await self.client.invoke(
                GetForumTopics(
                    channel=channel,
                    offset_date=0,
                    offset_id=0,
                    offset_topic=0,
                    limit=100
                )
            )
            
            # Сохраняем в кэш (ключи как строки для JSON)
            if hasattr(result, 'topics') and result.topics:
                for topic in result.topics:
                    self.topics_cache[str(topic.id)] = topic.title
                logger.info(f"✅ Загружено {len(self.topics_cache)} тем:")
                for topic_id, topic_name in self.topics_cache.items():
                    logger.info(f"   📁 {topic_name} (ID: {topic_id})")
            else:
                logger.info("ℹ️ В чате нет тем или форум отключен")
            
            return self.topics_cache
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки тем: {e}")
            return {}
    
    def get_topic_name(self, topic_id):
        """Получение названия темы по ID из кэша"""
        return self.topics_cache.get(str(topic_id))
    
    def get_topic_id_from_message(self, message):
        """
        Получение ID темы из сообщения
        В Telegram ID темы совпадает с ID первого сообщения в теме,
        на которое отвечают другие сообщения
        """
        if not message:
            return None
        
        # Проверяем наличие reply_to_message_id
        if hasattr(message, 'reply_to_message_id') and message.reply_to_message_id:
            reply_id = message.reply_to_message_id
            # Проверяем, есть ли это ID в кэше тем
            if str(reply_id) in self.topics_cache:
                logger.debug(f"✅ Найден ID темы в reply_to_message_id: {reply_id}")
                return reply_id
        
        return None
    
    async def get_messages(self, chat_id, min_id: int = 0, limit: int = 200):
        """Получение сообщений из чата"""
        try:
            messages = []
            async for msg in self.client.get_chat_history(chat_id, limit=limit):
                if msg.id > min_id:
                    messages.append(msg)
            
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
    
    @property
    def me(self):
        return self._me
