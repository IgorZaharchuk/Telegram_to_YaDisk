"""
Telegram клиент на базе Pyrogram
Полная версия с правильной инициализацией чата
Основано на: https://docs.pyrogram.org/api/methods/get_chat
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
        Получение информации о чате с правильной инициализацией
        Основано на: https://docs.pyrogram.org/api/methods/get_chat
        и реальных проектах
        """
        try:
            # Пробуем преобразовать в число, если это строка
            original_id = chat_id
            if isinstance(chat_id, str):
                try:
                    chat_id = int(chat_id)
                except ValueError:
                    # Если это username, оставляем как есть
                    pass
            
            logger.info(f"🔍 Получаю информацию о чате: {original_id}")
            
            # Пытаемся получить чат напрямую
            chat = await self.client.get_chat(chat_id)
            
            # Определяем тип чата
            if chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
                chat_type = "группа"
            elif chat.type == ChatType.CHANNEL:
                chat_type = "канал"
            else:
                chat_type = "личный"
            
            logger.info(f"✅ Чат найден: {getattr(chat, 'title', 'Личный чат')} ({chat_type})")
            logger.info(f"   ID: {chat.id}")
            return chat
            
        except (KeyError, ValueError) as e:
            logger.warning(f"⚠️ Не удалось получить чат {original_id} напрямую: {e}")
            logger.info("🔄 Получаю список всех диалогов для инициализации...")
            
            # Получаем все диалоги, чтобы "познакомить" клиента с чатом
            found = None
            async for dialog in self.client.get_dialogs():
                if str(dialog.chat.id) == str(original_id) or dialog.chat.username == original_id:
                    found = dialog.chat
                    logger.info(f"✅ Чат найден в диалогах: {getattr(dialog.chat, 'title', 'Личный чат')}")
                    break
            
            if found:
                return found
            
            # Если не нашли, пробуем resolve_peer
            try:
                logger.info(f"🔄 Пробую resolve_peer для {original_id}...")
                peer = await self.client.resolve_peer(original_id)
                # Теперь должен работать
                chat = await self.client.get_chat(original_id)
                logger.info(f"✅ Чат успешно инициализирован через resolve_peer")
                return chat
            except Exception as e2:
                logger.error(f"❌ Не удалось инициализировать чат: {e2}")
                raise ValueError(f"Чат {original_id} не найден. Убедитесь, что аккаунт подписан на этот чат")
    
    def get_topic_id_from_message(self, message):
        """
        Получение ID темы из сообщения
        В Pyrogram ID темы лежит в message.reply_to_top_id
        """
        if not message:
            return None
        
        if hasattr(message, 'reply_to_top_id') and message.reply_to_top_id:
            return message.reply_to_top_id
        
        return None
    
    async def get_topic_name(self, chat_id, topic_id: int) -> str | None:
        """
        Получение названия темы по ID через Pyrogram
        Основано на: https://docs.pyrogram.org/api/methods/invoke
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
                    logger.debug(f"Атрибуты темы: {dir(topic)}")
            else:
                logger.warning(f"⚠️ topics пуст или отсутствует в ответе")
                    
        except Exception as e:
            logger.error(f"❌ Ошибка получения названия темы {topic_id}: {e}", exc_info=True)
        
        return None
    
    async def get_messages(self, chat_id, min_id: int = 0, reverse: bool = True):
        """
        Получение сообщений из чата
        Основано на: https://docs.pyrogram.org/api/methods/get_chat_history
        """
        try:
            # Преобразуем ID в число, если это строка
            if isinstance(chat_id, str):
                try:
                    chat_id = int(chat_id)
                except ValueError:
                    # Если это username, оставляем как есть
                    pass
            
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
        """
        Скачивание медиафайла
        Основано на: https://docs.pyrogram.org/api/methods/download_media
        """
        logger.info(f"📥 Скачивание: {path}")
        return await self.client.download_media(message, file_name=path)
