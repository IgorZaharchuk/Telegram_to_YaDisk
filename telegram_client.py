"""
Telegram клиент на базе Pyrogram
Полная версия с правильным получением чата
Основано на: https://docs.pyrogram.org/
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
        self._me = None
    
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
            
            # Сохраняем информацию о пользователе
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
        Получение информации о чате с правильной инициализацией
        Основано на: https://docs.pyrogram.org/api/methods/get_chat
        """
        try:
            # Сохраняем исходное значение для логирования
            original_id = chat_id
            chat_id_for_log = str(chat_id).replace(str(self.api_id), "***") if self.api_id else "***"
            
            logger.info(f"🔍 Получаю информацию о чате: {chat_id_for_log}")
            
            # Пробуем преобразовать в число, если это строка
            if isinstance(chat_id, str):
                try:
                    chat_id = int(chat_id)
                    logger.debug(f"✅ Преобразовано в число: {chat_id}")
                except ValueError:
                    logger.debug(f"ℹ️ Оставляем как строку (username): {chat_id}")
            
            # Способ 1: Прямой get_chat
            try:
                chat = await self.client.get_chat(chat_id)
                logger.info(f"✅ Чат найден через get_chat")
                
                # Определяем тип чата
                if chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
                    chat_type = "группа"
                elif chat.type == ChatType.CHANNEL:
                    chat_type = "канал"
                else:
                    chat_type = "личный"
                
                chat_title = getattr(chat, 'title', f"Личный чат с {chat.first_name}")
                logger.info(f"   Название: {chat_title}")
                logger.info(f"   Тип: {chat_type}")
                logger.info(f"   ID: {chat.id}")
                
                return chat
                
            except Exception as e:
                logger.debug(f"⚠️ get_chat не сработал: {e}")
            
            # Способ 2: Через resolve_peer
            try:
                logger.debug("🔄 Пробую resolve_peer...")
                peer = await self.client.resolve_peer(chat_id)
                logger.debug(f"✅ resolve_peer успешен: {peer}")
                
                # Теперь пробуем get_chat снова
                chat = await self.client.get_chat(chat_id)
                logger.info(f"✅ Чат найден через resolve_peer")
                
                return chat
                
            except Exception as e:
                logger.debug(f"⚠️ resolve_peer не сработал: {e}")
            
            # Способ 3: Через диалоги
            logger.debug("🔄 Ищу в диалогах...")
            async for dialog in self.client.get_dialogs(limit=200):
                if str(dialog.chat.id) == str(original_id) or dialog.chat.username == original_id:
                    logger.info(f"✅ Чат найден в диалогах: {getattr(dialog.chat, 'title', 'Личный чат')}")
                    
                    # Сохраняем в кэш для будущих запросов
                    try:
                        await self.client.resolve_peer(dialog.chat.id)
                    except:
                        pass
                    
                    return dialog.chat
            
            # Если ничего не сработало
            raise ValueError(f"Чат {chat_id_for_log} не найден. Убедитесь, что аккаунт подписан на этот чат")
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения чата: {e}")
            raise
    
    async def ensure_chat_known(self, chat_id):
        """
        Убедиться, что чат известен клиенту (загрузить в кэш)
        """
        try:
            await self.client.resolve_peer(chat_id)
            return True
        except:
            return False
    
    async def refresh_dialogs(self, limit=200):
        """
        Обновить кэш диалогов
        """
        count = 0
        async for dialog in self.client.get_dialogs(limit=limit):
            count += 1
            # Просто проходим по диалогам, они автоматически кэшируются
            pass
        logger.info(f"🔄 Загружено {count} диалогов в кэш")
        return count
    
    def get_topic_id_from_message(self, message):
        """
        Получение ID темы из сообщения
        В Pyrogram ID темы лежит в разных местах в зависимости от типа сообщения
        """
        if not message:
            return None
        
        # Способ 1: Прямое поле reply_to_top_id (основной для сообщений в темах)
        if hasattr(message, 'reply_to_top_id') and message.reply_to_top_id:
            logger.debug(f"✅ Найден reply_to_top_id: {message.reply_to_top_id}")
            return message.reply_to_top_id
        
        # Способ 2: Через message_thread_id (альтернативный способ)
        if hasattr(message, 'message_thread_id') and message.message_thread_id:
            logger.debug(f"✅ Найден message_thread_id: {message.message_thread_id}")
            return message.message_thread_id
        
        # Способ 3: Если это ответ на сообщение в теме
        if hasattr(message, 'reply_to_message_id') and message.reply_to_message_id:
            logger.debug(f"📎 Это ответ на сообщение {message.reply_to_message_id}")
            # Здесь мы не можем сразу определить тему, нужен parent message
            # Вернем None, чтобы обработать позже при необходимости
        
        return None
    
    async def get_topic_name(self, chat_id, topic_id: int) -> str | None:
        """
        Получение названия темы по ID
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
            message_count = 0
            
            async for message in self.client.get_chat_history(chat_id):
                message_count += 1
                if message.id > min_id:
                    messages.append(message)
                    
                    # Логируем каждый 10-й для отладки
                    if message_count % 10 == 0:
                        logger.debug(f"📨 Получено {message_count} сообщений...")
            
            if reverse:
                messages.sort(key=lambda x: x.id)
            
            logger.info(f"📨 Всего получено {len(messages)} новых сообщений (из {message_count} проверенных)")
            return messages
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения сообщений: {e}")
            raise
    
    async def download_media(self, message, path: str) -> str:
        """
        Скачивание медиафайла
        Основано на: https://docs.pyrogram.org/api/methods/download_media
        """
        try:
            logger.debug(f"📥 Начинаю скачивание в {path}")
            result = await self.client.download_media(message, file_name=path)
            logger.debug(f"✅ Скачивание завершено")
            return result
        except Exception as e:
            logger.error(f"❌ Ошибка скачивания: {e}")
            raise
    
    @property
    def me(self):
        """Информация о текущем пользователе"""
        return self._me
