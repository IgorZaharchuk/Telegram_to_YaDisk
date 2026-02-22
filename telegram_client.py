"""
Telegram клиент - самодостаточный модуль
Отвечает за: подключение, получение тем, скачивание файлов
ТОЧНО КАК В РАБОЧЕЙ ВЕРСИИ - ключи как строки
"""

import os
import tempfile
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, List, Any
import logging

from pyrogram import Client
from pyrogram.raw.functions.channels import GetForumTopics

logger = logging.getLogger(__name__)

@dataclass
class DownloadResult:
    """Результат скачивания файла из Telegram"""
    local_path: str
    original_filename: str
    file_type: str  # 'photo', 'video', 'document'
    topic_id: Optional[int]
    topic_name: Optional[str]
    chat_folder: str
    topic_folder: str
    message_id: int
    file_size: int

class TelegramDownloader:
    def __init__(self, config: dict):
        """
        :param config: Словарь с настройками:
            - api_id: int
            - api_hash: str
            - session_string: str (опционально)
            - session_file: str (опционально)
        """
        self.api_id = config['api_id']
        self.api_hash = config['api_hash']
        self.session_string = config.get('session_string')
        self.session_file = config.get('session_file', 'user_session')
        self.client = None
        self.topics_cache = {}  # {topic_id: topic_name} с ключами-строками!
        self._me = None
    
    async def connect(self) -> bool:
        """Подключение к Telegram"""
        try:
            if self.session_string:
                self.client = Client(
                    name="pyro_session",
                    api_id=self.api_id,
                    api_hash=self.api_hash,
                    session_string=self.session_string,
                    in_memory=True
                )
            else:
                self.client = Client(
                    name=self.session_file,
                    api_id=self.api_id,
                    api_hash=self.api_hash
                )
            
            await self.client.start()
            self._me = self.client.me
            logger.info(f"✅ Подключено к Telegram как @{self._me.username}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка подключения: {e}")
            return False
    
    async def disconnect(self):
        """Отключение от Telegram"""
        if self.client and self.client.is_connected:
            await self.client.stop()
            logger.info("🔒 Отключено от Telegram")
    
    async def find_chat(self, chat_id: int):
        """Поиск чата по ID через диалоги"""
        try:
            logger.info(f"🔍 Ищу чат {chat_id}...")
            async for dialog in self.client.get_dialogs(limit=200):
                if dialog.chat.id == chat_id:
                    logger.info(f"✅ Чат найден: {dialog.chat.title}")
                    return dialog.chat
            raise ValueError(f"Чат {chat_id} не найден")
        except Exception as e:
            logger.error(f"❌ Ошибка поиска чата: {e}")
            raise
    
    async def load_all_topics(self, chat_id: int) -> Dict[str, str]:
        """
        Загрузка всех тем чата через raw API GetForumTopics
        ТОЧНО КАК В РАБОЧЕЙ ВЕРСИИ - ключи как строки!
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
            
            # Сохраняем в кэш (ключи как СТРОКИ для JSON)
            if hasattr(result, 'topics') and result.topics:
                for topic in result.topics:
                    self.topics_cache[str(topic.id)] = topic.title  # 👈 СТРОКА!
                logger.info(f"✅ Загружено {len(self.topics_cache)} тем:")
                for topic_id, topic_name in self.topics_cache.items():
                    logger.info(f"   📁 {topic_name} (ID: {topic_id})")
            else:
                logger.info("ℹ️ В чате нет тем или форум отключен")
            
            return self.topics_cache
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки тем: {e}")
            return {}
    
    def get_topic_name(self, topic_id: int) -> Optional[str]:
        """Получение названия темы по ID из кэша"""
        return self.topics_cache.get(str(topic_id))  # 👈 СТРОКА!
    
    def get_topic_id_from_message(self, message) -> Optional[int]:
        """
        Получение ID темы из сообщения
        В Telegram ID темы совпадает с ID первого сообщения в теме
        """
        if not message:
            return None
        
        # Проверяем наличие reply_to_message_id
        if hasattr(message, 'reply_to_message_id') and message.reply_to_message_id:
            reply_id = message.reply_to_message_id
            # Проверяем, есть ли это ID в кэше тем (как строка!)
            if str(reply_id) in self.topics_cache:
                logger.debug(f"✅ Найден ID темы в reply_to_message_id: {reply_id}")
                return reply_id
        
        return None
    
    async def get_new_messages(self, chat_id: int, min_id: int = 0, limit: int = 200) -> List:
        """Получение новых сообщений"""
        try:
            messages = []
            async for msg in self.client.get_chat_history(chat_id, limit=limit):
                if msg.id > min_id:
                    messages.append(msg)
            messages.sort(key=lambda x: x.id)
            logger.info(f"📨 Получено {len(messages)} новых сообщений")
            return messages
        except Exception as e:
            logger.error(f"❌ Ошибка получения сообщений: {e}")
            return []
    
    async def download_media_with_topic(self, message) -> Optional[DownloadResult]:
        """
        Скачивание медиафайла с определением темы
        """
        try:
            # Определяем тип файла и имя
            filename = None
            file_type = None
            
            if message.photo:
                photo_date = message.date.strftime('%Y%m%d_%H%M%S')
                filename = f"photo_{photo_date}.jpg"
                file_type = 'photo'
                
            elif message.document:
                if hasattr(message.document, 'file_name'):
                    filename = message.document.file_name
                else:
                    ext = Path(message.document.mime_type or "").suffix or ".dat"
                    filename = f"document_{message.id}{ext}"
                file_type = 'document'
                
            elif message.video:
                if hasattr(message.video, 'file_name') and message.video.file_name:
                    filename = message.video.file_name
                else:
                    video_date = message.date.strftime('%Y%m%d_%H%M%S')
                    filename = f"video_{video_date}.mp4"
                file_type = 'video'
            
            if not filename:
                return None
            
            # Определяем тему
            topic_id = self.get_topic_id_from_message(message)
            topic_name = self.get_topic_name(topic_id) if topic_id else None
            
            # Формируем имена папок
            chat_title = getattr(message.chat, 'title', str(message.chat.id))
            chat_folder = self._sanitize_folder_name(chat_title)
            
            if topic_name:
                topic_folder = self._sanitize_folder_name(topic_name)
            elif topic_id:
                topic_folder = f"topic_{topic_id}"
            else:
                topic_folder = "general"
            
            # Скачиваем файл
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                local_path = tmp.name
            
            logger.info(f"📥 Скачиваю: {filename} -> {chat_folder}/{topic_folder}")
            await self.client.download_media(message, local_path)
            file_size = os.path.getsize(local_path)
            
            return DownloadResult(
                local_path=local_path,
                original_filename=filename,
                file_type=file_type,
                topic_id=topic_id,
                topic_name=topic_name,
                chat_folder=chat_folder,
                topic_folder=topic_folder,
                message_id=message.id,
                file_size=file_size
            )
            
        except Exception as e:
            logger.error(f"❌ Ошибка скачивания: {e}")
            return None
    
    def _sanitize_folder_name(self, name: str) -> str:
        """Безопасное имя папки"""
        import re
        if not name:
            return "general"
        name = re.sub(r'[<>:"/\\|?*]', '_', name)
        name = name.strip().replace(' ', '_')
        name = re.sub(r'_+', '_', name)
        return name[:100]
