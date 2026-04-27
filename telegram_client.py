#!/usr/bin/env python3
"""
Telegram клиент для скачивания файлов с поддержкой тем и кэширования.
ВЕРСИЯ 0.18.0 — БЕЗ ИЗМЕНЕНИЙ
"""

__version__ = "0.18.0"

import os
import asyncio
import json
import time
import re
import logging
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple, Set, Callable, Any, Union
from pathlib import Path
from collections import defaultdict

from database import (
    DatabaseManager, get_db,
    STATE_NEW, STATE_SELECTED, STATE_UPLOADED, STATE_SKIPPED, STATE_ERROR, STATE_UNLOADED,
    fmt_size
)

logger = logging.getLogger(__name__)


async def _call_with_timeout(func: Callable, timeout: float, *args, **kwargs) -> Any:
    """Выполняет асинхронную функцию с таймаутом."""
    try:
        return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
    except asyncio.TimeoutError:
        raise TimeoutError(f"Timeout after {timeout}s")


try:
    from aiolimiter import AsyncLimiter
    HAS_AIO_LIMITER = True
except ImportError:
    HAS_AIO_LIMITER = False


class RateLimiter:
    """Адаптивный ограничитель частоты запросов."""
    
    def __init__(self, max_calls: int, period: float) -> None:
        """Инициализирует ограничитель."""
        if HAS_AIO_LIMITER:
            self._limiter: Optional[AsyncLimiter] = AsyncLimiter(max_calls, period)
        else:
            self._limiter = None
            self._max_calls: int = max_calls
            self._period: float = period
            self._calls: List[float] = []
        
        self.stats: Dict[str, float] = {'total_waited': 0.0, 'max_wait': 0.0, 'avg_wait': 0.0, 'wait_count': 0}
        self._consecutive_waits: int = 0

    async def acquire(self) -> float:
        """Захватывает слот для выполнения запроса."""
        start: float = time.monotonic()
        
        if HAS_AIO_LIMITER and self._limiter:
            await self._limiter.acquire()
        else:
            now: float = time.time()
            self._calls = [c for c in self._calls if now - c < self._period]
            if len(self._calls) >= self._max_calls:
                sleep_time: float = self._calls[0] + self._period - now
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
            self._calls.append(time.time())
        
        wait_time: float = time.monotonic() - start
        
        if wait_time > 0.01:
            self._consecutive_waits += 1
            self.stats['wait_count'] += 1
            self.stats['total_waited'] += wait_time
            self.stats['max_wait'] = max(self.stats['max_wait'], wait_time)
            self.stats['avg_wait'] = self.stats['total_waited'] / self.stats['wait_count']
            
            if self._consecutive_waits > 3:
                await asyncio.sleep(wait_time * 0.5)
        else:
            self._consecutive_waits = 0
        
        return wait_time

    def get_stats(self) -> dict:
        """Возвращает статистику ограничителя."""
        return self.stats.copy()


from pyrogram import Client
from pyrogram.raw.functions.channels import GetForumTopics
from pyrogram.errors import FloodWait, MessageIdInvalid, MessageEmpty
from pyrogram.types import Message


@dataclass
class DownloadResult:
    """Результат скачивания файла из Telegram."""
    success: bool
    local_path: str
    filename: str
    file_type: str
    chat_id: int
    message_id: int
    topic_id: Optional[int]
    topic_name: Optional[str]
    file_size: int
    already_existed: bool
    from_cache: bool
    chat_folder: str
    topic_folder: str
    relative_path: str
    error: str = ""
    duration_sec: float = 0


class SimpleAdaptiveDownloader:
    """Адаптивный лимитер для параллельных скачиваний."""
    
    def __init__(self, initial_limit: int = 3, min_limit: int = 1, max_limit: int = 5,
                 flood_threshold: int = 3, success_threshold: int = 10,
                 cooldown_seconds: float = 30.0, name: str = "") -> None:
        """Инициализирует адаптивный лимитер."""
        self.min_limit: int = min_limit
        self.max_limit: int = max_limit
        self.flood_threshold: int = flood_threshold
        self.success_threshold: int = success_threshold
        self.cooldown_seconds: float = cooldown_seconds
        self.name: str = name or "downloader"

        self._current_limit: int = initial_limit
        self._active_count: int = 0
        self._condition: asyncio.Condition = asyncio.Condition()
        self._lock: asyncio.Lock = asyncio.Lock()
        
        self._closed: bool = False
        self._flood_count: int = 0
        self._success_count: int = 0
        self._last_adjust_time: float = 0
        self._adjustments_total: int = 0
        self.stats: Dict[str, int] = {'current': initial_limit, 'flood_count': 0, 'success_count': 0, 'adjustments': 0}
        logger.info(f"📊 {self.name}: лимит {initial_limit} [{min_limit}–{max_limit}]")

    async def acquire(self) -> None:
        """Захватывает слот для скачивания."""
        if self._closed:
            raise RuntimeError(f"{self.name} семафор закрыт")
        async with self._condition:
            while self._active_count >= self._current_limit:
                await self._condition.wait()
            self._active_count += 1

    async def release(self) -> None:
        """Освобождает слот скачивания."""
        async with self._condition:
            self._active_count -= 1
            self._condition.notify()

    async def __aenter__(self) -> 'SimpleAdaptiveDownloader':
        """Вход в контекстный менеджер."""
        await self.acquire()
        return self

    async def __aexit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Any) -> bool:
        """Выход из контекстного менеджера."""
        await self.release()
        if exc_val is None and exc_type is None:
            await self._record_success()
        elif self._is_flood_error(exc_val):
            await self._record_flood()
        return False

    async def _adjust_limit(self, new_limit: int, reason: str) -> None:
        """Изменяет лимит параллельных скачиваний."""
        if new_limit == self._current_limit:
            return
        old_limit: int = self._current_limit
        self._current_limit = max(self.min_limit, min(self.max_limit, new_limit))
        if self._current_limit != old_limit:
            logger.info(f"📊 {self.name}: лимит {old_limit}→{self._current_limit} ({reason})")
            self._adjustments_total += 1
            self.stats['current'] = self._current_limit
            self.stats['adjustments'] = self._adjustments_total
            async with self._condition:
                self._condition.notify_all()

    async def _record_success(self) -> None:
        """Записывает успешное скачивание."""
        async with self._lock:
            self._success_count += 1
            self.stats['success_count'] = self._success_count
            self._flood_count = 0
            if self._success_count >= self.success_threshold:
                if self._current_limit < self.max_limit:
                    new_limit: int = min(self.max_limit, self._current_limit + 1)
                    await self._adjust_limit(new_limit, f"success×{self._success_count}")
                self._success_count = 0

    async def _record_flood(self) -> None:
        """Записывает ошибку FloodWait."""
        async with self._lock:
            self._flood_count += 1
            self.stats['flood_count'] = self._flood_count
            self._success_count = 0
            if self._flood_count >= self.flood_threshold:
                new_limit: int = max(self.min_limit, self._current_limit - 1)
                await self._adjust_limit(new_limit, f"flood×{self._flood_count}")
                self._flood_count = 0
                await asyncio.sleep(self.cooldown_seconds)

    @staticmethod
    def _is_flood_error(exc: Optional[Exception]) -> bool:
        """Проверяет, является ли исключение FloodWait."""
        if exc is None:
            return False
        if isinstance(exc, FloodWait):
            return True
        if getattr(exc, 'CODE', None) == 420:
            return True
        s: str = str(exc).lower()
        return any(k in s for k in ('floodwait', '420', 'too many requests'))

    async def close(self) -> bool:
        """Закрывает лимитер."""
        self._closed = True
        async with self._condition:
            self._condition.notify_all()
        return True

    def get_stats(self) -> dict:
        """Возвращает статистику лимитера."""
        return self.stats.copy()


class TelegramDownloader:
    """Клиент для скачивания файлов из Telegram с поддержкой тем и кэшированием."""
    
    _TYPE_ATTR_MAP: List[Tuple[str, Callable]] = [
        ('photo', lambda m: m.photo),
        ('video', lambda m: m.video),
        ('audio', lambda m: m.audio),
        ('voice', lambda m: m.voice),
        ('sticker', lambda m: m.sticker),
        ('animation', lambda m: m.animation),
        ('video_note', lambda m: m.video_note),
    ]
    
    def __init__(self, config: dict) -> None:
        """Инициализирует клиент Telegram."""
        self.api_id: int = config['api_id']
        self.api_hash: str = config['api_hash']
        self.session_string: Optional[str] = config.get('session_string')
        self.session_file: str = config.get('session_file', 'user_session')
        self.download_dir: str = config.get('download_dir', 'downloads')
        os.makedirs(self.download_dir, exist_ok=True)
        self._shutdown: bool = False

        file_types: Dict[str, List[str]] = config.get('file_types', {})
        self.photo_extensions: Set[str] = set(file_types.get('photo', []))
        self.video_extensions: Set[str] = set(file_types.get('video', []))
        self.audio_extensions: Set[str] = set(file_types.get('audio', []))
        self.document_extensions: Set[str] = set(file_types.get('document', []))
        self.archive_extensions: Set[str] = set(file_types.get('archive', []))

        self._ext_type_map: Dict[str, str] = {}
        for ext in self.photo_extensions:
            self._ext_type_map[ext.lower()] = 'photo'
        for ext in self.video_extensions:
            self._ext_type_map[ext.lower()] = 'video'
        for ext in self.audio_extensions:
            self._ext_type_map[ext.lower()] = 'audio'
        for ext in self.archive_extensions:
            self._ext_type_map[ext.lower()] = 'archive'

        self.db: Optional[DatabaseManager] = None
        
        self._dialogs_cache: Dict[int, Any] = {}
        self._dialogs_cache_time: float = 0
        self._dialogs_cache_ttl: int = config.get('dialogs_cache_ttl', 300)
        
        self._topics_cache: Dict[int, Dict[int, str]] = {}
        self._topics_cache_time: Dict[int, float] = {}
        self._topics_cache_ttl: int = config.get('topics_cache_ttl', 3600)

        self.rate_limiter: RateLimiter = RateLimiter(
            max_calls=config.get('rate_limit_calls', 30),
            period=config.get('rate_limit_period', 60)
        )

        self.download_semaphore: SimpleAdaptiveDownloader = SimpleAdaptiveDownloader(
            initial_limit=config.get('max_concurrent_downloads', 3),
            min_limit=config.get('min_concurrent_downloads', 1),
            max_limit=config.get('max_concurrent_downloads_max', 5),
            flood_threshold=config.get('flood_threshold', 3),
            success_threshold=config.get('success_threshold', 10),
            name="downloader"
        )

        self.client: Optional[Client] = None
        self._me: Optional[Any] = None

        self.stats: Dict[str, Any] = {
            'total_downloads': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'cache_hits': 0,
            'total_bytes': 0,
            'dialogs_cache_hits': 0,
            'dialogs_cache_misses': 0,
            'rate_limiter': self.rate_limiter.get_stats(),
            'downloader': self.download_semaphore.get_stats()
        }

    # ========================================================================
    # ИНИЦИАЛИЗАЦИЯ И ПОДКЛЮЧЕНИЕ
    # ========================================================================

    async def _get_db(self) -> DatabaseManager:
        """Возвращает экземпляр менеджера БД."""
        if self.db is None:
            self.db = await get_db()
        return self.db

    async def _get_dialogs(self, force: bool = False) -> Dict[int, Any]:
        """Возвращает кэшированный список диалогов."""
        now: float = time.time()
        if not force and self._dialogs_cache and (now - self._dialogs_cache_time) < self._dialogs_cache_ttl:
            self.stats['dialogs_cache_hits'] += 1
            return self._dialogs_cache
        
        self.stats['dialogs_cache_misses'] += 1
        dialogs: Dict[int, Any] = {}
        max_retries: int = 3
        
        for attempt in range(max_retries):
            try:
                await self.rate_limiter.acquire()
                async for dialog in self.client.get_dialogs(limit=200):
                    dialogs[dialog.chat.id] = dialog.chat
                self._dialogs_cache = dialogs
                self._dialogs_cache_time = now
                return dialogs
            except FloodWait as e:
                wait_time: float = min(e.value + 1, 60)
                logger.warning(f"🌊 FloodWait в _get_dialogs: ждём {wait_time}с")
                await asyncio.sleep(wait_time)
                if attempt == max_retries - 1:
                    if self._dialogs_cache:
                        return self._dialogs_cache
                    raise
            except Exception as e:
                if attempt == max_retries - 1:
                    if self._dialogs_cache:
                        return self._dialogs_cache
                    raise
                await asyncio.sleep(2 ** attempt)
        
        return self._dialogs_cache if self._dialogs_cache else {}

    async def find_chat(self, chat_id: int) -> Any:
        """Находит чат по ID."""
        dialogs: Dict[int, Any] = await self._get_dialogs()
        if chat_id in dialogs:
            return dialogs[chat_id]
        await self.rate_limiter.acquire()
        chat: Any = await _call_with_timeout(self.client.get_chat, 15.0, str(chat_id))
        self._dialogs_cache[chat_id] = chat
        return chat

    async def invalidate_dialogs_cache(self) -> None:
        """Инвалидирует кэш диалогов."""
        self._dialogs_cache = {}
        self._dialogs_cache_time = 0

    async def is_healthy(self) -> bool:
        """Проверяет здоровье подключения."""
        try:
            if not self.client or not self.client.is_connected:
                return False
            await asyncio.wait_for(self.client.get_me(), timeout=5.0)
            return True
        except Exception:
            return False

    async def reconnect(self) -> bool:
        """Переподключается к Telegram."""
        try:
            await self.disconnect()
            await asyncio.sleep(2)
            return await self.connect()
        except Exception:
            return False

    async def connect(self) -> bool:
        """Подключается к Telegram."""
        try:
            if self.session_string:
                self.client = Client(
                    name=self.session_file,
                    api_id=self.api_id,
                    api_hash=self.api_hash,
                    session_string=self.session_string,
                    in_memory=False,
                    workdir=self.download_dir
                )
            else:
                self.client = Client(
                    name=self.session_file,
                    api_id=self.api_id,
                    api_hash=self.api_hash,
                    in_memory=False,
                    workdir=self.download_dir
                )
            await self.client.start()
            self._me = self.client.me
            await self._get_dialogs()
            logger.info(f"✅ Подключено к Telegram как {self._me.first_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка подключения: {e}")
            return False

    async def disconnect(self) -> None:
        """Отключается от Telegram."""
        if self.client and self.client.is_connected:
            await self.client.stop()
            
        self._dialogs_cache = {}
        self._dialogs_cache_time = 0
        self._topics_cache.clear()
        self._topics_cache_time.clear()
        logger.info("🔌 Отключено от Telegram")

    # ========================================================================
    # РАБОТА С ТЕМАМИ
    # ========================================================================

    async def load_all_topics(self, chat_id: int, force_refresh: bool = False) -> Dict[str, str]:
        """Загружает все темы чата."""
        try:
            db: DatabaseManager = await self._get_db()
            
            if not force_refresh:
                topics: List[dict] = await db.get_topics(chat_id)
                if topics:
                    self._topics_cache[chat_id] = {t['topic_id']: t['topic_name'] for t in topics}
                    self._topics_cache_time[chat_id] = time.time()
                    return {str(t['topic_id']): t['topic_name'] for t in topics}
            
            logger.info(f"📂 Загрузка тем чата {chat_id}...")
            target_chat: Any = await self.find_chat(chat_id)
            channel: Any = await self.client.resolve_peer(target_chat.id)
            offset_topic: int = 0
            topics_dict: Dict[str, str] = {}
            topic_count: int = 0
            
            while True:
                await self.rate_limiter.acquire()
                result: Any = await self.client.invoke(
                    GetForumTopics(channel=channel, offset_date=0, offset_id=0, offset_topic=offset_topic, limit=100)
                )
                if not hasattr(result, 'topics') or not result.topics:
                    break
                for topic in result.topics:
                    topics_dict[str(topic.id)] = topic.title
                    topic_count += 1
                if len(result.topics) < 100:
                    break
                offset_topic = result.topics[-1].id
                await asyncio.sleep(0.5)
            
            await db.update_topics(chat_id, topics_dict)
            
            self._topics_cache[chat_id] = {int(tid): tname for tid, tname in topics_dict.items()}
            self._topics_cache_time[chat_id] = time.time()
            
            logger.info(f"✅ Загружено {topic_count} тем для чата {chat_id}")
            return topics_dict
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки тем: {e}")
            return {}

    async def get_topic_name(self, chat_id: int, topic_id: int) -> Optional[str]:
        """Возвращает имя темы с кэшированием."""
        if topic_id is None or topic_id == 0:
            return None
        
        now: float = time.time()
        
        if chat_id in self._topics_cache:
            cache_time: float = self._topics_cache_time.get(chat_id, 0)
            if now - cache_time < self._topics_cache_ttl:
                return self._topics_cache[chat_id].get(topic_id)
        
        db: DatabaseManager = await self._get_db()
        topics: List[dict] = await db.get_topics(chat_id)
        
        self._topics_cache[chat_id] = {t['topic_id']: t['topic_name'] for t in topics}
        self._topics_cache_time[chat_id] = now
        
        return self._topics_cache[chat_id].get(topic_id)

    def get_topic_id_from_message(self, message: Message) -> Optional[int]:
        """Извлекает ID темы из сообщения."""
        if not message:
            return None
        if hasattr(message, 'reply_to_top_id') and message.reply_to_top_id:
            return message.reply_to_top_id
        if hasattr(message, 'reply_to_message_id') and message.reply_to_message_id:
            return message.reply_to_message_id
        return 0

    # ========================================================================
    # СКАНИРОВАНИЕ ЧАТОВ
    # ========================================================================

    async def _scan_chat(self, chat_id: int, mode: str = 'full',
                         progress_callback: Optional[Callable] = None) -> List[Message]:
        """Универсальный метод сканирования чата."""
        files: List[Message] = []
        last_id: int = 0
        max_id: int = 0
        current_topic: Optional[str] = None

        chat: Any = await self.find_chat(chat_id)
        chat_name: str = chat.title
        chat_id_str: str = str(chat_id)
        
        logger.info(f"🔍 Начинаем поиск файлов в чате {chat_name} ({chat_id})")

        while True:
            try:
                await self.rate_limiter.acquire()
                messages: List[Message] = []
                async for msg in self.client.get_chat_history(chat_id_str, limit=100, offset_id=last_id):
                    messages.append(msg)

                if not messages:
                    break

                if max_id == 0 and messages:
                    max_id = messages[0].id
                    logger.info(f"   📊 Максимальный ID сообщения: {max_id}")

                for msg in messages:
                    if self._has_file(msg):
                        files.append(msg)
                        msg_topic_id: Optional[int] = self.get_topic_id_from_message(msg)
                        if msg_topic_id:
                            current_topic = await self.get_topic_name(chat_id, msg_topic_id) or f"Тема {msg_topic_id}"
                        else:
                            current_topic = "общая тема"

                last_msg_id: int = messages[-1].id
                if progress_callback and max_id > 0:
                    await progress_callback(
                        chat_id=chat_id, chat_name=chat_name,
                        current_id=last_msg_id, max_id=max_id,
                        files_found=len(files), current_topic=current_topic, completed=False
                    )

                last_id = messages[-1].id
                logger.debug(f"   📊 Обработано {len(messages)} сообщений, найдено {len(files)} файлов")
                
                if len(messages) < 100:
                    break
                await asyncio.sleep(1)

            except FloodWait as e:
                logger.warning(f"🌊 FloodWait: ждём {e.value + 1}с")
                await asyncio.sleep(e.value + 1)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Ошибка сканирования: {e}")
                break

        logger.info(f"✅ Поиск завершён. Найдено {len(files)} файлов в чате {chat_name}")
        
        if progress_callback:
            await progress_callback(
                chat_id=chat_id, chat_name=chat_name,
                current_id=0, max_id=max_id,
                files_found=len(files), current_topic=None, completed=True
            )
        return files

    async def _save_files_to_db(self, chat_id: int, messages: List[Message], state: int) -> int:
        """Сохраняет файлы в БД, группируя по темам."""
        db: DatabaseManager = await self._get_db()
        
        by_topic: defaultdict = defaultdict(list)
        for msg in messages:
            tid: int = self.get_topic_id_from_message(msg) or 0
            by_topic[tid].append(msg)
        
        total_files: int = 0
        for tid, msgs in by_topic.items():
            topic_name: Optional[str] = await self.get_topic_name(chat_id, tid) if tid else None
            logger.info(f"   📂 Обработка темы {tid} ({topic_name or 'общая'}) - {len(msgs)} файлов")
            
            files_info: List[Dict[str, Any]] = []
            for msg in msgs:
                if self._has_file(msg):
                    files_info.append({
                        'message_id': msg.id,
                        'filename': self.get_filename_from_message(msg),
                        'type': self.get_file_type(msg),
                        'size': self.get_file_size(msg),
                        'timestamp': msg.date.timestamp() if msg.date else time.time(),
                        'state': state,
                        'file_id': getattr(msg, 'file_id', None),
                        'dc_id': getattr(msg, 'dc_id', None)
                    })
            
            if files_info:
                await db.add_files(chat_id, tid, files_info, topic_name)
                total_files += len(files_info)
                logger.info(f"   ✅ Сохранено {len(files_info)} файлов в тему {tid}")
        
        return total_files

    async def full_scan_chat(self, chat_id: int, db: DatabaseManager, 
                             progress_callback: Optional[Callable] = None) -> int:
        """Выполняет полное сканирование чата."""
        logger.info(f"🔍 Начинаем полное сканирование чата {chat_id}")
        
        try:
            real_chat: Any = await self.get_chat(chat_id)
            if real_chat:
                real_name: Optional[str] = getattr(real_chat, 'title', None) or getattr(real_chat, 'first_name', None)
                if real_name:
                    await db.set_chat_name(chat_id, real_name)
                    logger.info(f"📝 Обновлено имя чата: {real_name}")
        except Exception as e:
            logger.debug(f"Не удалось получить имя чата: {e}")
        
        await self.load_all_topics(chat_id, force_refresh=True)
        
        messages: List[Message] = await self._scan_chat(chat_id, 'full', progress_callback)
        if not messages:
            logger.warning(f"⚠️ В чате {chat_id} не найдено файлов")
            return 0
        
        total_files: int = await self._save_files_to_db(chat_id, messages, STATE_UNLOADED)
        
        if progress_callback:
            await progress_callback(
                chat_id=chat_id, chat_name=await db.get_chat_name(chat_id),
                current_id=0, max_id=0,
                files_found=total_files, current_topic=None, completed=True
            )
        
        logger.info(f"✅ Полное сканирование чата {chat_id} завершено. Всего файлов: {total_files}")
        return total_files

    async def incremental_scan_chat(self, chat_id: int, db: DatabaseManager,
                                    progress_callback: Optional[Callable] = None) -> int:
        """Выполняет инкрементальное сканирование чата."""
        logger.info(f"🔍 Начинаем инкрементальное сканирование чата {chat_id}")
        
        existing_ids: Set[int] = set()
        files: List[dict] = await db.get_files(chat_id, None)
        for f in files:
            existing_ids.add(f['message_id'])
        
        logger.debug(f"   Существующих файлов: {len(existing_ids)}")
        
        messages: List[Message] = await self._scan_chat(chat_id, 'incremental', progress_callback)
        new_messages: List[Message] = [m for m in messages if m.id not in existing_ids and self._has_file(m)]
        
        if not new_messages:
            logger.info(f"✅ Новых файлов в чате {chat_id} не найдено")
            return 0
        
        logger.info(f"   Найдено {len(new_messages)} новых файлов")
        total_new: int = await self._save_files_to_db(chat_id, new_messages, STATE_NEW)
        
        if progress_callback:
            await progress_callback(
                chat_id=chat_id, chat_name=await db.get_chat_name(chat_id),
                current_id=0, max_id=0,
                files_found=total_new, current_topic=None, completed=True
            )
        
        logger.info(f"✅ Инкрементальное сканирование чата {chat_id} завершено. Новых файлов: {total_new}")
        return total_new

    # ========================================================================
    # ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
    # ========================================================================

    def _has_file(self, message: Message) -> bool:
        """Проверяет, содержит ли сообщение файл."""
        for _, attr_getter in self._TYPE_ATTR_MAP:
            if attr_getter(message):
                return True
        return bool(message.document)

    def _sanitize_name(self, name: str) -> str:
        """Очищает имя папки от недопустимых символов."""
        if not name:
            return "general"
        name = re.sub(r'\.{2,}', '_', name)
        name = re.sub(r'[<>:"/\\|?*]', '_', name)
        name = name.strip().replace(' ', '_')
        name = re.sub(r'_+', '_', name)
        return name[:100]

    def _sanitize_filename_minimal(self, filename: str) -> str:
        """Очищает имя файла от недопустимых символов."""
        if not filename:
            return ""
        filename = os.path.basename(filename)
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        filename = re.sub(r'[\x00-\x1f\x7f]', '', filename)
        if len(filename) > 200:
            name, ext = os.path.splitext(filename)
            filename = name[:195] + ext
        return filename

    def _generate_filename_by_type(self, message: Message) -> str:
        """Генерирует имя файла на основе типа."""
        file_type: str = self.get_file_type(message)
        
        if file_type == 'photo':
            return f"photo_{message.id}.jpg"
        elif file_type == 'video':
            return f"video_{message.id}.mp4"
        elif file_type == 'audio':
            return f"audio_{message.id}.mp3"
        elif file_type == 'voice':
            return f"voice_{message.id}.ogg"
        elif file_type == 'sticker':
            return f"sticker_{message.id}.webp"
        elif file_type == 'animation':
            return f"animation_{message.id}.mp4"
        elif file_type == 'video_note':
            return f"video_note_{message.id}.mp4"
        elif file_type == 'archive':
            return f"archive_{message.id}.zip"
        else:
            return f"doc_{message.id}.bin"

    def get_filename_from_message(self, message: Message) -> Optional[str]:
        """Извлекает имя файла из сообщения."""
        if not self._has_file(message):
            return None
        
        for _, attr_getter in self._TYPE_ATTR_MAP:
            attr: Any = attr_getter(message)
            if attr:
                filename: Optional[str] = getattr(attr, 'file_name', None)
                if filename:
                    filename = self._sanitize_filename_minimal(filename)
                    if filename:
                        return filename
        
        if message.document:
            filename: Optional[str] = getattr(message.document, 'file_name', None)
            if filename:
                filename = self._sanitize_filename_minimal(filename)
                if filename:
                    return filename
        
        return self._generate_filename_by_type(message)

    async def get_chat(self, identifier: Union[int, str]) -> Any:
        """Возвращает чат по идентификатору."""
        try:
            if isinstance(identifier, int):
                return await self.find_chat(identifier)
            await self.rate_limiter.acquire()
            return await _call_with_timeout(self.client.get_chat, 15.0, str(identifier))
        except Exception as e:
            logger.error(f"❌ Ошибка получения чата {identifier}: {e}")
            return None

    async def get_message_by_id(self, chat_id: int, message_id: int) -> Optional[Message]:
        """Возвращает сообщение по ID."""
        try:
            chat_id_str: str = str(chat_id)
            await self.find_chat(chat_id)
        except Exception:
            pass
        
        max_retries: int = 5
        for attempt in range(max_retries):
            try:
                await self.rate_limiter.acquire()
                return await _call_with_timeout(
                    self.client.get_messages, 15.0, chat_id_str, message_ids=message_id
                )
            except (asyncio.TimeoutError, TimeoutError):
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                logger.error(f"Таймаут получения сообщения {message_id} после {max_retries} попыток")
                return None
            except (MessageIdInvalid, MessageEmpty):
                logger.debug(f"Сообщение {message_id} не найдено в чате {chat_id}")
                return None
            except FloodWait as e:
                wait_time: float = min(e.value + 1, 60)
                logger.warning(f"🌊 FloodWait в get_message_by_id: ждём {wait_time}с")
                await asyncio.sleep(wait_time)
                continue
            except Exception as e:
                logger.error(f"❌ Ошибка получения сообщения {message_id}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return None
        return None

    async def get_message_for_file(self, chat_id: int, file_info: dict) -> Optional[Message]:
        """Возвращает сообщение для файла."""
        return await self.get_message_by_id(chat_id, file_info['message_id'])

    # ========================================================================
    # СКАЧИВАНИЕ ФАЙЛОВ
    # ========================================================================

    async def _get_client_for_dc(self, dc_id: Optional[int] = None) -> Client:
        """Возвращает клиент для скачивания. Pyrogram сам маршрутизирует в нужный DC."""
        return self.client

    async def download(self, message: Message, progress_callback: Optional[Callable] = None) -> DownloadResult:
        """Скачивает файл из сообщения."""
        start_time: float = time.time()
        if message is None:
            return DownloadResult(success=False, local_path="", filename="", file_type="unknown",
                                  chat_id=0, message_id=0, topic_id=None, topic_name=None, file_size=0,
                                  already_existed=False, from_cache=False, chat_folder="", topic_folder="",
                                  relative_path="", error="Message is None", duration_sec=time.time()-start_time)
        if not self._has_file(message):
            return DownloadResult(success=False, local_path="", filename="", file_type="unknown",
                                  chat_id=message.chat.id, message_id=message.id, topic_id=None, topic_name=None,
                                  file_size=0, already_existed=False, from_cache=False, chat_folder="", topic_folder="",
                                  relative_path="", error="No file in message")

        file_type: str = self.get_file_type(message)
        file_size: int = self.get_file_size(message)
        
        filename: str = self.get_filename_from_message(message) or f"file_{message.id}.bin"

        topic_id: Optional[int] = self.get_topic_id_from_message(message)
        topic_name: Optional[str] = await self.get_topic_name(message.chat.id, topic_id) if topic_id else None
        chat_title: str = getattr(message.chat, 'title', f"chat_{message.chat.id}")
        chat_folder: str = self._sanitize_name(chat_title)
        topic_folder: str = self._sanitize_name(topic_name) if topic_name else "general"
        relative_path: str = os.path.join(chat_folder, topic_folder, filename)
        full_path: str = os.path.join(self.download_dir, relative_path)

        logger.debug(f"📥 Скачивание {filename} из чата {chat_title}" + (f", тема {topic_name}" if topic_name else ""))

        if os.path.exists(full_path):
            existing_size: int = os.path.getsize(full_path)
            if existing_size > 0 and (file_size == 0 or existing_size == file_size):
                self.stats['cache_hits'] += 1
                logger.debug(f"📦 Файл уже существует: {full_path} ({existing_size} байт)")
                return DownloadResult(success=True, local_path=full_path, filename=filename, file_type=file_type,
                                      chat_id=message.chat.id, message_id=message.id, topic_id=topic_id,
                                      topic_name=topic_name, file_size=existing_size, already_existed=True,
                                      from_cache=True, chat_folder=chat_folder, topic_folder=topic_folder,
                                      relative_path=relative_path, duration_sec=time.time()-start_time)

        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        self.stats['total_downloads'] += 1
        timeout: int = max(3600, int(file_size / (1024 * 1024) * 2)) if file_size else 3600

        try:
            dc_id: Optional[int] = getattr(message, 'dc_id', None)
            client: Client = await self._get_client_for_dc(dc_id)
            
            async with self.download_semaphore:
                await self.rate_limiter.acquire()
                logger.info(f"📥 Начинаем скачивание {filename} ({file_size/1024/1024:.1f}MB)" if file_size else f"📥 Начинаем скачивание {filename}")
                
                if progress_callback:
                    last_percent: float = 0.0
                    async def filtered_progress(current: int, total: int) -> None:
                        nonlocal last_percent
                        if total > 0:
                            percent: float = (current / total) * 100
                            if percent >= 99.9 and current < total:
                                return
                            if percent - last_percent >= 5 or percent >= 99:
                                last_percent = percent
                                await progress_callback(percent, current, total)
                    
                    downloaded: str = await asyncio.wait_for(
                        client.download_media(message, file_name=full_path,
                                              progress=filtered_progress),
                        timeout=timeout
                    )
                else:
                    downloaded = await asyncio.wait_for(client.download_media(message, file_name=full_path),
                                                        timeout=timeout)
            
            if downloaded and os.path.exists(downloaded):
                final_size: int = os.path.getsize(downloaded)
                if file_size > 0 and final_size != file_size:
                    os.unlink(downloaded)
                    self.stats['failed_downloads'] += 1
                    logger.error(f"❌ Размер не совпадает: ожидалось {file_size}, получено {final_size}")
                    return DownloadResult(success=False, local_path="", filename=filename, file_type=file_type,
                                          chat_id=message.chat.id, message_id=message.id, topic_id=topic_id,
                                          topic_name=topic_name, file_size=0, already_existed=False, from_cache=False,
                                          chat_folder=chat_folder, topic_folder=topic_folder, relative_path=relative_path,
                                          error=f"Size mismatch: {final_size} vs {file_size}",
                                          duration_sec=time.time()-start_time)
                self.stats['successful_downloads'] += 1
                self.stats['total_bytes'] += final_size
                logger.info(f"✅ Скачан {filename} ({final_size/1024/1024:.1f}MB)")
                return DownloadResult(success=True, local_path=downloaded, filename=filename, file_type=file_type,
                                      chat_id=message.chat.id, message_id=message.id, topic_id=topic_id,
                                      topic_name=topic_name, file_size=final_size, already_existed=False,
                                      from_cache=False, chat_folder=chat_folder, topic_folder=topic_folder,
                                      relative_path=relative_path, duration_sec=time.time()-start_time)
            else:
                raise Exception("Download failed")
        except Exception as e:
            self.stats['failed_downloads'] += 1
            logger.error(f"❌ Ошибка скачивания {filename}: {e}")
            return DownloadResult(success=False, local_path="", filename=filename, file_type=file_type,
                                  chat_id=message.chat.id, message_id=message.id, topic_id=topic_id,
                                  topic_name=topic_name, file_size=0, already_existed=False, from_cache=False,
                                  chat_folder=chat_folder, topic_folder=topic_folder, relative_path=relative_path,
                                  error=str(e), duration_sec=time.time()-start_time)

    # ========================================================================
    # ОПРЕДЕЛЕНИЕ ТИПА И РАЗМЕРА
    # ========================================================================

    def get_file_type(self, message: Message) -> str:
        """Определяет тип файла в сообщении."""
        if not message:
            return 'unknown'

        for file_type, attr_getter in self._TYPE_ATTR_MAP:
            if attr_getter(message):
                return file_type

        if message.document:
            filename: str = getattr(message.document, 'file_name', '') or ''
            _, ext = os.path.splitext(filename)
            ext = ext.lower()
            return self._ext_type_map.get(ext, 'document')

        return 'other'

    def get_file_size(self, message: Message) -> int:
        """Возвращает размер файла в сообщении."""
        for _, attr_getter in self._TYPE_ATTR_MAP:
            attr: Any = attr_getter(message)
            if attr:
                return getattr(attr, 'file_size', 0)
        if message.document:
            return message.document.file_size or 0
        return 0

    def get_stats(self) -> dict:
        """Возвращает статистику клиента."""
        self.stats['rate_limiter'] = self.rate_limiter.get_stats()
        self.stats['downloader'] = self.download_semaphore.get_stats()
        return self.stats.copy()

    # ========================================================================
    # СКАНИРОВАНИЕ ВСЕХ ЧАТОВ
    # ========================================================================

    async def scan_all_chats(self, db: DatabaseManager, full: bool = True, 
                             chat_id: Optional[int] = None) -> int:
        """Сканирует все чаты на наличие файлов."""
        mode: str = "ПОЛНОГО" if full else "ИНКРЕМЕНТАЛЬНОГО"
        logger.info(f"📋 РЕЖИМ {mode} СКАНИРОВАНИЯ")
        
        chat_ids: List[int] = await db.get_chat_ids()
        if chat_id:
            chat_ids = [cid for cid in chat_ids if cid == chat_id]
        
        total_files: int = 0
        
        for cid in chat_ids:
            if self._shutdown:
                break
                
            chat_name: str = await db.get_chat_name(cid)
            logger.info(f"🔍 {mode.lower()} сканирование чата {chat_name} ({cid})")
            
            async def progress_callback(chat_id: int, chat_name: str, current_id: int, max_id: int,
                                        files_found: int, current_topic: Optional[str], completed: bool) -> None:
                await db.update_scan_progress(
                    chat_id=chat_id, chat_name=chat_name,
                    current_id=current_id, max_id=max_id,
                    files_found=files_found, current_topic=current_topic,
                    completed=completed
                )
            
            if full:
                files_count: int = await self.full_scan_chat(cid, db, progress_callback)
            else:
                files_count = await self.incremental_scan_chat(cid, db, progress_callback)
            
            total_files += files_count
            logger.info(f"   ✅ Сохранено {files_count} файлов в кэш")
            
            await db.update_scan_progress(
                chat_id=cid, chat_name=chat_name,
                current_id=0, max_id=0,
                files_found=files_count, current_topic=None,
                completed=True
            )
        
        await db.clear_scan_progress()
        logger.info(f"✅ {mode} сканирование завершено. Всего файлов: {total_files}")
        return total_files
