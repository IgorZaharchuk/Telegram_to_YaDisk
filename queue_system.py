#!/usr/bin/env python3
"""
Система очередей - С ПОДДЕРЖКОЙ ПРОГРЕССА И СТАТУСОМ UPLOADED
ВЕРСИЯ 0.17.13 — ИСПРАВЛЕНИЯ: УБРАНА ОЧИСТКА ДО ЗАГРУЗКИ, ВАЛИДАЦИЯ ПЕРЕД UPLOAD
"""

__version__ = "0.17.13"

import os
import asyncio
import logging
import time
import shutil
import hashlib
import json
import psutil
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any, Set, Callable, Tuple
from enum import Enum
from pathlib import Path

from database import (
    DatabaseManager, get_db,
    STATE_NEW, STATE_SELECTED, STATE_UPLOADED, STATE_SKIPPED, STATE_ERROR, STATE_UNLOADED,
    STATUS_PENDING_CHECK, STATUS_PENDING_DOWNLOAD, STATUS_PENDING_COMPRESS,
    STATUS_PENDING_UPLOAD, STATUS_COMPLETED, STATUS_FAILED,
    build_local_path, build_compressed_path, fmt_size,
    calculate_md5, is_valid_image, is_valid_video
)

logger = logging.getLogger(__name__)


class FileStatus(Enum):
    """Статусы обработки файла в очереди."""
    PENDING_CHECK = STATUS_PENDING_CHECK
    PENDING_DOWNLOAD = STATUS_PENDING_DOWNLOAD
    PENDING_COMPRESS = STATUS_PENDING_COMPRESS
    PENDING_UPLOAD = STATUS_PENDING_UPLOAD
    COMPLETED = STATUS_COMPLETED
    FAILED = STATUS_FAILED


@dataclass
class QueueItem:
    """Элемент очереди обработки."""
    chat_id: int
    message_id: int
    filename: str
    remote_dir: str
    message: Any = None
    status: FileStatus = FileStatus.PENDING_CHECK
    local_path: str = ""
    compressed_path: str = ""
    file_size: int = 0
    attempts: int = 0
    max_attempts: int = 3
    last_error: str = ""
    last_attempt_time: float = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)
    file_info: dict = field(default_factory=dict)

    @property
    def key(self) -> str:
        """Уникальный ключ элемента."""
        return f"{self.chat_id}:{self.message_id}"

    @property
    def topic_id(self) -> Optional[int]:
        """ID темы из метаданных."""
        return self.metadata.get('topic_id')

    def can_retry(self) -> bool:
        """Можно ли повторить попытку."""
        return self.attempts < self.max_attempts

    def get_retry_delay(self) -> float:
        """Задержка перед повторной попыткой."""
        return min(5 * (2 ** (self.attempts - 1)), 60.0)

    def is_retryable_error(self, error: str) -> bool:
        """Является ли ошибка повторяемой."""
        non_retryable: List[str] = [
            "File not found", "Message not found", "MessageIdInvalid",
            "MessageEmpty", "Unauthorized", "Invalid token", "Access denied",
            "Permission denied", "PeerIdInvalid", "Chat not found",
            "Path not found", "Insufficient storage", "No space left"
        ]
        error_lower: str = error.lower()
        return not any(non.lower() in error_lower for non in non_retryable)

    def record_attempt(self, error: str) -> None:
        """Записывает попытку обработки."""
        self.attempts += 1
        self.last_error = error
        self.last_attempt_time = time.time()


class WorkerPool:
    """Пул воркеров для параллельной обработки задач с автонастройкой размера."""
    
    def __init__(self, name: str, worker_func: Callable, min_workers: int = 1, max_workers: int = 5) -> None:
        """Инициализирует пул воркеров."""
        self.name: str = name
        self.worker_func: Callable = worker_func
        self.min: int = min_workers
        self.max: int = max_workers
        self.current: int = min_workers
        self.target: int = min_workers
        self.tasks: List[asyncio.Task] = []
        self.running: bool = False
        self._lock: asyncio.Lock = asyncio.Lock()
        self._stop_lock: asyncio.Lock = asyncio.Lock()
        self._stop_event: asyncio.Event = asyncio.Event()
        self._last_adjust_time: float = 0
        self._high_load_since: float = 0
        self._processed_count: int = 0
        logger.info(f"📊 {self.name}: пул {min_workers}-{max_workers}")

    async def start(self) -> None:
        """Запускает пул воркеров."""
        self.running = True
        self._stop_event.clear()
        async with self._lock:
            for i in range(self.current):
                self.tasks.append(asyncio.create_task(self.worker_func(i), name=f"{self.name}_{i}"))
        logger.info(f"✅ {self.name}: запущено {self.current} воркеров")

    async def stop(self) -> None:
        """Останавливает пул воркеров."""
        async with self._stop_lock:
            if not self.running:
                return
            logger.info(f"🛑 Останавливаю {self.name}...")
            self.running = False
            self._stop_event.set()
            await asyncio.sleep(1)
            
            stop_tasks: List[asyncio.Task] = []
            async with self._lock:
                for task in self.tasks:
                    if not task.done():
                        task.cancel()
                        stop_tasks.append(task)
            
            if stop_tasks:
                try:
                    await asyncio.wait_for(asyncio.gather(*stop_tasks, return_exceptions=True), timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning(f"⚠️ {self.name}: не все воркеры остановились за 5 секунд")
            
            async with self._lock:
                self.tasks.clear()
            
            logger.info(f"🛑 {self.name}: остановлен, обработано {self._processed_count} файлов")

    async def adjust(self, queue_size: int, cpu_percent: float) -> None:
        """Адаптирует размер пула под нагрузку."""
        if not self.running:
            return
        now: float = time.time()
        new_target: int = self.target
        
        if self.name in ('check', 'download', 'upload'):
            if queue_size > self.target * 2 and self.target < self.max:
                new_target = min(self.max, self.target + 1)
            elif queue_size == 0 and self.target > self.min:
                new_target = max(self.min, self.target - 1)
        elif self.name == 'compress_photo':
            if cpu_percent > 85:
                if self._high_load_since == 0:
                    self._high_load_since = now
                new_target = self.min
            else:
                self._high_load_since = 0
                if queue_size > self.target * 2 and self.target < self.max:
                    new_target = min(self.max, self.target + 1)
                elif queue_size == 0 and self.target > self.min:
                    new_target = max(self.min, self.target - 1)
        elif self.name == 'compress_video':
            if queue_size > 5 and self.target < self.max:
                new_target = min(self.max, self.target + 1)
            elif queue_size == 0 and self.target > self.min:
                new_target = max(self.min, self.target - 1)
        
        if new_target != self.target and now - self._last_adjust_time >= 60:
            logger.info(f"🔄 {self.name}: {self.target}→{new_target} (очередь {queue_size}, CPU {cpu_percent:.0f}%)")
            self.target = new_target
            self._last_adjust_time = now
            await self._resize()

    async def _resize(self) -> None:
        """Изменяет количество воркеров."""
        to_remove: List[asyncio.Task] = []
        to_add_count: int = 0
        old_current: int = self.current
        
        async with self._lock:
            if not self.running:
                return
            self.current = self.target
            
            while len(self.tasks) > self.current:
                task: asyncio.Task = self.tasks.pop()
                if not task.done():
                    task.cancel()
                to_remove.append(task)
            
            to_add_count = self.current - len(self.tasks)
        
        for task in to_remove:
            try:
                await asyncio.wait_for(task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        
        new_tasks: List[asyncio.Task] = []
        for i in range(to_add_count):
            if not self.running:
                break
            task: asyncio.Task = asyncio.create_task(self.worker_func(old_current + i), name=f"{self.name}_{old_current + i}")
            new_tasks.append(task)
            await asyncio.sleep(0.5)
        
        if new_tasks:
            async with self._lock:
                self.tasks.extend(new_tasks)
        
        if old_current != self.current:
            logger.info(f"✅ {self.name}: теперь {self.current} воркеров")

    def add_processed(self, count: int) -> None:
        """Добавляет количество обработанных файлов."""
        self._processed_count += count


class FileProcessor:
    """Обрабатывает один файл через стейт-машину: check → download → compress → upload."""
    
    def __init__(self, tg: Any, ya: Any, comp: Any, db: DatabaseManager, download_dir: str, 
                 queue_system: 'QueueSystem') -> None:
        """Инициализирует процессор файлов."""
        self.tg: Any = tg
        self.ya: Any = ya
        self.comp: Any = comp
        self.db: DatabaseManager = db
        self.download_dir: str = download_dir
        self.qs: 'QueueSystem' = queue_system
        self._chat_name_cache: Dict[int, str] = {}
        self._topic_name_cache: Dict[str, str] = {}
    
    async def _get_chat_name(self, chat_id: int) -> str:
        """Возвращает имя чата с кэшированием."""
        if chat_id not in self._chat_name_cache:
            self._chat_name_cache[chat_id] = await self.db.get_chat_name(chat_id)
        return self._chat_name_cache[chat_id]
    
    async def _get_topic_name(self, chat_id: int, topic_id: int) -> Optional[str]:
        """Возвращает имя темы с кэшированием."""
        if topic_id is None:
            return None
        cache_key: str = f"{chat_id}:{topic_id}"
        if cache_key not in self._topic_name_cache:
            topics: List[dict] = await self.db.get_topics(chat_id)
            for t in topics:
                if t['topic_id'] == topic_id:
                    self._topic_name_cache[cache_key] = t['topic_name']
                    break
            else:
                self._topic_name_cache[cache_key] = f"Тема {topic_id}"
        return self._topic_name_cache[cache_key]
    
    async def _get_names(self, item: QueueItem) -> Tuple[str, Optional[str]]:
        """Возвращает имена чата и темы."""
        chat_name: str = await self._get_chat_name(item.chat_id)
        topic_name: Optional[str] = await self._get_topic_name(item.chat_id, item.topic_id) if item.topic_id else None
        return chat_name, topic_name
    
    async def _needs_compress(self, filename: str, file_size: int) -> bool:
        """Проверяет, нужно ли сжимать файл."""
        settings: dict = await self.db.get_compression_settings()
        if await self.db.is_photo(filename):
            return file_size >= settings.get('min_photo_size_kb', 500) * 1024
        if await self.db.is_video(filename):
            return file_size >= settings.get('min_video_size_mb', 15) * 1024 * 1024
        return False
    
    async def _update_progress(self, key: str, data: dict) -> None:
        """Обновляет прогресс обработки."""
        await self.db.update_progress(key, data)
    
    async def cleanup_partial(self, item: QueueItem) -> None:
        """Удаляет частично скачанный файл при отмене."""
        try:
            if item.local_path and os.path.exists(item.local_path):
                os.unlink(item.local_path)
                logger.debug(f"🗑️ Удалён частичный файл: {os.path.basename(item.local_path)}")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка удаления частичного файла: {e}")
    
    async def check(self, item: QueueItem, worker_id: int) -> bool:
        """Проверяет существование файла на Яндекс.Диске."""
        try:
            chat_name, topic_name = await self._get_names(item)
            exists: bool
            size: int
            exists, size, _ = await self.ya.file_exists(item.remote_dir, item.filename)
            
            if exists and size > 0:
                await self.db.record_skipped(
                    chat_id=item.chat_id, message_id=item.message_id,
                    filename=item.filename, size=size, reason="exists_on_yandex",
                    topic_id=item.topic_id, chat_name=chat_name, topic_name=topic_name)
                await self.db.update_file_state(item.chat_id, item.message_id, STATE_SKIPPED)
                await self.qs._complete_item(item)
                logger.info(f"⏭️ Пропущен (уже на диске): {item.filename}")
                return False
            
            await self.qs._update_item(item, FileStatus.PENDING_DOWNLOAD)
            logger.info(f"🔍→📥 {item.filename}")
            return True
        except Exception as e:
            await self.qs._fail_item(item, str(e))
            return False
    
    async def download(self, item: QueueItem, worker_id: int, 
                       progress_callback: Optional[Callable] = None) -> bool:
        """Скачивает файл из Telegram."""
        try:
            if item.message is None:
                item.message = await self.tg.get_message_by_id(item.chat_id, item.message_id)
                if item.message is None:
                    raise Exception(f"Message {item.message_id} not found")
            
            chat_name, topic_name = await self._get_names(item)
            
            async def dl_progress(percent: float, current: int, total: int) -> None:
                await self._update_progress(item.key, {'stage': 'download', 'progress': percent,
                                                       'downloaded': current, 'total_size': total})
                if progress_callback:
                    if asyncio.iscoroutinefunction(progress_callback):
                        await progress_callback(percent, current, total)
                    else:
                        progress_callback(percent, current, total)
            
            result: Any = await self.tg.download(item.message, progress_callback=dl_progress)
            
            if result.success:
                item.local_path = result.local_path
                if item.file_size == 0:
                    item.file_size = result.file_size
                
                file_md5: str = await asyncio.to_thread(calculate_md5, result.local_path)
                await self.db.set_file_md5(item.chat_id, item.message_id, file_md5)
                await self.db.update_queue_item_paths(item.key, local_path=result.local_path, file_size=result.file_size)
                await self.db.record_downloaded(
                    chat_id=item.chat_id, message_id=item.message_id, filename=item.filename,
                    size=result.file_size, from_cache=result.from_cache, topic_id=item.topic_id,
                    chat_name=chat_name, topic_name=topic_name)
                
                next_status: FileStatus = FileStatus.PENDING_COMPRESS if await self._needs_compress(item.filename, item.file_size) else FileStatus.PENDING_UPLOAD
                await self.qs._update_item(item, next_status)
                return True
            
            raise Exception(result.error)
        except Exception as e:
            await self.qs._fail_item(item, str(e))
            return False
    
    async def compress(self, item: QueueItem, worker_id: int, 
                       progress_callback: Optional[Callable] = None) -> bool:
        """Сжимает медиафайл."""
        chat_name: Optional[str] = None
        topic_name: Optional[str] = None
        
        try:
            if not os.path.exists(item.local_path):
                raise Exception(f"File not found: {item.local_path}")
            
            chat_name, topic_name = await self._get_names(item)
            start_time: float = time.time()
            is_video: bool = await self.db.is_video(item.filename)
            
            async def comp_progress(percent: float, speed: float, eta: Optional[float]) -> None:
                if is_video:
                    if eta is None and speed > 0 and 0 < percent < 100:
                        elapsed: float = time.time() - start_time
                        if percent > 0:
                            eta = (100 - percent) * (elapsed / percent)
                    await self._update_progress(item.key, {'stage': 'compress', 'progress': percent,
                                                           'speed': speed, 'eta': eta})
                if progress_callback:
                    if asyncio.iscoroutinefunction(progress_callback):
                        await progress_callback(percent, speed, eta)
                    else:
                        progress_callback(percent, speed, eta)
            
            result: Any = await self.comp.compress(item.local_path, progress_callback=comp_progress if is_video else None)
            
            if result.success and result.was_compressed:
                is_valid: bool = await is_valid_video(result.compressed_path) if is_video else await asyncio.to_thread(is_valid_image, result.compressed_path)
                if not is_valid:
                    logger.warning(f"⚠️ Сжатый файл повреждён: {result.compressed_path}, используем оригинал")
                    if os.path.exists(result.compressed_path):
                        os.unlink(result.compressed_path)
                    item.compressed_path = item.local_path
                else:
                    logger.info(f"{'🎬' if is_video else '📸'} СЖАТО: {item.filename} "
                                f"{result.original_size/1024:.1f}KB → {result.compressed_size/1024:.1f}KB "
                                f"(экономия {result.saved_percent:.1f}%)")
                    item.compressed_path = result.compressed_path
                    await self.db.update_queue_item_paths(item.key, compressed_path=result.compressed_path)
                    await self.db.record_compressed(
                        chat_id=item.chat_id, message_id=item.message_id, filename=item.filename,
                        original_size=result.original_size, compressed_size=result.compressed_size,
                        compression_type=result.compression_type, topic_id=item.topic_id,
                        chat_name=chat_name, topic_name=topic_name)
            elif result.success and not result.was_compressed:
                logger.info(f"{'🎬' if is_video else '📸'} {item.filename} не требует сжатия: {result.decision}")
                item.compressed_path = item.local_path
            else:
                logger.warning(f"⚠️ Ошибка сжатия {item.filename}: {result.error}, загружаем оригинал")
                await self.db.record_file_error(
                    chat_id=item.chat_id, message_id=item.message_id,
                    filename=item.filename, stage='compress',
                    error=f"Сжатие не удалось: {result.error[:200]}",
                    topic_id=item.topic_id, chat_name=chat_name, topic_name=topic_name)
                item.compressed_path = item.local_path
            
            await self.qs._update_item(item, FileStatus.PENDING_UPLOAD)
            return True
        except Exception as e:
            logger.warning(f"⚠️ Исключение при сжатии {item.filename}: {e}, загружаем оригинал")
            await self.db.record_file_error(
                chat_id=item.chat_id, message_id=item.message_id,
                filename=item.filename, stage='compress',
                error=f"Исключение: {str(e)[:200]}",
                topic_id=item.topic_id, chat_name=chat_name, topic_name=topic_name)
            item.compressed_path = item.local_path
            await self.qs._update_item(item, FileStatus.PENDING_UPLOAD)
            return True
    
    async def upload(self, item: QueueItem, worker_id: int, 
                     progress_callback: Optional[Callable] = None) -> bool:
        """Загружает файл на Яндекс.Диск с предварительной проверкой валидности."""
        try:
            chat_name, topic_name = await self._get_names(item)
            path: str = item.compressed_path or item.local_path
            
            if not os.path.exists(path):
                if item.compressed_path and os.path.exists(item.local_path):
                    path = item.local_path
                    item.compressed_path = item.local_path
                else:
                    raise Exception(f"File not found: {path}")
            
            if await self.db.is_photo(item.filename):
                if not await asyncio.to_thread(is_valid_image, path):
                    os.unlink(path)
                    await self.db.record_file_error(
                        chat_id=item.chat_id, message_id=item.message_id,
                        filename=item.filename, stage='upload',
                        error="Invalid/corrupted image file",
                        topic_id=item.topic_id, chat_name=chat_name, topic_name=topic_name)
                    raise Exception("Invalid/corrupted image file")
            elif await self.db.is_video(item.filename):
                if not await is_valid_video(path):
                    os.unlink(path)
                    await self.db.record_file_error(
                        chat_id=item.chat_id, message_id=item.message_id,
                        filename=item.filename, stage='upload',
                        error="Invalid/corrupted video file",
                        topic_id=item.topic_id, chat_name=chat_name, topic_name=topic_name)
                    raise Exception("Invalid/corrupted video file")
            
            def up_progress(current: int, total: int) -> None:
                percent: float = (current / total * 100) if total > 0 else 0
                asyncio.create_task(self._update_progress(item.key, {'stage': 'upload', 'progress': percent,
                                                                      'uploaded': current, 'total_size': total}))
                if progress_callback:
                    if asyncio.iscoroutinefunction(progress_callback):
                        asyncio.create_task(progress_callback(current, total))
                    else:
                        progress_callback(current, total)
            
            result: Any = await self.ya.upload(local_path=path, remote_dir=item.remote_dir, filename=item.filename,
                                          check_exists=True, progress_callback=up_progress)
            
            if result.success:
                await self.db.update_file_state(item.chat_id, item.message_id, STATE_UPLOADED)
                await self.db.record_uploaded(
                    chat_id=item.chat_id, message_id=item.message_id, filename=item.filename,
                    size=result.size, compressed_size=os.path.getsize(path) if path != item.local_path else 0,
                    topic_id=item.topic_id, chat_name=chat_name, topic_name=topic_name)
                return True
            
            raise Exception(result.error)
        except Exception as e:
            await self.qs._fail_item(item, str(e))
            return False
    
    async def cleanup(self, item: QueueItem) -> None:
        """Удаляет временные файлы после успешной загрузки."""
        try:
            if item.compressed_path and item.compressed_path != item.local_path and os.path.exists(item.compressed_path):
                os.unlink(item.compressed_path)
                logger.debug(f"   🗑️ Удалён сжатый файл: {os.path.basename(item.compressed_path)}")
            if item.local_path and os.path.exists(item.local_path):
                os.unlink(item.local_path)
                logger.debug(f"   🗑️ Удалён локальный файл: {os.path.basename(item.local_path)}")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка очистки файлов: {e}")


class QueueSystem:
    """Основной класс системы очередей — оркестрация скачивания, сжатия и загрузки."""
    
    BATCH_SIZE: int = 1000
    REFILL_THRESHOLD: int = 100
    SCAN_INTERVAL: int = 3600
    
    def __init__(self, tg: Any, ya: Any, comp: Any, db: DatabaseManager, download_dir: str = "downloads") -> None:
        """Инициализирует систему очередей."""
        self.tg: Any = tg
        self.ya: Any = ya
        self.comp: Any = comp
        self.db: DatabaseManager = db
        self.download_dir: str = download_dir
        self.processor: FileProcessor = FileProcessor(tg, ya, comp, db, download_dir, self)
        self.running: bool = False
        self.pools: Dict[str, WorkerPool] = {}
        self.cpu_monitor: Optional[asyncio.Task] = None
        self._shutdown_manager: Optional[Any] = None
        self._retry_tasks: Set[asyncio.Task] = set()
        self._retry_tasks_lock: asyncio.Lock = asyncio.Lock()

    def set_shutdown_manager(self, shutdown_manager: Any) -> None:
        """Устанавливает менеджер завершения."""
        self._shutdown_manager = shutdown_manager

    async def _get_next(self, status: FileStatus, file_type: Optional[str] = None) -> Optional[QueueItem]:
        """Возвращает следующий элемент очереди."""
        exclude_keys: Set[str] = await self.db.get_processing_keys()
        item_dict: Optional[dict] = await self.db.get_next_queue_item(status.value, file_type, list(exclude_keys))
        if not item_dict:
            return None
        
        return QueueItem(
            chat_id=item_dict['chat_id'], message_id=item_dict['message_id'],
            filename=item_dict['filename'], remote_dir=item_dict['remote_dir'],
            status=FileStatus(item_dict['status']), local_path=item_dict['local_path'],
            compressed_path=item_dict['compressed_path'], file_size=item_dict['file_size'],
            attempts=item_dict['attempts'], max_attempts=item_dict['max_attempts'],
            last_error=item_dict['last_error'], last_attempt_time=item_dict['last_attempt_time'],
            created_at=item_dict['created_at'], updated_at=item_dict['updated_at'],
            metadata=item_dict['metadata'], file_info=item_dict['file_info'])

    async def _release_item(self, key: str) -> None:
        """Освобождает элемент очереди."""
        await self.db.remove_processing(key)

    async def _update_item(self, item: QueueItem, new_status: Optional[FileStatus] = None, 
                           release: bool = True) -> None:
        """Обновляет статус элемента."""
        if new_status:
            item.status = new_status
            await self.db.update_queue_status(item.key, new_status.value, item.attempts, item.last_error)
        item.updated_at = time.time()
        if release:
            await self._release_item(item.key)

    async def _complete_item(self, item: QueueItem) -> None:
        """Завершает обработку элемента."""
        db_conn: Any = await self.db.get_connection()
        await db_conn.execute("BEGIN IMMEDIATE")
        try:
            await db_conn.execute("DELETE FROM queue_items WHERE key = ?", (item.key,))
            await db_conn.execute("DELETE FROM queue_retry WHERE key = ?", (item.key,))
            await db_conn.execute("DELETE FROM active_progress WHERE key = ?", (item.key,))
            await db_conn.execute("DELETE FROM queue_processing WHERE key = ?", (item.key,))
            await db_conn.commit()
        except Exception as e:
            await db_conn.rollback()
            raise
        finally:
            await self._release_item(item.key)

    async def _fail_item(self, item: QueueItem, error: str) -> None:
        """Обрабатывает ошибку элемента."""
        db_conn: Any = await self.db.get_connection()
        await db_conn.execute("BEGIN IMMEDIATE")
        try:
            await db_conn.execute("DELETE FROM queue_retry WHERE key = ?", (item.key,))
            
            if not item.is_retryable_error(error):
                item.attempts = item.max_attempts
                item.last_error = error
                item.status = FileStatus.FAILED
                await db_conn.execute(
                    "UPDATE queue_items SET status = ?, attempts = ?, last_error = ?, updated_at = ? WHERE key = ?",
                    (item.status.value, item.attempts, item.last_error, time.time(), item.key))
                await db_conn.execute("UPDATE files SET state = ? WHERE chat_id = ? AND message_id = ?",
                                      (STATE_ERROR, item.chat_id, item.message_id))
                await db_conn.commit()
                await self._release_item(item.key)
                return
            
            item.record_attempt(error)
            
            if item.can_retry():
                item.status = FileStatus.PENDING_CHECK
                delay: float = item.get_retry_delay()
                logger.warning(f"⚠️ {item.filename} ошибка {item.attempts}/{item.max_attempts}: {error[:200]}")
                logger.info(f"   🔄 Повторная попытка через {delay:.1f} секунд")
                
                await db_conn.execute(
                    "UPDATE queue_items SET status = ?, attempts = ?, last_error = ?, updated_at = ? WHERE key = ?",
                    (item.status.value, item.attempts, item.last_error, time.time(), item.key))
                await db_conn.execute("INSERT OR REPLACE INTO queue_retry (key, retry_at) VALUES (?, ?)",
                                      (item.key, time.time() + delay))
                await db_conn.commit()
                asyncio.create_task(self._delayed_retry(item.key, delay))
                await self._release_item(item.key)
            else:
                item.status = FileStatus.FAILED
                logger.error(f"❌ {item.filename} окончательная ошибка после {item.attempts} попыток: {error[:200]}")
                await db_conn.execute(
                    "UPDATE queue_items SET status = ?, attempts = ?, last_error = ?, updated_at = ? WHERE key = ?",
                    (item.status.value, item.attempts, item.last_error, time.time(), item.key))
                await db_conn.execute("UPDATE files SET state = ? WHERE chat_id = ? AND message_id = ?",
                                      (STATE_ERROR, item.chat_id, item.message_id))
                await db_conn.commit()
                await self._release_item(item.key)
        except Exception as e:
            await db_conn.rollback()
            try:
                await self._release_item(item.key)
            except Exception:
                pass
            raise

    async def _delayed_retry(self, key: str, delay: float) -> None:
        """Отложенная повторная попытка."""
        task: Optional[asyncio.Task] = asyncio.current_task()
        async with self._retry_tasks_lock:
            if task:
                self._retry_tasks.add(task)
        
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            raise
        finally:
            async with self._retry_tasks_lock:
                if task:
                    self._retry_tasks.discard(task)
        
        if self.running:
            current: Optional[dict] = await self.db.get_queue_item(key)
            if current and current['status'] == STATUS_PENDING_CHECK:
                logger.info(f"🔄 Повторная попытка для {current['filename']} (попытка {current['attempts'] + 1}/{current['max_attempts']})")

    async def _worker(self, worker_id: int, pool_name: str, status: FileStatus, 
                      file_type: Optional[str] = None) -> None:
        """Универсальный воркер."""
        logger.info(f"{pool_name} worker {worker_id} запущен")
        processed: int = 0
        
        while self.running and not self.pools[pool_name]._stop_event.is_set():
            item: Optional[QueueItem] = None
            try:
                item = await self._get_next(status, file_type)
                if not item:
                    await asyncio.sleep(1)
                    continue
                
                await self.db.add_processing(item.key, worker_id, pool_name)
                processed += 1
                
                if pool_name == 'check':
                    await self.processor.check(item, worker_id)
                elif pool_name == 'download':
                    await self.processor.download(item, worker_id)
                elif pool_name in ('compress_photo', 'compress_video'):
                    await self.processor.compress(item, worker_id)
                elif pool_name == 'upload':
                    if await self.processor.upload(item, worker_id):
                        await self._complete_item(item)
                        await self.processor.cleanup(item)
                        continue
            except asyncio.CancelledError:
                if item:
                    if pool_name == 'download':
                        await self.processor.cleanup_partial(item)
                    await self._release_item(item.key)
                break
            except Exception as e:
                if item:
                    await self._release_item(item.key)
                if self.running:
                    await asyncio.sleep(5)
            finally:
                if item and pool_name != 'upload':
                    await self._release_item(item.key)
        
        self.pools[pool_name].add_processed(processed)
        logger.info(f"{pool_name} worker {worker_id} остановлен, обработано {processed} файлов")

    async def _check_worker(self, worker_id: int) -> None:
        """Воркер проверки."""
        await self._worker(worker_id, 'check', FileStatus.PENDING_CHECK)

    async def _download_worker(self, worker_id: int) -> None:
        """Воркер скачивания."""
        await self._worker(worker_id, 'download', FileStatus.PENDING_DOWNLOAD)

    async def _compress_photo_worker(self, worker_id: int) -> None:
        """Воркер сжатия фото."""
        await self._worker(worker_id, 'compress_photo', FileStatus.PENDING_COMPRESS, 'photo')

    async def _compress_video_worker(self, worker_id: int) -> None:
        """Воркер сжатия видео."""
        await self._worker(worker_id, 'compress_video', FileStatus.PENDING_COMPRESS, 'video')

    async def _upload_worker(self, worker_id: int) -> None:
        """Воркер загрузки."""
        await self._worker(worker_id, 'upload', FileStatus.PENDING_UPLOAD)

    async def _recover_inconsistent_tasks(self) -> None:
        """Восстанавливает несогласованные задачи после краша."""
        logger.info("🔍 Проверка несогласованных задач...")
        cursor: Any = await self.db.execute(
            """SELECT qi.* FROM queue_items qi
               LEFT JOIN queue_processing qp ON qi.key = qp.key
               WHERE qi.status IN (?, ?, ?) AND qp.key IS NULL""",
            (STATUS_PENDING_DOWNLOAD, STATUS_PENDING_COMPRESS, STATUS_PENDING_UPLOAD))
        rows: List[Any] = await cursor.fetchall()
        
        if not rows:
            logger.info("✅ Нет несогласованных задач")
            return
        
        recovered: int = 0
        failed: int = 0
        skipped: int = 0
        for row in rows:
            item: dict = dict(row)
            if item['status'] == STATUS_PENDING_UPLOAD:
                skipped += 1
                continue
            
            new_status: str = STATUS_PENDING_DOWNLOAD if item['status'] == STATUS_PENDING_COMPRESS else STATUS_PENDING_CHECK
            attempts: int = item.get('attempts', 0)
            
            if attempts >= item.get('max_attempts', 3):
                await self.db.update_queue_status(item['key'], STATUS_FAILED, attempts, "Recovery: max attempts exceeded")
                await self.db.update_file_state(item['chat_id'], item['message_id'], STATE_ERROR)
                failed += 1
            else:
                await self.db.update_queue_status(item['key'], new_status, attempts, f"Recovered from {item['status']} after crash")
                await self.db.execute("DELETE FROM active_progress WHERE key = ?", (item['key'],))
                recovered += 1
                logger.info(f"🔄 [{item['filename']}] Восстановлен: {item['status']} → {new_status}")
        
        await self.db.commit()
        logger.info(f"📊 Восстановление: {recovered} задач, {skipped} оставлены, {failed} FAILED")

    async def start(self) -> None:
        """Запускает систему очередей."""
        logger.info("🚀 Запуск системы очередей...")
        
        for item in await self.db.get_queue_items(STATUS_FAILED):
            await self.db.update_queue_status(item['key'], STATUS_PENDING_CHECK, 0, None)
        
        await self.db.clear_processing()
        await self._recover_inconsistent_tasks()
        
        self.running = True
        queue_settings: dict = await self.db.get_queue_settings()
        
        self.pools = {
            'check': WorkerPool("Check", self._check_worker, 1, queue_settings.get('check_workers', 5) * 2),
            'download': WorkerPool("Download", self._download_worker, 1, queue_settings.get('download_workers', 3) * 2),
            'compress_photo': WorkerPool("CompressPhoto", self._compress_photo_worker, 1, queue_settings.get('photo_workers', 2)),
            'compress_video': WorkerPool("CompressVideo", self._compress_video_worker, 1, queue_settings.get('video_workers', 1)),
            'upload': WorkerPool("Upload", self._upload_worker, 1, queue_settings.get('upload_workers', 3) * 2)
        }
        
        for name, pool in self.pools.items():
            try:
                await asyncio.wait_for(pool.start(), timeout=10.0)
                logger.info(f"✅ Пул {name} запущен")
            except Exception as e:
                logger.error(f"❌ Ошибка запуска пула {name}: {e}")
        
        self.cpu_monitor = asyncio.create_task(self._monitor_cpu())
        if total := await self.db.count_pending():
            logger.info(f"📊 В очереди {total} файлов")

    async def stop(self) -> None:
        """Останавливает систему очередей."""
        logger.info("🛑 Остановка системы очередей...")
        self.running = False
        
        async with self._retry_tasks_lock:
            for task in list(self._retry_tasks):
                if not task.done():
                    task.cancel()
            self._retry_tasks.clear()
        
        if self.cpu_monitor:
            self.cpu_monitor.cancel()
            try:
                await self.cpu_monitor
            except asyncio.CancelledError:
                pass
        
        stop_tasks: List[Any] = [pool.stop() for pool in self.pools.values()]
        try:
            await asyncio.wait_for(asyncio.gather(*stop_tasks, return_exceptions=True), timeout=15.0)
        except asyncio.TimeoutError:
            logger.warning("⚠️ Не все воркеры остановились за 15 секунд")
        
        await asyncio.sleep(2)
        
        if self.comp:
            if hasattr(self.comp, 'request_shutdown'):
                self.comp.request_shutdown()
            await self.comp.stop_all_ffmpeg()
            await asyncio.sleep(2)
        
        await self.db.checkpoint()
        logger.info("✅ Система очередей остановлена")

    async def _monitor_cpu(self) -> None:
        """Мониторит загрузку CPU и адаптирует пулы."""
        psutil.cpu_percent(interval=None)
        last_checkpoint: float = time.time()
        
        while self.running:
            try:
                await asyncio.sleep(1)
                cpu: float = psutil.cpu_percent(interval=None)
                counts: Dict[str, int] = await self.db.get_queue_counts()
                
                for name, pool in self.pools.items():
                    queue_size: int = counts.get(STATUS_PENDING_CHECK if name == 'check' else
                                            STATUS_PENDING_DOWNLOAD if name == 'download' else
                                            STATUS_PENDING_UPLOAD if name == 'upload' else
                                            STATUS_PENDING_COMPRESS, 0)
                    await pool.adjust(queue_size // 2 if name == 'compress_photo' else queue_size, cpu)
                
                if time.time() - last_checkpoint > 300:
                    await self.db.checkpoint()
                    last_checkpoint = time.time()
                
                await asyncio.sleep(9)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Ошибка монитора CPU: {e}")
                await asyncio.sleep(10)

    async def add_file(self, chat_id: int, message_id: int, filename: str, message: Any,
                       remote_dir: str, topic_id: Optional[int] = None, file_info: Optional[dict] = None) -> bool:
        """Добавляет файл в очередь."""
        await self.db.ensure_loaded()
        key: str = f"{chat_id}:{message_id}"
        
        if await self.db.get_queue_item(key):
            logger.debug(f"🔄 Файл {filename} уже в очереди")
            return False
        
        chat_name: str = await self.processor._get_chat_name(chat_id)
        topic_name: str = await self.processor._get_topic_name(chat_id, topic_id) if topic_id else "general"
        local_path: str = build_local_path(self.download_dir, chat_name, topic_name, filename)
        compressed_path: str = build_compressed_path(self.download_dir, chat_name, topic_name, filename)
        
        file_size: int = 0
        if file_info:
            file_size = file_info.get('size', 0)
        elif message:
            if hasattr(message, 'document') and message.document:
                file_size = message.document.file_size or 0
            elif hasattr(message, 'video') and message.video:
                file_size = message.video.file_size or 0
            elif hasattr(message, 'photo') and message.photo:
                file_size = max([s.file_size for s in message.photo.sizes]) if message.photo.sizes else 0
        
        next_status: FileStatus = FileStatus.PENDING_CHECK
        
        if os.path.exists(compressed_path) and os.path.getsize(compressed_path) > 0:
            is_valid: bool = False
            if await self.db.is_photo(filename):
                is_valid = await asyncio.to_thread(is_valid_image, compressed_path)
            elif await self.db.is_video(filename):
                is_valid = await is_valid_video(compressed_path)
            else:
                is_valid = True
            
            if is_valid:
                next_status = FileStatus.PENDING_UPLOAD
                if file_size == 0:
                    file_size = os.path.getsize(compressed_path)
            else:
                os.unlink(compressed_path)
        
        if next_status == FileStatus.PENDING_CHECK and os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            is_valid = False
            saved_md5: Optional[str] = await self.db.get_file_md5(chat_id, message_id)
            if saved_md5:
                if await asyncio.to_thread(calculate_md5, local_path) != saved_md5:
                    os.unlink(local_path)
                else:
                    is_valid = True
            else:
                if await self.db.is_photo(filename):
                    is_valid = await asyncio.to_thread(is_valid_image, local_path)
                elif await self.db.is_video(filename):
                    is_valid = await is_valid_video(local_path)
                else:
                    is_valid = True
                if is_valid:
                    await self.db.set_file_md5(chat_id, message_id, await asyncio.to_thread(calculate_md5, local_path))
            
            if is_valid:
                if file_size == 0:
                    file_size = os.path.getsize(local_path)
                next_status = FileStatus.PENDING_COMPRESS if await self.processor._needs_compress(filename, file_size) else FileStatus.PENDING_UPLOAD
            else:
                os.unlink(local_path)
        
        item: QueueItem = QueueItem(
            chat_id=chat_id, message_id=message_id, filename=filename, remote_dir=remote_dir,
            message=message, status=next_status,
            local_path=local_path if os.path.exists(local_path) else "",
            compressed_path=compressed_path if os.path.exists(compressed_path) else "",
            file_size=file_size, file_info=file_info or {})
        if topic_id is not None:
            item.metadata['topic_id'] = topic_id
        
        success: bool = await self.db.add_queue_item({
            'chat_id': item.chat_id, 'message_id': item.message_id, 'topic_id': item.topic_id,
            'filename': item.filename, 'remote_dir': item.remote_dir,
            'local_path': item.local_path, 'compressed_path': item.compressed_path,
            'file_size': item.file_size, 'status': item.status.value,
            'attempts': item.attempts, 'max_attempts': item.max_attempts,
            'created_at': item.created_at, 'updated_at': item.updated_at,
            'metadata': item.metadata, 'file_info': item.file_info})
        if success:
            await self.db.record_queued(chat_id, filename, file_size, topic_id)
            logger.debug(f"✅ Добавлен в очередь: {filename} (статус: {item.status.value}, размер: {fmt_size(file_size)})")
        return success

    async def get_status(self) -> dict:
        """Возвращает статус системы очередей."""
        counts: Dict[str, int] = await self.db.get_queue_counts()
        active_items: List[dict] = await self.db.get_active_items()
        
        active_files: List[dict] = []
        downloading: List[dict] = []
        compressing: List[dict] = []
        uploading: List[dict] = []
        for item in active_items:
            worker_type: str = item.get('worker_type', '')
            active_item: dict = {
                'filename': item['filename'], 'message_id': item['message_id'], 'chat_id': item['chat_id'],
                'stage': worker_type, 'size': item.get('file_size', 0), 'topic_id': item.get('topic_id'),
                'progress': item.get('progress'), 'speed': item.get('speed'), 'eta': item.get('eta'),
                'downloaded': item.get('downloaded'), 'uploaded': item.get('uploaded'), 'total_size': item.get('total_size')}
            active_files.append(active_item)
            if worker_type == 'download':
                downloading.append(active_item)
            elif worker_type in ('compress_photo', 'compress_video'):
                compressing.append(active_item)
            elif worker_type == 'upload':
                uploading.append(active_item)
        
        return {
            'running': self.running, 'queues': counts,
            'active_files': active_files, 'downloading': downloading,
            'compressing': compressing, 'uploading': uploading,
            'total_pending': await self.db.count_pending()}

    async def _add_selected_files_batch(self, chat_ids: List[int], limit: int) -> int:
        """Добавляет пачку выбранных файлов в очередь (все STATE_SELECTED)."""
        added: int = 0
        files_to_check: List[str] = []
        files_info_map: Dict[str, Tuple[int, dict, dict, str]] = {}
        
        for cid in chat_ids:
            if added >= limit:
                break
                
            chat_name: str = await self.processor._get_chat_name(cid)
            
            for topic in await self.db.get_topics(cid):
                if added >= limit:
                    break
                
                remaining: int = limit - added
                
                # Берём ВСЕ файлы со статусом STATE_SELECTED в этой теме
                files: List[dict] = await self.db.get_files(cid, topic['topic_id'], state_filter=STATE_SELECTED)
                files = files[:remaining]
                
                for file_info in files:
                    key: str = f"{cid}:{file_info['message_id']}"
                    files_to_check.append(key)
                    files_info_map[key] = (cid, topic, file_info, chat_name)
        
        if not files_to_check:
            return 0
        
        existing_keys: Set[str] = await self.db.are_files_in_queue(files_to_check)
        
        for key in files_to_check:
            if key in existing_keys:
                continue
                
            cid, topic, file_info, chat_name = files_info_map[key]
            remote_dir: str = self.ya.build_remote_path(chat_name, topic['topic_name'])
            
            if await self.add_file(
                chat_id=cid,
                message_id=file_info['message_id'],
                filename=file_info['filename'],
                message=None,
                remote_dir=remote_dir,
                topic_id=topic['topic_id'],
                file_info=file_info
            ):
                added += 1
                logger.info(f"   ✅ Добавлен: {file_info['filename']}")
        
        return added

    async def _scan_new_files(self, chat_ids: List[int]) -> None:
        """Инкрементальное сканирование новых файлов."""
        logger.info("🔄 Проверка новых файлов в выбранных темах...")
        for cid in chat_ids:
            if self._shutdown_manager and self._shutdown_manager.is_requested():
                break
            await self.tg.incremental_scan_chat(cid, self.db)

    async def _mark_new_files_as_selected(self, chat_ids: List[int]) -> None:
        """Переводит новые файлы в статус SELECTED."""
        logger.info("📌 Перевод новых файлов в выбранных темах в статус SELECTED...")
        for cid in chat_ids:
            for topic in await self.db.get_topics(cid):
                if topic['is_selected']:
                    cursor: Any = await self.db.execute(
                        "UPDATE files SET state = ? WHERE chat_id = ? AND topic_id = ? AND state = ?",
                        (STATE_SELECTED, cid, topic['topic_id'], STATE_NEW))
                    if cursor.rowcount > 0:
                        logger.info(f"   ✅ Тема '{topic['topic_name']}': {cursor.rowcount} новых файлов выбрано")
                    await self.db.commit()

    async def process_selected_files(self) -> int:
        """Главный метод обработки выбранных файлов."""
        logger.info("📋 ОБРАБОТКА ВЫБРАННЫХ ФАЙЛОВ")
        await self.db.ensure_loaded()
        
        chat_ids: List[int] = await self.db.get_chat_ids()
        all_stats: dict = await self.db.get_all_stats()
        await self.db.set_app_state('selected_snapshot', all_stats.get('selected_files', 0))
        
        if not self.running:
            await self.start()
        
        # 🔑 Сразу запускаем инкрементальное сканирование при старте
        await self._scan_new_files(chat_ids)
        await self._mark_new_files_as_selected(chat_ids)
        
        total_processed: int = 0
        last_scan_time: float = time.time()
        
        while True:
            if self._shutdown_manager and self._shutdown_manager.is_requested():
                break
            
            pending: int = await self.db.count_pending()
            
            if pending < self.REFILL_THRESHOLD:
                logger.info(f"📦 Очередь: {pending} файлов. Проверяем новые файлы...")
                
                now: float = time.time()
                if now - last_scan_time > self.SCAN_INTERVAL:
                    logger.info("🔄 Периодическое инкрементальное сканирование...")
                    await self._scan_new_files(chat_ids)
                    await self._mark_new_files_as_selected(chat_ids)
                    last_scan_time = now
                
                added: int = await self._add_selected_files_batch(chat_ids, self.BATCH_SIZE)
                
                if added == 0 and pending == 0:
                    has_more: bool = await self.db.has_selected_files_remaining(chat_ids)
                    if not has_more:
                        logger.info("✅ Все выбранные файлы обработаны")
                        break
                    else:
                        logger.warning("⚠️ Есть выбранные файлы, но они не добавились в очередь. Проверяем через 30с.")
                        await asyncio.sleep(30)
                        continue
                
                if added > 0:
                    logger.info(f"📊 Добавлено в очередь: {added} файлов")
                    total_processed += added
            
            await asyncio.sleep(30)
            
            if int(time.time()) % 300 < 30:
                await self.db.checkpoint()
        
        if not (self._shutdown_manager and self._shutdown_manager.is_requested()):
            await self.cleanup_all_downloads(force=True)
        
        logger.info(f"✅ Загружено {total_processed} файлов")
        return total_processed

    async def cleanup_all_downloads(self, force: bool = False) -> None:
        """Очищает папку downloads. Вызывается ТОЛЬКО когда очередь пуста и все файлы обработаны."""
        if not os.path.exists(self.download_dir):
            return
        
        logger.info("🧹 Очистка папки downloads... (ПОЛНАЯ)")
        download_path: Path = Path(self.download_dir)
        
        for item in download_path.iterdir():
            if item.name.endswith('.session') or item.name.endswith('.session-journal'):
                continue
            
            try:
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()
            except Exception as e:
                logger.warning(f"⚠️ Не удалось удалить {item.name}: {e}")
        
        logger.info("✅ Очистка завершена")
