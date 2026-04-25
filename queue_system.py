#!/usr/bin/env python3
"""
Система очередей - ОДНОРАЗОВЫЕ ВОРКЕРЫ, АТОМАРНЫЙ ЗАХВАТ, ВАЛИДАЦИЯ
ВЕРСИЯ 0.17.17 — ПОЛНЫЙ РЕФАКТОРИНГ
"""

__version__ = "0.17.17"

import os
import asyncio
import logging
import time
import shutil
import json
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


# =============================================================================
# КОНСТАНТЫ
# =============================================================================

STUCK_TASK_TIMEOUT = 1800  # 30 минут — задача считается зависшей
WORKER_SPAWN_DELAY = 0.1   # Задержка между созданием воркеров
POOL_MAINTAIN_INTERVAL = 2 # Интервал проверки пула
MONITOR_INTERVAL = 10      # Интервал мониторинга ресурсов
CHECKPOINT_INTERVAL = 300  # Интервал checkpoint БД


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ СТРУКТУРЫ
# =============================================================================

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
        non_retryable = [
            "File not found", "Message not found", "MessageIdInvalid",
            "MessageEmpty", "Unauthorized", "Invalid token", "Access denied",
            "Permission denied", "PeerIdInvalid", "Chat not found",
            "Path not found", "Insufficient storage", "No space left"
        ]
        error_lower = error.lower()
        return not any(non.lower() in error_lower for non in non_retryable)

    def record_attempt(self, error: str) -> None:
        """Записывает попытку обработки."""
        self.attempts += 1
        self.last_error = error
        self.last_attempt_time = time.time()

    def get_upload_path(self) -> str:
        """Возвращает путь файла для загрузки."""
        return self.compressed_path or self.local_path


@dataclass
class ValidationResult:
    """Результат валидации файла."""
    is_valid: bool
    reason: str = ""
    warning: str = ""
    can_retry: bool = True
    suggestion: Optional[str] = None
    details: dict = field(default_factory=dict)


# =============================================================================
# ПУЛ ОДНОРАЗОВЫХ ВОРКЕРОВ
# =============================================================================

class WorkerPool:
    """Пул одноразовых воркеров с автоматическим поддержанием размера."""

    def __init__(self, name: str, worker_factory: Callable,
                 min_workers: int = 1, max_workers: int = 5) -> None:
        """Инициализирует пул воркеров."""
        self.name = name
        self.worker_factory = worker_factory
        self.min = min_workers
        self.max = max_workers
        self.target = min_workers
        self.current = 0
        self.tasks: List[asyncio.Task] = []
        self.running = False
        self._maintainer: Optional[asyncio.Task] = None
        self._processed_count = 0
        self._lock = asyncio.Lock()
        self._last_adjust = 0.0
        logger.info(f"📊 {self.name}: пул {min_workers}-{max_workers}")

    async def start(self) -> None:
        """Запускает поддержание пула."""
        self.running = True
        self._maintainer = asyncio.create_task(self._maintain_pool())
        logger.info(f"✅ {self.name}: пул запущен (target={self.target})")

    async def stop(self) -> None:
        """Останавливает пул."""
        if not self.running:
            return

        logger.info(f"🛑 Останавливаю {self.name}...")
        self.running = False

        if self._maintainer:
            self._maintainer.cancel()
            try:
                await self._maintainer
            except asyncio.CancelledError:
                pass

        active = [t for t in self.tasks if not t.done()]
        if active:
            logger.info(f"   ⏳ Ожидание {len(active)} активных воркеров...")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*active, return_exceptions=True),
                    timeout=30.0
                )
            except asyncio.TimeoutError:
                logger.warning(f"   ⚠️ Не все воркеры завершились за 30с")
                for t in active:
                    if not t.done():
                        t.cancel()

        logger.info(f"🛑 {self.name}: остановлен, обработано {self._processed_count} файлов")

    async def _maintain_pool(self) -> None:
        """Поддерживает целевое количество воркеров."""
        while self.running:
            try:
                alive = sum(1 for t in self.tasks if not t.done())

                for i in range(self.target - alive):
                    if not self.running:
                        break
                    worker_id = f"{self.name}_{int(time.time()*1000)}_{alive + i}"
                    task = asyncio.create_task(
                        self._run_one_worker(worker_id),
                        name=worker_id
                    )
                    async with self._lock:
                        self.tasks.append(task)
                        self.current = len([t for t in self.tasks if not t.done()])
                    await asyncio.sleep(WORKER_SPAWN_DELAY)

                async with self._lock:
                    self.tasks = [t for t in self.tasks if not t.done()]
                    self.current = len(self.tasks)

                await asyncio.sleep(POOL_MAINTAIN_INTERVAL)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Ошибка поддержания пула {self.name}: {e}")
                await asyncio.sleep(5)

    async def _run_one_worker(self, worker_id: str) -> None:
        """Запускает один воркер для обработки одной задачи."""
        logger.debug(f"🚀 Воркер {worker_id} запущен")
        try:
            await self.worker_factory(worker_id)
            self._processed_count += 1
        except asyncio.CancelledError:
            logger.debug(f"🛑 Воркер {worker_id} отменен")
        except Exception as e:
            logger.error(f"❌ Воркер {worker_id} упал: {e}")
        finally:
            logger.debug(f"🏁 Воркер {worker_id} завершен")

    async def adjust(self, queue_size: int, cpu_percent: float) -> None:
        """Адаптирует целевое количество воркеров."""
        if not self.running:
            return

        now = time.time()
        new_target = self.target

        if self.name.lower() == 'check':
            if queue_size > self.target * 5 and self.target < self.max:
                new_target = min(self.max, self.target + 2)
            elif queue_size == 0 and self.target > self.min:
                new_target = max(self.min, self.target - 1)

        elif self.name.lower() == 'download':
            if queue_size > self.target * 3 and self.target < self.max:
                new_target = min(self.max, self.target + 1)
            elif queue_size == 0 and self.target > self.min:
                new_target = max(self.min, self.target - 1)

        elif self.name.lower() == 'compress_photo':
            if cpu_percent > 60:
                new_target = self.min
            elif cpu_percent < 40 and queue_size > self.target * 3 and self.target < self.max:
                new_target = min(self.max, self.target + 1)
            elif queue_size == 0 and self.target > self.min:
                new_target = max(self.min, self.target - 1)

        elif self.name.lower() == 'compress_video':
            if cpu_percent > 50:
                new_target = self.min
            elif cpu_percent < 25 and queue_size >= 2 and self.target < self.max:
                new_target = min(self.max, self.target + 1)
            elif queue_size == 0 and self.target > self.min:
                new_target = max(self.min, self.target - 1)

        elif self.name.lower() == 'upload':
            if queue_size > self.target * 2 and self.target < self.max:
                new_target = min(self.max, self.target + 1)
            elif queue_size == 0 and self.target > self.min:
                new_target = max(self.min, self.target - 1)

        logger.debug(f"🔧 {self.name}: new={new_target} != target={self.target} = {new_target != self.target}, diff={now - self._last_adjust:.0f} >= 30 = {now - self._last_adjust >= 30}")
        if new_target != self.target and now - self._last_adjust >= 30:
            logger.info(
                f"🔄 {self.name}: target {self.target}→{new_target} "
                f"(очередь: {queue_size}, CPU: {cpu_percent:.0f}%)"
            )
            self.target = new_target
            self._last_adjust = now

    def add_processed(self, count: int = 1) -> None:
        """Добавляет количество обработанных файлов."""
        self._processed_count += count


# =============================================================================
# ОБРАБОТЧИК ФАЙЛОВ
# =============================================================================

class FileProcessor:
    """Обрабатывает один файл через стейт-машину: check → download → compress → upload."""

    def __init__(self, tg: Any, ya: Any, comp: Any, db: DatabaseManager,
                 download_dir: str, queue_system: 'QueueSystem') -> None:
        """Инициализирует процессор файлов."""
        self.tg = tg
        self.ya = ya
        self.comp = comp
        self.db = db
        self.download_dir = download_dir
        self.qs = queue_system
        self._chat_name_cache: Dict[int, str] = {}
        self._topic_name_cache: Dict[str, str] = {}

    # -------------------------------------------------------------------------
    # КЭШИРОВАНИЕ ИМЁН
    # -------------------------------------------------------------------------

    async def _get_chat_name(self, chat_id: int) -> str:
        """Возвращает имя чата с кэшированием."""
        if chat_id not in self._chat_name_cache:
            self._chat_name_cache[chat_id] = await self.db.get_chat_name(chat_id)
        return self._chat_name_cache[chat_id]

    async def _get_topic_name(self, chat_id: int, topic_id: Optional[int]) -> Optional[str]:
        """Возвращает имя темы с кэшированием."""
        if topic_id is None:
            return None
        cache_key = f"{chat_id}:{topic_id}"
        if cache_key not in self._topic_name_cache:
            topics = await self.db.get_topics(chat_id)
            for t in topics:
                if t['topic_id'] == topic_id:
                    self._topic_name_cache[cache_key] = t['topic_name']
                    break
            else:
                self._topic_name_cache[cache_key] = f"Тема {topic_id}"
        return self._topic_name_cache[cache_key]

    async def _get_names(self, item: QueueItem) -> Tuple[str, Optional[str]]:
        """Возвращает имена чата и темы."""
        chat_name = await self._get_chat_name(item.chat_id)
        topic_name = await self._get_topic_name(item.chat_id, item.topic_id) if item.topic_id else None
        return chat_name, topic_name

    # -------------------------------------------------------------------------
    # ПРОВЕРКА НЕОБХОДИМОСТИ СЖАТИЯ
    # -------------------------------------------------------------------------

    async def _needs_compress(self, filename: str, file_size: int) -> bool:
        """Проверяет, нужно ли сжимать файл."""
        settings = await self.db.get_compression_settings()
        if await self.db.is_photo(filename):
            return file_size >= settings.get('min_photo_size_kb', 500) * 1024
        if await self.db.is_video(filename):
            return file_size >= settings.get('min_video_size_mb', 15) * 1024 * 1024
        return False

    # -------------------------------------------------------------------------
    # ПРОГРЕСС
    # -------------------------------------------------------------------------

    async def _update_progress(self, key: str, data: dict) -> None:
        """Обновляет прогресс обработки."""
        await self.db.update_progress(key, data)

    # -------------------------------------------------------------------------
    # ОЧИСТКА
    # -------------------------------------------------------------------------

    async def cleanup_partial(self, item: QueueItem) -> None:
        """Удаляет частично скачанный файл при отмене."""
        try:
            if item.local_path and os.path.exists(item.local_path):
                os.unlink(item.local_path)
                logger.debug(f"🗑️ Удалён частичный файл: {os.path.basename(item.local_path)}")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка удаления частичного файла: {e}")

    async def cleanup(self, item: QueueItem) -> None:
        """Удаляет временные файлы после успешной загрузки."""
        try:
            if item.compressed_path and item.compressed_path != item.local_path \
                    and os.path.exists(item.compressed_path):
                os.unlink(item.compressed_path)
                logger.debug(f"   🗑️ Удалён сжатый файл: {os.path.basename(item.compressed_path)}")
            if item.local_path and os.path.exists(item.local_path):
                os.unlink(item.local_path)
                logger.debug(f"   🗑️ Удалён локальный файл: {os.path.basename(item.local_path)}")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка очистки файлов: {e}")

    # -------------------------------------------------------------------------
    # ВАЛИДАЦИЯ
    # -------------------------------------------------------------------------

    async def _validate_downloaded_file(self, path: str, expected_size: int = 0) -> ValidationResult:
        """Валидация скачанного файла."""
        if not os.path.exists(path):
            return ValidationResult(False, "File not found", can_retry=True)

        actual_size = os.path.getsize(path)
        if actual_size == 0:
            return ValidationResult(False, "Empty file", can_retry=False)

        if expected_size > 0 and actual_size != expected_size:
            return ValidationResult(
                False,
                f"Size mismatch: expected {expected_size}, got {actual_size}",
                can_retry=True,
                suggestion="re_download"
            )

        return ValidationResult(True, details={'size': actual_size})

    async def _validate_compressed_file(self, path: str, filename: str) -> ValidationResult:
        """Валидация сжатого файла."""
        if not os.path.exists(path):
            return ValidationResult(False, "Compressed file not found", can_retry=True)

        if os.path.getsize(path) == 0:
            return ValidationResult(False, "Empty compressed file", can_retry=False)

        is_video = await self.db.is_video(filename)

        if is_video:
            return await self._validate_video(path)
        else:
            return await self._validate_photo(path)

    async def _validate_photo(self, path: str) -> ValidationResult:
        """Валидация изображения."""
        try:
            from PIL import Image
            with Image.open(path) as img:
                img.verify()
            return ValidationResult(True, details={'type': 'photo', 'verified': True})
        except Exception as e:
            return ValidationResult(
                False,
                f"Invalid image: {str(e)[:100]}",
                can_retry=False,
                suggestion="use_original"
            )

    async def _validate_video(self, path: str) -> ValidationResult:
        """Валидация видео."""
        import shutil
        if not shutil.which('ffprobe'):
            return ValidationResult(True, warning="ffprobe not available, skipping check")

        try:
            cmd = [
                'ffprobe', '-v', 'quiet', '-print_format', 'json',
                '-show_streams', path
            ]
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await asyncio.wait_for(process.communicate(), timeout=30)

            if process.returncode != 0:
                return ValidationResult(
                    False, "ffprobe failed", can_retry=False, suggestion="use_original"
                )

            data = json.loads(stdout)
            has_video = any(
                s.get('codec_type') == 'video'
                for s in data.get('streams', [])
            )

            if not has_video:
                return ValidationResult(
                    False, "No video stream found", can_retry=False, suggestion="use_original"
                )

            return ValidationResult(True, details={'type': 'video', 'verified': True})

        except asyncio.TimeoutError:
            return ValidationResult(
                False, "Validation timeout", can_retry=True, suggestion="use_original"
            )
        except Exception as e:
            return ValidationResult(
                False,
                f"Validation error: {str(e)[:100]}",
                can_retry=True,
                suggestion="use_original"
            )

    # -------------------------------------------------------------------------
    # ЭТАПЫ ОБРАБОТКИ
    # -------------------------------------------------------------------------

    async def check(self, item: QueueItem, worker_id: str) -> bool:
        """Проверяет существование файла на Яндекс.Диске."""
        try:
            # Быстрая проверка — если уже загружен, не ходим в Яндекс
            current_state = await self.db.get_file_state(item.chat_id, item.message_id)
            if current_state == STATE_UPLOADED:
                await self.db.record_skipped(
                    chat_id=item.chat_id, message_id=item.message_id,
                    filename=item.filename, size=item.file_size, reason="already_uploaded",
                    topic_id=item.topic_id, chat_name=await self._get_chat_name(item.chat_id),
                    topic_name=await self._get_topic_name(item.chat_id, item.topic_id) if item.topic_id else None)
                await self.qs._complete_item(item)
                logger.info(f"⏭️ Пропущен (уже загружен): {item.filename}")
                return False
            
            chat_name, topic_name = await self._get_names(item)
            exists, size, _ = await self.ya.file_exists(item.remote_dir, item.filename)

            if exists and size > 0:
                await self.db.record_skipped(
                    chat_id=item.chat_id, message_id=item.message_id,
                    filename=item.filename, size=size, reason="exists_on_yandex",
                    topic_id=item.topic_id, chat_name=chat_name, topic_name=topic_name
                )
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

    async def download(self, item: QueueItem, worker_id: str) -> bool:
        """Скачивает файл из Telegram с валидацией после загрузки."""
        try:
            if item.message is None:
                item.message = await self.tg.get_message_by_id(item.chat_id, item.message_id)
                if item.message is None:
                    raise Exception(f"Message {item.message_id} not found")

            chat_name, topic_name = await self._get_names(item)

            async def dl_progress(percent, current, total):
                await self._update_progress(item.key, {
                    'stage': 'download', 'progress': percent,
                    'downloaded': current, 'total_size': total
                })

            result = await self.tg.download(item.message, progress_callback=dl_progress)

            if not result.success:
                raise Exception(result.error)

            # ВАЛИДАЦИЯ ПОСЛЕ СКАЧИВАНИЯ
            validation = await self._validate_downloaded_file(
                result.local_path, result.file_size
            )

            if not validation.is_valid:
                if os.path.exists(result.local_path):
                    os.unlink(result.local_path)
                raise Exception(f"Downloaded file invalid: {validation.reason}")

            item.local_path = result.local_path
            if item.file_size == 0:
                item.file_size = result.file_size

            file_md5 = await asyncio.to_thread(calculate_md5, result.local_path)
            await self.db.set_file_md5(item.chat_id, item.message_id, file_md5)
            await self.db.update_queue_item_paths(
                item.key, local_path=result.local_path, file_size=result.file_size
            )
            await self.db.record_downloaded(
                chat_id=item.chat_id, message_id=item.message_id, filename=item.filename,
                size=result.file_size, from_cache=result.from_cache, topic_id=item.topic_id,
                chat_name=chat_name, topic_name=topic_name
            )

            next_status = (
                FileStatus.PENDING_COMPRESS
                if await self._needs_compress(item.filename, item.file_size)
                else FileStatus.PENDING_UPLOAD
            )
            await self.qs._update_item(item, next_status)
            logger.info(f"📥 Скачан: {item.filename} ({fmt_size(item.file_size)})")
            return True

        except Exception as e:
            await self.qs._fail_item(item, str(e))
            return False

    async def compress(self, item: QueueItem, worker_id: str) -> bool:
        """Сжимает медиафайл с валидацией результата."""
        chat_name = None
        topic_name = None

        try:
            if not os.path.exists(item.local_path):
                raise Exception(f"File not found: {item.local_path}")

            chat_name, topic_name = await self._get_names(item)
            is_video = await self.db.is_video(item.filename)

            async def comp_progress(percent, speed, eta):
                await self._update_progress(item.key, {
                    'stage': 'compress', 'progress': percent,
                    'speed': speed, 'eta': eta
                })

            if not is_video:
                await self._update_progress(item.key, {'stage': 'compress_photo'})
            result = await self.comp.compress(
                item.local_path,
                progress_callback=comp_progress if is_video else None
            )

            if result.success and result.was_compressed:
                # ВАЛИДАЦИЯ ПОСЛЕ СЖАТИЯ
                validation = await self._validate_compressed_file(
                    result.compressed_path, item.filename
                )

                if not validation.is_valid:
                    logger.warning(
                        f"⚠️ Сжатый файл невалиден: {validation.reason}, "
                        f"используем оригинал"
                    )
                    if os.path.exists(result.compressed_path):
                        os.unlink(result.compressed_path)
                    item.compressed_path = item.local_path
                    await self.db.record_file_error(
                        chat_id=item.chat_id, message_id=item.message_id,
                        filename=item.filename, stage='compress_validation',
                        error=f"Invalid compressed file: {validation.reason}",
                        topic_id=item.topic_id, chat_name=chat_name, topic_name=topic_name
                    )
                else:
                    logger.info(
                        f"{'🎬' if is_video else '📸'} СЖАТО: {item.filename} "
                        f"{result.original_size/1024:.1f}KB → {result.compressed_size/1024:.1f}KB "
                        f"(экономия {result.saved_percent:.1f}%)"
                    )
                    item.compressed_path = result.compressed_path
                    try:
                        await self.db.update_queue_item_paths(
                            item.key, compressed_path=result.compressed_path
                        )
                        await self.db.record_compressed(
                        chat_id=item.chat_id, message_id=item.message_id,
                        filename=item.filename,
                        original_size=result.original_size,
                        compressed_size=result.compressed_size,
                        compression_type=result.compression_type,
                        topic_id=item.topic_id, chat_name=chat_name, topic_name=topic_name
                    )
                    except Exception:
                        pass  # БД занята — не критично для сжатия

            elif result.success and not result.was_compressed:
                logger.info(
                    f"{'🎬' if is_video else '📸'} {item.filename} "
                    f"не требует сжатия: {result.decision}"
                )
                item.compressed_path = item.local_path
            else:
                logger.warning(
                    f"⚠️ Ошибка сжатия {item.filename}: {result.error}, "
                    f"загружаем оригинал"
                )
                await self.db.record_file_error(
                    chat_id=item.chat_id, message_id=item.message_id,
                    filename=item.filename, stage='compress',
                    error=f"Сжатие не удалось: {result.error[:200]}",
                    topic_id=item.topic_id, chat_name=chat_name, topic_name=topic_name
                )
                item.compressed_path = item.local_path

            await self.qs._update_item(item, FileStatus.PENDING_UPLOAD)
            return True

        except Exception as e:
            logger.warning(
                f"⚠️ Исключение при сжатии {item.filename}: {e}, "
                f"загружаем оригинал"
            )
            await self.db.record_file_error(
                chat_id=item.chat_id, message_id=item.message_id,
                filename=item.filename, stage='compress',
                error=f"Исключение: {str(e)[:200]}",
                topic_id=item.topic_id, chat_name=chat_name, topic_name=topic_name
            )
            item.compressed_path = item.local_path
            await self.qs._update_item(item, FileStatus.PENDING_UPLOAD)
            return True

    async def upload(self, item: QueueItem, worker_id: str) -> bool:
        """Загружает файл на Яндекс.Диск с предварительной валидацией."""
        try:
            chat_name, topic_name = await self._get_names(item)
            path = item.get_upload_path()

            if not os.path.exists(path):
                if item.compressed_path and os.path.exists(item.local_path):
                    path = item.local_path
                    item.compressed_path = item.local_path
                else:
                    raise Exception(f"File not found: {path}")

            # ВАЛИДАЦИЯ ПЕРЕД ЗАГРУЗКОЙ
            if await self.db.is_photo(item.filename):
                if not await asyncio.to_thread(is_valid_image, path):
                    os.unlink(path)
                    await self.db.record_file_error(
                        chat_id=item.chat_id, message_id=item.message_id,
                        filename=item.filename, stage='upload_validation',
                        error="Invalid/corrupted image file",
                        topic_id=item.topic_id, chat_name=chat_name, topic_name=topic_name
                    )
                    raise Exception("Invalid/corrupted image file")

            elif await self.db.is_video(item.filename):
                if not await is_valid_video(path):
                    os.unlink(path)
                    await self.db.record_file_error(
                        chat_id=item.chat_id, message_id=item.message_id,
                        filename=item.filename, stage='upload_validation',
                        error="Invalid/corrupted video file",
                        topic_id=item.topic_id, chat_name=chat_name, topic_name=topic_name
                    )
                    raise Exception("Invalid/corrupted video file")

            def up_progress(current, total):
                percent = (current / total * 100) if total > 0 else 0
                asyncio.create_task(self._update_progress(item.key, {
                    'stage': 'upload', 'progress': percent,
                    'uploaded': current, 'total_size': total
                }))

            result = await self.ya.upload(
                local_path=path,
                remote_dir=item.remote_dir,
                filename=item.filename,
                check_exists=True,
                progress_callback=up_progress
            )

            if result.success:
                # Яндекс сам проверил MD5 и размер внутри upload()
                await self.db.update_file_state(
                    item.chat_id, item.message_id, STATE_UPLOADED
                )
                await self.db.record_uploaded(
                    chat_id=item.chat_id, message_id=item.message_id,
                    filename=item.filename, size=result.size,
                    compressed_size=os.path.getsize(path) if path != item.local_path else 0,
                    topic_id=item.topic_id, chat_name=chat_name, topic_name=topic_name
                )
                logger.info(f"📤 Загружен: {item.filename}")
                return True

            raise Exception(result.error)

        except Exception as e:
            await self.qs._fail_item(item, str(e))
            return False


# =============================================================================
# СИСТЕМА ОЧЕРЕДЕЙ
# =============================================================================

class QueueSystem:
    """Основной класс системы очередей — оркестрация скачивания, сжатия и загрузки."""

    BATCH_SIZE = 1000
    REFILL_THRESHOLD = 100
    
    # Маппинг пулов на статусы очереди
    POOL_STATUS_MAP = {
        'check': STATUS_PENDING_CHECK,
        'download': STATUS_PENDING_DOWNLOAD,
        'compress_photo': STATUS_PENDING_COMPRESS,
        'compress_video': STATUS_PENDING_COMPRESS,
        'upload': STATUS_PENDING_UPLOAD,
    }

    def __init__(self, tg: Any, ya: Any, comp: Any, db: DatabaseManager,
                 download_dir: str = "downloads") -> None:
        """Инициализирует систему очередей."""
        self.tg = tg
        self.ya = ya
        self.comp = comp
        self.db = db
        self.download_dir = download_dir
        self.processor = FileProcessor(tg, ya, comp, db, download_dir, self)
        self.running = False
        self.pools: Dict[str, WorkerPool] = {}
        self._monitor_task: Optional[asyncio.Task] = None
        self._shutdown_manager: Optional[Any] = None
        self._retry_tasks: Set[asyncio.Task] = set()
        self._retry_tasks_lock = asyncio.Lock()

    def set_shutdown_manager(self, shutdown_manager: Any) -> None:
        """Устанавливает менеджер завершения."""
        self._shutdown_manager = shutdown_manager

    # =========================================================================
    # АТОМАРНЫЕ ОПЕРАЦИИ С БД
    # =========================================================================

    async def _get_next_atomic(self, status: FileStatus, worker_id: str,
                               pool_name: str, file_type: Optional[str] = None) -> Optional[QueueItem]:
        """Атомарно получает и блокирует следующую задачу.
        
        Использует INSERT OR IGNORE для атомарного захвата без BEGIN IMMEDIATE,
        чтобы избежать ошибки вложенных транзакций в aiosqlite.
        """
        db_conn = await self.db.get_connection()
        
        # 1. Сначала атомарно вставляем запись в processing
        # Используем INSERT с подзапросом — если задача уже занята, INSERT не сработает
        cursor = await db_conn.execute(
            """INSERT OR IGNORE INTO queue_processing (key, worker_id, worker_type, started_at, updated_at)
               SELECT key, ?, ?, ?, ?
               FROM queue_items
               WHERE status = ?
               AND (? IS NULL OR file_type = ?)
               AND key NOT IN (SELECT key FROM queue_processing)
               ORDER BY created_at
               LIMIT 1
               RETURNING key""",
            (worker_id, pool_name, time.time(), time.time(),
             status.value, file_type, file_type)
        )
        
        row = await cursor.fetchone()
        if not row:
            return None
        
        key = row['key']
        return await self._load_item(key)

    async def _load_item(self, key: str) -> Optional[QueueItem]:
        """Загружает полный объект QueueItem из БД."""
        item_dict = await self.db.get_queue_item(key)
        if not item_dict:
            return None

        return QueueItem(
            chat_id=item_dict['chat_id'],
            message_id=item_dict['message_id'],
            filename=item_dict['filename'],
            remote_dir=item_dict['remote_dir'],
            status=FileStatus(item_dict['status']),
            local_path=item_dict.get('local_path', ''),
            compressed_path=item_dict.get('compressed_path', ''),
            file_size=item_dict.get('file_size', 0),
            attempts=item_dict.get('attempts', 0),
            max_attempts=item_dict.get('max_attempts', 3),
            last_error=item_dict.get('last_error', ''),
            last_attempt_time=item_dict.get('last_attempt_time', 0),
            created_at=item_dict.get('created_at', time.time()),
            updated_at=item_dict.get('updated_at', time.time()),
            metadata=self._parse_json_field(item_dict.get('metadata')),
            file_info=self._parse_json_field(item_dict.get('file_info'))
        )

    @staticmethod
    def _parse_json_field(value: Any) -> dict:
        """Парсит JSON-поле, которое может быть строкой или уже словарём."""
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return {}
        return {}

    async def _release_item(self, key: str) -> None:
        """Освобождает элемент очереди."""
        await self.db.remove_processing(key)

    async def _update_item(self, item: QueueItem, new_status: Optional[FileStatus] = None,
                           release: bool = True) -> None:
        """Обновляет статус элемента."""
        if new_status:
            item.status = new_status
            await self.db.update_queue_status(
                item.key, new_status.value, item.attempts, item.last_error
            )
        item.updated_at = time.time()
        if release:
            await self._release_item(item.key)

    async def _complete_item(self, item: QueueItem) -> None:
        """Завершает обработку элемента."""
        db_conn = await self.db.get_connection()
        try:
            await db_conn.execute("BEGIN IMMEDIATE")
        except Exception:
            pass  # Транзакция уже активна — ок
        try:
            await db_conn.execute("DELETE FROM queue_items WHERE key = ?", (item.key,))
            await db_conn.execute("DELETE FROM queue_retry WHERE key = ?", (item.key,))
            await db_conn.execute("DELETE FROM active_progress WHERE key = ?", (item.key,))
            await db_conn.execute("DELETE FROM queue_processing WHERE key = ?", (item.key,))
            await db_conn.commit()
        except Exception:
            await db_conn.rollback()
            raise

    async def _fail_item(self, item: QueueItem, error: str) -> None:
        """Обрабатывает ошибку элемента с полной очисткой."""
        db_conn = await self.db.get_connection()
        try:
            await db_conn.execute("BEGIN IMMEDIATE")
        except Exception:
            pass  # Транзакция уже активна — ок
        try:
            # Очищаем ВСЕ связанные записи
            await db_conn.execute("DELETE FROM queue_retry WHERE key = ?", (item.key,))
            await db_conn.execute("DELETE FROM active_progress WHERE key = ?", (item.key,))
            await db_conn.execute("DELETE FROM queue_processing WHERE key = ?", (item.key,))

            if not item.is_retryable_error(error):
                item.attempts = item.max_attempts
                item.last_error = error
                item.status = FileStatus.FAILED
                await db_conn.execute(
                    """UPDATE queue_items SET status = ?, attempts = ?,
                       last_error = ?, updated_at = ? WHERE key = ?""",
                    (item.status.value, item.attempts, item.last_error, time.time(), item.key)
                )
                await db_conn.execute(
                    "UPDATE files SET state = ? WHERE chat_id = ? AND message_id = ?",
                    (STATE_ERROR, item.chat_id, item.message_id)
                )
                await db_conn.commit()
                logger.error(f"❌ {item.filename}: фатальная ошибка — {error[:200]}")
                return

            item.record_attempt(error)

            if item.can_retry():
                item.status = FileStatus.PENDING_CHECK
                delay = item.get_retry_delay()
                logger.warning(f"⚠️ {item.filename} ошибка {item.attempts}/{item.max_attempts}: {error[:200]}")
                logger.info(f"   🔄 Повтор через {delay:.1f}с")

                await db_conn.execute(
                    """UPDATE queue_items SET status = ?, attempts = ?,
                       last_error = ?, updated_at = ? WHERE key = ?""",
                    (item.status.value, item.attempts, item.last_error, time.time(), item.key)
                )
                await db_conn.execute(
                    "INSERT OR REPLACE INTO queue_retry (key, retry_at) VALUES (?, ?)",
                    (item.key, time.time() + delay)
                )
                await db_conn.commit()
                asyncio.create_task(self._delayed_retry(item.key, delay))
            else:
                item.status = FileStatus.FAILED
                logger.error(
                    f"❌ {item.filename} окончательная ошибка "
                    f"после {item.attempts} попыток: {error[:200]}"
                )
                await db_conn.execute(
                    """UPDATE queue_items SET status = ?, attempts = ?,
                       last_error = ?, updated_at = ? WHERE key = ?""",
                    (item.status.value, item.attempts, item.last_error, time.time(), item.key)
                )
                await db_conn.execute(
                    "UPDATE files SET state = ? WHERE chat_id = ? AND message_id = ?",
                    (STATE_ERROR, item.chat_id, item.message_id)
                )
                await db_conn.commit()

        except Exception:
            await db_conn.rollback()
            raise

    # =========================================================================
    # ОТЛОЖЕННЫЕ ПОВТОРНЫЕ ПОПЫТКИ
    # =========================================================================

    async def _delayed_retry(self, key: str, delay: float) -> None:
        """Отложенная повторная попытка."""
        task = asyncio.current_task()
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
            current = await self.db.get_queue_item(key)
            if current and current['status'] == STATUS_PENDING_CHECK:
                logger.info(
                    f"🔄 Повторная попытка для {current['filename']} "
                    f"(попытка {current['attempts'] + 1}/{current['max_attempts']})"
                )

    # =========================================================================
    # ОДНОРАЗОВЫЕ ВОРКЕРЫ (5 ТИПОВ)
    # =========================================================================

    async def _ensure_tg_connected(self) -> None:
        """Проверяет Telegram, переподключает не чаще раза в минуту."""
        now = time.time()
        if not hasattr(self, '_last_tg_reconnect'):
            self._last_tg_reconnect = 0
        if now - self._last_tg_reconnect < 60:
            return
        self._last_tg_reconnect = now
        if not await self.tg.is_healthy():
            logger.warning("⚠️ Telegram отвалился, переподключаю...")
            await self.tg.reconnect()

    async def _ensure_ya_connected(self) -> None:
        """Проверяет Яндекс.Диск, переподключает не чаще раза в минуту."""
        now = time.time()
        if not hasattr(self, '_last_ya_reconnect'):
            self._last_ya_reconnect = 0
        if now - self._last_ya_reconnect < 60:
            return
        self._last_ya_reconnect = now
        if not await self.ya.is_healthy():
            logger.warning("⚠️ Яндекс.Диск отвалился, переподключаю...")
            await self.ya.reconnect()

    async def _run_check_worker(self, worker_id: str) -> None:
        """Одноразовый воркер проверки существования на Яндекс.Диске."""
        await self._ensure_ya_connected()
        item = await self._get_next_atomic(FileStatus.PENDING_CHECK, worker_id, 'check')
        if not item:
            return
        await self.processor.check(item, worker_id)

    async def _run_download_worker(self, worker_id: str) -> None:
        """Одноразовый воркер скачивания из Telegram."""
        await self._ensure_tg_connected()
        item = await self._get_next_atomic(FileStatus.PENDING_DOWNLOAD, worker_id, 'download')
        if not item:
            return

        try:
            await self.processor.download(item, worker_id)
        except asyncio.CancelledError:
            await self.processor.cleanup_partial(item)
            raise

    async def _run_compress_photo_worker(self, worker_id: str) -> None:
        """Одноразовый воркер сжатия фото."""
        item = await self._get_next_atomic(
            FileStatus.PENDING_COMPRESS, worker_id, 'compress_photo', 'photo'
        )
        if not item:
            return
        await self.processor.compress(item, worker_id)

    async def _run_compress_video_worker(self, worker_id: str) -> None:
        """Одноразовый воркер сжатия видео."""
        item = await self._get_next_atomic(
            FileStatus.PENDING_COMPRESS, worker_id, 'compress_video', 'video'
        )
        if not item:
            return
        await self.processor.compress(item, worker_id)

    async def _run_upload_worker(self, worker_id: str) -> None:
        """Одноразовый воркер загрузки на Яндекс.Диск."""
        await self._ensure_ya_connected()
        item = await self._get_next_atomic(FileStatus.PENDING_UPLOAD, worker_id, 'upload')
        if not item:
            return

        if await self.processor.upload(item, worker_id):
            await self._complete_item(item)
            await self.processor.cleanup(item)

    # =========================================================================
    # ВОССТАНОВЛЕНИЕ ПОСЛЕ КРАША
    # =========================================================================

    async def _recover_from_crash(self) -> None:
        """Восстанавливает систему после аварийного завершения."""
        logger.info("🔍 Восстановление после краша...")

        # 1. Сбрасываем FAILED задачи для повторной попытки
        failed_items = await self.db.get_queue_items(STATUS_FAILED)
        for item in failed_items:
            await self.db.update_queue_status(item['key'], STATUS_PENDING_CHECK, 0, None)
        if failed_items:
            logger.info(f"   🔄 Сброшено {len(failed_items)} FAILED задач")

        # 2. Откатываем зависшие задачи
        cursor = await self.db.execute(
            """SELECT qi.key, qi.filename, qi.status, qi.attempts, qi.max_attempts
               FROM queue_items qi
               JOIN queue_processing qp ON qi.key = qp.key
               WHERE qp.started_at < ?""",
            (time.time() - STUCK_TASK_TIMEOUT,)
        )

        stuck_count = 0
        for row in await cursor.fetchall():
            attempts = row['attempts'] or 0
            max_attempts = row['max_attempts'] if row['max_attempts'] else 3

            if attempts >= max_attempts:
                await self.db.update_queue_status(
                    row['key'], STATUS_FAILED, attempts, "Recovery: max attempts exceeded"
                )
                await self.db.update_file_state(
                    row['chat_id'], row['message_id'], STATE_ERROR
                )
            else:
                rollback_status = self._get_rollback_status(row['status'])
                await self.db.update_queue_status(
                    row['key'], rollback_status, attempts,
                    f"Recovered from {row['status']} after crash"
                )
                await self.db.execute("DELETE FROM active_progress WHERE key = ?", (row['key'],))
                await self.db.remove_processing(row['key'])
                logger.info(f"🔄 [{row['filename']}] Восстановлен: {row['status']} → {rollback_status}")
                stuck_count += 1

        if stuck_count > 0:
            logger.info(f"   🔄 Откачено {stuck_count} зависших задач")

        # 3. Чистим осиротевшие записи
        cursor = await self.db.execute(
            "DELETE FROM queue_processing WHERE key NOT IN (SELECT key FROM queue_items)"
        )
        if cursor.rowcount > 0:
            logger.info(f"   🗑️ Удалено {cursor.rowcount} осиротевших processing записей")

        cursor = await self.db.execute(
            "DELETE FROM active_progress WHERE key NOT IN (SELECT key FROM queue_items)"
        )
        if cursor.rowcount > 0:
            logger.info(f"   🗑️ Удалено {cursor.rowcount} осиротевших progress записей")

        await self.db.commit()
        logger.info("✅ Восстановление завершено")

    @staticmethod
    def _get_rollback_status(current_status: str) -> str:
        """Определяет статус для отката."""
        rollback_map = {
            STATUS_PENDING_UPLOAD: STATUS_PENDING_COMPRESS,
            STATUS_PENDING_COMPRESS: STATUS_PENDING_DOWNLOAD,
            STATUS_PENDING_DOWNLOAD: STATUS_PENDING_CHECK,
        }
        return rollback_map.get(current_status, STATUS_PENDING_CHECK)

    # =========================================================================
    # ЗАПУСК / ОСТАНОВКА
    # =========================================================================

    async def start(self) -> None:
        """Запускает систему очередей с 5 пулами одноразовых воркеров."""
        logger.info("🚀 Запуск системы очередей...")

        # Восстановление после краша
        await self._recover_from_crash()

        # Очистка старых записей processing
        await self.db.clear_processing()

        self.running = True
        queue_settings = await self.db.get_queue_settings()

        # Создаём 5 пулов с фабриками одноразовых воркеров
        self.pools = {
            'check': WorkerPool(
                "Check",
                worker_factory=lambda wid: self._run_check_worker(wid),
                min_workers=1,
                max_workers=queue_settings.get('check_workers', 5)
            ),
            'download': WorkerPool(
                "Download",
                worker_factory=lambda wid: self._run_download_worker(wid),
                min_workers=1,
                max_workers=queue_settings.get('download_workers', 3)
            ),
            'compress_photo': WorkerPool(
                "CompressPhoto",
                worker_factory=lambda wid: self._run_compress_photo_worker(wid),
                min_workers=1,
                max_workers=queue_settings.get('photo_workers', 2)
            ),
            'compress_video': WorkerPool(
                "CompressVideo",
                worker_factory=lambda wid: self._run_compress_video_worker(wid),
                min_workers=1,
                max_workers=queue_settings.get('video_workers', 1)
            ),
            'upload': WorkerPool(
                "Upload",
                worker_factory=lambda wid: self._run_upload_worker(wid),
                min_workers=1,
                max_workers=queue_settings.get('upload_workers', 3)
            ),
        }

        for name, pool in self.pools.items():
            try:
                await asyncio.wait_for(pool.start(), timeout=10.0)
                logger.info(f"✅ Пул {name} запущен")
            except Exception as e:
                logger.error(f"❌ Ошибка запуска пула {name}: {e}")

        self._monitor_task = asyncio.create_task(self._monitor_resources())

        if total := await self.db.count_pending():
            logger.info(f"📊 В очереди {total} файлов")

    async def stop(self) -> None:
        """Останавливает систему очередей."""
        logger.info("🛑 Остановка системы очередей...")
        self.running = False

        # Отменяем задачи retry
        async with self._retry_tasks_lock:
            for task in list(self._retry_tasks):
                if not task.done():
                    task.cancel()
            self._retry_tasks.clear()

        # Останавливаем мониторинг
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

        # Останавливаем все пулы
        stop_tasks = [pool.stop() for pool in self.pools.values()]
        try:
            await asyncio.wait_for(
                asyncio.gather(*stop_tasks, return_exceptions=True),
                timeout=15.0
            )
        except asyncio.TimeoutError:
            logger.warning("⚠️ Не все воркеры остановились за 15 секунд")

        await asyncio.sleep(2)

        # Останавливаем ffmpeg
        if self.comp:
            if hasattr(self.comp, 'request_shutdown'):
                self.comp.request_shutdown()
            await self.comp.stop_all_ffmpeg()
            await asyncio.sleep(2)

        try:
            await self.db.checkpoint()
        except Exception:
            pass
        logger.info("✅ Система очередей остановлена")

    # =========================================================================
    # МОНИТОРИНГ РЕСУРСОВ
    # =========================================================================

    async def _monitor_resources(self) -> None:
        """Мониторит ресурсы и адаптирует все 5 пулов."""
        import psutil
        psutil.cpu_percent(interval=None)
        last_checkpoint = time.time()
        logger.info("📡 Монитор ресурсов запущен")

        while self.running:
            try:
                await asyncio.sleep(2)
                cpu = psutil.cpu_percent(interval=None)
                try:
                    counts = await self.db.get_queue_counts()
                except Exception:
                    await asyncio.sleep(1)
                    continue
                logger.debug(f"📡 Монитор: counts={counts}")

                for pool_name, pool in self.pools.items():
                    queue_size = counts.get(self.POOL_STATUS_MAP.get(pool_name, ''), 0)
                    # Для compress пулов делим очередь по типам
                    if pool_name == 'compress_photo':
                        queue_size = queue_size // 2
                    elif pool_name == 'compress_video':
                        queue_size = min(queue_size, 5)

                    logger.debug(f"📊 {pool_name}: queue={queue_size}, cpu={cpu:.0f}%, target={pool.target}")
                    logger.debug(f"📡 ВЫЗОВ adjust для {pool_name}: q={queue_size}, cpu={cpu:.0f}")
                    await pool.adjust(queue_size, cpu)

                if time.time() - last_checkpoint > CHECKPOINT_INTERVAL:
                    await self.db.checkpoint()
                    last_checkpoint = time.time()

                await asyncio.sleep(MONITOR_INTERVAL - 2)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Ошибка мониторинга: {e}")
                await asyncio.sleep(30)

    # =========================================================================
    # ДОБАВЛЕНИЕ ФАЙЛОВ В ОЧЕРЕДЬ
    # =========================================================================

    async def add_file(self, chat_id: int, message_id: int, filename: str,
                       message: Any, remote_dir: str, topic_id: Optional[int] = None,
                       file_info: Optional[dict] = None) -> bool:
        """Добавляет файл в очередь."""
        await self.db.ensure_loaded()
        key = f"{chat_id}:{message_id}"

        if await self.db.get_queue_item(key):
            logger.debug(f"🔄 Файл {filename} уже в очереди")
            return False

        chat_name = await self.processor._get_chat_name(chat_id)
        topic_name = await self.processor._get_topic_name(chat_id, topic_id) if topic_id else "general"
        local_path = build_local_path(self.download_dir, chat_name, topic_name, filename)
        compressed_path = build_compressed_path(self.download_dir, chat_name, topic_name, filename)

        file_size = 0
        if file_info:
            file_size = file_info.get('size', 0)
        elif message:
            if hasattr(message, 'document') and message.document:
                file_size = message.document.file_size or 0
            elif hasattr(message, 'video') and message.video:
                file_size = message.video.file_size or 0
            elif hasattr(message, 'photo') and message.photo:
                file_size = max([s.file_size for s in message.photo.sizes]) if message.photo.sizes else 0

        # Проверяем текущий статус файла — если уже загружен, не добавляем
        current_state = await self.db.get_file_state(chat_id, message_id)
        if current_state == STATE_UPLOADED:
            logger.debug(f"⏭️ Файл {filename} уже загружен (state=UPLOADED)")
            return False
        
        next_status = FileStatus.PENDING_CHECK

        # Проверяем сжатый файл
        if os.path.exists(compressed_path) and os.path.getsize(compressed_path) > 0:
            is_valid = False
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

        # Проверяем локальный файл
        if next_status == FileStatus.PENDING_CHECK and os.path.exists(local_path) \
                and os.path.getsize(local_path) > 0:
            is_valid = False
            saved_md5 = await self.db.get_file_md5(chat_id, message_id)
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
                    await self.db.set_file_md5(
                        chat_id, message_id,
                        await asyncio.to_thread(calculate_md5, local_path)
                    )

            if is_valid:
                if file_size == 0:
                    file_size = os.path.getsize(local_path)
                next_status = (
                    FileStatus.PENDING_COMPRESS
                    if await self.processor._needs_compress(filename, file_size)
                    else FileStatus.PENDING_UPLOAD
                )
            else:
                os.unlink(local_path)

        item = QueueItem(
            chat_id=chat_id, message_id=message_id, filename=filename,
            remote_dir=remote_dir, message=message, status=next_status,
            local_path=local_path if os.path.exists(local_path) else "",
            compressed_path=compressed_path if os.path.exists(compressed_path) else "",
            file_size=file_size, file_info=file_info or {}
        )
        if topic_id is not None:
            item.metadata['topic_id'] = topic_id

        success = await self.db.add_queue_item({
            'chat_id': item.chat_id, 'message_id': item.message_id,
            'topic_id': item.topic_id, 'filename': item.filename,
            'remote_dir': item.remote_dir, 'local_path': item.local_path,
            'compressed_path': item.compressed_path, 'file_size': item.file_size,
            'status': item.status.value, 'attempts': item.attempts,
            'max_attempts': item.max_attempts, 'created_at': item.created_at,
            'updated_at': item.updated_at, 'metadata': item.metadata,
            'file_info': item.file_info
        })

        if success:
            await self.db.record_queued(chat_id, filename, file_size, topic_id)
            logger.debug(
                f"✅ Добавлен в очередь: {filename} "
                f"(статус: {item.status.value}, размер: {fmt_size(file_size)})"
            )

        return success

    # =========================================================================
    # СТАТУС
    # =========================================================================

    async def get_status(self) -> dict:
        """Возвращает статус системы очередей."""
        counts = await self.db.get_queue_counts()
        active_items = await self.db.get_active_items()

        active_files = []
        downloading = []
        compressing = []
        uploading = []

        for item in active_items:
            worker_type = item.get('worker_type', '')
            active_item = {
                'filename': item['filename'],
                'message_id': item['message_id'],
                'chat_id': item['chat_id'],
                'stage': worker_type,
                'size': item.get('file_size', 0),
                'topic_id': item.get('topic_id'),
                'progress': item.get('progress'),
                'speed': item.get('speed'),
                'eta': item.get('eta'),
                'downloaded': item.get('downloaded'),
                'uploaded': item.get('uploaded'),
                'total_size': item.get('total_size')
            }
            active_files.append(active_item)
            if worker_type == 'download':
                downloading.append(active_item)
            elif worker_type in ('compress_photo', 'compress_video'):
                compressing.append(active_item)
            elif worker_type == 'upload':
                uploading.append(active_item)

        return {
            'running': self.running,
            'queues': counts,
            'active_files': active_files,
            'downloading': downloading,
            'compressing': compressing,
            'uploading': uploading,
            'total_pending': await self.db.count_pending()
        }

    # =========================================================================
    # ОБРАБОТКА ВЫБРАННЫХ ФАЙЛОВ
    # =========================================================================

    async def _add_selected_files_batch(self, chat_ids: List[int], limit: int) -> int:
        """Добавляет пачку выбранных файлов в очередь."""
        added = 0
        files_to_check = []
        files_info_map: Dict[str, Tuple[int, dict, dict, str]] = {}

        for cid in chat_ids:
            if added >= limit:
                break

            chat_name = await self.processor._get_chat_name(cid)

            for topic in await self.db.get_topics(cid):
                if added >= limit:
                    break

                remaining = limit - added
                files = await self.db.get_files(
                    cid, topic['topic_id'], state_filter=STATE_SELECTED
                )
                files = files[:remaining]

                for file_info in files:
                    key = f"{cid}:{file_info['message_id']}"
                    files_to_check.append(key)
                    files_info_map[key] = (cid, topic, file_info, chat_name)

        if not files_to_check:
            return 0

        existing_keys = await self.db.are_files_in_queue(files_to_check)

        for key in files_to_check:
            if key in existing_keys:
                continue

            cid, topic, file_info, chat_name = files_info_map[key]
            remote_dir = self.ya.build_remote_path(chat_name, topic['topic_name'])

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
                    cursor = await self.db.execute(
                        """UPDATE files SET state = ?
                           WHERE chat_id = ? AND topic_id = ? AND state = ?""",
                        (STATE_SELECTED, cid, topic['topic_id'], STATE_NEW)
                    )
                    if cursor.rowcount > 0:
                        logger.info(
                            f"   ✅ Тема '{topic['topic_name']}': "
                            f"{cursor.rowcount} новых файлов выбрано"
                        )
                    await self.db.commit()

    async def process_selected_files(self) -> int:
        """Главный метод обработки выбранных файлов."""
        logger.info("📋 ОБРАБОТКА ВЫБРАННЫХ ФАЙЛОВ")
        await self.db.ensure_loaded()

        chat_ids = await self.db.get_chat_ids()
        all_stats = await self.db.get_all_stats()
        await self.db.set_app_state('selected_snapshot', all_stats.get('selected_files', 0))

        if not self.running:
            await self.start()

        total_processed = 0

        while True:
            if self._shutdown_manager and self._shutdown_manager.is_requested():
                break

            pending = await self.db.count_pending()
            processing = await self.db.get_processing_keys()

            if pending < self.REFILL_THRESHOLD and not processing:
                logger.info(f"📦 Очередь: {pending} файлов. Добавляем следующую пачку...")

                added = await self._add_selected_files_batch(chat_ids, self.BATCH_SIZE)

                if added == 0 and pending == 0 and not processing:
                    has_more = await self.db.has_selected_files_remaining(chat_ids)
                    if not has_more:
                        # Сканируем новые файлы в выбранных темах
                        logger.info("🔍 Проверяем новые файлы в выбранных темах...")
                        await self._scan_new_files(chat_ids)
                        await self._mark_new_files_as_selected(chat_ids)
                        
                        # Проверяем появились ли новые
                        has_more = await self.db.has_selected_files_remaining(chat_ids)
                        if not has_more:
                            logger.info("✅ Все выбранные файлы обработаны, новых нет")
                            break
                        else:
                            logger.info("🆕 Найдены новые файлы, продолжаем обработку")
                            continue
                    else:
                        logger.warning(
                            "⚠️ Есть выбранные файлы, но они не добавились в очередь. "
                            "Проверяем через 30с."
                        )
                        await asyncio.sleep(30)
                        continue

                if added > 0:
                    logger.info(f"📊 Добавлено в очередь: {added} файлов")
                total_processed += added

            await asyncio.sleep(30)

        checked = self.pools['check']._processed_count if 'check' in self.pools else 0
        downloaded = self.pools['download']._processed_count if 'download' in self.pools else 0
        compressed = (self.pools.get('compress_photo') and self.pools['compress_photo']._processed_count or 0) +                      (self.pools.get('compress_video') and self.pools['compress_video']._processed_count or 0)
        uploaded = self.pools['upload']._processed_count if 'upload' in self.pools else 0
        
        if not (self._shutdown_manager and self._shutdown_manager.is_requested()):
            await self.cleanup_all_downloads(force=True)
            logger.info(f"✅ Сессия завершена")
        else:
            logger.info(f"🛑 Сессия прервана")
        logger.info(f"   🔍 Проверено: {checked}")
        logger.info(f"   📥 Скачано: {downloaded}")
        logger.info(f"   🗜️ Сжато: {compressed}")
        logger.info(f"   📤 Загружено: {uploaded}")
        return uploaded

    async def cleanup_all_downloads(self, force: bool = False) -> None:
        """Очищает папку downloads."""
        if not os.path.exists(self.download_dir):
            return

        logger.info("🧹 Очистка папки downloads...")
        download_path = Path(self.download_dir)

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