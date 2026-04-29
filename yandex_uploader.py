#!/usr/bin/env python3
"""
Модуль загрузки на Яндекс.Диск - ПОЛНОСТЬЮ АВТОНОМНЫЙ
ВЕРСИЯ 0.18.0 — БЕЗ ИЗМЕНЕНИЙ
"""

__version__ = "0.18.0"

import os
import asyncio
import hashlib
import time
import re
import logging
from dataclasses import dataclass
from typing import Optional, Tuple, Set, Callable, Dict, Any

import yadisk
from aiolimiter import AsyncLimiter

logger = logging.getLogger(__name__)


async def _call_with_timeout(func: Callable, timeout: float, *args: Any, **kwargs: Any) -> Any:
    """Выполняет асинхронную функцию с таймаутом."""
    try:
        return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
    except asyncio.TimeoutError:
        raise TimeoutError(f"Timeout after {timeout}s")


class RateLimiter:
    """Ограничитель частоты запросов на основе aiolimiter."""
    
    def __init__(self, max_calls: int, period: float) -> None:
        """Инициализирует ограничитель."""
        self._limiter: AsyncLimiter = AsyncLimiter(max_calls, period)
        self.stats: Dict[str, float] = {'total_waited': 0.0, 'max_wait': 0.0, 'wait_count': 0}

    async def acquire(self) -> float:
        """Захватывает слот для запроса."""
        start: float = time.monotonic()
        await self._limiter.acquire()
        wait_time: float = time.monotonic() - start
        if wait_time > 0.01:
            self.stats['wait_count'] += 1
            self.stats['total_waited'] += wait_time
            self.stats['max_wait'] = max(self.stats['max_wait'], wait_time)
        return wait_time

    def get_stats(self) -> dict:
        """Возвращает статистику ограничителя."""
        return self.stats.copy()


@dataclass
class UploadResult:
    """Результат загрузки файла."""
    success: bool
    status: str
    remote_path: str
    local_path: str
    filename: str
    size: int
    md5: Optional[str]
    existed: bool
    error: str = ""
    duration_sec: float = 0


class PathSanitizer:
    """Санитизация путей для Яндекс.Диска."""
    
    DANGEROUS_CHARS: re.Pattern = re.compile(r'[<>:"\\|?*\x00-\x1f\x7f]')
    
    @classmethod
    def sanitize(cls, name: str, max_length: int = 100) -> str:
        """Очищает имя от недопустимых символов."""
        if not name:
            return "unnamed"
        safe: str = cls.DANGEROUS_CHARS.sub('_', name)
        safe = re.sub(r'\.{2,}', '.', safe).strip('.')
        safe = safe.replace(' ', '_')
        return safe[:max_length] or "unnamed"
    
    @classmethod
    def build_path(cls, base: str, *components: str) -> str:
        """Строит полный путь на Яндекс.Диске."""
        base = base.rstrip('/')
        if not base.startswith('/'):
            base = '/' + base
        parts: list = [base] + [cls.sanitize(c) for c in components if c and c != 'general']
        return '/'.join(parts)


class ProgressReader:
    """Обёртка для файла с отслеживанием прогресса."""
    
    def __init__(self, file_path: str, callback: Optional[Callable[[int, int], None]], total_size: int) -> None:
        """Инициализирует ридер с прогрессом."""
        self.file: Any = open(file_path, 'rb')
        self.callback: Optional[Callable[[int, int], None]] = callback
        self.total_size: int = total_size
        self._position: int = 0
        
    def __enter__(self) -> 'ProgressReader':
        """Вход в контекстный менеджер."""
        return self
        
    def __exit__(self, *args: Any) -> None:
        """Выход из контекстного менеджера."""
        self.file.close()
    
    def read(self, size: int = -1) -> bytes:
        """Читает чанк данных и вызывает колбэк прогресса."""
        chunk: bytes = self.file.read(size)
        if chunk and self.callback:
            self._position += len(chunk)
            self.callback(self._position, self.total_size)
        return chunk
    
    def seek(self, offset: int, whence: int = 0) -> int:
        """Перемещает указатель в файле."""
        result: int = self.file.seek(offset, whence)
        self._position = offset if whence == 0 else (self._position + offset if whence == 1 else self.total_size + offset)
        return result
    
    def tell(self) -> int:
        """Возвращает текущую позицию."""
        return self._position


def calculate_file_hash(file_path: str) -> str:
    """Вычисляет MD5 хеш файла."""
    hash_md5: hashlib._Hash = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logger.error(f"❌ Ошибка вычисления хэша: {e}")
        return ""


def retry(max_attempts: int = 3, base_delay: float = 1.0) -> Callable:
    """Декоратор для повторных попыток с экспоненциальной задержкой."""
    def decorator(func: Callable) -> Callable:
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_error: Optional[Exception] = None
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except (yadisk.exceptions.InsufficientStorageError,
                        yadisk.exceptions.UnauthorizedError) as e:
                    raise
                except Exception as e:
                    last_error = e
                    if attempt < max_attempts - 1:
                        delay: float = base_delay * (2 ** attempt)
                        err_msg = str(e) if str(e) else type(e).__name__
                        logger.warning(f"⚠️ Попытка {attempt + 1}/{max_attempts} не удалась: {err_msg}, повтор через {delay:.1f}с")
                        await asyncio.sleep(delay)
            raise last_error
        return wrapper
    return decorator


class YandexUploader:
    """АВТОНОМНЫЙ загрузчик на Яндекс.Диск."""
    
    def __init__(self, config: dict) -> None:
        """Инициализирует загрузчик."""
        self.token: str = config['token']
        self.base_path: str = config.get('base_path', '/tg_backup').rstrip('/')
        self.min_free_space_mb: int = config.get('min_free_space_mb', 100)
        self.upload_timeout: int = config.get('upload_timeout', 60)
        
        self.rate_limiter: RateLimiter = RateLimiter(
            max_calls=config.get('rate_limit_calls', 100),
            period=config.get('rate_limit_period', 60))
        
        self.max_concurrent: int = config.get('max_concurrent_uploads', 3)
        self.semaphore: asyncio.Semaphore = asyncio.Semaphore(self.max_concurrent)
        
        self.client: Optional[yadisk.AsyncClient] = None
        self._in_context: bool = False
        self._folder_cache: Set[str] = set()
        
        self.stats: Dict[str, Any] = {
            'total_uploads': 0, 'successful_uploads': 0, 'failed_uploads': 0,
            'skipped_exists': 0, 'total_bytes': 0, 'rate_limiter': self.rate_limiter.get_stats()}
    
    def build_remote_path(self, chat_name: str, topic_name: str) -> str:
        """Строит удалённый путь для загрузки."""
        return PathSanitizer.build_path(self.base_path, chat_name, topic_name or "general")

    async def is_healthy(self) -> bool:
        """Проверяет здоровье подключения."""
        try:
            return self.client is not None and self._in_context and await asyncio.wait_for(self.client.get_disk_info(), timeout=5.0) is not None
        except Exception:
            return False
    
    async def reconnect(self) -> bool:
        """Переподключается к Яндекс.Диску."""
        logger.warning("🔄 Переподключение к Яндекс.Диску...")
        await self.disconnect()
        await asyncio.sleep(2)
        return await self.connect()
    
    async def connect(self) -> bool:
        """Подключается к Яндекс.Диску."""
        try:
            self.client = yadisk.AsyncClient(token=self.token)
            await self.client.__aenter__()
            self._in_context = True
            
            try:
                await self.client.get_disk_info()
            except yadisk.exceptions.UnauthorizedError as e:
                logger.error(f"❌ Ошибка авторизации: {e}")
                return False
            
            if not await self.client.exists(self.base_path):
                await self.client.mkdir(self.base_path)
                self._folder_cache.add(self.base_path)
                logger.info(f"✅ Создана папка {self.base_path}")
            
            free: int
            total: int
            free, total = await self._get_space()
            logger.info(f"✅ Подключено к Яндекс.Диску (свободно {free/1024/1024:.1f}MB)")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка подключения: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Отключается от Яндекс.Диска."""
        if self.client and self._in_context:
            try:
                await self.client.__aexit__(None, None, None)
                self._in_context = False
            except Exception as e:
                logger.error(f"❌ Ошибка при отключении: {e}")
        self._folder_cache.clear()
        await asyncio.sleep(0.5)
        logger.info("🔒 Отключено от Яндекс.Диска")
    
    async def _get_space(self) -> Tuple[int, int]:
        """Возвращает свободное и общее место на Диске."""
        await self.rate_limiter.acquire()
        info: Any = await _call_with_timeout(self.client.get_disk_info, 30.0)
        return info.total_space - info.used_space, info.total_space
    
    async def check_space(self, required_bytes: int = 0) -> Tuple[bool, int, int]:
        """Проверяет, достаточно ли места на Диске."""
        try:
            free: int
            total: int
            free, total = await self._get_space()
            min_free: int = self.min_free_space_mb * 1024 * 1024
            
            if free < min_free:
                logger.error(f"❌ Критически мало места: {free/1024/1024:.1f}MB < {self.min_free_space_mb}MB")
                return False, free, total
            
            if required_bytes > 0 and free < required_bytes + min_free:
                logger.error(f"❌ Не хватит места: нужно {required_bytes/1024/1024:.1f}MB, свободно {free/1024/1024:.1f}MB")
                return False, free, total
            
            logger.debug(f"💾 Свободно: {free/1024/1024:.1f}MB, всего: {total/1024/1024:.1f}MB")
            return True, free, total
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке места: {e}")
            return False, 0, 0
    
    async def file_exists(self, remote_dir: str, filename: str) -> Tuple[bool, int, Optional[str]]:
        """Проверяет существование файла на Диске."""
        full_path: str = PathSanitizer.build_path(remote_dir, filename)
        try:
            await self.rate_limiter.acquire()
            info: Any = await _call_with_timeout(self.client.get_meta, 30.0, full_path)
            return True, getattr(info, 'size', 0), getattr(info, 'md5', None)
        except yadisk.exceptions.PathNotFoundError:
            return False, 0, None
        except Exception as e:
            logger.error(f"❌ Ошибка проверки {full_path}: {e}")
            return False, 0, None
    
    async def ensure_path(self, remote_path: str) -> bool:
        """Создаёт все папки в пути, если их нет."""
        if remote_path in self._folder_cache:
            return True
        
        try:
            parts: list = remote_path.strip('/').split('/')
            current: str = ""
            for part in parts:
                current = f"{current}/{part}" if current else f"/{part}"
                if current not in self._folder_cache:
                    await self.rate_limiter.acquire()
                    if not await self.client.exists(current):
                        await self.client.mkdir(current)
                        logger.debug(f"📁 Создана папка {current}")
                    self._folder_cache.add(current)
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка при создании папок: {e}")
            return False
    
    @retry(max_attempts=3, base_delay=1.0)
    async def _do_upload(self, local_path: str, remote_path: str, 
                         progress_callback: Optional[Callable[[int, int], None]] = None) -> None:
        """Выполняет загрузку файла с повторными попытками."""
        file_size: int = os.path.getsize(local_path)
        async with self.semaphore:
            await self.rate_limiter.acquire()
            with ProgressReader(local_path, progress_callback, file_size) as reader:
                timeout: int = max(self.upload_timeout, int(file_size / (1024 * 1024) * 2))
                await asyncio.wait_for(
                    self.client.upload(reader, remote_path, overwrite=True),
                    timeout=timeout
                )
    
    async def upload(self, local_path: str, remote_dir: str, filename: Optional[str] = None,
                     check_exists: bool = True, force: bool = False,
                     progress_callback: Optional[Callable[[int, int], None]] = None) -> UploadResult:
        """Загружает файл на Яндекс.Диск."""
        start: float = time.time()
        
        if not os.path.exists(local_path):
            return UploadResult(
                success=False, status='error', remote_path="", local_path=local_path,
                filename=filename or os.path.basename(local_path), size=0, md5=None, existed=False,
                error=f"File not found: {local_path}")
        
        file_size: int = os.path.getsize(local_path)
        if file_size == 0:
            return UploadResult(
                success=False, status='error', remote_path="", local_path=local_path,
                filename=filename or os.path.basename(local_path), size=0, md5=None, existed=False,
                error="File is empty")
        
        if not filename:
            filename = os.path.basename(local_path)
        
        safe_filename: str = PathSanitizer.sanitize(filename, 200)
        remote_path: str = PathSanitizer.build_path(remote_dir, safe_filename)
        
        self.stats['total_uploads'] += 1
        
        space_ok: bool
        free: int
        total: int
        space_ok, free, total = await self.check_space(file_size)
        if not space_ok:
            self.stats['failed_uploads'] += 1
            return UploadResult(
                success=False, status='error', remote_path=remote_path, local_path=local_path,
                filename=filename, size=file_size, md5=None, existed=False,
                error="Недостаточно места на Яндекс.Диске", duration_sec=time.time() - start)
        
        local_md5: str = calculate_file_hash(local_path) if check_exists else ""
        
        if check_exists and not force:
            exists: bool
            remote_size: int
            remote_md5: Optional[str]
            exists, remote_size, remote_md5 = await self.file_exists(remote_dir, safe_filename)
            if exists and remote_size > 0:
                if remote_md5 and local_md5 and remote_md5 == local_md5:
                    self.stats['skipped_exists'] += 1
                    logger.info(f"⏭️ Файл уже есть: {filename}")
                    return UploadResult(
                        success=True, status='skipped', remote_path=remote_path, local_path=local_path,
                        filename=filename, size=remote_size, md5=remote_md5, existed=True,
                        duration_sec=time.time() - start)
                logger.info(f"⚠️ Файл есть, но отличается - будет перезаписан")
        
        if not await self.ensure_path(remote_dir):
            self.stats['failed_uploads'] += 1
            return UploadResult(
                success=False, status='error', remote_path=remote_path, local_path=local_path,
                filename=filename, size=file_size, md5=None, existed=False,
                error=f"Failed to create directory: {remote_dir}", duration_sec=time.time() - start)
        
        try:
            logger.info(f"📤 Загрузка {filename} ({file_size/1024/1024:.1f}MB)")
            await self._do_upload(local_path, remote_path, progress_callback)
            
            exists, final_size, final_md5 = await self.file_exists(remote_dir, safe_filename)
            if not exists or final_size == 0:
                raise Exception("Upload verification failed - file not found")
            if final_size != file_size:
                raise Exception(f"Size mismatch: local {file_size} vs remote {final_size}")
            if final_md5 and local_md5 and final_md5 != local_md5:
                raise Exception(f"MD5 mismatch")
            
            self.stats['successful_uploads'] += 1
            self.stats['total_bytes'] += file_size
            logger.info(f"✅ Загружено: {filename} ({file_size/1024/1024:.1f}MB)")
            
            return UploadResult(
                success=True, status='uploaded', remote_path=remote_path, local_path=local_path,
                filename=filename, size=file_size, md5=local_md5, existed=False,
                duration_sec=time.time() - start)
                
        except Exception as e:
            self.stats['failed_uploads'] += 1
            err_msg = str(e) if str(e) else type(e).__name__
            logger.error(f"❌ Ошибка загрузки {filename}: {err_msg}")
            return UploadResult(
                success=False, status='error', remote_path=remote_path, local_path=local_path,
                filename=filename, size=file_size, md5=local_md5, existed=False,
                error=str(e), duration_sec=time.time() - start)
    
    def get_stats(self) -> dict:
        """Возвращает статистику загрузчика."""
        self.stats['rate_limiter'] = self.rate_limiter.get_stats()
        return self.stats.copy()
