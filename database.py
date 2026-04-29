#!/usr/bin/env python3
"""
Единый модуль работы с базой данных SQLite
ВЕРСИЯ 0.18.3 — _write_lock ДЛЯ ВСЕХ ТРАНЗАКЦИЙ, COUNT(DISTINCT), SESSION_STATS
"""

__version__ = "0.18.3"

import os
import json
import asyncio
import logging
import time
import re
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple, Set, Callable, Awaitable

import sqlite3
import aiosqlite

logger = logging.getLogger(__name__)

DB_FILE: str = "backup.db"
SCHEMA_VERSION: int = 4

# ========== КОНСТАНТЫ СОСТОЯНИЙ ФАЙЛОВ ==========
STATE_NEW: int = 0
STATE_SELECTED: int = 1
STATE_UPLOADED: int = 2
STATE_SKIPPED: int = 3
STATE_ERROR: int = 4
STATE_UNLOADED: int = 5

# ========== КОНСТАНТЫ СТАТУСОВ ОЧЕРЕДИ ==========
STATUS_PENDING_CHECK: str = "pending_check"
STATUS_PENDING_DOWNLOAD: str = "pending_download"
STATUS_PENDING_COMPRESS: str = "pending_compress"
STATUS_PENDING_UPLOAD: str = "pending_upload"
STATUS_COMPLETED: str = "completed"
STATUS_FAILED: str = "failed"

# ========== СХЕМА БД ==========
DB_SCHEMA: str = """
CREATE TABLE IF NOT EXISTS topics (chat_id INTEGER, topic_id INTEGER, topic_name TEXT, is_selected BOOLEAN DEFAULT 0, PRIMARY KEY (chat_id, topic_id));
CREATE TABLE IF NOT EXISTS files (chat_id INTEGER, topic_id INTEGER, message_id INTEGER, filename TEXT, file_type TEXT, size INTEGER, state INTEGER DEFAULT 5, attempts INTEGER DEFAULT 0, last_error TEXT, file_id TEXT, dc_id INTEGER, timestamp REAL, md5 TEXT, PRIMARY KEY (chat_id, message_id));
CREATE TABLE IF NOT EXISTS chat_names (chat_id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE IF NOT EXISTS queue_items (key TEXT PRIMARY KEY, chat_id INTEGER, message_id INTEGER, topic_id INTEGER, filename TEXT, remote_dir TEXT, local_path TEXT, compressed_path TEXT, file_size INTEGER, file_type TEXT, status TEXT DEFAULT 'pending_check', attempts INTEGER DEFAULT 0, max_attempts INTEGER DEFAULT 3, last_error TEXT, last_attempt_time REAL, created_at REAL, updated_at REAL, metadata TEXT, file_info TEXT);
CREATE TABLE IF NOT EXISTS queue_processing (key TEXT PRIMARY KEY, worker_id INTEGER, worker_type TEXT, started_at REAL, updated_at REAL, FOREIGN KEY (key) REFERENCES queue_items(key) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS queue_retry (key TEXT PRIMARY KEY, retry_at REAL, FOREIGN KEY (key) REFERENCES queue_items(key) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS active_progress (key TEXT PRIMARY KEY, stage TEXT, progress REAL, speed REAL, eta REAL, downloaded INTEGER, uploaded INTEGER, total_size INTEGER, updated_at REAL, FOREIGN KEY (key) REFERENCES queue_items(key) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp REAL, chat_id INTEGER, topic_id INTEGER, message_id INTEGER, filename TEXT, status TEXT, size INTEGER, compressed_size INTEGER, stage TEXT, error TEXT, details TEXT, chat_name TEXT, topic_name TEXT);
CREATE TABLE IF NOT EXISTS chat_stats (chat_id INTEGER PRIMARY KEY, total INTEGER DEFAULT 0, uploaded INTEGER DEFAULT 0, skipped INTEGER DEFAULT 0, errors INTEGER DEFAULT 0, compressed INTEGER DEFAULT 0, total_bytes INTEGER DEFAULT 0, uploaded_bytes INTEGER DEFAULT 0, saved_bytes INTEGER DEFAULT 0, updated_at REAL);
CREATE TABLE IF NOT EXISTS stage_stats (stage TEXT PRIMARY KEY, processed INTEGER DEFAULT 0, skipped INTEGER DEFAULT 0, from_cache INTEGER DEFAULT 0, success INTEGER DEFAULT 0, failed INTEGER DEFAULT 0, saved_bytes INTEGER DEFAULT 0, updated_at REAL);
CREATE TABLE IF NOT EXISTS file_errors (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp REAL, chat_id INTEGER, chat_name TEXT, topic_id INTEGER, topic_name TEXT, message_id INTEGER, filename TEXT, stage TEXT, error TEXT);
CREATE TABLE IF NOT EXISTS system_errors (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp REAL, component TEXT, error TEXT, details TEXT);
CREATE TABLE IF NOT EXISTS scan_progress (chat_id INTEGER PRIMARY KEY, chat_name TEXT, current_id INTEGER, max_id INTEGER, percent REAL, files_found INTEGER, current_topic TEXT, completed BOOLEAN DEFAULT 0, updated_at REAL);
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT, updated_at REAL);
CREATE TABLE IF NOT EXISTS app_state (key TEXT PRIMARY KEY, value TEXT, updated_at REAL);
CREATE INDEX IF NOT EXISTS idx_files_state ON files(state);
CREATE INDEX IF NOT EXISTS idx_files_chat_topic ON files(chat_id, topic_id);
CREATE INDEX IF NOT EXISTS idx_queue_status ON queue_items(status);
CREATE INDEX IF NOT EXISTS idx_queue_created ON queue_items(created_at);
CREATE INDEX IF NOT EXISTS idx_queue_chat_msg ON queue_items(chat_id, message_id);
CREATE INDEX IF NOT EXISTS idx_files_chat_msg_state ON files(chat_id, message_id, state);
CREATE INDEX IF NOT EXISTS idx_history_timestamp ON history(timestamp);
CREATE INDEX IF NOT EXISTS idx_history_chat ON history(chat_id);
"""


def fmt_size(b: int) -> str:
    """Форматирует размер в байтах в читаемый вид."""
    if b < 1024:
        return f"{b}B"
    if b < 1024**2:
        return f"{b/1024:.1f}KB"
    if b < 1024**3:
        return f"{b/(1024**2):.1f}MB"
    return f"{b/(1024**3):.1f}GB"


def _sanitize_name(name: str) -> str:
    """Очищает имя папки от недопустимых символов."""
    if not name:
        return "general"
    name = re.sub(r'\.{2,}', '_', name)
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = name.strip().replace(' ', '_')
    return re.sub(r'_+', '_', name)[:100]


def _sanitize_filename(filename: str) -> str:
    """Очищает имя файла от недопустимых символов."""
    if not filename:
        return "unnamed"
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    filename = re.sub(r'[\x00-\x1f\x7f]', '', filename)
    if len(filename) > 200:
        name, ext = os.path.splitext(filename)
        filename = name[:195] + ext
    return filename


def build_local_path(download_dir: str, chat_name: str, topic_name: str, filename: str) -> str:
    """Строит локальный путь для сохранения файла."""
    safe_chat: str = _sanitize_name(chat_name)
    safe_topic: str = _sanitize_name(topic_name) if topic_name else "general"
    safe_filename: str = _sanitize_filename(filename)
    return os.path.join(download_dir, safe_chat, safe_topic, safe_filename)


def build_compressed_path(download_dir: str, chat_name: str, topic_name: str, filename: str) -> str:
    """Строит путь для сжатого файла."""
    base: str = build_local_path(download_dir, chat_name, topic_name, filename)
    name, ext = os.path.splitext(base)
    if ext.lower() in ('.jpg', '.jpeg', '.png', '.heic', '.heif', '.webp', '.bmp', '.tiff'):
        return f"{name}_compressed.jpg"
    elif ext.lower() in ('.mp4', '.avi', '.mkv', '.mov', '.webm', '.flv', '.m4v', '.3gp'):
        return f"{name}_compressed.mp4"
    return f"{name}_compressed{ext}"


def calculate_md5(file_path: str) -> str:
    """Вычисляет MD5 хеш файла."""
    import hashlib
    hash_md5: hashlib._Hash = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception:
        return ""


def is_valid_image(path: str) -> bool:
    """Проверяет, является ли файл валидным изображением."""
    try:
        from PIL import Image
        with Image.open(path) as img:
            img.verify()
        return True
    except Exception:
        return False


async def is_valid_video(path: str) -> bool:
    """Проверяет, является ли файл валидным видео."""
    import shutil
    if not shutil.which('ffprobe'):
        return os.path.getsize(path) > 0
    try:
        cmd: List[str] = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', path]
        process: asyncio.subprocess.Process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, _ = await process.communicate()
        if process.returncode != 0:
            return False
        data: dict = json.loads(stdout)
        return any(s.get('codec_type') == 'video' for s in data.get('streams', []))
    except Exception:
        return False


class DatabaseManager:
    """Менеджер базы данных SQLite."""
    
    _instance: Optional['DatabaseManager'] = None
    _db: Optional[aiosqlite.Connection] = None
    _lock: asyncio.Lock = asyncio.Lock()
    _write_lock: asyncio.Lock = asyncio.Lock()
    
    DEFAULT_PATHS: Dict[str, str] = {'download_dir': 'downloads', 'logs_dir': 'logs', 'sessions_dir': 'sessions'}
    DEFAULT_DOWNLOAD: Dict[str, int] = {'max_concurrent': 3, 'min_concurrent': 1, 'max_concurrent_max': 5, 'rate_limit_calls': 30, 'rate_limit_period': 60}
    DEFAULT_UPLOAD: Dict[str, Any] = {'base_path': '/tg_backup', 'min_free_space_mb': 100, 'rate_limit_calls': 100, 'rate_limit_period': 60, 'max_concurrent_uploads': 3, 'upload_timeout': 60}
    DEFAULT_COMPRESSION: Dict[str, Any] = {'min_photo_size_kb': 500, 'image_quality': 92, 'convert_heic': True, 'min_video_size_mb': 15, 'min_video_duration': 10, 'video_crf': 23, 'video_preset': 'veryfast', 'video_threads': 2, 'photo_processes': 4, 'skip_efficient_codecs': True, 'use_cpulimit': True, 'video_cpu_limit': 80, 'low_priority': True}
    DEFAULT_QUEUE: Dict[str, int] = {'check_workers': 10, 'download_workers': 6, 'photo_workers': 2, 'video_workers': 1, 'upload_workers': 5}
    DEFAULT_TELEGRAM_CLIENT: Dict[str, Any] = {'topics_cache_ttl': 86400, 'dialogs_cache_ttl': 300, 'rate_limit_calls': 30, 'rate_limit_period': 60, 'max_concurrent_downloads': 3, 'min_concurrent_downloads': 1, 'max_concurrent_downloads_max': 8}
    DEFAULT_FILE_TYPES: Dict[str, List[str]] = {
        'photo': ['.jpg', '.jpeg', '.png', '.webp', '.heic', '.heif', '.gif', '.bmp', '.tiff'],
        'video': ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.webm', '.flv', '.m4v', '.3gp'],
        'audio': ['.mp3', '.wav', '.ogg', '.flac', '.m4a', '.aac', '.opus'],
        'document': ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.txt', '.rtf', '.md']
    }
    
    def __new__(cls) -> 'DatabaseManager':
        """Создаёт singleton экземпляр."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self) -> None:
        """Инициализирует менеджер БД."""
        self._cache: Dict[str, Any] = {}
        self._loaded: bool = False
        self._settings_lock: asyncio.Lock = asyncio.Lock()
    
    async def _migrate_schema(self) -> None:
        """Выполняет миграцию схемы БД до актуальной версии."""
        cursor: aiosqlite.Cursor = await self._db.execute("SELECT value FROM app_state WHERE key = 'schema_version'")
        row: Optional[aiosqlite.Row] = await cursor.fetchone()
        current_version: int = 1
        if row:
            try:
                current_version = json.loads(row['value'])
            except Exception:
                current_version = 1
        
        if current_version >= SCHEMA_VERSION:
            return
        
        logger.info(f"📦 Миграция схемы БД: v{current_version} → v{SCHEMA_VERSION}")
        
        if current_version < 4:
            await self._db.execute("""
                CREATE INDEX IF NOT EXISTS idx_files_state_selected 
                ON files(state, chat_id, topic_id, message_id) 
                WHERE state = 1
            """)
            await self._db.commit()
            current_version = 4
            await self._db.execute("INSERT OR REPLACE INTO app_state (key, value, updated_at) VALUES ('schema_version', ?, ?)", (json.dumps(current_version), time.time()))
            await self._db.commit()
        
        logger.info(f"✅ Миграция схемы завершена (v{SCHEMA_VERSION})")
    
    async def init(self) -> None:
        """Инициализирует подключение к БД и создаёт таблицы."""
        async with self._lock:
            if self._db is None:
                self._db = await aiosqlite.connect(DB_FILE)
                self._db.row_factory = aiosqlite.Row
                await self._db.execute("PRAGMA foreign_keys = ON;")
                await self._db.execute("PRAGMA journal_mode=WAL;")
                await self._db.execute("PRAGMA synchronous=NORMAL;")
                await self._db.execute("PRAGMA busy_timeout=15000;")
                await self._db.execute("PRAGMA cache_size=-8000;")
                await self._db.execute("PRAGMA temp_store=MEMORY;")
                await self._db.executescript(DB_SCHEMA)
                await self._db.commit()
                await self._migrate_schema()
                logger.info("🗄️ База данных инициализирована")
    
    async def get_connection(self) -> aiosqlite.Connection:
        """Возвращает активное подключение к БД."""
        await self.init()
        return self._db
    
    async def execute(self, sql: str, params: tuple = ()) -> aiosqlite.Cursor:
        """Выполняет SQL запрос."""
        db: aiosqlite.Connection = await self.get_connection()
        return await db.execute(sql, params)
    
    async def executemany(self, sql: str, params: list) -> aiosqlite.Cursor:
        """Выполняет множественный SQL запрос."""
        db: aiosqlite.Connection = await self.get_connection()
        return await db.executemany(sql, params)
    
    async def commit(self) -> None:
        """Фиксирует транзакцию."""
        if self._db:
            await self._db.commit()
    
    async def rollback(self) -> None:
        """Откатывает транзакцию."""
        if self._db:
            await self._db.rollback()
    
    async def close(self) -> None:
        """Закрывает подключение к БД."""
        if self._db:
            await self._db.close()
            self._db = None
            logger.info("🔌 База данных закрыта")
    
    async def checkpoint(self) -> None:
        """Выполняет WAL checkpoint."""
        if self._db:
            await self._db.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            logger.debug("✅ Checkpoint выполнен")
    
    async def get_topics(self, chat_id: int) -> List[dict]:
        """Возвращает список тем чата."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT topic_id, topic_name, is_selected FROM topics WHERE chat_id = ?", (chat_id,))
        return [dict(r) for r in await cursor.fetchall()]
    
    async def update_topics(self, chat_id: int, topics_dict: Dict[str, str]) -> None:
        """Обновляет список тем чата."""
        for t_id, t_name in topics_dict.items():
            await self.execute("INSERT OR IGNORE INTO topics (chat_id, topic_id, topic_name) VALUES (?, ?, ?)", (chat_id, int(t_id), t_name))
            await self.execute("UPDATE topics SET topic_name = ? WHERE chat_id = ? AND topic_id = ?", (t_name, chat_id, int(t_id)))
        await self.commit()
    
    async def get_chat_name(self, chat_id: int) -> str:
        """Возвращает имя чата."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT name FROM chat_names WHERE chat_id = ?", (chat_id,))
        row: Optional[aiosqlite.Row] = await cursor.fetchone()
        if row and row['name']:
            return row['name']
        cursor = await self.execute("SELECT topic_name FROM topics WHERE chat_id = ? LIMIT 1", (chat_id,))
        row = await cursor.fetchone()
        return row['topic_name'] if row else f"Chat {chat_id}"
    
    async def set_chat_name(self, chat_id: int, name: str) -> None:
        """Устанавливает имя чата."""
        await self.execute("INSERT OR REPLACE INTO chat_names (chat_id, name) VALUES (?, ?)", (chat_id, name))
        await self.commit()
    
    async def get_files(self, chat_id: int, topic_id: Optional[int] = None, state_filter: Optional[int] = None) -> List[dict]:
        """Возвращает список файлов с опциональной фильтрацией."""
        if topic_id is not None:
            if state_filter is not None:
                cursor: aiosqlite.Cursor = await self.execute("SELECT * FROM files WHERE chat_id = ? AND topic_id = ? AND state = ?", (chat_id, topic_id, state_filter))
            else:
                cursor = await self.execute("SELECT * FROM files WHERE chat_id = ? AND topic_id = ?", (chat_id, topic_id))
        else:
            if state_filter is not None:
                cursor = await self.execute("SELECT * FROM files WHERE chat_id = ? AND state = ?", (chat_id, state_filter))
            else:
                cursor = await self.execute("SELECT * FROM files WHERE chat_id = ?", (chat_id,))
        return [dict(r) for r in await cursor.fetchall()]
    
    async def get_unuploaded_files(self, chat_id: int, topic_id: int, limit: Optional[int] = None) -> List[dict]:
        """Возвращает нескачанные файлы из выбранной темы."""
        sql: str = """
            SELECT * FROM files 
            WHERE chat_id = ? AND topic_id = ? 
            AND state IN (?, ?, ?)
            ORDER BY message_id
        """
        params: List[Any] = [chat_id, topic_id, STATE_UNLOADED, STATE_NEW, STATE_SELECTED]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        cursor: aiosqlite.Cursor = await self.execute(sql, tuple(params))
        return [dict(row) for row in await cursor.fetchall()]
    
    async def get_selected_files(self, chat_id: int, topic_id: int) -> List[dict]:
        """Возвращает выбранные файлы темы."""
        return await self.get_files(chat_id, topic_id, STATE_SELECTED)
    
    async def add_files(self, chat_id: int, topic_id: int, files_info: List[dict], topic_name: Optional[str] = None) -> None:
        """Добавляет информацию о файлах в БД с защитой от дубликатов."""
        if topic_name:
            await self.execute("INSERT OR IGNORE INTO topics (chat_id, topic_id, topic_name) VALUES (?, ?, ?)", (chat_id, topic_id, topic_name))
        
        # Проверяем существующие ID чтобы не добавлять дубликаты
        msg_ids = [f['message_id'] for f in files_info]
        placeholders = ','.join('?' * len(msg_ids))
        cursor = await self.execute(
            f"SELECT message_id FROM files WHERE chat_id = ? AND message_id IN ({placeholders})",
            [chat_id] + msg_ids
        )
        existing_ids = {row['message_id'] for row in await cursor.fetchall()}
        
        data: List[tuple] = []
        for f in files_info:
            if f['message_id'] not in existing_ids:
                data.append((chat_id, topic_id, f['message_id'], f['filename'], f.get('type', 'document'), f.get('size', 0), f.get('state', STATE_UNLOADED), 0, "", f.get('file_id'), f.get('dc_id'), f.get('timestamp', time.time()), f.get('md5', None)))
        
        if data:
            await self.executemany("INSERT OR IGNORE INTO files (chat_id, topic_id, message_id, filename, file_type, size, state, attempts, last_error, file_id, dc_id, timestamp, md5) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data)
            await self.commit()
            # Пересчитываем chat_stats из реальных данных
            cursor = await self.execute("SELECT COUNT(*), COALESCE(SUM(size), 0) FROM files WHERE chat_id = ?", (chat_id,))
            row = await cursor.fetchone()
            actual_total = row[0]
            actual_bytes = row[1] or 0
            await self.execute("INSERT OR REPLACE INTO chat_stats (chat_id, total, total_bytes, updated_at) VALUES (?, ?, ?, ?)", (chat_id, actual_total, actual_bytes, time.time()))
            await self.commit()
    
    async def update_file_state(self, chat_id: int, message_id: int, state: int) -> bool:
        """Обновляет статус файла."""
        async def op(db_conn):
            cursor = await db_conn.execute("UPDATE files SET state = ?, attempts = 0, last_error = NULL WHERE chat_id = ? AND message_id = ?", (state, chat_id, message_id))
            return cursor.rowcount > 0
        return await self._with_transaction(op)
    
    async def _with_transaction(self, operation: Callable[[aiosqlite.Connection], Awaitable[Any]], max_retries: int = 5) -> Any:
        """Выполняет операцию в транзакции с retry при блокировках."""
        last_error = None
        for attempt in range(max_retries):
            try:
                async with self._write_lock:
                    db: aiosqlite.Connection = await self.get_connection()
                    try:
                        await db.execute("BEGIN IMMEDIATE")
                    except Exception:
                        pass
                    try:
                        result: Any = await operation(db)
                        try:
                            await db.commit()
                        except Exception:
                            pass
                        return result
                    except Exception as e:
                        try:
                            await db.rollback()
                        except Exception:
                            pass
                        raise
            except (sqlite3.OperationalError, aiosqlite.OperationalError) as e:
                last_error = e
                if 'locked' in str(e).lower() or 'busy' in str(e).lower():
                    if attempt < max_retries - 1:
                        delay = 0.1 * (2 ** attempt)
                        logger.debug(f"🔄 БД занята, повтор через {delay:.2f}с (попытка {attempt+1}/{max_retries})")
                        import asyncio as _asyncio
                        await _asyncio.sleep(delay)
                        continue
                raise
            except Exception as e:
                last_error = e
                raise
        raise last_error or Exception("Max retries exceeded for DB transaction")
    
    async def select_topic(self, chat_id: int, topic_id: int) -> None:
        """Выбирает тему для загрузки."""
        async def op(db: aiosqlite.Connection) -> None:
            await db.execute("UPDATE topics SET is_selected = 1 WHERE chat_id = ? AND topic_id = ?", (chat_id, topic_id))
            await db.execute("UPDATE files SET state = ? WHERE chat_id = ? AND topic_id = ? AND state NOT IN (?, ?, ?)", (STATE_SELECTED, chat_id, topic_id, STATE_UPLOADED, STATE_SKIPPED, STATE_ERROR))
        await self._with_transaction(op)
    
    async def deselect_topic(self, chat_id: int, topic_id: int) -> None:
        """Снимает выбор с темы."""
        async def op(db: aiosqlite.Connection) -> None:
            await db.execute("UPDATE topics SET is_selected = 0 WHERE chat_id = ? AND topic_id = ?", (chat_id, topic_id))
            await db.execute("UPDATE files SET state = ? WHERE chat_id = ? AND topic_id = ? AND state = ?", (STATE_UNLOADED, chat_id, topic_id, STATE_SELECTED))
        await self._with_transaction(op)
    
    async def is_topic_selected(self, chat_id: int, topic_id: int) -> bool:
        """Проверяет, выбрана ли тема."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT is_selected FROM topics WHERE chat_id = ? AND topic_id = ?", (chat_id, topic_id))
        row: Optional[aiosqlite.Row] = await cursor.fetchone()
        return bool(row['is_selected']) if row else False
    
    async def toggle_file_selected(self, chat_id: int, topic_id: int, message_id: int) -> int:
        """Переключает выбор отдельного файла."""
        async def op(db: aiosqlite.Connection) -> int:
            cursor: aiosqlite.Cursor = await db.execute("SELECT state FROM files WHERE chat_id = ? AND message_id = ?", (chat_id, message_id))
            row: Optional[aiosqlite.Row] = await cursor.fetchone()
            if not row:
                return -1
            current: int = row['state']
            if current in (STATE_UNLOADED, STATE_NEW, STATE_ERROR):
                new: int = STATE_SELECTED
            elif current == STATE_SELECTED:
                new = STATE_UNLOADED
            else:
                return current
            await db.execute("UPDATE files SET state = ? WHERE chat_id = ? AND message_id = ?", (new, chat_id, message_id))
            return new
        return await self._with_transaction(op)
    
    async def select_all_files(self, chat_id: int, topic_id: int) -> int:
        """Выбирает все файлы в теме."""
        async def op(db: aiosqlite.Connection) -> int:
            cursor: aiosqlite.Cursor = await db.execute("UPDATE files SET state = ? WHERE chat_id = ? AND topic_id = ? AND state IN (?, ?, ?)", (STATE_SELECTED, chat_id, topic_id, STATE_UNLOADED, STATE_NEW, STATE_ERROR))
            return cursor.rowcount
        return await self._with_transaction(op)
    
    async def deselect_all_files(self, chat_id: int, topic_id: int) -> int:
        """Снимает выбор со всех файлов в теме."""
        async def op(db: aiosqlite.Connection) -> int:
            cursor: aiosqlite.Cursor = await db.execute("UPDATE files SET state = ? WHERE chat_id = ? AND topic_id = ? AND state = ?", (STATE_UNLOADED, chat_id, topic_id, STATE_SELECTED))
            return cursor.rowcount
        return await self._with_transaction(op)
    
    async def reset_chat_errors(self, chat_id: int) -> int:
        """Сбрасывает ошибки для всех файлов чата."""
        async def op(db: aiosqlite.Connection) -> int:
            cursor: aiosqlite.Cursor = await db.execute("UPDATE files SET state = ?, attempts = 0, last_error = NULL WHERE chat_id = ? AND state = ?", (STATE_UNLOADED, chat_id, STATE_ERROR))
            count: int = cursor.rowcount
            await db.execute("UPDATE chat_stats SET errors = 0 WHERE chat_id = ?", (chat_id,))
            return count
        return await self._with_transaction(op)
    
    async def get_stats(self, chat_id: int, topic_id: Optional[int] = None) -> dict:
        """Возвращает статистику по чату/теме."""
        if topic_id is not None:
            cursor: aiosqlite.Cursor = await self.execute("SELECT COUNT(*) as total, SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as uploaded, SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as new_count, SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as error_count, SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as selected_count, SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as unloaded_count FROM files WHERE chat_id = ? AND topic_id = ?", (STATE_UPLOADED, STATE_NEW, STATE_ERROR, STATE_SELECTED, STATE_UNLOADED, chat_id, topic_id))
        else:
            cursor = await self.execute("SELECT COUNT(*) as total, SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as uploaded, SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as new_count, SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as error_count, SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as selected_count, SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as unloaded_count FROM files WHERE chat_id = ?", (STATE_UPLOADED, STATE_NEW, STATE_ERROR, STATE_SELECTED, STATE_UNLOADED, chat_id))
        row: Optional[aiosqlite.Row] = await cursor.fetchone()
        if not row:
            return {'total': 0, 'uploaded': 0, 'new': 0, 'error': 0, 'selected': 0, 'unloaded': 0, 'pending': 0}
        total: int = row['total'] or 0
        uploaded: int = row['uploaded'] or 0
        return {'total': total, 'uploaded': uploaded, 'new': row['new_count'] or 0, 'error': row['error_count'] or 0, 'selected': row['selected_count'] or 0, 'unloaded': row['unloaded_count'] or 0, 'pending': total - uploaded}
    
    async def get_all_stats(self) -> dict:
        """Возвращает общую статистику."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT COUNT(*) as total FROM files")
        total: int = (await cursor.fetchone())['total'] or 0
        cursor = await self.execute("SELECT COUNT(*) FROM files WHERE state = ?", (STATE_UPLOADED,))
        uploaded: int = (await cursor.fetchone())[0] or 0
        cursor = await self.execute("SELECT COUNT(*) FROM files WHERE state = ?", (STATE_NEW,))
        new_files: int = (await cursor.fetchone())[0] or 0
        cursor = await self.execute("SELECT COUNT(*) FROM files WHERE state = ?", (STATE_ERROR,))
        error_files: int = (await cursor.fetchone())[0] or 0
        cursor = await self.execute("SELECT COUNT(*) FROM files WHERE state = ?", (STATE_SELECTED,))
        selected_files: int = (await cursor.fetchone())[0] or 0
        cursor = await self.execute("SELECT COUNT(*) FROM files WHERE state = ?", (STATE_SKIPPED,))
        skipped_files: int = (await cursor.fetchone())[0] or 0
        cursor = await self.execute("SELECT COUNT(*) FROM files WHERE state = ?", (STATE_UNLOADED,))
        unloaded_files: int = (await cursor.fetchone())[0] or 0
        return {'total_files': total, 'uploaded': uploaded, 'new_files': new_files, 'error_files': error_files, 'selected_files': selected_files, 'skipped_files': skipped_files, 'unloaded_files': unloaded_files, 'pending': total - uploaded - skipped_files - error_files}
    
    async def get_topic_name(self, chat_id: int, topic_id: int) -> str:
        """Возвращает имя темы."""
        topics: List[dict] = await self.get_topics(chat_id)
        for t in topics:
            if t['topic_id'] == topic_id:
                return t['topic_name']
        return f"Тема {topic_id}"
    
    async def set_file_md5(self, chat_id: int, message_id: int, md5: str) -> bool:
        """Устанавливает MD5 хеш файла."""
        async def op(db_conn):
            cursor = await db_conn.execute("UPDATE files SET md5 = ? WHERE chat_id = ? AND message_id = ?", (md5, chat_id, message_id))
            return cursor.rowcount > 0
        return await self._with_transaction(op)
    
    async def get_file_md5(self, chat_id: int, message_id: int) -> Optional[str]:
        """Возвращает MD5 хеш файла."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT md5 FROM files WHERE chat_id = ? AND message_id = ?", (chat_id, message_id))
        row: Optional[aiosqlite.Row] = await cursor.fetchone()
        return row['md5'] if row else None
    
    async def get_file_state(self, chat_id: int, message_id: int) -> Optional[int]:
        """Возвращает статус файла."""
        cursor: aiosqlite.Cursor = await self.execute(
            "SELECT state FROM files WHERE chat_id = ? AND message_id = ?",
            (chat_id, message_id))
        row: Optional[aiosqlite.Row] = await cursor.fetchone()
        return row['state'] if row else None
    
    async def add_queue_item(self, item: dict) -> bool:
        """Добавляет элемент в очередь (атомарно)."""
        await self.ensure_loaded()
        key: str = f"{item['chat_id']}:{item['message_id']}"
        
        async def do_add(db_conn):
            cursor = await db_conn.execute("SELECT key FROM queue_items WHERE key = ?", (key,))
            if await cursor.fetchone():
                return False
            ext: str = os.path.splitext(item['filename'])[1].lower()
            file_type: str = 'other'
            for ft, exts in self._cache.get('file_types', {}).items():
                if ext in exts:
                    file_type = ft
                    break
            await db_conn.execute("INSERT INTO queue_items (key, chat_id, message_id, topic_id, filename, remote_dir, local_path, compressed_path, file_size, file_type, status, attempts, max_attempts, created_at, updated_at, metadata, file_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (key, item['chat_id'], item['message_id'], item.get('topic_id'), item['filename'], item['remote_dir'], item.get('local_path', ''), item.get('compressed_path', ''), item.get('file_size', 0), file_type, item.get('status', STATUS_PENDING_CHECK), item.get('attempts', 0), item.get('max_attempts', 3), item.get('created_at', time.time()), item.get('updated_at', time.time()), json.dumps(item.get('metadata', {})), json.dumps(item.get('file_info', {}))))
            return True
        
        return await self._with_transaction(do_add)
    
    async def get_queue_item(self, key: str) -> Optional[dict]:
        """Возвращает элемент очереди по ключу (None если нет)."""
        try:
            cursor: aiosqlite.Cursor = await self.execute("SELECT 1 FROM queue_items WHERE key = ? LIMIT 1", (key,))
            if not await cursor.fetchone():
                return None
        except Exception:
            return None
        cursor: aiosqlite.Cursor = await self.execute("SELECT * FROM queue_items WHERE key = ?", (key,))
        row: Optional[aiosqlite.Row] = await cursor.fetchone()
        if not row:
            return None
        item: dict = dict(row)
        item['metadata'] = json.loads(item['metadata']) if item['metadata'] else {}
        item['file_info'] = json.loads(item['file_info']) if item['file_info'] else {}
        return item
    
    async def get_queue_items(self, status: Optional[str] = None, limit: Optional[int] = None) -> List[dict]:
        """Возвращает список элементов очереди."""
        if status:
            sql, params = "SELECT * FROM queue_items WHERE status = ? ORDER BY created_at", (status,)
        else:
            sql, params = "SELECT * FROM queue_items WHERE status NOT IN (?, ?) ORDER BY created_at", (STATUS_COMPLETED, STATUS_FAILED)
        if limit:
            sql += " LIMIT ?"
            params += (limit,)
        cursor: aiosqlite.Cursor = await self.execute(sql, params)
        return [dict(r) for r in await cursor.fetchall()]
    
    async def get_next_queue_item(self, status: str, file_type: Optional[str] = None, exclude_keys: Optional[List[str]] = None) -> Optional[dict]:
        """Возвращает следующий элемент очереди для обработки."""
        sql, params = "SELECT * FROM queue_items WHERE status = ?", [status]
        if file_type:
            sql += " AND file_type = ?"
            params.append(file_type)
        if exclude_keys:
            placeholders: str = ','.join('?' * len(exclude_keys))
            sql += f" AND key NOT IN ({placeholders})"
            params.extend(exclude_keys)
        sql += " ORDER BY created_at LIMIT 1"
        cursor: aiosqlite.Cursor = await self.execute(sql, tuple(params))
        row: Optional[aiosqlite.Row] = await cursor.fetchone()
        if not row:
            return None
        item: dict = dict(row)
        item['metadata'] = json.loads(item['metadata']) if item['metadata'] else {}
        item['file_info'] = json.loads(item['file_info']) if item['file_info'] else {}
        return item
    
    async def update_queue_status(self, key: str, status: str, attempts: Optional[int] = None, last_error: Optional[str] = None) -> None:
        """Обновляет статус элемента очереди."""
        async def op(db_conn):
            if attempts is not None:
                await db_conn.execute("UPDATE queue_items SET status = ?, attempts = ?, last_error = ?, updated_at = ? WHERE key = ?", (status, attempts, last_error, time.time(), key))
            else:
                await db_conn.execute("UPDATE queue_items SET status = ?, updated_at = ? WHERE key = ?", (status, time.time(), key))
        await self._with_transaction(op)
    
    async def update_queue_item_paths(self, key: str, local_path: Optional[str] = None, compressed_path: Optional[str] = None, file_size: Optional[int] = None) -> None:
        """Атомарно обновляет пути и размер элемента очереди."""
        updates: List[str] = []
        params: List[Any] = []
        if local_path is not None:
            updates.append("local_path = ?")
            params.append(local_path)
        if compressed_path is not None:
            updates.append("compressed_path = ?")
            params.append(compressed_path)
        if file_size is not None:
            updates.append("file_size = ?")
            params.append(file_size)
        if updates:
            updates.append("updated_at = ?")
            params.append(time.time())
            params.append(key)
            async def do_update(db_conn):
                await db_conn.execute(f"UPDATE queue_items SET {', '.join(updates)} WHERE key = ?", tuple(params))
            await self._with_transaction(do_update)
    
    async def delete_queue_item(self, key: str) -> None:
        """Удаляет элемент из очереди."""
        async def op(db_conn):
            await db_conn.execute("DELETE FROM queue_items WHERE key = ?", (key,))
        await self._with_transaction(op)
    
    async def clear_queue(self) -> None:
        """Очищает все очереди."""
        async with self._write_lock:
            await self.execute("DELETE FROM queue_items")
            await self.execute("DELETE FROM queue_processing")
            await self.execute("DELETE FROM queue_retry")
            await self.execute("DELETE FROM active_progress")
            await self.commit()
            logger.info("🗑️ Очередь очищена")
    
    async def count_pending(self) -> int:
        """Возвращает количество ожидающих задач."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT COUNT(*) FROM queue_items WHERE status NOT IN (?, ?)", (STATUS_COMPLETED, STATUS_FAILED))
        row: aiosqlite.Row = await cursor.fetchone()
        return row[0] if row else 0
    
    async def get_queue_counts(self) -> Dict[str, int]:
        """Возвращает количество задач по статусам."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT status, COUNT(*) as cnt FROM queue_items GROUP BY status")
        return {r['status']: r['cnt'] for r in await cursor.fetchall()}
    
    async def are_files_in_queue(self, keys: List[str]) -> Set[str]:
        """Пакетная проверка наличия файлов в очереди."""
        if not keys:
            return set()
        result: Set[str] = set()
        chunk_size: int = 500
        for i in range(0, len(keys), chunk_size):
            chunk: List[str] = keys[i:i + chunk_size]
            placeholders: str = ','.join('?' * len(chunk))
            cursor: aiosqlite.Cursor = await self.execute(f"SELECT key FROM queue_items WHERE key IN ({placeholders})", chunk)
            result.update(row['key'] for row in await cursor.fetchall())
        return result
    
    async def has_selected_files_remaining(self, chat_ids: Optional[List[int]] = None) -> bool:
        """Проверяет, есть ли ещё выбранные файлы для обработки."""
        if chat_ids:
            placeholders: str = ','.join('?' * len(chat_ids))
            sql: str = f"""
                SELECT COUNT(*) FROM files f 
                JOIN topics t ON f.chat_id = t.chat_id AND f.topic_id = t.topic_id
                WHERE t.is_selected = 1 
                AND f.state IN (?, ?, ?)
                AND f.chat_id IN ({placeholders})
            """
            params: List[Any] = [STATE_UNLOADED, STATE_NEW, STATE_SELECTED] + list(chat_ids)
        else:
            sql = """
                SELECT COUNT(*) FROM files f 
                JOIN topics t ON f.chat_id = t.chat_id AND f.topic_id = t.topic_id
                WHERE t.is_selected = 1 
                AND f.state IN (?, ?, ?)
            """
            params = [STATE_UNLOADED, STATE_NEW, STATE_SELECTED]
        cursor: aiosqlite.Cursor = await self.execute(sql, tuple(params))
        row: aiosqlite.Row = await cursor.fetchone()
        return (row[0] or 0) > 0
    
    async def add_processing(self, key: str, worker_id: int, worker_type: str) -> bool:
        """Добавляет запись о обработке элемента."""
        try:
            async def op(db_conn):
                now: float = time.time()
                await db_conn.execute("INSERT OR IGNORE INTO queue_processing (key, worker_id, worker_type, started_at, updated_at) VALUES (?, ?, ?, ?, ?)", (key, worker_id, worker_type, now, now))
                return True
            return await self._with_transaction(op)
        except Exception:
            return False
    
    async def remove_processing(self, key: str) -> None:
        """Удаляет запись о обработке элемента."""
        async def op(db_conn):
            await db_conn.execute("DELETE FROM queue_processing WHERE key = ?", (key,))
        await self._with_transaction(op)
    
    async def clear_processing(self) -> None:
        """Очищает все записи о обработке."""
        async with self._write_lock:
            await self.execute("DELETE FROM queue_processing")
            await self.commit()
    
    async def get_processing_keys(self) -> Set[str]:
        """Возвращает ключи обрабатываемых элементов."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT key FROM queue_processing")
        return {r['key'] for r in await cursor.fetchall()}
    
    async def get_active_items(self) -> List[dict]:
        """Возвращает активные элементы очереди."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT qi.*, qp.worker_id, qp.worker_type, qp.started_at, qp.updated_at as qp_updated_at, ap.stage, ap.progress, ap.speed, ap.eta, ap.downloaded, ap.uploaded, ap.total_size, ap.updated_at as ap_updated_at FROM queue_items qi JOIN queue_processing qp ON qi.key = qp.key LEFT JOIN active_progress ap ON qi.key = ap.key")
        items: List[dict] = []
        for row in await cursor.fetchall():
            item: dict = dict(row)
            item['metadata'] = json.loads(item['metadata']) if item['metadata'] else {}
            item['file_info'] = json.loads(item['file_info']) if item['file_info'] else {}
            items.append(item)
        return items
    
    async def add_retry(self, key: str, delay: float) -> None:
        """Добавляет задачу на повторную попытку."""
        async def op(db_conn):
            await db_conn.execute("INSERT OR REPLACE INTO queue_retry (key, retry_at) VALUES (?, ?)", (key, time.time() + delay))
        await self._with_transaction(op)
    
    async def cancel_retry(self, key: str) -> None:
        """Отменяет повторную попытку."""
        async def op(db_conn):
            await db_conn.execute("DELETE FROM queue_retry WHERE key = ?", (key,))
        await self._with_transaction(op)
    
    async def update_progress(self, key: str, data: dict) -> None:
        """Обновляет прогресс обработки."""
        now: float = time.time()
        try:
            await self.execute("INSERT OR REPLACE INTO active_progress (key, stage, progress, speed, eta, downloaded, uploaded, total_size, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (key, data.get('stage', ''), data.get('progress'), data.get('speed'), data.get('eta'), data.get('downloaded'), data.get('uploaded'), data.get('total_size'), now))
            await self.execute("UPDATE queue_processing SET updated_at = ? WHERE key = ?", (now, key))
            await self.commit()
        except Exception:
            pass
    
    async def clear_progress(self, key: str) -> None:
        """Очищает прогресс элемента."""
        async def op(db_conn):
            await db_conn.execute("DELETE FROM active_progress WHERE key = ?", (key,))
        await self._with_transaction(op)
    
    async def add_history(self, entry: dict) -> None:
        """Добавляет запись в историю."""
        async def op(db_conn):
            await db_conn.execute("INSERT INTO history (timestamp, chat_id, topic_id, message_id, filename, status, size, compressed_size, stage, error, details, chat_name, topic_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (entry.get('timestamp', time.time()), entry.get('chat_id'), entry.get('topic_id'), entry.get('message_id'), entry.get('filename'), entry.get('status'), entry.get('size', 0), entry.get('compressed_size', 0), entry.get('stage', ''), entry.get('error', ''), json.dumps(entry.get('details', {})), entry.get('chat_name', ''), entry.get('topic_name', '')))
        await self._with_transaction(op)
    
    async def _record_event(self, event_type: str, **kwargs: Any) -> None:
        """Записывает событие в историю и обновляет статистику."""
        chat_id: Optional[int] = kwargs.get('chat_id')
        topic_id: Optional[int] = kwargs.get('topic_id')
        chat_name: str = kwargs.get('chat_name') or (await self.get_chat_name(chat_id) if chat_id else f"Chat {chat_id}")
        topic_name: str = kwargs.get('topic_name') or (await self.get_topic_name(chat_id, topic_id) if chat_id and topic_id else "Общая тема")
        entry: dict = {'timestamp': time.time(), 'chat_id': chat_id, 'topic_id': topic_id, 'message_id': kwargs.get('message_id'), 'filename': kwargs.get('filename', ''), 'status': event_type, 'size': kwargs.get('size', 0), 'compressed_size': kwargs.get('compressed_size', 0), 'stage': kwargs.get('stage', event_type), 'error': kwargs.get('error', ''), 'details': kwargs.get('details', {}), 'chat_name': chat_name, 'topic_name': topic_name}
        try:
            await self.add_history(entry)
        except Exception as e:
            await self.rollback()
            logger.error(f"❌ Ошибка записи в history: {e}")
        if event_type == 'uploaded':
            actual_size: int = kwargs.get('compressed_size') or kwargs.get('size', 0)
            await self.update_chat_stats(chat_id, {'total': 1, 'uploaded': 1, 'total_bytes': actual_size, 'uploaded_bytes': actual_size})
            await self.update_stage_stats('upload', {'processed': 1, 'success': 1})
        elif event_type == 'downloaded':
            await self.update_stage_stats('download', {'processed': 1, 'from_cache': 1 if kwargs.get('from_cache') else 0})
        elif event_type == 'compressed':
            original_size: int = kwargs.get('size', 0)
            compressed_size: int = kwargs.get('compressed_size', 0)
            saved: int = original_size - compressed_size
            if original_size > 0 and (saved / original_size * 100) >= 5:
                await self.update_chat_stats(chat_id, {'compressed': 1, 'saved_bytes': saved})
                await self.update_stage_stats('compress', {'processed': 1, 'saved_bytes': saved})
            else:
                await self.update_stage_stats('compress', {'skipped': 1})
        elif event_type == 'skipped':
            await self.update_chat_stats(chat_id, {'total': 1, 'skipped': 1, 'total_bytes': kwargs.get('size', 0), 'uploaded_bytes': kwargs.get('size', 0)})
            await self.update_stage_stats('check', {'processed': 1, 'skipped': 1})
    
    async def record_queued(self, chat_id: int, filename: str, size: int, topic_id: Optional[int] = None) -> None:
        """Записывает событие добавления в очередь."""
        await self._record_event('queued', chat_id=chat_id, topic_id=topic_id, filename=filename, size=size)
    
    async def record_uploaded(self, chat_id: int, message_id: int, filename: str, size: int, topic_id: Optional[int] = None, compressed_size: int = 0, chat_name: Optional[str] = None, topic_name: Optional[str] = None) -> None:
        """Записывает событие успешной загрузки."""
        await self._record_event('uploaded', chat_id=chat_id, topic_id=topic_id, message_id=message_id, filename=filename, size=size, compressed_size=compressed_size, chat_name=chat_name, topic_name=topic_name)
    
    async def record_downloaded(self, chat_id: int, message_id: int, filename: str, size: int, topic_id: Optional[int] = None, from_cache: bool = False, chat_name: Optional[str] = None, topic_name: Optional[str] = None) -> None:
        """Записывает событие успешного скачивания."""
        await self._record_event('downloaded', chat_id=chat_id, topic_id=topic_id, message_id=message_id, filename=filename, size=size, details={'from_cache': from_cache}, chat_name=chat_name, topic_name=topic_name)
    
    async def record_compressed(self, chat_id: int, message_id: int, filename: str, original_size: int, compressed_size: int, compression_type: str, topic_id: Optional[int] = None, chat_name: Optional[str] = None, topic_name: Optional[str] = None) -> None:
        """Записывает событие сжатия файла."""
        await self._record_event('compressed', chat_id=chat_id, topic_id=topic_id, message_id=message_id, filename=filename, size=original_size, compressed_size=compressed_size, details={'type': compression_type}, chat_name=chat_name, topic_name=topic_name)
    
    async def record_skipped(self, chat_id: int, message_id: int, filename: str, size: int, reason: str = "", topic_id: Optional[int] = None, chat_name: Optional[str] = None, topic_name: Optional[str] = None) -> None:
        """Записывает событие пропуска файла."""
        await self._record_event('skipped', chat_id=chat_id, topic_id=topic_id, message_id=message_id, filename=filename, size=size, details={'reason': reason}, chat_name=chat_name, topic_name=topic_name)
    
    async def record_file_error(self, chat_id: int, message_id: int, filename: str, stage: str, error: str, topic_id: Optional[int] = None, chat_name: Optional[str] = None, topic_name: Optional[str] = None) -> None:
        """Записывает ошибку обработки файла."""
        if chat_name is None:
            chat_name = await self.get_chat_name(chat_id)
        if topic_name is None and topic_id:
            topic_name = await self.get_topic_name(chat_id, topic_id)
        elif topic_name is None:
            topic_name = "Общая тема"
        await self._record_event('error', chat_id=chat_id, topic_id=topic_id, message_id=message_id, filename=filename, stage=stage, error=error, chat_name=chat_name, topic_name=topic_name)
        await self.add_file_error({'timestamp': time.time(), 'chat_id': chat_id, 'chat_name': chat_name, 'topic_id': topic_id, 'topic_name': topic_name, 'message_id': message_id, 'filename': filename, 'stage': stage, 'error': error[:500]})
        await self.update_chat_stats(chat_id, {'errors': 1})
        await self.update_stage_stats(stage, {'failed': 1})
    
    async def record_system_error(self, component: str, error: str, details: Optional[dict] = None) -> None:
        """Записывает системную ошибку."""
        await self.add_system_error({'timestamp': time.time(), 'component': component, 'error': error[:500], 'details': details or {}})
    
    async def get_history(self, limit: int = 20) -> List[dict]:
        """Возвращает историю операций."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT * FROM history ORDER BY timestamp DESC LIMIT ?", (limit,))
        return [dict(r) for r in await cursor.fetchall()]
    
    async def update_chat_stats(self, chat_id: int, data: dict) -> None:
        """Обновляет статистику чата."""
        async def op(db_conn):
            await db_conn.execute("INSERT INTO chat_stats (chat_id, total, uploaded, skipped, errors, compressed, total_bytes, uploaded_bytes, saved_bytes, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(chat_id) DO UPDATE SET total = total + ?, uploaded = uploaded + ?, skipped = skipped + ?, errors = errors + ?, compressed = compressed + ?, total_bytes = total_bytes + ?, uploaded_bytes = uploaded_bytes + ?, saved_bytes = saved_bytes + ?, updated_at = ?", (chat_id, data.get('total', 0), data.get('uploaded', 0), data.get('skipped', 0), data.get('errors', 0), data.get('compressed', 0), data.get('total_bytes', 0), data.get('uploaded_bytes', 0), data.get('saved_bytes', 0), time.time(), data.get('total', 0), data.get('uploaded', 0), data.get('skipped', 0), data.get('errors', 0), data.get('compressed', 0), data.get('total_bytes', 0), data.get('uploaded_bytes', 0), data.get('saved_bytes', 0), time.time()))
        await self._with_transaction(op)
    
    async def update_stage_stats(self, stage: str, data: dict) -> None:
        """Обновляет статистику этапа обработки."""
        async def op(db_conn):
            await db_conn.execute("INSERT INTO stage_stats (stage, processed, skipped, from_cache, success, failed, saved_bytes, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(stage) DO UPDATE SET processed = processed + ?, skipped = skipped + ?, from_cache = from_cache + ?, success = success + ?, failed = failed + ?, saved_bytes = saved_bytes + ?, updated_at = ?", (stage, data.get('processed', 0), data.get('skipped', 0), data.get('from_cache', 0), data.get('success', 0), data.get('failed', 0), data.get('saved_bytes', 0), time.time(), data.get('processed', 0), data.get('skipped', 0), data.get('from_cache', 0), data.get('success', 0), data.get('failed', 0), data.get('saved_bytes', 0), time.time()))
        await self._with_transaction(op)
    
    async def get_chat_stats(self, chat_id: Optional[int] = None) -> dict:
        """Возвращает статистику по всем чатам или конкретному."""
        if chat_id:
            cursor: aiosqlite.Cursor = await self.execute("SELECT * FROM chat_stats WHERE chat_id = ?", (chat_id,))
            row: Optional[aiosqlite.Row] = await cursor.fetchone()
            return dict(row) if row else {}
        cursor = await self.execute("SELECT * FROM chat_stats")
        return {str(r['chat_id']): dict(r) for r in await cursor.fetchall()}
    
    async def get_stage_stats(self) -> dict:
        """Возвращает статистику по этапам."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT * FROM stage_stats")
        return {r['stage']: dict(r) for r in await cursor.fetchall()}
    
    async def add_file_error(self, error: dict) -> None:
        """Добавляет ошибку файла."""
        async def op(db_conn):
            await db_conn.execute("INSERT INTO file_errors (timestamp, chat_id, chat_name, topic_id, topic_name, message_id, filename, stage, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (error.get('timestamp', time.time()), error.get('chat_id'), error.get('chat_name', ''), error.get('topic_id'), error.get('topic_name', ''), error.get('message_id'), error.get('filename'), error.get('stage', ''), error.get('error', '')[:500]))
        await self._with_transaction(op)
    
    async def add_system_error(self, error: dict) -> None:
        """Добавляет системную ошибку."""
        async def op(db_conn):
            await db_conn.execute("INSERT INTO system_errors (timestamp, component, error, details) VALUES (?, ?, ?, ?)", (error.get('timestamp', time.time()), error.get('component', ''), error.get('error', '')[:500], json.dumps(error.get('details', {}))))
        await self._with_transaction(op)
    
    async def get_file_errors(self, limit: int = 50) -> List[dict]:
        """Возвращает ошибки файлов."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT * FROM file_errors ORDER BY timestamp DESC LIMIT ?", (limit,))
        return [dict(r) for r in await cursor.fetchall()]
    
    async def get_system_errors(self, limit: int = 50) -> List[dict]:
        """Возвращает системные ошибки."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT * FROM system_errors ORDER BY timestamp DESC LIMIT ?", (limit,))
        return [dict(r) for r in await cursor.fetchall()]
    
    async def clear_errors(self) -> None:
        """Очищает все ошибки."""
        async with self._write_lock:
            await self.execute("DELETE FROM file_errors")
            await self.execute("DELETE FROM system_errors")
            await self.commit()
            logger.info("📋 Ошибки очищены")
    
    async def get_error_counts(self) -> Tuple[int, int]:
        """Возвращает количество ошибок файлов и системы."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT COUNT(*) FROM file_errors")
        file_count: int = (await cursor.fetchone())[0] or 0
        cursor = await self.execute("SELECT COUNT(*) FROM system_errors")
        system_count: int = (await cursor.fetchone())[0] or 0
        return file_count, system_count
    
    async def update_scan_progress(self, chat_id: int, chat_name: str, current_id: int, max_id: int, files_found: int, current_topic: Optional[str] = None, completed: bool = False) -> None:
        """Обновляет прогресс сканирования."""
        async def op(db_conn):
            percent: float = 100 if completed else (0 if max_id == 0 else min(100, max(0, ((max_id - current_id) / max_id) * 100)))
            await db_conn.execute("INSERT INTO scan_progress (chat_id, chat_name, current_id, max_id, percent, files_found, current_topic, completed, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(chat_id) DO UPDATE SET current_id = ?, max_id = ?, percent = ?, files_found = ?, current_topic = ?, completed = ?, updated_at = ?", (chat_id, chat_name, current_id, max_id, percent, files_found, current_topic, completed, time.time(), current_id, max_id, percent, files_found, current_topic, completed, time.time()))
        await self._with_transaction(op)
    
    async def get_scan_progress(self) -> Dict[str, dict]:
        """Возвращает прогресс сканирования."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT * FROM scan_progress")
        return {str(r['chat_id']): dict(r) for r in await cursor.fetchall()}
    
    async def clear_scan_progress(self) -> None:
        """Очищает прогресс сканирования."""
        await self.execute("UPDATE scan_progress SET completed = 1, percent = 100, updated_at = ?", (time.time(),))
        await self.commit()
    
    async def ensure_loaded(self) -> None:
        """Гарантирует загрузку настроек."""
        if not self._loaded:
            async with self._settings_lock:
                if not self._loaded:
                    await self.init()
                    await self._load_settings()
                    
                    for key in ['paths', 'download', 'upload', 'compression', 'queue', 'telegram_client', 'file_types']:
                        cursor = await self.execute("SELECT value FROM settings WHERE key = ?", (key,))
                        if not await cursor.fetchone():
                            await self._save_key(key, self._cache[key])
                    
                    self._loaded = True
    
    async def _load_settings(self) -> None:
        """Загружает настройки из БД."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT key, value FROM settings")
        rows: Dict[str, str] = {r['key']: r['value'] for r in await cursor.fetchall()}
        def safe_json_loads(data: str, default: Any) -> Any:
            try:
                return json.loads(data) if data else default
            except Exception:
                return default
        self._cache = {
            'paths': safe_json_loads(rows.get('paths'), self.DEFAULT_PATHS),
            'download': safe_json_loads(rows.get('download'), self.DEFAULT_DOWNLOAD),
            'upload': safe_json_loads(rows.get('upload'), self.DEFAULT_UPLOAD),
            'compression': safe_json_loads(rows.get('compression'), self.DEFAULT_COMPRESSION),
            'queue': safe_json_loads(rows.get('queue'), self.DEFAULT_QUEUE),
            'telegram_client': safe_json_loads(rows.get('telegram_client'), self.DEFAULT_TELEGRAM_CLIENT),
            'file_types': safe_json_loads(rows.get('file_types'), self.DEFAULT_FILE_TYPES),
            'chat_ids': safe_json_loads(rows.get('chat_ids'), []),
            'windows': safe_json_loads(rows.get('windows'), []),
            'auto_backup_enabled': safe_json_loads(rows.get('auto_backup_enabled'), False),
        }
        logger.info("📁 Настройки загружены из БД")
    
    async def _save_key(self, key: str, value: Any) -> None:
        """Сохраняет отдельный ключ настроек."""
        await self.execute("INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)", (key, json.dumps(value, ensure_ascii=False), time.time()))
        await self.commit()
    
    async def save_settings(self) -> None:
        """Сохраняет все настройки."""
        await self.ensure_loaded()
        for key, value in self._cache.items():
            await self._save_key(key, value)
    
    async def get_chat_ids(self) -> List[int]:
        """Возвращает список ID чатов."""
        await self.ensure_loaded()
        chat_ids: List[int] = self._cache.get('chat_ids', [])
        seen: Set[int] = set()
        unique_ids: List[int] = [x for x in chat_ids if not (x in seen or seen.add(x))]
        if len(unique_ids) != len(chat_ids):
            logger.warning(f"⚠️ Обнаружены дубликаты chat_ids: {chat_ids}")
            self._cache['chat_ids'] = unique_ids
            await self._save_key('chat_ids', unique_ids)
        return unique_ids
    
    async def get_windows(self) -> List[dict]:
        """Возвращает список окон работы."""
        await self.ensure_loaded()
        return self._cache.get('windows', [])
    
    async def is_auto_enabled(self) -> bool:
        """Проверяет, включён ли автозапуск."""
        await self.ensure_loaded()
        return self._cache.get('auto_backup_enabled', False)
    
    async def get_download_dir(self) -> str:
        """Возвращает директорию для скачивания."""
        await self.ensure_loaded()
        return self._cache.get('paths', {}).get('download_dir', 'downloads')
    
    async def get_compression_settings(self) -> dict:
        """Возвращает настройки сжатия."""
        await self.ensure_loaded()
        return self._cache.get('compression', self.DEFAULT_COMPRESSION.copy())
    
    async def get_queue_settings(self) -> dict:
        """Возвращает настройки очереди."""
        await self.ensure_loaded()
        return self._cache.get('queue', self.DEFAULT_QUEUE.copy())
    
    async def get_telegram_client_settings(self) -> dict:
        """Возвращает настройки Telegram клиента."""
        await self.ensure_loaded()
        return self._cache.get('telegram_client', self.DEFAULT_TELEGRAM_CLIENT.copy())
    
    async def get_upload_settings(self) -> dict:
        """Возвращает настройки загрузки."""
        await self.ensure_loaded()
        return self._cache.get('upload', self.DEFAULT_UPLOAD.copy())
    
    async def get_file_types(self) -> dict:
        """Возвращает типы файлов."""
        await self.ensure_loaded()
        return self._cache.get('file_types', self.DEFAULT_FILE_TYPES.copy())
    
    async def is_photo(self, filename: str) -> bool:
        """Проверяет, является ли файл фото."""
        await self.ensure_loaded()
        ext: str = os.path.splitext(filename)[1].lower()
        return ext in self._cache.get('file_types', {}).get('photo', [])
    
    async def is_video(self, filename: str) -> bool:
        """Проверяет, является ли файл видео."""
        await self.ensure_loaded()
        ext: str = os.path.splitext(filename)[1].lower()
        return ext in self._cache.get('file_types', {}).get('video', [])
    
    async def add_chat_id(self, chat_id: int) -> bool:
        """Добавляет ID чата."""
        await self.ensure_loaded()
        chat_ids: List[int] = self._cache.get('chat_ids', [])
        if chat_id not in chat_ids:
            chat_ids.append(chat_id)
            self._cache['chat_ids'] = chat_ids
            await self._save_key('chat_ids', chat_ids)
            return True
        return False
    
    async def remove_chat_id(self, chat_id: int) -> bool:
        """Удаляет ID чата."""
        await self.ensure_loaded()
        chat_ids: List[int] = self._cache.get('chat_ids', [])
        if chat_id in chat_ids:
            chat_ids.remove(chat_id)
            self._cache['chat_ids'] = chat_ids
            await self._save_key('chat_ids', chat_ids)
            return True
        return False
    
    async def delete_chat_completely(self, chat_id: int) -> bool:
        """Полностью удаляет чат и все связанные данные."""
        await self.ensure_loaded()
        chat_ids: List[int] = self._cache.get('chat_ids', [])
        if chat_id in chat_ids:
            chat_ids.remove(chat_id)
            self._cache['chat_ids'] = chat_ids
            await self._save_key('chat_ids', chat_ids)
        cursor: aiosqlite.Cursor = await self.execute("SELECT uploaded, compressed, saved_bytes, total_bytes FROM chat_stats WHERE chat_id = ?", (chat_id,))
        row: Optional[aiosqlite.Row] = await cursor.fetchone()
        chat_uploaded: int
        chat_compressed: int
        chat_saved_bytes: int
        chat_total_bytes: int
        chat_uploaded, chat_compressed, chat_saved_bytes, chat_total_bytes = (row['uploaded'], row['compressed'], row['saved_bytes'], row['total_bytes']) if row else (0, 0, 0, 0)
        async with self._write_lock:
            for table in ['queue_items', 'files', 'topics', 'chat_names', 'chat_stats', 'scan_progress', 'history', 'file_errors']:
                try:
                    await self.execute(f"DELETE FROM {table} WHERE chat_id = ?", (chat_id,))
                except Exception as e:
                    logger.error(f"❌ Ошибка удаления из {table}: {e}")
            if chat_uploaded > 0:
                await self.execute("UPDATE stage_stats SET processed = MAX(0, processed - ?), success = MAX(0, success - ?), updated_at = ? WHERE stage = 'upload'", (chat_uploaded, chat_uploaded, time.time()))
            if chat_compressed > 0 or chat_saved_bytes > 0:
                await self.execute("UPDATE stage_stats SET processed = MAX(0, processed - ?), saved_bytes = MAX(0, saved_bytes - ?), updated_at = ? WHERE stage = 'compress'", (chat_compressed, chat_saved_bytes, time.time()))
            if chat_total_bytes > 0:
                await self.execute("UPDATE stage_stats SET processed = MAX(0, processed - ?), updated_at = ? WHERE stage = 'download'", (chat_uploaded + chat_compressed, time.time()))
            await self.commit()
        cursor = await self.execute("SELECT COUNT(*) FROM chat_names")
        if (await cursor.fetchone())[0] == 0:
            await self.set_app_state('selected_snapshot', 0)
        logger.info(f"🗑️ Чат {chat_id} полностью удалён")
        return True
    
    async def reset_all_stats(self) -> bool:
        """Сбрасывает всю статистику."""
        await self.ensure_loaded()
        self._cache['chat_ids'] = []
        await self._save_key('chat_ids', [])
        async with self._write_lock:
            for table in ['chat_names', 'topics', 'files', 'chat_stats', 'scan_progress', 'history', 'file_errors', 'system_errors', 'stage_stats', 'queue_items', 'queue_processing', 'queue_retry', 'active_progress']:
                await self.execute(f"DELETE FROM {table}")
            await self.set_app_state('selected_snapshot', 0)
            await self.commit()
        logger.info("🗑️ Вся статистика сброшена")
        return True
    
    async def add_window(self, start: str, end: str) -> bool:
        """Добавляет окно работы."""
        await self.ensure_loaded()
        windows: List[dict] = self._cache.get('windows', [])
        if any(w['start'] == start and w['end'] == end for w in windows):
            return False
        windows.append({'start': start, 'end': end})
        self._cache['windows'] = windows
        await self._save_key('windows', windows)
        return True
    
    async def remove_window(self, index: int) -> bool:
        """Удаляет окно работы по индексу."""
        await self.ensure_loaded()
        windows: List[dict] = self._cache.get('windows', [])
        if 0 <= index < len(windows):
            windows.pop(index)
            self._cache['windows'] = windows
            await self._save_key('windows', windows)
            return True
        return False
    
    async def clear_windows(self) -> None:
        """Очищает все окна работы."""
        await self.ensure_loaded()
        self._cache['windows'] = []
        await self._save_key('windows', [])
    
    async def set_auto_enabled(self, enabled: bool) -> None:
        """Включает/выключает автозапуск."""
        await self.ensure_loaded()
        self._cache['auto_backup_enabled'] = enabled
        await self._save_key('auto_backup_enabled', enabled)
    
    async def should_run_now(self) -> bool:
        """Проверяет, нужно ли запускать бэкап сейчас."""
        await self.ensure_loaded()
        if not self._cache.get('auto_backup_enabled', False):
            return False
        windows: List[dict] = self._cache.get('windows', [])
        if not windows:
            return True
        now: time.struct_time = time.localtime()
        current: int = now.tm_hour * 60 + now.tm_min
        for w in windows:
            start: int = self._time_to_minutes(w['start'])
            end: int = self._time_to_minutes(w['end'])
            if start <= end:
                if start <= current < end:
                    return True
            elif current >= start or current < end:
                return True
        return False
    
    @staticmethod
    def _time_to_minutes(time_str: str) -> int:
        """Переводит время в минуты."""
        h, m = map(int, time_str.split(':'))
        return h * 60 + m
    
    async def get_app_state(self, key: str, default: Any = None) -> Any:
        """Возвращает состояние приложения."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT value FROM app_state WHERE key = ?", (key,))
        row: Optional[aiosqlite.Row] = await cursor.fetchone()
        if row:
            try:
                return json.loads(row['value'])
            except Exception:
                return default
        return default
    
    async def set_app_state(self, key: str, value: Any) -> None:
        """Устанавливает состояние приложения."""
        async def op(db_conn):
            await db_conn.execute("INSERT OR REPLACE INTO app_state (key, value, updated_at) VALUES (?, ?, ?)", (key, json.dumps(value, ensure_ascii=False), time.time()))
        await self._with_transaction(op)
    
    async def get_chat_topics_status(self, chat_id: int) -> List[dict]:
        """Возвращает статус тем чата."""
        topics: List[dict] = await self.get_topics(chat_id)
        result: List[dict] = []
        for t in topics:
            tid: int = t['topic_id']
            files: List[dict] = await self.get_files(chat_id, tid)
            if not files:
                continue
            total: int = len(files)
            uploaded: int = sum(1 for f in files if f['state'] == STATE_UPLOADED)
            new_count: int = sum(1 for f in files if f['state'] == STATE_NEW)
            unloaded: int = sum(1 for f in files if f['state'] == STATE_UNLOADED)
            errors: int = sum(1 for f in files if f['state'] == STATE_ERROR)
            selected: int = sum(1 for f in files if f['state'] == STATE_SELECTED)
            result.append({'topic_id': tid, 'topic_name': t['topic_name'], 'total': total, 'uploaded': uploaded, 'new': new_count, 'unloaded': unloaded, 'errors': errors, 'selected': selected, 'pending': total - uploaded - sum(1 for f in files if f['state'] == STATE_SKIPPED), 'is_selected': t['is_selected'], 'is_fully_uploaded': uploaded == total})
        return result
    
    async def generate_bot_status(self) -> dict:
        """Генерирует полный статус для бота (облегчённый)."""
        await self.ensure_loaded()
        all_stats: dict = await self.get_all_stats()
        chat_stats: dict = await self.get_chat_stats()
        stage_stats: dict = await self.get_stage_stats()
        file_errors_count: int
        system_errors_count: int
        file_errors_count, system_errors_count = await self.get_error_counts()
        pending: int = await self.count_pending()
        queue_counts: Dict[str, int] = await self.get_queue_counts()
        pending_by_stage: Dict[str, int] = {
            'check': queue_counts.get(STATUS_PENDING_CHECK, 0),
            'download': queue_counts.get(STATUS_PENDING_DOWNLOAD, 0),
            'compress': queue_counts.get(STATUS_PENDING_COMPRESS, 0),
            'upload': queue_counts.get(STATUS_PENDING_UPLOAD, 0),
        }
        active_items: List[dict] = await self.get_active_items()
        now_ts = time.time()
        # Проверить, жив ли main.py (PID-файл)
        import os as _os
        _pid_file = Path('backup.pid')
        _main_alive = False
        if _pid_file.exists():
            try:
                with open(_pid_file) as _f:
                    _os.kill(int(_f.read().strip()), 0)
                _main_alive = True
            except (OSError, ValueError):
                pass
        paused = False
        if _main_alive and active_items:
            max_updated = 0.0
            for item in active_items:
                item_upd = item.get('qp_updated_at') or item.get('ap_updated_at') or item.get('updated_at') or 0
                max_updated = max(max_updated, item_upd)
            if max_updated > 0 and now_ts - max_updated > 300:
                paused = True
        selected_snapshot: Any = await self.get_app_state('selected_snapshot', 0)
        chat_ids: List[int] = self._cache.get('chat_ids', [])
        windows: List[dict] = self._cache.get('windows', [])
        auto_enabled: bool = self._cache.get('auto_backup_enabled', False)
        chat_names: Dict[str, str] = {str(cid): await self.get_chat_name(cid) for cid in chat_ids}
        topics_status: Dict[str, List[dict]] = {str(cid): await self.get_chat_topics_status(cid) for cid in chat_ids}
        active_files: List[dict] = []
        downloading: List[dict] = []
        compressing: List[dict] = []
        uploading: List[dict] = []
        for item in active_items:
            worker_type: str = item.get('worker_type', '')
            active_item: dict = {'filename': item['filename'], 'message_id': item['message_id'], 'chat_id': item['chat_id'], 'stage': worker_type, 'size': item.get('file_size', 0), 'topic_id': item.get('topic_id'), 'progress': item.get('progress'), 'speed': item.get('speed'), 'eta': item.get('eta'), 'downloaded': item.get('downloaded'), 'uploaded': item.get('uploaded'), 'total_size': item.get('total_size')}
            active_files.append(active_item)
            if worker_type == 'download':
                downloading.append(active_item)
            elif worker_type in ('compress_photo', 'compress_video'):
                compressing.append(active_item)
            elif worker_type == 'upload':
                uploading.append(active_item)
        chat_summary: Dict[str, dict] = {}
        for cid in chat_ids:
            cid_str: str = str(cid)
            topics: List[dict] = topics_status.get(cid_str, [])
            stats: dict = chat_stats.get(cid_str, {})
            chat_summary[cid_str] = {'selected': sum(t['selected'] for t in topics), 'new': sum(t['new'] for t in topics), 'unloaded': sum(t['unloaded'] for t in topics if not t['is_fully_uploaded']), 'errors': sum(t['errors'] for t in topics), 'pending': sum(t['pending'] for t in topics), 'compressed': stats.get('compressed', 0), 'saved_bytes': stats.get('saved_bytes', 0)}
        compress_speed = 0.0
        for item in active_items:
            if item.get('worker_type') in ('compress_photo', 'compress_video'):
                s = item.get('speed')
                if s and s > 0:
                    compress_speed = max(compress_speed, s)
        
        session_stats = await self.get_app_state('session_stats', {})
        session_uploaded = session_stats.get('uploaded', 0)
        session_downloaded = session_stats.get('downloaded', 0)
        session_compressed = session_stats.get('compressed', 0)
        session_checked = session_stats.get('checked', 0)
        session_skipped = session_stats.get('skipped', 0)
        
        return {'running': False, 'paused': paused, 'start_time': await self.get_app_state('start_time', time.time()), 'summary': {'total_files': all_stats['total_files'], 'uploaded': session_uploaded, 'downloaded': session_downloaded, 'compressed': session_compressed, 'checked': session_checked, 'skipped': session_skipped, 'total_uploaded': all_stats['uploaded'], 'total_skipped': all_stats['skipped_files'], 'total_compressed': stage_stats.get('compress', {}).get('processed', 0), 'saved_bytes': stage_stats.get('compress', {}).get('saved_bytes', 0), 'file_errors': file_errors_count, 'system_errors': system_errors_count, 'new_files': all_stats['new_files'], 'unloaded_files': all_stats['unloaded_files'], 'error_files': all_stats['error_files'], 'uploaded_bytes': sum(s.get('uploaded_bytes', 0) for s in chat_stats.values()), 'pending': pending, 'pending_by_stage': pending_by_stage, 'queue_counts': queue_counts, 'selected_snapshot': selected_snapshot, 'compress_speed': compress_speed}, 'chat_stats': chat_stats, 'stage_stats': stage_stats, 'topics_status': topics_status, 'chat_names': chat_names, 'chat_ids': chat_ids, 'windows': windows, 'auto_enabled': auto_enabled, 'active_files': active_files, 'downloading': downloading, 'compressing': compressing, 'uploading': uploading, 'chat_summary': chat_summary, 'timestamp': time.time()}
    
    async def has_selected_with_pending(self, chat_ids: Optional[List[int]] = None) -> bool:
        """Проверяет, есть ли выбранные файлы для обработки."""
        if chat_ids:
            placeholders: str = ','.join('?' * len(chat_ids))
            cursor: aiosqlite.Cursor = await self.execute(f"""SELECT COUNT(*) FROM files f JOIN topics t ON f.chat_id = t.chat_id AND f.topic_id = t.topic_id WHERE t.is_selected = 1 AND f.state NOT IN (?, ?) AND f.chat_id IN ({placeholders})""", (STATE_UPLOADED, STATE_SKIPPED, *chat_ids))
        else:
            cursor = await self.execute("""SELECT COUNT(*) FROM files f JOIN topics t ON f.chat_id = t.chat_id AND f.topic_id = t.topic_id WHERE t.is_selected = 1 AND f.state NOT IN (?, ?)""", (STATE_UPLOADED, STATE_SKIPPED))
        row: aiosqlite.Row = await cursor.fetchone()
        return (row[0] or 0) > 0
    
    async def get_retry_items(self) -> List[dict]:
        """Возвращает элементы для повторной попытки."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT key FROM queue_retry WHERE retry_at <= ?", (time.time(),))
        keys: List[str] = [r['key'] for r in await cursor.fetchall()]
        items: List[dict] = []
        for key in keys:
            item: Optional[dict] = await self.get_queue_item(key)
            if item:
                items.append(item)
            await self.execute("DELETE FROM queue_retry WHERE key = ?", (key,))
        if keys:
            await self.commit()
        return items
    
    async def mark_file_failed(self, chat_id: int, message_id: int, error: str, max_attempts: int = 3) -> bool:
        """Помечает файл как ошибочный."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT attempts FROM files WHERE chat_id = ? AND message_id = ?", (chat_id, message_id))
        row: Optional[aiosqlite.Row] = await cursor.fetchone()
        if not row:
            return False
        attempts: int = (row['attempts'] or 0) + 1
        if attempts >= max_attempts:
            await self.execute("UPDATE files SET state = ?, attempts = ?, last_error = ? WHERE chat_id = ? AND message_id = ?", (STATE_ERROR, attempts, error[:200], chat_id, message_id))
        else:
            await self.execute("UPDATE files SET attempts = ?, last_error = ? WHERE chat_id = ? AND message_id = ?", (attempts, error[:200], chat_id, message_id))
        await self.commit()
        return True
    
    async def get_selected_topics(self, chat_id: int) -> List[int]:
        """Возвращает ID выбранных тем."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT topic_id FROM topics WHERE chat_id = ? AND is_selected = 1", (chat_id,))
        return [r['topic_id'] for r in await cursor.fetchall()]
    
    async def get_progress(self, key: str) -> Optional[dict]:
        """Возвращает прогресс элемента."""
        cursor: aiosqlite.Cursor = await self.execute("SELECT * FROM active_progress WHERE key = ?", (key,))
        row: Optional[aiosqlite.Row] = await cursor.fetchone()
        return dict(row) if row else None


_db_manager: Optional[DatabaseManager] = None
_db_lock: asyncio.Lock = asyncio.Lock()
_db_initialized: bool = False


async def get_db() -> DatabaseManager:
    """Возвращает глобальный экземпляр DatabaseManager."""
    global _db_manager, _db_initialized
    if _db_manager is not None and _db_initialized:
        return _db_manager
    async with _db_lock:
        if _db_manager is None:
            _db_manager = DatabaseManager()
            await _db_manager.init()
            _db_initialized = True
            logger.info("🆕 Создан глобальный экземпляр DatabaseManager")
        return _db_manager


get_settings = get_db

__all__ = ['DatabaseManager', 'get_db', 'get_settings', 'STATE_NEW', 'STATE_SELECTED', 'STATE_UPLOADED', 'STATE_SKIPPED', 'STATE_ERROR', 'STATE_UNLOADED', 'STATUS_PENDING_CHECK', 'STATUS_PENDING_DOWNLOAD', 'STATUS_PENDING_COMPRESS', 'STATUS_PENDING_UPLOAD', 'STATUS_COMPLETED', 'STATUS_FAILED', 'fmt_size', 'build_local_path', 'build_compressed_path', 'calculate_md5', 'is_valid_image', 'is_valid_video']