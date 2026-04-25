#!/usr/bin/env python3
"""
Telegram Bot для управления Backup
ВЕРСИЯ 0.17.18 — ИСПРАВЛЕНИЯ: УВЕДОМЛЕНИЕ ПРИ ОТКЛЮЧЕНИИ WATCHER
"""

__version__ = "0.17.18"

import os
import sys
import asyncio
import signal
import time
import logging
import logging.handlers
import subprocess
import hashlib
import re
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any, Set, Callable
from datetime import datetime
from dataclasses import dataclass, field

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest, RetryAfter
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes
)

from dotenv import load_dotenv

from database import (
    DatabaseManager, get_db,
    STATE_NEW, STATE_SELECTED, STATE_UPLOADED, STATE_SKIPPED, STATE_ERROR, STATE_UNLOADED,
    STATUS_PENDING_CHECK, STATUS_PENDING_DOWNLOAD, STATUS_PENDING_COMPRESS,
    STATUS_PENDING_UPLOAD, STATUS_COMPLETED, STATUS_FAILED,
    fmt_size
)

sys.path.insert(0, str(Path(__file__).parent))
load_dotenv()

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
ALLOWED_USERS: List[int] = [int(uid.strip()) for uid in os.getenv("ALLOWED_USERS", "").split(",") if uid.strip()]

if not ALLOWED_USERS or not BOT_TOKEN:
    print("❌ Ошибка: ALLOWED_USERS или BOT_TOKEN не заданы")
    sys.exit(1)

PID_FILE: str = "backup.pid"

os.makedirs("logs", exist_ok=True)
file_handler: logging.handlers.RotatingFileHandler = logging.handlers.RotatingFileHandler(
    "logs/bot.log", maxBytes=20*1024*1024, backupCount=3, encoding='utf-8'
)
file_handler.setLevel(logging.DEBUG)
console_handler: logging.StreamHandler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
formatter: logging.Formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])

for noisy in ("yadisk", "asyncio", "urllib3", "httpcore", "httpx", "aiosqlite"):
    logging.getLogger(noisy).setLevel(logging.ERROR)

logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext.Application").setLevel(logging.WARNING)
logging.getLogger("telegram.ext.ExtBot").setLevel(logging.WARNING)

class CancelledErrorFilter(logging.Filter):
    """Фильтр для подавления логов CancelledError."""
    def filter(self, record: logging.LogRecord) -> bool:
        msg: str = record.getMessage()
        if "CancelledError" in msg or "cancelled" in msg.lower():
            return False
        if "Fetching updates was aborted" in msg:
            return False
        return True

logging.getLogger("telegram.ext.Application").addFilter(CancelledErrorFilter())
logging.getLogger("telegram.ext.Application").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)
logger.info(f"📋 Разрешённые пользователи: {ALLOWED_USERS}")


class C:
    """Константы бота."""
    ITEMS_PER_PAGE: int = 10
    MAX_MSG_LEN: int = 4000
    PROGRESS_BAR_WIDTH: int = 13
    UPDATE_INTERVAL: float = 2.0
    LOG_LINES_PER_PAGE: int = 30
    MAX_ACTIVE_FILES_TOTAL: int = 10
    MAX_HISTORY_ITEMS: int = 10
    MAX_QUEUE_ITEMS: int = 20
    FILE_EMOJI: Dict[str, str] = {'photo': '📸', 'video': '🎬', 'audio': '🎵', 'document': '📄', 'archive': '🗜️', 'other': '📎'}

TYPE_NAMES: Dict[str, str] = {'all': 'Все', 'photo': 'Фото', 'video': 'Видео', 'audio': 'Аудио', 'document': 'Документы', 'archive': 'Архивы'}
STATUS_NAMES: Dict[str, str] = {'all': 'Все', 'uploaded': 'Скачано', 'unuploaded': 'Не скачано', 'new': 'Новые', 'skipped': 'Пропущено', 'error': 'Ошибки'}

_bot_status_cache: Optional[dict] = None
_bot_status_cache_time: float = 0
BOT_STATUS_CACHE_TTL: float = 1.0


async def get_cached_bot_status(db: DatabaseManager) -> dict:
    """Возвращает кэшированный статус для бота."""
    global _bot_status_cache, _bot_status_cache_time
    now: float = time.time()
    if _bot_status_cache is not None and now - _bot_status_cache_time < BOT_STATUS_CACHE_TTL:
        return _bot_status_cache
    _bot_status_cache = await db.generate_bot_status()
    _bot_status_cache_time = now
    return _bot_status_cache


def invalidate_bot_status_cache() -> None:
    """Инвалидирует кэш статуса бота."""
    global _bot_status_cache, _bot_status_cache_time
    _bot_status_cache = None
    _bot_status_cache_time = 0


def is_backup_running() -> bool:
    """Проверяет, запущен ли процесс бэкапа."""
    pid_file: Path = Path(PID_FILE)
    if not pid_file.exists():
        return False
    try:
        with open(pid_file, 'r') as f:
            os.kill(int(f.read().strip()), 0)
        return True
    except (OSError, ValueError, ProcessLookupError):
        pid_file.unlink(missing_ok=True)
        return False


def get_heartbeat_age() -> float:
    """Возвращает возраст последнего heartbeat."""
    try:
        import sqlite3
        conn: sqlite3.Connection = sqlite3.connect("backup.db")
        cursor: sqlite3.Cursor = conn.execute("SELECT MAX(updated_at) FROM queue_processing WHERE started_at > ?", (time.time() - 3600,))
        row: Optional[tuple] = cursor.fetchone()
        conn.close()
        if row and row[0]:
            age: float = time.time() - row[0]
            return min(age, 999)
    except Exception:
        pass
    return 0


@dataclass
class BotState:
    """Состояние бота."""
    _msg: Dict[int, int] = field(default_factory=dict)
    _menu: Dict[int, str] = field(default_factory=dict)
    _files_state: Dict[str, dict] = field(default_factory=dict)
    _watcher_tasks: Dict[str, asyncio.Task] = field(default_factory=dict)
    _edit_in_progress: Dict[int, bool] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    
    async def is_editing(self, uid: int) -> bool:
        async with self._lock:
            return self._edit_in_progress.get(uid, False)
    
    async def set_editing(self, uid: int, value: bool) -> None:
        async with self._lock:
            self._edit_in_progress[uid] = value
    
    async def get_msg(self, uid: int) -> Optional[int]:
        """Возвращает ID сообщения для пользователя."""
        async with self._lock:
            return self._msg.get(uid)
    
    async def set_msg(self, uid: int, msg_id: int) -> None:
        """Сохраняет ID сообщения для пользователя."""
        async with self._lock:
            self._msg[uid] = msg_id
    
    async def pop_msg(self, uid: int) -> Optional[int]:
        """Удаляет и возвращает ID сообщения."""
        async with self._lock:
            return self._msg.pop(uid, None)
    
    async def get_menu(self, uid: int) -> Optional[str]:
        """Возвращает текущее меню пользователя."""
        async with self._lock:
            return self._menu.get(uid)
    
    async def set_menu(self, uid: int, menu: str) -> None:
        """Сохраняет текущее меню пользователя."""
        async with self._lock:
            self._menu[uid] = menu
    
    async def get_files_state(self, key: str) -> dict:
        """Возвращает состояние файлового меню."""
        async with self._lock:
            return self._files_state.get(key, {}).copy()
    
    async def set_files_state(self, key: str, state: dict) -> None:
        """Сохраняет состояние файлового меню."""
        async with self._lock:
            self._files_state[key] = state
    
    async def get_watcher_task(self, key: str) -> Optional[asyncio.Task]:
        """Возвращает задачу watcher."""
        async with self._lock:
            return self._watcher_tasks.get(key)
    
    async def set_watcher_task(self, key: str, task: asyncio.Task) -> None:
        """Сохраняет задачу watcher."""
        async with self._lock:
            self._watcher_tasks[key] = task
    
    async def pop_watcher_task(self, key: str) -> Optional[asyncio.Task]:
        """Удаляет и возвращает задачу watcher."""
        async with self._lock:
            return self._watcher_tasks.pop(key, None)
    
    async def cancel_watcher_task(self, key: str) -> bool:
        """Отменяет задачу watcher."""
        async with self._lock:
            task: Optional[asyncio.Task] = self._watcher_tasks.pop(key, None)
            if task and not task.done():
                task.cancel()
                return True
            return False
    
    async def clear_watcher_tasks(self) -> None:
        """Отменяет все задачи watcher."""
        async with self._lock:
            for task in self._watcher_tasks.values():
                if not task.done():
                    task.cancel()
            self._watcher_tasks.clear()


_bot_state: BotState = BotState()


class FloodControl:
    """Простейший контроль флуда."""
    def __init__(self) -> None:
        self._wait_until: float = 0
        self._lock: asyncio.Lock = asyncio.Lock()
    
    async def wait(self) -> None:
        """Ожидает, если активен флуд-контроль."""
        async with self._lock:
            now: float = time.time()
            if self._wait_until > now:
                await asyncio.sleep(self._wait_until - now)
                self._wait_until = 0
    
    def handle_retry_after(self, e: RetryAfter) -> None:
        """Обрабатывает RetryAfter."""
        self._wait_until = time.time() + e.retry_after + 1
        logger.warning(f"🌊 RetryAfter: ждём {e.retry_after}с")


_flood: FloodControl = FloodControl()


async def _telegram_request(bot: Any, uid: int, action: str, text: str, msg_id: Optional[int],
                            kb: Optional[InlineKeyboardMarkup], parse_mode: str, timeout: float) -> Optional[int]:
    """Выполняет запрос к Telegram API с повторными попытками."""
    for attempt in range(3):
        try:
            await _flood.wait()
            if action == 'send':
                msg: Any = await asyncio.wait_for(
                    bot.send_message(chat_id=uid, text=text, reply_markup=kb, parse_mode=parse_mode),
                    timeout=timeout)
                return msg.message_id
            else:
                await asyncio.wait_for(
                    bot.edit_message_text(chat_id=uid, message_id=msg_id, text=text,
                                          reply_markup=kb, parse_mode=parse_mode),
                    timeout=timeout)
                return msg_id
        except asyncio.TimeoutError:
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
        except RetryAfter as e:
            _flood.handle_retry_after(e)
            if e.retry_after < 60:
                continue
            return 0
        except BadRequest as e:
            if "Message is not modified" in str(e):
                return msg_id if action == 'edit' else 0
            if action == 'edit' and "message to edit not found" in str(e):
                await _bot_state.pop_msg(uid)
                return await _telegram_request(bot, uid, 'send', text, None, kb, parse_mode, timeout)
            logger.error(f"❌ BadRequest: {str(e)[:100]}")
            return 0
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
            else:
                logger.error(f"❌ Ошибка: {e}")
    return 0


async def safe_edit_or_send(bot: Any, uid: int, edit_id: Optional[int], text: str,
                            kb: Optional[InlineKeyboardMarkup] = None, parse_mode: str = 'HTML') -> Optional[int]:
    """Безопасно редактирует или отправляет новое сообщение."""
    if edit_id:
        result: Optional[int] = await _telegram_request(bot, uid, 'edit', text, edit_id, kb, parse_mode, 10.0)
        if result:
            return result
    result = await _telegram_request(bot, uid, 'send', text, None, kb, parse_mode, 10.0)
    if result:
        await _bot_state.set_msg(uid, result)
    return result


async def safe_edit(bot: Any, uid: int, msg_id: int, text: str, kb: Optional[InlineKeyboardMarkup] = None) -> bool:
    """Безопасно редактирует сообщение."""
    return await _telegram_request(bot, uid, 'edit', text, msg_id, kb, 'HTML', 10.0) is not None


async def send_temp_message(bot: Any, uid: int, text: str, delay: int = 3, kb: Optional[InlineKeyboardMarkup] = None) -> None:
    """Отправляет временное сообщение."""
    msg_id: Optional[int] = await _telegram_request(bot, uid, 'send', text, None, kb, 'HTML', 10.0)
    if msg_id:
        asyncio.create_task(_delete_later(bot, uid, msg_id, delay))


async def _delete_later(bot: Any, uid: int, msg_id: int, delay: int) -> None:
    """Удаляет сообщение через задержку."""
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id=uid, message_id=msg_id)
    except Exception:
        pass


class Keyboard:
    """Фабрика клавиатур."""
    
    @staticmethod
    def _btn(text: str, callback: str) -> InlineKeyboardButton:
        """Создаёт кнопку."""
        return InlineKeyboardButton(text, callback_data=callback)
    
    @staticmethod
    def main(running: bool) -> InlineKeyboardMarkup:
        """Главное меню."""
        return InlineKeyboardMarkup([[
            Keyboard._btn("⏹️ Остановить" if running else "▶️ Запустить",
                          "backup_stop" if running else "backup_start")
        ], [
            Keyboard._btn("📊 Статистика", "stats")
        ], [
            Keyboard._btn("⚙️ Настройки", "settings")
        ]])
    
    @staticmethod
    def stats() -> InlineKeyboardMarkup:
        """Меню статистики."""
        return InlineKeyboardMarkup([
            [Keyboard._btn("📋 Лог", "logs"), Keyboard._btn("📋 Очередь", "queue")],
            [Keyboard._btn("⚠️ Ошибки", "errors_view"), Keyboard._btn("◀️ Назад", "main")]
        ])
    
    @staticmethod
    def settings() -> InlineKeyboardMarkup:
        """Меню настроек."""
        return InlineKeyboardMarkup([
            [Keyboard._btn("📁 Управление чатами", "chats")],
            [Keyboard._btn("🕐 Окна работы", "windows")],
            [Keyboard._btn("🛠️ Админ-панель", "admin")],
            [Keyboard._btn("◀️ Назад", "main")]
        ])
    
    @staticmethod
    def back(callback: str) -> InlineKeyboardMarkup:
        """Кнопка назад."""
        return InlineKeyboardMarkup([[Keyboard._btn("◀️ Назад", callback)]])
    
    @staticmethod
    def admin() -> InlineKeyboardMarkup:
        """Админ-панель."""
        return InlineKeyboardMarkup([
            [Keyboard._btn("💀 Завершить main.py", "force_kill")],
            [Keyboard._btn("🗑️ Очистить очередь", "clear_queue")],
            [Keyboard._btn("🧹 Очистить temp", "cleanup_temp")],
            [Keyboard._btn("🔄 Сбросить статистику", "reset_all_stats")],
            [Keyboard._btn("◀️ Назад", "settings")]
        ])
    
    @staticmethod
    def errors() -> InlineKeyboardMarkup:
        """Меню ошибок."""
        return InlineKeyboardMarkup([
            [Keyboard._btn("🗑️ Очистить ошибки", "errors_clear")],
            [Keyboard._btn("◀️ Назад", "main")]
        ])
    
    @staticmethod
    def windows(windows: List[Dict[str, str]], auto: bool) -> InlineKeyboardMarkup:
        """Меню окон работы."""
        buttons: List[List[InlineKeyboardButton]] = [[Keyboard._btn("➕ Добавить окно", "add_window")]]
        if windows:
            buttons.append([Keyboard._btn("❌ Удалить окно", "remove_window")])
            buttons.append([Keyboard._btn("🗑️ Очистить все окна", "clear_windows")])
        buttons.append([Keyboard._btn(f"🔄 Автозапуск: {'✅' if auto else '❌'}", "toggle_auto")])
        buttons.append([Keyboard._btn("◀️ Назад", "settings")])
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def logs(page: int, total_pages: int) -> InlineKeyboardMarkup:
        """Меню логов."""
        btns: List[InlineKeyboardButton] = [Keyboard._btn("🔄 Обновить", "logs_refresh")]
        if page < total_pages - 1:
            btns.append(Keyboard._btn("◀️ Старые", f"logs_page:{page+1}"))
        if page > 0:
            btns.append(Keyboard._btn("▶️ Новые", f"logs_page:{page-1}"))
        kb_rows: List[List[InlineKeyboardButton]] = [btns] if btns else []
        kb_rows.append([Keyboard._btn("◀️ Назад в меню", "main")])
        return InlineKeyboardMarkup(kb_rows)
    
    @staticmethod
    def topics_with_checkboxes(chat_id: int, topics: List[dict], page: int, total_pages: int) -> InlineKeyboardMarkup:
        """Меню тем с чекбоксами."""
        kb: List[List[InlineKeyboardButton]] = []
        for t in topics:
            emoji: str = "✅" if t.get('is_full') else ("🔘" if t.get('is_partial') else "⬜")
            kb.append([Keyboard._btn(f"{emoji} 📂 {t['name']} ({t.get('selected', 0)}/{t.get('total', 0)})",
                                     f"topic_menu:{chat_id}:{t['id']}")])
        nav: List[InlineKeyboardButton] = []
        if total_pages > 1:
            if page > 0:
                nav.append(Keyboard._btn(f"◀️ Стр. {page}/{total_pages}", f"topics_page:{chat_id}:{page-1}"))
            if page < total_pages - 1:
                nav.append(Keyboard._btn(f"▶️ Стр. {page+2}/{total_pages}", f"topics_page:{chat_id}:{page+1}"))
        if nav:
            kb.append(nav)
        kb.append([Keyboard._btn("✅ Выбрать все темы", f"topics_select_all:{chat_id}"),
                   Keyboard._btn("⬜ Снять все", f"topics_select_none:{chat_id}")])
        kb.append([Keyboard._btn("◀️ Назад", f"chat_manage:{chat_id}")])
        return InlineKeyboardMarkup(kb)
    
    @staticmethod
    def files_with_filters(chat_id: int, topic_id: str, files: List[dict], selected_ids: Set[int],
                           page: int, total_pages: int, type_filter: str, status_filter: str,
                           sort_by: str, sort_order: str) -> InlineKeyboardMarkup:
        """Меню файлов с фильтрами."""
        kb: List[List[InlineKeyboardButton]] = []
        
        type_btns: List[InlineKeyboardButton] = []
        for ft, emoji in [('all', '📋'), ('photo', '📸'), ('video', '🎬'), ('audio', '🎵'), ('document', '📄'), ('archive', '🗜️')]:
            prefix: str = "✅" if type_filter == ft else ""
            type_btns.append(Keyboard._btn(f"{prefix}{emoji}", f"files_type_filter:{chat_id}:{topic_id}:{ft}:{status_filter}"))
        kb.append(type_btns)
        
        status_btns: List[InlineKeyboardButton] = []
        for sf, emoji in [('all', '📋'), ('unuploaded', '⬜'), ('uploaded', '📥'), ('new', '🆕'), ('skipped', '⏭️'), ('error', '❌')]:
            prefix = "✅" if status_filter == sf else ""
            status_btns.append(Keyboard._btn(f"{prefix}{emoji}", f"files_status_filter:{chat_id}:{topic_id}:{type_filter}:{sf}"))
        kb.append(status_btns)
        
        sort_btns: List[InlineKeyboardButton] = []
        for field, label in [('date', '📅 Дата'), ('name', '🔤 Имя'), ('size', '📦 Размер')]:
            order_text: str = "▲" if sort_by == field and sort_order == 'asc' else "▼"
            new_order: str = 'asc' if sort_by != field or sort_order == 'desc' else 'desc'
            sort_btns.append(Keyboard._btn(f"{label} {order_text}", 
                                           f"files_sort:{chat_id}:{topic_id}:{field}:{new_order}:{type_filter}:{status_filter}"))
        kb.append(sort_btns)
        
        per_page: int = 10
        start: int = page * per_page
        for f in files[start:start + per_page]:
            is_selected: bool = f['message_id'] in selected_ids
            emoji: str = '☑️' if is_selected else '⬜'
            file_type: str = f.get('file_type', f.get('type', 'other'))
            type_emoji: str = C.FILE_EMOJI.get(file_type, '📎')
            state: int = f.get('state', STATE_UNLOADED)
            status_emoji: str = '📥' if state == STATE_UPLOADED else ('🆕' if state == STATE_NEW else ('⏭️' if state == STATE_SKIPPED else ('❌' if state == STATE_ERROR else '⬜')))
            filename: str = f['filename'][:35] if 'filename' in f else 'unknown'
            size: int = f.get('size', f.get('file_size', 0))
            btn_text: str = f"{emoji} {status_emoji} {type_emoji} {filename} ({fmt_size(size)})"
            kb.append([Keyboard._btn(btn_text, f"file_toggle:{chat_id}:{topic_id}:{f['message_id']}:{type_filter}:{status_filter}:{page}")])
        
        nav = []
        if total_pages > 1:
            if page > 0:
                nav.append(Keyboard._btn(f"◀️ Стр. {page}/{total_pages}", 
                                         f"files_page:{chat_id}:{topic_id}:{page-1}:{type_filter}:{status_filter}:{sort_by}:{sort_order}"))
            if page < total_pages - 1:
                nav.append(Keyboard._btn(f"Стр. {page+2}/{total_pages} ▶️", 
                                         f"files_page:{chat_id}:{topic_id}:{page+1}:{type_filter}:{status_filter}:{sort_by}:{sort_order}"))
        if nav:
            kb.append(nav)
        
        kb.append([Keyboard._btn("✅ Выбрать всё", f"files_select_all:{chat_id}:{topic_id}:{type_filter}:{status_filter}"),
                   Keyboard._btn("⬜ Снять всё", f"files_select_none:{chat_id}:{topic_id}:{type_filter}:{status_filter}")])
        kb.append([Keyboard._btn("🆕 Выбрать новые", f"files_select_new:{chat_id}:{topic_id}:{type_filter}:{status_filter}"),
                   Keyboard._btn("⬜ Выбрать нескачанные", f"files_select_unuploaded:{chat_id}:{topic_id}:{type_filter}:{status_filter}")])
        
        if any(f.get('state') == STATE_ERROR for f in files):
            kb.append([Keyboard._btn("🔄 Сбросить ошибки", f"files_reset_errors:{chat_id}:{topic_id}:{type_filter}:{status_filter}")])
        
        kb.append([Keyboard._btn(f"💾 Сохранить ({len(selected_ids)}/{len(files)})", f"files_save:{chat_id}:{topic_id}"),
                   Keyboard._btn("◀️ Назад", f"files_topics:{chat_id}")])
        
        return InlineKeyboardMarkup(kb)


def fmt_bar(p: Optional[float]) -> str:
    """Форматирует прогресс-бар."""
    if p is None:
        p = 0.0
    try:
        p = float(p)
    except (ValueError, TypeError):
        p = 0.0
    p = max(0.0, min(100.0, p))
    width: int = C.PROGRESS_BAR_WIDTH
    filled: int = int(width * p / 100.0)
    filled = max(0, min(width, filled))
    empty: int = width - filled
    bar: str = '▰' * filled + '▱' * empty
    return f"[{bar}] {p:.0f}%"


_animation_counter: int = 0
_compress_speed_cache: Dict[str, Tuple[float, Optional[float]]] = {}
_last_animation_time: float = 0


def get_animated_header() -> str:
    """Возвращает анимированный заголовок."""
    global _animation_counter, _last_animation_time
    now: float = time.time()
    if now - _last_animation_time >= 2.0:
        _animation_counter += 1
        _last_animation_time = now
    return "🟢" if _animation_counter % 2 == 0 else "⚪"


def format_active_files(status: dict, context: str = 'main', max_items: Optional[int] = None) -> List[str]:
    """Форматирует список активных файлов."""
    if max_items is None:
        max_items = C.MAX_ACTIVE_FILES_TOTAL
    
    lines: List[str] = []
    downloading: List[dict] = status.get('downloading', [])
    compressing: List[dict] = status.get('compressing', [])
    uploading: List[dict] = status.get('uploading', [])
    
    total: int = len(downloading) + len(compressing) + len(uploading)
    if total == 0:
        return []
    
    shown_download: List[dict] = []
    shown_compress: List[dict] = []
    shown_upload: List[dict] = []
    remaining: int = max_items
    
    if downloading:
        take: int = min(len(downloading), max(1, remaining // 3))
        shown_download = downloading[:take]
        remaining -= len(shown_download)
    if compressing:
        take = min(len(compressing), max(1, remaining // 2 if uploading else remaining))
        shown_compress = compressing[:take]
        remaining -= len(shown_compress)
    if uploading:
        take = min(len(uploading), remaining)
        shown_upload = uploading[:take]
        remaining -= len(shown_upload)
    
    if remaining > 0:
        all_remaining: List[Tuple[str, dict]] = []
        if len(downloading) > len(shown_download):
            all_remaining.extend([('download', item) for item in downloading[len(shown_download):]])
        if len(compressing) > len(shown_compress):
            all_remaining.extend([('compress', item) for item in compressing[len(shown_compress):]])
        if len(uploading) > len(shown_upload):
            all_remaining.extend([('upload', item) for item in uploading[len(shown_upload):]])
        
        for op_type, item in all_remaining[:remaining]:
            if op_type == 'download':
                shown_download.append(item)
            elif op_type == 'compress':
                shown_compress.append(item)
            else:
                shown_upload.append(item)
    
    all_shown: List[Tuple[str, dict]] = []
    all_shown.extend([('download', item) for item in shown_download])
    all_shown.extend([('compress', item) for item in shown_compress])
    all_shown.extend([('upload', item) for item in shown_upload])
    
    hidden_download: int = len(downloading) - len(shown_download)
    hidden_compress: int = len(compressing) - len(shown_compress)
    hidden_upload: int = len(uploading) - len(shown_upload)
    
    for op_type, item in all_shown:
        filename: str = item.get('filename', 'unknown')[:40]
        size: int = item.get('size', 0)
        
        progress: Optional[float] = item.get('progress')
        if progress is not None:
            try:
                progress = float(progress)
                progress = max(0.0, min(100.0, progress))
            except (ValueError, TypeError):
                progress = 0.0
        else:
            progress = 0.0
        
        size_str: str = f"({fmt_size(size)})" if size else ""
        show_progress: bool = size > 10 * 1024 * 1024
        
        if op_type == 'download':
            lines.append(f"📥 {filename}  {size_str}")
            if context == 'main' and progress > 0 and show_progress:
                lines.append(f"   {fmt_bar(progress)}")
            elif context == 'stats' and progress > 0:
                lines[-1] = f"   {size_str} {progress:.0f}%"
                
        elif op_type == 'compress':
            speed: float = item.get('speed', 0)
            eta: Optional[float] = item.get('eta')
            cache_key: str = f"compress_speed_{item.get('filename', '')}"
            if (speed or 0) > 0:
                _compress_speed_cache[cache_key] = (speed, eta)
            elif cache_key in _compress_speed_cache:
                speed, eta = _compress_speed_cache[cache_key]
            
            lines.append(f"🗜️ {filename}  {size_str}")
            if context == 'main' and progress > 0 and show_progress:
                lines.append(f"   {fmt_bar(progress)}")
                if (speed or 0) > 0:
                    info: str = f"   x{speed:.1f}"
                    if eta:
                        info += f" | ETA {int(eta)}с"
                    lines.append(info)
            elif context == 'stats':
                if progress > 0:
                    lines[-1] = f"   {size_str} {progress:.0f}%"
                if (speed or 0) > 0:
                    info = f"   x{speed:.1f}"
                    if eta:
                        info += f" | ETA {int(eta)}с"
                    lines.append(info)
                    
        elif op_type == 'upload':
            lines.append(f"📤 {filename}  {size_str}")
            if context == 'main' and progress > 0 and show_progress:
                lines.append(f"   {fmt_bar(progress)}")
            elif context == 'stats' and progress > 0:
                lines[-1] = f"   {size_str} {progress:.0f}%"
    
    total_hidden: int = hidden_download + hidden_compress + hidden_upload
    if total_hidden > 0:
        lines.append(f"\n📋 И ещё {total_hidden} активных файлов...")
        if hidden_download:
            lines.append(f"   📥 скачивание: {hidden_download}")
        if hidden_compress:
            lines.append(f"   🗜️ сжатие: {hidden_compress}")
        if hidden_upload:
            lines.append(f"   📤 загрузка: {hidden_upload}")
    
    return lines


async def _format_menu(db: DatabaseManager, detailed: bool = False) -> str:
    """Форматирует главное меню или статистику."""
    status: dict = await get_cached_bot_status(db)
    is_running: bool = is_backup_running()
    age: float = get_heartbeat_age()
    
    if detailed:
        lines: List[str] = [f"📊 ДЕТАЛЬНАЯ СТАТИСТИКА {get_animated_header()}\n"]
    else:
        lines = [
            f"⚡ Telegram Backup {get_animated_header()} {age:.1f}с" if is_running else f"⚡ Telegram Backup {get_animated_header()}",
            "🟢 Выполняется" if is_running else "⚪ Не активен",
            ""
        ]
        active_lines: List[str] = format_active_files(status, 'main')
        if active_lines:
            lines.extend(active_lines)
            lines.append("")
    
    summary: dict = status['summary']
    
    lines.append(f"📄 Очередь: {summary.get('pending', 0)} / {summary.get('selected_snapshot', 0)}")
    line: str = f"✅ Скачано: {summary.get('uploaded', 0)}"
    if summary.get('skipped', 0) > 0:
        line += f" ⏭️ Пропущено: {summary.get('skipped', 0)}"
    lines.append(line)
    
    file_errors: int = summary.get('file_errors') or 0
    system_errors: int = summary.get('system_errors') or 0
    if file_errors > 0 or system_errors > 0:
        lines.append(f"❌ Ошибок: 📄{file_errors} / ⚙️{system_errors}")
    
    total_bytes: int = sum(s.get('total_bytes', 0) for s in status.get('chat_stats', {}).values())
    uploaded_bytes: int = summary.get('uploaded_bytes', 0)
    lines.append(f"💾 {fmt_size(uploaded_bytes)} / {fmt_size(total_bytes)}")
    
    if detailed:
        active_lines = format_active_files(status, 'stats')
        if active_lines:
            lines.append("\n⚡ АКТИВНЫЕ ФАЙЛЫ")
            lines.extend(active_lines)
        
        history: List[dict] = status.get('history', [])
        if history:
            lines.append("\n📋 ПОСЛЕДНИЕ ОПЕРАЦИИ")
            grouped: Dict[str, Dict[str, List[dict]]] = {}
            icons: Dict[str, str] = {'uploaded': '✅', 'downloaded': '📥', 'compressed': '🗜️', 'skipped': '⏭️', 'queued': '⏳', 'error': '❌'}
            
            for entry in history:
                chat: str = entry.get('chat_name', 'Неизвестный чат')
                topic: str = entry.get('topic_name', 'Общая тема')
                grouped.setdefault(chat, {}).setdefault(topic, []).append(entry)
            
            shown: int = 0
            for chat, topics in grouped.items():
                if shown >= C.MAX_HISTORY_ITEMS:
                    break
                lines.append(f"📁 {chat}")
                shown += 1
                for topic, entries in topics.items():
                    if shown >= C.MAX_HISTORY_ITEMS:
                        break
                    if topic != "Общая тема":
                        lines.append(f"   📂 {topic}")
                        shown += 1
                    take: int = min(len(entries), max(1, (C.MAX_HISTORY_ITEMS - shown) // max(1, len(topics))))
                    for entry in entries[:take]:
                        if shown >= C.MAX_HISTORY_ITEMS:
                            break
                        icon: str = icons.get(entry.get('status', ''), '📎')
                        filename: str = entry.get('filename', 'unknown')[:35]
                        size: int = entry.get('size', 0)
                        compressed: int = entry.get('compressed_size', 0)
                        if entry.get('status') == 'compressed' and compressed > 0:
                            size_str = f" {fmt_size(size)} → {fmt_size(compressed)}"
                        elif size > 0:
                            size_str = f" ({fmt_size(size)})"
                        else:
                            size_str = ""
                        lines.append(f"      {icon} {filename}{size_str}")
                        shown += 1
                    if len(entries) > take and shown < C.MAX_HISTORY_ITEMS:
                        lines.append(f"      ... и ещё {len(entries) - take}")
                        shown += 1
                if shown < C.MAX_HISTORY_ITEMS:
                    lines.append("")
                    shown += 1
    
    return "\n".join(lines)[:C.MAX_MSG_LEN]


async def format_main_menu(db: DatabaseManager) -> str:
    """Форматирует главное меню."""
    return await _format_menu(db, detailed=False)


async def format_stats_menu(db: DatabaseManager) -> str:
    """Форматирует меню статистики."""
    return await _format_menu(db, detailed=True)


async def watch_menu(uid: int, bot: Any, menu_name: str, format_func: Callable, kb_func: Callable) -> None:
    """Наблюдает за меню и обновляет при изменениях."""
    last_hash: Optional[str] = None
    errors: int = 0
    try:
        while True:
            await asyncio.sleep(C.UPDATE_INTERVAL)
            if await _bot_state.get_menu(uid) != menu_name:
                break
            if errors >= 5:
                logger.warning(f"⚠️ Watcher для {uid} остановлен после {errors} ошибок")
                break
            try:
                if await _bot_state.is_editing(uid):
                    continue
                    
                text: str = await format_func()
                text_hash: str = hashlib.md5(text.encode()).hexdigest()
                if text_hash != last_hash:
                    last_hash = text_hash
                    msg_id: Optional[int] = await _bot_state.get_msg(uid)
                    if msg_id:
                        kb: InlineKeyboardMarkup = kb_func()
                        await _bot_state.set_editing(uid, True)
                        try:
                            await safe_edit(bot, uid, msg_id, text, kb)
                            errors = 0
                        finally:
                            await _bot_state.set_editing(uid, False)
            except RetryAfter as e:
                wait = min(e.retry_after + 5, 120)
                logger.warning(f"🌊 Watcher flood: ждём {wait}с")
                await asyncio.sleep(wait)
                errors = 0
            except Exception:
                errors += 1
                await asyncio.sleep(min(errors * 5, 30))
    except asyncio.CancelledError:
        pass


async def monitor_scan_progress(uid: int, bot: Any, msg_id: int, chat_id: Optional[int] = None, chat_name: str = "") -> None:
    """Мониторит прогресс сканирования."""
    last_upd: float = 0
    start_time: float = time.time()
    timeout: int = 600
    no_data_timeout: int = 60
    last_data_time: float = time.time()
    scan_was_active: bool = False
    
    is_full_scan: bool = chat_id is None
    task_key: str = f"full_scan_{uid}" if is_full_scan else f"scan_{uid}_{chat_id}"
    title: str = "всех чатов" if is_full_scan else f"чата {chat_name}"
    
    renderer: 'MenuRenderer' = MenuRenderer(bot, uid)
    db: DatabaseManager = await get_db()
    
    await asyncio.sleep(2)
    
    try:
        while True:
            await asyncio.sleep(1)
            
            if time.time() - start_time > timeout:
                await safe_edit(bot, uid, msg_id, f"✅ <b>Сканирование {title} завершено</b> (по таймауту)")
                if is_full_scan:
                    await renderer.render('chats', msg_id)
                else:
                    await renderer.render('chat_manage', msg_id, chat_id=chat_id)
                break
            
            status: dict = await get_cached_bot_status(db)
            scan_progress: Dict[str, dict] = status.get('scan_progress', {})
            
            if is_full_scan:
                completed: bool = bool(scan_progress) and all(data.get('completed', False) for data in scan_progress.values())
                has_data: bool = bool(scan_progress)
            else:
                chat_data: dict = scan_progress.get(str(chat_id), {})
                completed = chat_data.get('completed', False)
                has_data = bool(chat_data)
            
            if completed:
                await asyncio.sleep(1)
                await safe_edit(bot, uid, msg_id, f"✅ <b>Сканирование {title} завершено!</b>\n\nСписок файлов обновлён.")
                await asyncio.sleep(2)
                if is_full_scan:
                    await renderer.render('chats', msg_id)
                else:
                    await renderer.render('chat_manage', msg_id, chat_id=chat_id)
                return
            
            if scan_was_active and not has_data:
                await asyncio.sleep(1)
                await safe_edit(bot, uid, msg_id, f"✅ <b>Сканирование {title} завершено!</b>\n\nСписок файлов обновлён.")
                await asyncio.sleep(2)
                if is_full_scan:
                    await renderer.render('chats', msg_id)
                else:
                    await renderer.render('chat_manage', msg_id, chat_id=chat_id)
                return
            
            if not has_data:
                if time.time() - last_data_time > no_data_timeout:
                    await safe_edit(bot, uid, msg_id, f"✅ <b>Сканирование {title} завершено</b> (таймаут ожидания данных)")
                    if is_full_scan:
                        await renderer.render('chats', msg_id)
                    else:
                        await renderer.render('chat_manage', msg_id, chat_id=chat_id)
                    break
                continue
            
            scan_was_active = True
            last_data_time = time.time()
            
            now: float = time.time()
            if now - last_upd >= 2:
                last_upd = now
                
                if not is_backup_running():
                    await asyncio.sleep(1)
                    total: int = sum(data.get('files_found', 0) for data in scan_progress.values()) if is_full_scan else chat_data.get('files_found', 0)
                    await send_temp_message(bot, uid, f"✅ <b>Сканирование {title} завершено!</b>\n\n📊 Найдено файлов: {total}", delay=2)
                    await asyncio.sleep(2)
                    if is_full_scan:
                        await renderer.render('chats', msg_id)
                    else:
                        await renderer.render('chat_manage', msg_id, chat_id=chat_id)
                    break
                
                if is_full_scan:
                    lines = [f"🔄 <b>Полное сканирование {title}</b>\n"]
                    for cid, data in scan_progress.items():
                        cname: str = data.get('chat_name', f'Чат {cid}')
                        percent: float = data.get('percent', 0)
                        files_found: int = data.get('files_found', 0)
                        if data.get('completed'):
                            bar: str = "▰" * C.PROGRESS_BAR_WIDTH
                            lines.append(f"   ✅ {cname[:30]}: {bar} 100%")
                        else:
                            bar = "▰" * int(C.PROGRESS_BAR_WIDTH * percent / 100) + "▱" * (C.PROGRESS_BAR_WIDTH - int(C.PROGRESS_BAR_WIDTH * percent / 100))
                            lines.append(f"   {cname[:30]}: {bar} {percent:.0f}%")
                        lines.append(f"      📊 {files_found} файлов")
                    text = "\n".join(lines)
                else:
                    percent = chat_data.get('percent', 0)
                    files_found = chat_data.get('files_found', 0)
                    current_topic: str = chat_data.get('current_topic', '')
                    bar = "▰" * int(C.PROGRESS_BAR_WIDTH * percent / 100) + "▱" * (C.PROGRESS_BAR_WIDTH - int(C.PROGRESS_BAR_WIDTH * percent / 100))
                    text = f"🔄 <b>Сканирование чата</b>\n📁 {chat_name}\n{bar} {percent:.0f}%\n📊 Найдено: {files_found} файлов"
                    if current_topic:
                        text += f"\n📂 Тема: {current_topic[:40]}"
                
                await safe_edit(bot, uid, msg_id, text)
                
    except asyncio.CancelledError:
        pass
    finally:
        await _bot_state.pop_watcher_task(task_key)


class MenuRenderer:
    """Универсальный рендерер меню."""
    
    def __init__(self, bot: Any, uid: int) -> None:
        """Инициализирует рендерер."""
        self.bot: Any = bot
        self.uid: int = uid
        self._watcher_task: Optional[asyncio.Task] = None
    
    async def _stop_watcher(self) -> None:
        """Останавливает watcher."""
        if self._watcher_task and not self._watcher_task.done():
            self._watcher_task.cancel()
            try:
                await self._watcher_task
            except asyncio.CancelledError:
                pass
            self._watcher_task = None
    
    async def render(self, menu_type: str, edit_id: Optional[int] = None, **kwargs) -> Optional[int]:
        """Рендерит меню указанного типа."""
        await self._stop_watcher()
        await _bot_state.set_menu(self.uid, menu_type)
        
        handlers: Dict[str, Callable] = {
            'main': self._render_main,
            'stats': self._render_stats,
            'queue': self._render_queue,
            'errors': self._render_errors,
            'logs': self._render_logs,
            'settings': self._render_settings,
            'admin': self._render_admin,
            'windows': self._render_windows,
            'chats': self._render_chats,
            'chat_manage': self._render_chat_manage,
            'files_topics': self._render_files_topics,
            'files': self._render_files,
        }
        
        handler: Optional[Callable] = handlers.get(menu_type)
        if not handler:
            return None
        
        text, kb = await handler(**kwargs)
        msg_id: Optional[int] = await safe_edit_or_send(self.bot, self.uid, edit_id, text, kb)
        
        if msg_id and menu_type in ('main', 'stats'):
            db: DatabaseManager = await get_db()
            format_func: Callable = format_main_menu if menu_type == 'main' else format_stats_menu
            kb_func: Callable = lambda: Keyboard.main(is_backup_running()) if menu_type == 'main' else Keyboard.stats()
            self._watcher_task = asyncio.create_task(watch_menu(self.uid, self.bot, menu_type, 
                                                                lambda: format_func(db), kb_func))
        return msg_id
    
    async def _render_main(self, **kwargs) -> Tuple[str, InlineKeyboardMarkup]:
        """Рендерит главное меню."""
        db: DatabaseManager = await get_db()
        return await format_main_menu(db), Keyboard.main(is_backup_running())
    
    async def _render_stats(self, **kwargs) -> Tuple[str, InlineKeyboardMarkup]:
        """Рендерит меню статистики."""
        db: DatabaseManager = await get_db()
        return await format_stats_menu(db), Keyboard.stats()
    
    async def _render_queue(self, **kwargs) -> Tuple[str, InlineKeyboardMarkup]:
        """Рендерит меню очереди."""
        db: DatabaseManager = await get_db()
        status: dict = await get_cached_bot_status(db)
        items: List[dict] = status.get('queue_items', [])
        chat_names: Dict[str, str] = status.get('chat_names', {})
        
        if not items:
            return "📋 ОЧЕРЕДЬ ПУСТА", Keyboard.back("stats")
        
        grouped: Dict[str, Dict[str, List[dict]]] = {}
        icons: Dict[str, str] = {'pending_check': '🔍', 'pending_download': '📥', 'pending_compress': '🎬', 'pending_upload': '📤'}
        for item in items:
            chat: str = chat_names.get(str(item['chat_id']), f"Chat {item['chat_id']}")
            topic: str = "Общая тема"
            if item.get('topic_id'):
                topic = f"Тема {item['topic_id']}"
            grouped.setdefault(chat, {}).setdefault(topic, []).append(item)
        
        lines: List[str] = ["📋 ОЧЕРЕДЬ ФАЙЛОВ"]
        shown: int = 0
        for chat, topics in grouped.items():
            if shown >= C.MAX_QUEUE_ITEMS:
                break
            lines.append(f"📁 {chat}")
            shown += 1
            for topic, files in topics.items():
                if shown >= C.MAX_QUEUE_ITEMS:
                    break
                if topic != "Общая тема":
                    lines.append(f"   📂 {topic}")
                    shown += 1
                take: int = min(len(files), max(1, (C.MAX_QUEUE_ITEMS - shown) // max(1, len(topics))))
                for f in files[:take]:
                    if shown >= C.MAX_QUEUE_ITEMS:
                        break
                    icon: str = icons.get(f.get('status', ''), '⏳')
                    size: str = f" ({fmt_size(f.get('file_size', 0))})" if f.get('file_size') else ""
                    lines.append(f"      {icon} {f['filename'][:45]}{size}")
                    shown += 1
                if len(files) > take and shown < C.MAX_QUEUE_ITEMS:
                    lines.append(f"      ... и ещё {len(files) - take}")
                    shown += 1
            if shown < C.MAX_QUEUE_ITEMS:
                lines.append("")
                shown += 1
        
        lines.append(f"📊 Всего в очереди: {status['summary'].get('pending', 0)} файлов")
        return "\n".join(lines)[:C.MAX_MSG_LEN], Keyboard.back("stats")
    
    async def _render_errors(self, **kwargs) -> Tuple[str, InlineKeyboardMarkup]:
        """Рендерит меню ошибок."""
        db: DatabaseManager = await get_db()
        status: dict = await get_cached_bot_status(db)
        all_errors: List[dict] = status.get('all_errors', [])
        
        file_errs: List[dict] = [e for e in all_errors if e.get('type') == 'file']
        sys_errs: List[dict] = [e for e in all_errors if e.get('type') == 'system']
        
        if not file_errs and not sys_errs:
            return "⚠️ Нет сохранённых ошибок", Keyboard.errors()
        
        lines: List[str] = ["⚠️ ПОСЛЕДНИЕ ОШИБКИ\n"]
        for err in file_errs[:20]:
            ts: str = datetime.fromtimestamp(err.get('timestamp', time.time())).strftime('%H:%M:%S')
            lines.append(f"📄 [{ts}] {err.get('chat_name', '')} / {err.get('topic_name', '')}")
            lines.append(f"   {err.get('filename', '')[:40]}")
            lines.append(f"   ❌ {err.get('error', '')[:100]}")
            lines.append("")
        for err in sys_errs[:10]:
            ts = datetime.fromtimestamp(err.get('timestamp', time.time())).strftime('%H:%M:%S')
            lines.append(f"⚙️ [{ts}] {err.get('component', '')}")
            lines.append(f"   ❌ {err.get('error', '')[:100]}")
            lines.append("")
        return "\n".join(lines)[:C.MAX_MSG_LEN], Keyboard.errors()
    
    async def _render_logs(self, page: int = 1, **kwargs) -> Tuple[str, InlineKeyboardMarkup]:
        """Рендерит меню логов."""
        log_file: Path = Path("logs/backup.log")
        try:
            if not log_file.exists():
                raise Exception("Файл не найден")
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                all_lines: List[str] = f.readlines()
            total: int = len(all_lines)
            if total == 0:
                raise Exception("Файл пуст")
            total_pages: int = (total + C.LOG_LINES_PER_PAGE - 1) // C.LOG_LINES_PER_PAGE
            page = max(1, min(page, total_pages))
            page_idx: int = total_pages - page
            start: int = page_idx * C.LOG_LINES_PER_PAGE
            end: int = min(start + C.LOG_LINES_PER_PAGE, total)
            lines: List[str] = [f"📋 ЛОГ (стр. {page}/{total_pages})\n", "```"]
            lines.extend([line.strip()[:147] + "..." if len(line.strip()) > 150 else line.strip() 
                         for line in all_lines[start:end]])
            lines.append("```")
            text: str = "\n".join(lines)[:C.MAX_MSG_LEN]
            kb: InlineKeyboardMarkup = Keyboard.logs(page, total_pages)
        except Exception:
            text = "📋 Лог пуст или файл не найден"
            kb = Keyboard.back("main")
        return text, kb
    
    async def _render_settings(self, **kwargs) -> Tuple[str, InlineKeyboardMarkup]:
        """Рендерит меню настроек."""
        return "⚙️ <b>НАСТРОЙКИ</b>\nВыберите раздел:", Keyboard.settings()
    
    async def _render_admin(self, **kwargs) -> Tuple[str, InlineKeyboardMarkup]:
        """Рендерит админ-панель."""
        return "🛠️ <b>АДМИН-ПАНЕЛЬ</b>\n⚠️ Осторожно!", Keyboard.admin()
    
    async def _render_windows(self, **kwargs) -> Tuple[str, InlineKeyboardMarkup]:
        """Рендерит меню окон работы."""
        db: DatabaseManager = await get_db()
        status: dict = await get_cached_bot_status(db)
        windows: List[Dict[str, str]] = status.get('windows', [])
        auto: bool = status.get('auto_enabled', False)
        
        lines: List[str] = [f"🕐 ОКНА РАБОТЫ\n", f"🤖 Автозапуск: {'✅ Вкл' if auto else '❌ Выкл'}\n"]
        if windows:
            lines.append("📋 Окна:")
            lines.extend([f"   • {w['start']} – {w['end']}" for w in windows])
        else:
            lines.append("❌ Окна не заданы")
        return "\n".join(lines), Keyboard.windows(windows, auto)
    
    async def _render_chats(self, **kwargs) -> Tuple[str, InlineKeyboardMarkup]:
        """Рендерит меню управления чатами."""
        db: DatabaseManager = await get_db()
        status: dict = await get_cached_bot_status(db)
        chat_ids: List[int] = status.get('chat_ids', [])
        chat_stats: Dict[str, dict] = status.get('chat_stats', {})
        chat_names: Dict[str, str] = status.get('chat_names', {})
        chat_summary: Dict[str, dict] = status.get('chat_summary', {})
        
        seen: Set[int] = set()
        chat_ids = [x for x in chat_ids if not (x in seen or seen.add(x))]
        
        if not chat_ids:
            kb: InlineKeyboardMarkup = InlineKeyboardMarkup([[Keyboard._btn("➕ Добавить чат", "add_chat")],
                                       [Keyboard._btn("◀️ Назад", "settings")]])
            return "📁 УПРАВЛЕНИЕ ЧАТАМИ\n\n❌ Нет добавленных чатов", kb
        
        lines: List[str] = ["📁 <b>УПРАВЛЕНИЕ ЧАТАМИ</b>"]
        for cid in chat_ids:
            cid_str: str = str(cid)
            name: str = chat_names.get(cid_str, f"Chat {cid}")
            stats: dict = chat_stats.get(cid_str, {})
            summary: dict = chat_summary.get(cid_str, {})
            
            total: int = stats.get('total', 0)
            uploaded: int = stats.get('uploaded', 0)
            skipped: int = stats.get('skipped', 0)
            errors: int = stats.get('errors', 0)
            total_bytes: int = stats.get('total_bytes', 0)
            uploaded_bytes: int = stats.get('uploaded_bytes', 0)
            
            selected: int = summary.get('selected', 0)
            new_count: int = summary.get('new', 0)
            unloaded: int = summary.get('unloaded', 0)
            compressed: int = summary.get('compressed', 0)
            saved_bytes: int = summary.get('saved_bytes', 0)
            
            line: str = f"\n📁 <b>{name}</b>\n📤 {uploaded}/{total}"
            if skipped > 0:
                line += f" ⏭️{skipped}"
            if selected > 0:
                line += f" ☑️{selected}"
            if new_count > 0:
                line += f" 🆕{new_count}"
            if unloaded > 0:
                line += f" ⬜{unloaded}"
            if errors > 0:
                line += f" ❌{errors}"
            line += f"\n💾 {fmt_size(uploaded_bytes)}/{fmt_size(total_bytes)}"
            if compressed > 0:
                line += f" 🗜️{compressed}/{fmt_size(saved_bytes)}"
            lines.append(line)
        
        kb = [[Keyboard._btn(f"📁 {chat_names.get(str(cid), f'Chat {cid}')}", f"chat_manage:{cid}")] 
              for cid in chat_ids]
        kb.append([Keyboard._btn("🔄 Обновить кэш", "refresh_all_cache")])
        kb.append([Keyboard._btn("➕ Добавить чат", "add_chat")])
        kb.append([Keyboard._btn("◀️ Назад", "settings")])
        return "\n".join(lines), InlineKeyboardMarkup(kb)
    
    async def _render_chat_manage(self, chat_id: int, **kwargs) -> Tuple[str, InlineKeyboardMarkup]:
        """Рендерит меню управления конкретным чатом."""
        db: DatabaseManager = await get_db()
        status: dict = await get_cached_bot_status(db)
        chat_stats: Dict[str, dict] = status.get('chat_stats', {})
        chat_names: Dict[str, str] = status.get('chat_names', {})
        chat_summary: Dict[str, dict] = status.get('chat_summary', {})
        
        cid_str: str = str(chat_id)
        name: str = chat_names.get(cid_str, f"Chat {chat_id}")
        stats: dict = chat_stats.get(cid_str, {})
        summary: dict = chat_summary.get(cid_str, {})
        
        total: int = stats.get('total', 0)
        uploaded: int = stats.get('uploaded', 0)
        skipped: int = stats.get('skipped', 0)
        errors: int = stats.get('errors', 0)
        total_bytes: int = stats.get('total_bytes', 0)
        uploaded_bytes: int = stats.get('uploaded_bytes', 0)
        
        selected: int = summary.get('selected', 0)
        new_count: int = summary.get('new', 0)
        unloaded: int = summary.get('unloaded', 0)
        total_pending: int = summary.get('pending', 0)
        compressed: int = summary.get('compressed', 0)
        saved_bytes: int = summary.get('saved_bytes', 0)
        
        text: str = f"📁 <b>{name}</b>\n\n"
        text += f"✅ Загружено: {uploaded} из {total}\n"
        if total_pending > 0:
            text += f"⏳ Осталось загрузить: {total_pending}\n"
        text += f"💾 Загружено: {fmt_size(uploaded_bytes)} из {fmt_size(total_bytes)}\n"
        if skipped > 0:
            text += f"⏭️ Пропущено: {skipped}\n"
        if compressed > 0:
            text += f"🗜️ Сжато: {compressed} файлов, экономия {fmt_size(saved_bytes)}\n"
        if selected > 0:
            text += f"☑️ Выбрано для загрузки: {selected}\n"
        if new_count > 0:
            text += f"🆕 Новых файлов: {new_count}\n"
        if unloaded > 0:
            text += f"⬜ Не скачано: {unloaded}\n"
        if errors > 0:
            text += f"❌ Ошибок: {errors}\n"
        
        kb: List[List[InlineKeyboardButton]] = []
        if total_pending > 0:
            kb.append([Keyboard._btn("📁 Управление файлами", f"files_topics:{chat_id}")])
        if errors > 0:
            kb.append([Keyboard._btn("❌ Сбросить ошибки", f"reset_errors:{chat_id}")])
        kb.append([Keyboard._btn("🔄 Обновить кэш", f"refresh_cache:{chat_id}")])
        kb.append([Keyboard._btn("❌ Удалить чат", f"remove_chat:{chat_id}")])
        kb.append([Keyboard._btn("◀️ Назад", "chats")])
        return text.rstrip(), InlineKeyboardMarkup(kb)
    
    async def _render_files_topics(self, chat_id: int, page: int = 0, **kwargs) -> Tuple[str, InlineKeyboardMarkup]:
        """Рендерит список тем с файлами."""
        db: DatabaseManager = await get_db()
        status: dict = await get_cached_bot_status(db)
        chat_names: Dict[str, str] = status.get('chat_names', {})
        topics_status: Dict[str, List[dict]] = status.get('topics_status', {})
        
        name: str = chat_names.get(str(chat_id), f"Chat {chat_id}")
        topics_list: List[dict] = topics_status.get(str(chat_id), [])
        
        if not topics_list:
            kb: InlineKeyboardMarkup = InlineKeyboardMarkup([[Keyboard._btn("🔄 Обновить кэш", f"refresh_cache:{chat_id}"),
                                        Keyboard._btn("◀️ Назад", f"chat_manage:{chat_id}")]])
            return f"📁 {name}\n\n✅ Нет тем с нескачанными файлами", kb
        
        total_selected: int = sum(t['selected'] for t in topics_list)
        total_pending: int = sum(t['pending'] for t in topics_list)
        
        formatted_topics: List[dict] = []
        for t in topics_list:
            formatted_topics.append({
                'id': str(t['topic_id']),
                'name': t['topic_name'],
                'total': t['total'],
                'selected': t['selected'],
                'is_full': t['is_selected'],
                'is_partial': not t['is_selected'] and t['selected'] > 0
            })
        
        per_page: int = 10
        total_pages: int = (len(formatted_topics) + per_page - 1) // per_page
        current: List[dict] = formatted_topics[page * per_page:(page + 1) * per_page]
        
        lines: List[str] = [
            f"📁 <b>{name}</b>",
            f"📌 Выбрано: {total_selected} файлов",
            f"⏳ Осталось загрузить: {total_pending}",
            ""
        ]
        return "\n".join(lines), Keyboard.topics_with_checkboxes(chat_id, current, page, total_pages)
    
    async def _render_files(self, chat_id: int, topic_id: str, page: int = 0,
                            type_filter: str = 'all', status_filter: str = 'all',
                            sort_by: str = 'date', sort_order: str = 'desc', **kwargs) -> Tuple[str, InlineKeyboardMarkup]:
        """Рендерит список файлов в теме."""
        db: DatabaseManager = await get_db()
        status: dict = await get_cached_bot_status(db)
        chat_names: Dict[str, str] = status.get('chat_names', {})
        all_topics: Dict[str, List[dict]] = status.get('all_topics', {})
        
        name: str = chat_names.get(str(chat_id), f"Chat {chat_id}")
        topics: List[dict] = all_topics.get(str(chat_id), [])
        
        tid: int = int(topic_id) if topic_id != "0" else 0
        topic_name: str = "Общая тема"
        is_selected: bool = False
        for t in topics:
            if t['topic_id'] == tid:
                topic_name = t['topic_name']
                is_selected = t['is_selected']
                break
        
        state_key: str = f"{self.uid}_{chat_id}_{topic_id}"
        await _bot_state.set_files_state(state_key, {
            'page': page, 'type_filter': type_filter, 'status_filter': status_filter,
            'sort_by': sort_by, 'sort_order': sort_order
        })
        
        files: List[dict] = await db.get_files(chat_id, tid)
        if not files:
            kb: InlineKeyboardMarkup = InlineKeyboardMarkup([[Keyboard._btn("◀️ Назад", f"files_topics:{chat_id}")]])
            return f"📁 {name}\n📂 {topic_name}\n📭 В теме нет файлов", kb
        
        filtered: List[dict] = files
        if type_filter != 'all':
            filtered = [f for f in filtered if f.get('file_type', f.get('type')) == type_filter]
        if status_filter == 'uploaded':
            filtered = [f for f in filtered if f['state'] == STATE_UPLOADED]
        elif status_filter == 'unuploaded':
            filtered = [f for f in filtered if f['state'] in (STATE_UNLOADED, STATE_NEW, STATE_ERROR)]
        elif status_filter == 'new':
            filtered = [f for f in filtered if f['state'] == STATE_NEW]
        elif status_filter == 'skipped':
            filtered = [f for f in filtered if f['state'] == STATE_SKIPPED]
        elif status_filter == 'error':
            filtered = [f for f in filtered if f['state'] == STATE_ERROR]
        
        reverse: bool = sort_order == 'desc'
        if sort_by == 'date':
            filtered.sort(key=lambda x: x.get('timestamp', 0), reverse=reverse)
        elif sort_by == 'name':
            filtered.sort(key=lambda x: x.get('filename', '').lower(), reverse=reverse)
        elif sort_by == 'size':
            filtered.sort(key=lambda x: x.get('size', 0), reverse=reverse)
        
        uploaded_count: int = sum(1 for f in files if f['state'] == STATE_UPLOADED)
        new_count: int = sum(1 for f in files if f['state'] == STATE_NEW)
        unloaded_count: int = sum(1 for f in files if f['state'] == STATE_UNLOADED)
        error_count: int = sum(1 for f in files if f['state'] == STATE_ERROR)
        
        selected_ids: Set[int] = {f['message_id'] for f in files} if is_selected else {f['message_id'] for f in files if f['state'] == STATE_SELECTED}
        total_pages: int = (len(filtered) + C.ITEMS_PER_PAGE - 1) // C.ITEMS_PER_PAGE
        
        lines: List[str] = [
            f"📁 {name}", f"📂 {topic_name}",
            f"📊 Всего: {len(files)} | ✅ Скачано: {uploaded_count}",
            f"⬜ Не скачано: {unloaded_count} | 🆕 Новых: {new_count}",
        ]
        if error_count > 0:
            lines.append(f"❌ Ошибок: {error_count}")
        lines.append(f"🔍 Тип: {TYPE_NAMES.get(type_filter, type_filter)} | Статус: {STATUS_NAMES.get(status_filter, status_filter)}")
        lines.append("")
        
        return "\n".join(lines), Keyboard.files_with_filters(
            chat_id, topic_id, filtered, selected_ids,
            page, total_pages, type_filter, status_filter, sort_by, sort_order
        )


async def _show_files_with_state(uid: int, bot: Any, chat_id: int, topic_id: str, page: int,
                                  type_filter: str, status_filter: str, edit_id: int) -> None:
    """Показывает файлы с сохранённым состоянием."""
    state: dict = await _bot_state.get_files_state(f"{uid}_{chat_id}_{topic_id}")
    sort_by: str = state.get('sort_by', 'date')
    sort_order: str = state.get('sort_order', 'desc')
    await show_files(uid, bot, chat_id, topic_id, page, type_filter, status_filter, sort_by, sort_order, edit_id)


async def show_main(uid: int, bot: Any, edit_id: Optional[int] = None) -> None:
    """Показывает главное меню."""
    await MenuRenderer(bot, uid).render('main', edit_id)


async def show_stats(uid: int, bot: Any, edit_id: Optional[int] = None) -> None:
    """Показывает статистику."""
    await MenuRenderer(bot, uid).render('stats', edit_id)


async def show_queue(uid: int, bot: Any, edit_id: Optional[int] = None) -> None:
    """Показывает очередь."""
    await MenuRenderer(bot, uid).render('queue', edit_id)


async def show_errors(uid: int, bot: Any, edit_id: Optional[int] = None) -> None:
    """Показывает ошибки."""
    await MenuRenderer(bot, uid).render('errors', edit_id)


async def show_logs(uid: int, bot: Any, page: int = 1, edit_id: Optional[int] = None) -> None:
    """Показывает логи."""
    renderer: MenuRenderer = MenuRenderer(bot, uid)
    text, kb = await renderer._render_logs(page=page)
    msg_id: Optional[int] = await safe_edit_or_send(bot, uid, edit_id, text, kb, parse_mode='Markdown')
    if msg_id:
        await _bot_state.set_menu(uid, 'logs')


async def show_settings(uid: int, bot: Any, edit_id: Optional[int] = None) -> None:
    """Показывает настройки."""
    await MenuRenderer(bot, uid).render('settings', edit_id)


async def show_admin(uid: int, bot: Any, edit_id: Optional[int] = None) -> None:
    """Показывает админ-панель."""
    await MenuRenderer(bot, uid).render('admin', edit_id)


async def show_windows(uid: int, bot: Any, edit_id: Optional[int] = None) -> None:
    """Показывает окна работы."""
    await MenuRenderer(bot, uid).render('windows', edit_id)


async def show_chats(uid: int, bot: Any, edit_id: Optional[int] = None) -> None:
    """Показывает управление чатами."""
    await MenuRenderer(bot, uid).render('chats', edit_id)


async def show_chat_manage(uid: int, bot: Any, chat_id: int, edit_id: Optional[int] = None) -> None:
    """Показывает управление конкретным чатом."""
    await MenuRenderer(bot, uid).render('chat_manage', edit_id, chat_id=chat_id)


async def show_files_topics(uid: int, bot: Any, chat_id: int, edit_id: Optional[int] = None, page: int = 0) -> None:
    """Показывает темы с файлами."""
    state_key: str = f"files_topics_{uid}_{chat_id}"
    await _bot_state.set_files_state(state_key, {'page': page})
    await MenuRenderer(bot, uid).render('files_topics', edit_id, chat_id=chat_id, page=page)


async def show_files(uid: int, bot: Any, chat_id: int, topic_id: str, page: int = 0,
                     type_filter: str = 'all', status_filter: str = 'all',
                     sort_by: str = 'date', sort_order: str = 'desc', edit_id: Optional[int] = None) -> None:
    """Показывает файлы в теме."""
    await MenuRenderer(bot, uid).render('files', edit_id,
                                        chat_id=chat_id, topic_id=topic_id, page=page,
                                        type_filter=type_filter, status_filter=status_filter,
                                        sort_by=sort_by, sort_order=sort_order)


async def handle_backup_start(uid: int, bot: Any, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Запускает процесс бэкапа."""
    if is_backup_running():
        await send_temp_message(bot, uid, "⚠️ Бэкап уже запущен")
        return
    subprocess.Popen([sys.executable, "main.py"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    await asyncio.sleep(2)
    invalidate_bot_status_cache()
    await send_temp_message(bot, uid, "🚀 Запущен процесс бэкапа")
    await show_main(uid, bot, edit_id)


async def handle_backup_stop(uid: int, bot: Any, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Останавливает процесс бэкапа."""
    try:
        pid_file: Path = Path(PID_FILE)
        if pid_file.exists():
            with open(pid_file, 'r') as f:
                pid: int = int(f.read().strip())
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                pid_file.unlink(missing_ok=True)
                await send_temp_message(bot, uid, "⚠️ Процесс уже завершён")
                await show_main(uid, bot, edit_id)
                return
            os.kill(pid, signal.SIGTERM)
            await send_temp_message(bot, uid, "🛑 Отправлен сигнал остановки")
            asyncio.create_task(_wait_for_shutdown(uid, bot, edit_id))
        else:
            await send_temp_message(bot, uid, "⚠️ Процесс main.py не найден")
    except Exception as e:
        await send_temp_message(bot, uid, f"❌ Ошибка: {e}")


async def _wait_for_shutdown(uid: int, bot: Any, edit_id: Optional[int] = None) -> None:
    """Ожидает завершения процесса бэкапа."""
    for _ in range(30):
        await asyncio.sleep(1)
        if not is_backup_running():
            invalidate_bot_status_cache()
            await show_main(uid, bot, edit_id)
            return
    await send_temp_message(bot, uid, "⚠️ Процесс не завершился за 30 секунд")


async def handle_force_kill(uid: int, bot: Any, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Принудительно завершает процесс."""
    if not is_backup_running():
        await send_temp_message(bot, uid, "⚠️ Процесс main.py не запущен")
        return
    kb: InlineKeyboardMarkup = InlineKeyboardMarkup([
        [Keyboard._btn("✅ ДА, ЗАВЕРШИТЬ", "force_kill_confirm")],
        [Keyboard._btn("❌ НЕТ, НАЗАД", "admin")]
    ])
    await safe_edit_or_send(bot, uid, edit_id, "⚠️ ПРИНУДИТЕЛЬНОЕ ЗАВЕРШЕНИЕ\n\nЭто может привести к потере данных!\nПродолжить?", kb)


async def handle_force_kill_confirm(uid: int, bot: Any, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Подтверждает принудительное завершение."""
    pid_file: Path = Path(PID_FILE)
    if not pid_file.exists():
        await send_temp_message(bot, uid, "⚠️ PID-файл не найден")
        await show_main(uid, bot, edit_id)
        return
    try:
        with open(pid_file, 'r') as f:
            pid: int = int(f.read().strip())
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            pid_file.unlink(missing_ok=True)
            await send_temp_message(bot, uid, "⚠️ Процесс уже завершён")
            await show_main(uid, bot, edit_id)
            return
        os.kill(pid, signal.SIGTERM)
        logger.info(f"📤 SIGTERM отправлен процессу {pid}")
        await asyncio.sleep(3)
        try:
            os.kill(pid, 0)
            os.kill(pid, signal.SIGKILL)
            logger.warning(f"💀 SIGKILL отправлен процессу {pid}")
            await send_temp_message(bot, uid, "💀 Процесс принудительно завершён (SIGKILL)")
        except ProcessLookupError:
            await send_temp_message(bot, uid, "✅ Процесс завершён (SIGTERM)")
        pid_file.unlink(missing_ok=True)
        invalidate_bot_status_cache()
    except Exception as e:
        await send_temp_message(bot, uid, f"❌ Ошибка: {e}")
    await show_main(uid, bot, edit_id)


async def handle_clear_queue(uid: int, bot: Any, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Очищает очередь."""
    if is_backup_running():
        await send_temp_message(bot, uid, "⚠️ Сначала остановите backup")
        return
    db: DatabaseManager = await get_db()
    await db.clear_queue()
    invalidate_bot_status_cache()
    await safe_edit_or_send(bot, uid, edit_id, "✅ Очередь очищена", Keyboard.back("admin"))


async def handle_cleanup_temp(uid: int, bot: Any, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Очищает временные файлы."""
    db: DatabaseManager = await get_db()
    download_dir: str = await db.get_download_dir()
    deleted: int = 0
    for root, _, files in os.walk(download_dir):
        for f in files:
            if any(p in f for p in ('.tmp', '.compressed.', '_compressed.')):
                try:
                    os.unlink(os.path.join(root, f))
                    deleted += 1
                except Exception:
                    pass
    await safe_edit_or_send(bot, uid, edit_id, f"🧹 Удалено {deleted} временных файлов", Keyboard.back("admin"))


async def handle_errors_clear(uid: int, bot: Any, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Очищает ошибки."""
    db: DatabaseManager = await get_db()
    await db.clear_errors()
    invalidate_bot_status_cache()
    await send_temp_message(bot, uid, "✅ Ошибки очищены")
    await show_stats(uid, bot, edit_id)


async def handle_refresh_cache(uid: int, bot: Any, chat_id: int, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Обновляет кэш чата."""
    db: DatabaseManager = await get_db()
    status: dict = await get_cached_bot_status(db)
    name: str = status.get('chat_names', {}).get(str(chat_id), f"Chat {chat_id}")
    msg_id: Optional[int] = await safe_edit_or_send(bot, uid, edit_id, f"🔄 <b>Начинаю полное сканирование чата {name}...</b>\n\n⏳ Подготовка...", Keyboard.back(f"chat_manage:{chat_id}"))
    if msg_id:
        await _bot_state.set_watcher_task(f"scan_{uid}_{chat_id}", asyncio.create_task(monitor_scan_progress(uid, bot, msg_id, chat_id, name)))
    subprocess.Popen([sys.executable, "main.py", "--scan-only", "--full-scan", f"--chat-id={chat_id}"],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)


async def handle_refresh_all_cache(uid: int, bot: Any, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Обновляет кэш всех чатов."""
    msg_id: Optional[int] = await safe_edit_or_send(bot, uid, edit_id, "🔄 <b>Начинаю полное сканирование всех чатов...</b>\n\n⏳ Подготовка...", Keyboard.back("chats"))
    if msg_id:
        await _bot_state.set_watcher_task(f"full_scan_{uid}", asyncio.create_task(monitor_scan_progress(uid, bot, msg_id, chat_id=None)))
    subprocess.Popen([sys.executable, "main.py", "--scan-only", "--full-scan"],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)


async def handle_remove_chat(uid: int, bot: Any, chat_id: int, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Удаляет чат."""
    db: DatabaseManager = await get_db()
    await db.delete_chat_completely(chat_id)
    invalidate_bot_status_cache()
    await send_temp_message(bot, uid, "✅ Чат полностью удалён")
    await show_chats(uid, bot, edit_id)


async def handle_reset_errors(uid: int, bot: Any, chat_id: int, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Сбрасывает ошибки чата."""
    db: DatabaseManager = await get_db()
    count: int = await db.reset_chat_errors(chat_id)
    await db.commit()
    invalidate_bot_status_cache()
    if count > 0:
        await send_temp_message(bot, uid, f"✅ Сброшено {count} ошибок")
    else:
        await send_temp_message(bot, uid, "✅ Нет ошибок для сброса")
    await show_chat_manage(uid, bot, chat_id, edit_id)


async def handle_reset_all_stats(uid: int, bot: Any, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Сбрасывает всю статистику."""
    if is_backup_running():
        await send_temp_message(bot, uid, "⚠️ Сначала остановите backup")
        return
    kb: InlineKeyboardMarkup = InlineKeyboardMarkup([
        [Keyboard._btn("✅ ДА, СБРОСИТЬ", "reset_all_stats_confirm")],
        [Keyboard._btn("❌ НЕТ, НАЗАД", "admin")]
    ])
    await safe_edit_or_send(bot, uid, edit_id, 
        "⚠️ СБРОС ВСЕЙ СТАТИСТИКИ\n\nБудут удалены:\n• Все чаты из списка\n• Вся история операций\n• Статистика сжатия и загрузки\n• Кэш файлов\n\nПродолжить?", kb)


async def handle_reset_all_stats_confirm(uid: int, bot: Any, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Подтверждает сброс статистики."""
    db: DatabaseManager = await get_db()
    await db.reset_all_stats()
    invalidate_bot_status_cache()
    await send_temp_message(bot, uid, "✅ Вся статистика сброшена")
    await show_main(uid, bot, edit_id)


async def handle_topic_select(uid: int, bot: Any, chat_id: int, topic_id: Optional[str] = None, action: str = 'toggle',
                              edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Выбирает/снимает выбор с темы."""
    db: DatabaseManager = await get_db()
    state_key: str = f"files_topics_{uid}_{chat_id}"
    state: dict = await _bot_state.get_files_state(state_key)
    current_page: int = state.get('page', 0)
    
    if topic_id:
        tid: int = int(topic_id) if topic_id != "0" else 0
        if action == 'select':
            await db.select_topic(chat_id, tid)
            msg: str = "✅ Тема выбрана"
        elif action == 'deselect':
            await db.deselect_topic(chat_id, tid)
            msg = "⬜ Выбор снят"
        else:
            if await db.is_topic_selected(chat_id, tid):
                await db.deselect_topic(chat_id, tid)
                msg = "⬜ Выбор снят"
            else:
                await db.select_topic(chat_id, tid)
                msg = "✅ Тема выбрана"
        if callback:
            await callback.answer(msg, show_alert=False)
    else:
        topics: List[dict] = await db.get_topics(chat_id)
        for t in topics:
            if action == 'select':
                await db.select_topic(chat_id, t['topic_id'])
            else:
                await db.deselect_topic(chat_id, t['topic_id'])
        msg = "✅ Выбраны все темы" if action == 'select' else "⬜ Выбор снят со всех тем"
        if callback:
            await callback.answer(msg, show_alert=False)
    
    invalidate_bot_status_cache()
    await show_files_topics(uid, bot, chat_id, edit_id, page=current_page)


async def handle_topic_menu(uid: int, bot: Any, chat_id: int, topic_id: str, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Показывает меню темы."""
    db: DatabaseManager = await get_db()
    status: dict = await get_cached_bot_status(db)
    chat_names: Dict[str, str] = status.get('chat_names', {})
    all_topics: Dict[str, List[dict]] = status.get('all_topics', {})
    
    name: str = chat_names.get(str(chat_id), f"Chat {chat_id}")
    topics: List[dict] = all_topics.get(str(chat_id), [])
    
    topic_name: str = "Общая тема"
    is_selected: bool = False
    for t in topics:
        if str(t['topic_id']) == topic_id:
            topic_name = t['topic_name']
            is_selected = t['is_selected']
            break
    
    kb: InlineKeyboardMarkup = InlineKeyboardMarkup([
        [Keyboard._btn("📂 Открыть файлы", f"files:{chat_id}:{topic_id}")],
        [Keyboard._btn("➕ Добавить всё" if not is_selected else "✅ Уже выбрана",
                       f"topic_select_all:{chat_id}:{topic_id}" if not is_selected else "noop")],
        [Keyboard._btn("⬜ Снять выбор" if is_selected else "❌ Не выбрана",
                       f"topic_select_none:{chat_id}:{topic_id}" if is_selected else "noop")],
        [Keyboard._btn("◀️ Назад", f"files_topics:{chat_id}")]
    ])
    await safe_edit_or_send(bot, uid, edit_id, f"📁 {name} / {topic_name}\n\nВыберите действие:", kb)


async def handle_files_select(uid: int, bot: Any, chat_id: int, topic_id: str, select_type: str,
                              type_filter: str, status_filter: str, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Выбирает файлы по критерию."""
    db: DatabaseManager = await get_db()
    tid: int = int(topic_id) if topic_id != "0" else 0
    state: dict = await _bot_state.get_files_state(f"{uid}_{chat_id}_{topic_id}")
    current_page: int = state.get('page', 0)
    sort_by: str = state.get('sort_by', 'date')
    sort_order: str = state.get('sort_order', 'desc')
    
    if select_type == 'all':
        count: int = await db.select_all_files(chat_id, tid)
        msg = f"✅ Выбрано {count} файлов"
    elif select_type == 'none':
        count = await db.deselect_all_files(chat_id, tid)
        msg = f"⬜ Выбор снят ({count} файлов)"
    elif select_type == 'unuploaded':
        files: List[dict] = await db.get_files(chat_id, tid)
        count = 0
        for f in files:
            if f['state'] in (STATE_UNLOADED, STATE_NEW, STATE_ERROR):
                await db.update_file_state(chat_id, f['message_id'], STATE_SELECTED)
                count += 1
        msg = f"✅ Выбрано {count} нескачанных файлов"
    elif select_type == 'new':
        files = await db.get_files(chat_id, tid)
        count = 0
        for f in files:
            if f['state'] == STATE_NEW:
                await db.update_file_state(chat_id, f['message_id'], STATE_SELECTED)
                count += 1
        msg = f"🆕 Выбрано {count} новых файлов"
    else:
        return
    
    invalidate_bot_status_cache()
    await send_temp_message(bot, uid, msg)
    await show_files(uid, bot, chat_id, topic_id, current_page, type_filter, status_filter, sort_by, sort_order, edit_id)


async def handle_file_toggle(uid: int, bot: Any, chat_id: int, topic_id: str, file_id: int,
                             type_filter: str, status_filter: str, page: int, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Переключает выбор отдельного файла."""
    db: DatabaseManager = await get_db()
    tid: int = int(topic_id) if topic_id != "0" else 0
    await db.toggle_file_selected(chat_id, tid, file_id)
    invalidate_bot_status_cache()
    state: dict = await _bot_state.get_files_state(f"{uid}_{chat_id}_{topic_id}")
    sort_by: str = state.get('sort_by', 'date')
    sort_order: str = state.get('sort_order', 'desc')
    await show_files(uid, bot, chat_id, topic_id, page, type_filter, status_filter, sort_by, sort_order, edit_id)


async def handle_files_save(uid: int, bot: Any, chat_id: int, topic_id: str, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Сохраняет выбор файлов."""
    await send_temp_message(bot, uid, "✅ Выбор сохранён")
    invalidate_bot_status_cache()
    await show_files_topics(uid, bot, chat_id, edit_id, page=0)


async def handle_files_reset_errors(uid: int, bot: Any, chat_id: int, topic_id: str,
                                    type_filter: str, status_filter: str, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Сбрасывает ошибки файлов."""
    db: DatabaseManager = await get_db()
    tid: int = int(topic_id) if topic_id != "0" else 0
    state: dict = await _bot_state.get_files_state(f"{uid}_{chat_id}_{topic_id}")
    current_page: int = state.get('page', 0)
    sort_by: str = state.get('sort_by', 'date')
    sort_order: str = state.get('sort_order', 'desc')
    files: List[dict] = await db.get_files(chat_id, tid, STATE_ERROR)
    for f in files:
        await db.update_file_state(chat_id, f['message_id'], STATE_UNLOADED)
    invalidate_bot_status_cache()
    await send_temp_message(bot, uid, f"✅ Сброшено {len(files)} ошибок")
    await show_files(uid, bot, chat_id, topic_id, current_page, type_filter, status_filter, sort_by, sort_order, edit_id)


async def handle_add_chat(uid: int, bot: Any, context: ContextTypes.DEFAULT_TYPE, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Начинает добавление чата."""
    context.user_data['state'] = 'add_chat'
    context.user_data['current_edit_id'] = edit_id
    await safe_edit_or_send(bot, uid, edit_id, "➕ <b>ДОБАВЛЕНИЕ ЧАТА</b>\nОтправьте ID чата (-1001234567890):", Keyboard.back("chats"))


async def handle_add_chat_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает ввод ID чата."""
    uid: int = update.effective_user.id
    if uid not in ALLOWED_USERS:
        return
    text: str = update.message.text.strip()
    try:
        await update.message.delete()
    except Exception:
        pass
    if not text.lstrip('-').isdigit():
        await send_temp_message(context.bot, uid, "❌ Неверный формат. Введите ID чата (-1001234567890)")
        await show_chats(uid, context.bot, context.user_data.get('current_edit_id'))
        return
    chat_id: int = int(text)
    if chat_id > 0:
        await send_temp_message(context.bot, uid, "⚠️ ID чата обычно начинается с -100", 4)
    db: DatabaseManager = await get_db()
    await db.add_chat_id(chat_id)
    invalidate_bot_status_cache()
    context.user_data.pop('state', None)
    await send_temp_message(context.bot, uid, f"✅ Чат {chat_id} добавлен")
    await show_chats(uid, context.bot, context.user_data.get('current_edit_id'))


async def handle_add_window(uid: int, bot: Any, context: ContextTypes.DEFAULT_TYPE, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Начинает добавление окна работы."""
    context.user_data['state'] = 'add_window'
    context.user_data['current_edit_id'] = edit_id
    await safe_edit_or_send(bot, uid, edit_id, "➕ <b>ДОБАВЛЕНИЕ ОКНА</b>\nОтправьте время начала (9, 9:30, 9.30):", Keyboard.back("windows"))


async def handle_add_window_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает ввод начала окна."""
    uid: int = update.effective_user.id
    if uid not in ALLOWED_USERS:
        return
    text: str = update.message.text.strip()
    try:
        await update.message.delete()
    except Exception:
        pass
    t: str = re.sub(r'[^\d:.]', '', text)
    h: int
    m: int
    if ':' in t:
        h, m = map(int, t.split(':'))
    elif '.' in t:
        h, m = map(int, t.split('.'))
    elif t.isdigit():
        h, m = int(t), 0
    else:
        await send_temp_message(context.bot, uid, "❌ Неверный формат")
        await show_windows(uid, context.bot, context.user_data.get('current_edit_id'))
        return
    if not (0 <= h <= 23 and 0 <= m <= 59):
        await send_temp_message(context.bot, uid, "❌ Неверный формат")
        await show_windows(uid, context.bot, context.user_data.get('current_edit_id'))
        return
    start: str = f"{h:02d}:{m:02d}"
    context.user_data['window_start'] = start
    context.user_data['state'] = 'add_window_end'
    await send_temp_message(context.bot, uid, f"✅ Начало: {start}\nТеперь введите конец")
    await safe_edit_or_send(context.bot, uid, context.user_data.get('current_edit_id'),
                            "➕ <b>ДОБАВЛЕНИЕ ОКНА</b>\nВведите время окончания:", Keyboard.back("windows"))


async def handle_add_window_end(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает ввод конца окна."""
    uid: int = update.effective_user.id
    if uid not in ALLOWED_USERS:
        return
    text: str = update.message.text.strip()
    try:
        await update.message.delete()
    except Exception:
        pass
    t: str = re.sub(r'[^\d:.]', '', text)
    h: int
    m: int
    if ':' in t:
        h, m = map(int, t.split(':'))
    elif '.' in t:
        h, m = map(int, t.split('.'))
    elif t.isdigit():
        h, m = int(t), 0
    else:
        await send_temp_message(context.bot, uid, "❌ Неверный формат")
        await show_windows(uid, context.bot, context.user_data.get('current_edit_id'))
        return
    if not (0 <= h <= 23 and 0 <= m <= 59):
        await send_temp_message(context.bot, uid, "❌ Неверный формат")
        await show_windows(uid, context.bot, context.user_data.get('current_edit_id'))
        return
    end: str = f"{h:02d}:{m:02d}"
    start: Optional[str] = context.user_data.get('window_start')
    if not start:
        await send_temp_message(context.bot, uid, "❌ Ошибка: не найдено время начала")
        await show_windows(uid, context.bot, context.user_data.get('current_edit_id'))
        return
    db: DatabaseManager = await get_db()
    success: bool = await db.add_window(start, end)
    invalidate_bot_status_cache()
    await send_temp_message(context.bot, uid, f"{'✅' if success else '❌'} Окно {start}–{end}")
    context.user_data.pop('state', None)
    context.user_data.pop('window_start', None)
    await show_windows(uid, context.bot, context.user_data.get('current_edit_id'))


async def handle_remove_window(uid: int, bot: Any, context: ContextTypes.DEFAULT_TYPE, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Показывает список окон для удаления."""
    db: DatabaseManager = await get_db()
    status: dict = await get_cached_bot_status(db)
    windows: List[Dict[str, str]] = status.get('windows', [])
    if not windows:
        await send_temp_message(bot, uid, "❌ Нет окон для удаления")
        return
    kb: List[List[InlineKeyboardButton]] = [[Keyboard._btn(f"❌ {w['start']} – {w['end']}", f"remove_window_idx:{i}")] for i, w in enumerate(windows)]
    kb.append([Keyboard._btn("◀️ Назад", "windows")])
    await safe_edit_or_send(bot, uid, edit_id, "🕐 ВЫБЕРИТЕ ОКНО ДЛЯ УДАЛЕНИЯ\n", InlineKeyboardMarkup(kb))


async def handle_remove_window_idx(uid: int, bot: Any, idx: int, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Удаляет окно по индексу."""
    db: DatabaseManager = await get_db()
    await db.remove_window(idx)
    invalidate_bot_status_cache()
    await send_temp_message(bot, uid, "✅ Окно удалено")
    await show_windows(uid, bot, edit_id)


async def handle_clear_windows(uid: int, bot: Any, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Очищает все окна."""
    db: DatabaseManager = await get_db()
    await db.clear_windows()
    invalidate_bot_status_cache()
    await send_temp_message(bot, uid, "🗑️ Все окна очищены")
    await show_windows(uid, bot, edit_id)


async def handle_toggle_auto(uid: int, bot: Any, edit_id: Optional[int] = None, callback: Optional[Any] = None) -> None:
    """Переключает автозапуск."""
    db: DatabaseManager = await get_db()
    status: dict = await get_cached_bot_status(db)
    windows: List[Dict[str, str]] = status.get('windows', [])
    if not windows:
        await send_temp_message(bot, uid, "⚠️ Сначала добавьте окна работы")
        await show_windows(uid, bot, edit_id)
        return
    current: bool = status.get('auto_enabled', False)
    await db.set_auto_enabled(not current)
    invalidate_bot_status_cache()
    await send_temp_message(bot, uid, f"🔄 Автозапуск: {'включён' if not current else 'выключён'}")
    await show_windows(uid, bot, edit_id)


class CallbackRouter:
    """Роутер callback-запросов."""
    
    def __init__(self) -> None:
        """Инициализирует роутер."""
        self.routes: Dict[str, Callable] = {}
        self._register()
    
    def _register(self) -> None:
        """Регистрирует обработчики."""
        self.routes.update({
            "main": lambda c, b, u, a, e, ctx: show_main(u, b, e),
            "stats": lambda c, b, u, a, e, ctx: show_stats(u, b, e),
            "settings": lambda c, b, u, a, e, ctx: show_settings(u, b, e),
            "chats": lambda c, b, u, a, e, ctx: show_chats(u, b, e),
            "windows": lambda c, b, u, a, e, ctx: show_windows(u, b, e),
            "admin": lambda c, b, u, a, e, ctx: show_admin(u, b, e),
            "queue": lambda c, b, u, a, e, ctx: show_queue(u, b, e),
            "errors_view": lambda c, b, u, a, e, ctx: show_errors(u, b, e),
            "logs": lambda c, b, u, a, e, ctx: show_logs(u, b, 1, e),
            "logs_refresh": lambda c, b, u, a, e, ctx: show_logs(u, b, 1, e),
            "logs_page": lambda c, b, u, a, e, ctx: show_logs(u, b, int(a[0]), e),
            "noop": lambda *a: None,
            "backup_start": lambda c, b, u, a, e, ctx: handle_backup_start(u, b, e, c),
            "backup_stop": lambda c, b, u, a, e, ctx: handle_backup_stop(u, b, e, c),
            "force_kill": lambda c, b, u, a, e, ctx: handle_force_kill(u, b, e, c),
            "force_kill_confirm": lambda c, b, u, a, e, ctx: handle_force_kill_confirm(u, b, e, c),
            "clear_queue": lambda c, b, u, a, e, ctx: handle_clear_queue(u, b, e, c),
            "cleanup_temp": lambda c, b, u, a, e, ctx: handle_cleanup_temp(u, b, e, c),
            "errors_clear": lambda c, b, u, a, e, ctx: handle_errors_clear(u, b, e, c),
            "refresh_all_cache": lambda c, b, u, a, e, ctx: handle_refresh_all_cache(u, b, e, c),
            "toggle_auto": lambda c, b, u, a, e, ctx: handle_toggle_auto(u, b, e, c),
            "clear_windows": lambda c, b, u, a, e, ctx: handle_clear_windows(u, b, e, c),
            "add_window": lambda c, b, u, a, e, ctx: handle_add_window(u, b, ctx, e, c),
            "remove_window": lambda c, b, u, a, e, ctx: handle_remove_window(u, b, ctx, e, c),
            "add_chat": lambda c, b, u, a, e, ctx: handle_add_chat(u, b, ctx, e, c),
            "chat_manage": lambda c, b, u, a, e, ctx: show_chat_manage(u, b, int(a[0]), e),
            "remove_chat": lambda c, b, u, a, e, ctx: handle_remove_chat(u, b, int(a[0]), e, c),
            "reset_errors": lambda c, b, u, a, e, ctx: handle_reset_errors(u, b, int(a[0]), e, c),
            "refresh_cache": lambda c, b, u, a, e, ctx: handle_refresh_cache(u, b, int(a[0]), e, c),
            "remove_window_idx": lambda c, b, u, a, e, ctx: handle_remove_window_idx(u, b, int(a[0]), e, c),
            "reset_all_stats": lambda c, b, u, a, e, ctx: handle_reset_all_stats(u, b, e, c),
            "reset_all_stats_confirm": lambda c, b, u, a, e, ctx: handle_reset_all_stats_confirm(u, b, e, c),
            "files_topics": lambda c, b, u, a, e, ctx: show_files_topics(u, b, int(a[0]), e),
            "topic_menu": lambda c, b, u, a, e, ctx: handle_topic_menu(u, b, int(a[0]), a[1], e, c),
            "topic_select_all": lambda c, b, u, a, e, ctx: handle_topic_select(u, b, int(a[0]), a[1], 'select', e, c),
            "topic_select_none": lambda c, b, u, a, e, ctx: handle_topic_select(u, b, int(a[0]), a[1], 'deselect', e, c),
            "topics_select_all": lambda c, b, u, a, e, ctx: handle_topic_select(u, b, int(a[0]), None, 'select', e, c),
            "topics_select_none": lambda c, b, u, a, e, ctx: handle_topic_select(u, b, int(a[0]), None, 'deselect', e, c),
            "topics_page": lambda c, b, u, a, e, ctx: show_files_topics(u, b, int(a[0]), e, int(a[1])),
            "files": lambda c, b, u, a, e, ctx: show_files(u, b, int(a[0]), a[1], 0, 'all', 'all', 'date', 'desc', e),
            "files_save": lambda c, b, u, a, e, ctx: handle_files_save(u, b, int(a[0]), a[1], e, c),
            "files_type_filter": lambda c, b, u, a, e, ctx: _show_files_with_state(u, b, int(a[0]), a[1], 0, a[2], a[3], e),
            "files_status_filter": lambda c, b, u, a, e, ctx: _show_files_with_state(u, b, int(a[0]), a[1], 0, a[2], a[3], e),
            "files_select_all": lambda c, b, u, a, e, ctx: handle_files_select(u, b, int(a[0]), a[1], 'all', a[2], a[3], e, c),
            "files_select_none": lambda c, b, u, a, e, ctx: handle_files_select(u, b, int(a[0]), a[1], 'none', a[2], a[3], e, c),
            "files_select_unuploaded": lambda c, b, u, a, e, ctx: handle_files_select(u, b, int(a[0]), a[1], 'unuploaded', a[2], a[3], e, c),
            "files_select_new": lambda c, b, u, a, e, ctx: handle_files_select(u, b, int(a[0]), a[1], 'new', a[2], a[3], e, c),
            "files_reset_errors": lambda c, b, u, a, e, ctx: handle_files_reset_errors(u, b, int(a[0]), a[1], a[2], a[3], e, c),
            "files_sort": lambda c, b, u, a, e, ctx: show_files(u, b, int(a[0]), a[1], 0, a[4], a[5], a[2], a[3], e),
            "files_page": lambda c, b, u, a, e, ctx: show_files(u, b, int(a[0]), a[1], int(a[2]), a[3], a[4], a[5], a[6], e),
            "file_toggle": lambda c, b, u, a, e, ctx: handle_file_toggle(u, b, int(a[0]), a[1], int(a[2]), a[3], a[4], int(a[5]), e, c),
        })
    
    async def handle(self, callback: Any, bot: Any, uid: int, data: str, edit_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Обрабатывает callback."""
        parts: List[str] = data.split(":")
        cmd: str = parts[0]
        args: List[str] = parts[1:] if len(parts) > 1 else []
        handler: Optional[Callable] = self.routes.get(cmd)
        if handler:
            try:
                await handler(callback, bot, uid, args, edit_id, context)
            except Exception as e:
                logger.error(f"❌ Ошибка в обработчике {cmd}: {e}", exc_info=True)
                await safe_edit_or_send(bot, uid, edit_id, "⚠️ Ошибка", Keyboard.back("main"))
        else:
            logger.warning(f"⚠️ Неизвестная команда: {cmd}")
            await safe_edit_or_send(bot, uid, edit_id, "⚠️ Неизвестная команда", Keyboard.back("main"))


_schedule_exit_time: Dict[int, float] = {}
_shutdown: asyncio.Event = asyncio.Event()


async def schedule_manager(app: Application) -> None:
    """Менеджер расписания — ТОЛЬКО запуск/остановка main.py, без API запросов."""
    check_interval: int = 60
    while not _shutdown.is_set():
        try:
            db: DatabaseManager = await get_db()
            auto: bool = await db.is_auto_enabled()
            if not auto:
                await asyncio.sleep(check_interval)
                continue
            windows: List[dict] = await db.get_windows()
            if not windows:
                await asyncio.sleep(check_interval)
                continue
            should_run: bool = await db.should_run_now()
            
            if should_run:
                if not is_backup_running():
                    logger.info("🕐 Автозапуск бэкапа по расписанию")
                    subprocess.Popen([sys.executable, "main.py"], stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL, start_new_session=True)
                    invalidate_bot_status_cache()
                    _schedule_exit_time.clear()
            else:
                if auto and windows and is_backup_running():
                    if 0 not in _schedule_exit_time:
                        _schedule_exit_time[0] = time.time()
                        logger.info("🕐 Выход из окна, даю 5 минут на завершение...")
                    elif time.time() - _schedule_exit_time[0] > 300:
                        logger.info("🕐 Остановка бэкапа (выход из окна)")
                        pid_file: Path = Path(PID_FILE)
                        if pid_file.exists():
                            try:
                                with open(pid_file, 'r') as f:
                                    pid: int = int(f.read().strip())
                                os.kill(pid, signal.SIGTERM)
                                await asyncio.sleep(2)
                                try:
                                    os.kill(pid, 0)
                                    os.kill(pid, signal.SIGKILL)
                                except ProcessLookupError:
                                    pass
                                pid_file.unlink(missing_ok=True)
                                invalidate_bot_status_cache()
                            except Exception:
                                pass
                        _schedule_exit_time.clear()
            await asyncio.sleep(check_interval)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"❌ Ошибка в планировщике: {e}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик /start."""
    uid: int = update.effective_user.id
    if uid in ALLOWED_USERS:
        try:
            await _bot_state.cancel_watcher_task(f"menu_{uid}")
            await _bot_state.pop_msg(uid)
            await show_main(uid, context.bot)
        except RetryAfter as e:
            _flood.handle_retry_after(e)
            await update.message.reply_text(f"⏳ Telegram пауза {e.retry_after}с. Попробуйте позже.")


async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик текстовых сообщений."""
    uid: int = update.effective_user.id
    if uid not in ALLOWED_USERS:
        return
    state: Optional[str] = context.user_data.get('state')
    if state == 'add_chat':
        await handle_add_chat_input(update, context)
    elif state in ('add_window', 'add_window_end'):
        if state == 'add_window':
            await handle_add_window_input(update, context)
        else:
            await handle_add_window_end(update, context)
    else:
        await show_main(uid, context.bot)


async def callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик callback-запросов."""
    q: Any = update.callback_query
    uid: int = q.from_user.id
    if uid not in ALLOWED_USERS:
        await q.answer("⛔ Доступ запрещён")
        return
    logger.info(f"📱 [{uid}] Кнопка: {q.data}")
    edit_id: int = q.message.message_id
    await _bot_state.set_msg(uid, edit_id)
    try:
        await q.answer()
    except RetryAfter as e:
        _flood.handle_retry_after(e)
        try:
            await q.answer(f"⏳ Пауза {e.retry_after}с", show_alert=True)
        except Exception:
            pass
    except BadRequest as e:
        if "message not found" in str(e).lower():
            await _bot_state.cancel_watcher_task(f"menu_{uid}")
            await show_main(uid, context.bot)
            return
    if 'callback_router' not in context.bot_data:
        context.bot_data['callback_router'] = CallbackRouter()
    await context.bot_data['callback_router'].handle(q, context.bot, uid, q.data, edit_id, context)


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик ошибок."""
    error: Optional[Exception] = context.error
    if error and "CancelledError" in str(type(error)):
        return
    if isinstance(error, RetryAfter):
        _flood.handle_retry_after(error)
    elif "Unauthorized" in str(error) or "invalid token" in str(error).lower():
        logger.critical("🔴 Ошибка авторизации бота!")
        _shutdown.set()
    else:
        logger.error(f"❌ Ошибка: {type(error).__name__}: {error}")


async def shutdown_bot(app: Application, scheduler: Optional[asyncio.Task] = None) -> None:
    """Останавливает бота."""
    if _shutdown.is_set():
        return  # Уже останавливаемся
    logger.info("🛑 Останавливаем бота...")
    _shutdown.set()
    if scheduler and not scheduler.done():
        scheduler.cancel()
        try:
            await asyncio.wait_for(scheduler, timeout=3.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
    await _bot_state.clear_watcher_tasks()
    if app and app.running:
        try:
            if app.updater and app.updater.running:
                await app.updater.stop()
            await app.stop()
            await asyncio.sleep(0.5)
            await app.shutdown()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Ошибка при остановке приложения: {e}")
    logger.info("✅ Бот остановлен")


def main() -> None:
    """Точка входа."""
    logger.info("🔌 Подключение к Telegram API...")
    app: Application = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))
    app.add_error_handler(error_handler)
    logger.info(f"🚀 Бот запущен v{__version__}")
    logger.info("✅ Бот готов к приёму команд")
    
    loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    async def run() -> None:
        await asyncio.sleep(3)  # Задержка чтобы не флудить при старте
        scheduler: asyncio.Task = asyncio.create_task(schedule_manager(app))
        connect_errors: int = 0
        max_delay: int = 300
        
        while not _shutdown.is_set():
            try:
                await app.initialize()
                await app.start()
                await app.updater.start_polling(allowed_updates=["message", "callback_query"],
                                                drop_pending_updates=True, poll_interval=5.0, timeout=30)
                connect_errors = 0
                while not _shutdown.is_set():
                    await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                _shutdown.set()
                break
            except KeyboardInterrupt:
                _shutdown.set()
                break
            except Exception as e:
                connect_errors += 1
                delay: float = min(5 * (2 ** (connect_errors - 1)), max_delay)
                logger.warning(f"⚠️ Ошибка подключения (попытка {connect_errors}), повтор через {delay:.0f}с: {e}")
                try:
                    await shutdown_bot(app, scheduler)
                except Exception:
                    pass
                await asyncio.sleep(delay)
        
        await shutdown_bot(app, scheduler)
    
    try:
        loop.run_until_complete(run())
    except KeyboardInterrupt:
        _shutdown.set()
        logger.info("👋 Прервано пользователем")
    finally:
        try:
            pending: Set[asyncio.Task] = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except KeyboardInterrupt:
            pass
        except Exception:
            pass
        try:
            loop.close()
        except Exception:
            pass
        logger.info("👋 Бот завершён")
        import os as _os
        _os._exit(0)


if __name__ == "__main__":
    main()
