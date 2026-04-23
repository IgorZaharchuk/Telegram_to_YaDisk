#!/usr/bin/env python3
"""
Telegram MTProto Backup to Yandex Disk - ОРКЕСТРАТОР
ВЕРСИЯ 0.17.1 — ИСПРАВЛЕНИЯ: HEALTHCHECKER ПРОВЕРЯЕТ RECONNECT
"""

__version__ = "0.17.1"

import os
import sys
import asyncio
import signal
import argparse
import logging
import logging.handlers
import random
import subprocess
import time
import atexit
from pathlib import Path
from typing import Optional, Dict, List, Any, Set, Tuple
from dotenv import load_dotenv

# ========== НАСТРОЙКА ЛОГИРОВАНИЯ ==========
os.makedirs("logs", exist_ok=True)

file_handler: logging.handlers.RotatingFileHandler = logging.handlers.RotatingFileHandler(
    "logs/backup.log", maxBytes=20*1024*1024, backupCount=3, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
console_handler: logging.StreamHandler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
formatter: logging.Formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

root_logger: logging.Logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

for noisy in ("pyrogram", "yadisk", "asyncio", "urllib3", "httpcore", "httpx", "aiosqlite"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

logging.getLogger("telegram_client").setLevel(logging.INFO)
logging.getLogger("queue_system").setLevel(logging.INFO)
logging.getLogger("compress").setLevel(logging.INFO)

logger: logging.Logger = logging.getLogger(__name__)

from database import get_db, DatabaseManager
from queue_system import QueueSystem
from telegram_client import TelegramDownloader
from yandex_uploader import YandexUploader
from compressor import Compressor

PID_FILE: str = "backup.pid"


def is_process_healthy(pid: int) -> bool:
    """Проверяет, жив ли процесс и активна ли БД."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    try:
        db_path: Path = Path("backup.db")
        if db_path.exists() and time.time() - db_path.stat().st_mtime > 300:
            return False
    except Exception:
        pass
    return True


def cleanup_stale_process() -> None:
    """Удаляет PID-файл зависшего процесса."""
    pid_file: Path = Path(PID_FILE)
    if not pid_file.exists():
        return
    
    current_pid: int = os.getpid()
    try:
        with open(pid_file, 'r') as f:
            old_pid: int = int(f.read().strip())
        if old_pid == current_pid:
            return
        
        try:
            os.kill(old_pid, 0)
        except ProcessLookupError:
            pid_file.unlink()
            return
        
        if not is_process_healthy(old_pid):
            logger.warning(f"🔄 Обнаружен зависший процесс (PID {old_pid})")
            os.kill(old_pid, signal.SIGTERM)
            time.sleep(2)
            try:
                os.kill(old_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            pid_file.unlink()
        else:
            logger.warning(f"⚠️ Обнаружен работающий процесс с PID {old_pid}")
            sys.exit(1)
    except (ValueError, OSError):
        pid_file.unlink()


def write_pid() -> None:
    """Записывает PID текущего процесса."""
    cleanup_stale_process()
    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))
    logger.debug(f"📝 PID {os.getpid()} записан в {PID_FILE}")


def remove_pid() -> None:
    """Удаляет PID-файл."""
    try:
        pid_file: Path = Path(PID_FILE)
        if pid_file.exists():
            with open(pid_file, 'r') as f:
                if int(f.read().strip()) == os.getpid():
                    pid_file.unlink()
    except Exception:
        pass


def kill_orphan_ffmpeg() -> None:
    """Убивает осиротевшие процессы ffmpeg."""
    try:
        subprocess.run(['pkill', '-9', '-f', 'ffmpeg.*compressed'], timeout=5, capture_output=True)
        subprocess.run(['pkill', '-9', '-f', 'cpulimit.*ffmpeg'], timeout=5, capture_output=True)
    except Exception:
        pass


atexit.register(kill_orphan_ffmpeg)
atexit.register(remove_pid)


class ExponentialBackoff:
    """Экспоненциальная задержка для повторных попыток."""
    
    def __init__(self, base_delay: float = 1.0, max_delay: float = 60.0, max_retries: int = 5) -> None:
        """Инициализирует backoff."""
        self.base_delay: float = base_delay
        self.max_delay: float = max_delay
        self.max_retries: int = max_retries
        self._attempt: int = 0

    def reset(self) -> None:
        """Сбрасывает счётчик попыток."""
        self._attempt = 0

    async def wait(self) -> bool:
        """Ожидает перед следующей попыткой."""
        if self._attempt >= self.max_retries:
            return False
        if self._attempt > 0:
            delay: float = min(self.base_delay * (2 ** (self._attempt - 1)), self.max_delay)
            await asyncio.sleep(delay * (0.5 + random.random()))
        self._attempt += 1
        return True


class HealthChecker:
    """Проверяет здоровье сервисов и переподключает при необходимости."""
    
    def __init__(self, check_interval: float = 30.0, failure_threshold: int = 3) -> None:
        """Инициализирует HealthChecker."""
        self.check_interval: float = check_interval
        self.failure_threshold: int = failure_threshold
        self._status: Dict[str, str] = {}
        self._failure_counts: Dict[str, int] = {}
        self._task: Optional[asyncio.Task] = None
        self._shutdown_flag: bool = False
        self._tg: Optional[TelegramDownloader] = None
        self._ya: Optional[YandexUploader] = None
        self._shutdown_manager: Optional['ShutdownManager'] = None

    def set_shutdown_manager(self, shutdown_manager: 'ShutdownManager') -> None:
        """Устанавливает менеджер завершения."""
        self._shutdown_manager = shutdown_manager

    async def start(self, tg: TelegramDownloader, ya: YandexUploader) -> None:
        """Запускает проверку здоровья."""
        self._tg = tg
        self._ya = ya
        self._task = asyncio.create_task(self._checker())

    async def stop(self) -> None:
        """Останавливает проверку здоровья."""
        self._shutdown_flag = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _checker(self) -> None:
        """Основной цикл проверки."""
        await asyncio.sleep(10)
        while not self._shutdown_flag:
            for service, client in [('telegram', self._tg), ('yandex', self._ya)]:
                if client:
                    if await client.is_healthy():
                        self._failure_counts[service] = 0
                        self._status[service] = 'healthy'
                    else:
                        self._failure_counts[service] = self._failure_counts.get(service, 0) + 1
                        if self._failure_counts[service] >= self.failure_threshold:
                            self._status[service] = 'unhealthy'
                            if await client.reconnect():
                                self._failure_counts[service] = 0
                                self._status[service] = 'healthy'
                                logger.info(f"✅ {service} переподключён")
                            else:
                                logger.error(f"❌ {service} не удалось переподключить")
                                if self._shutdown_manager:
                                    asyncio.create_task(self._shutdown_manager.request(f"{service}_unhealthy"))
            await asyncio.sleep(self.check_interval)


class ShutdownManager:
    """Управляет graceful shutdown."""
    
    def __init__(self) -> None:
        """Инициализирует менеджер."""
        self.shutdown: bool = False
        self._shutting_down: bool = False
        self._components: Dict[str, Any] = {}
        self._event: asyncio.Event = asyncio.Event()

    def set_components(self, **components: Any) -> None:
        """Устанавливает компоненты для остановки."""
        self._components = components

    async def request(self, reason: str = "") -> None:
        """Запрашивает завершение."""
        if self._shutting_down:
            return
        self._shutting_down = True
        self.shutdown = True
        logger.info(f"🛑 Запрошено завершение: {reason}")
        self._event.set()

        if 'queue_system' in self._components:
            await self._components['queue_system'].stop()
        if 'comp' in self._components:
            await self._components['comp'].shutdown()
        if 'tg' in self._components:
            await self._components['tg'].disconnect()
        if 'ya' in self._components:
            await self._components['ya'].disconnect()
        if 'health_checker' in self._components:
            await self._components['health_checker'].stop()
        
        logger.info("✅ Все компоненты остановлены")

    def is_requested(self) -> bool:
        """Проверяет, запрошено ли завершение."""
        return self.shutdown


shutdown: ShutdownManager = ShutdownManager()


def signal_handler(sig: int, frame: Any) -> None:
    """Обработчик сигналов."""
    sig_name: str = signal.Signals(sig).name if hasattr(signal, 'Signals') else str(sig)
    logger.info(f"🛑 Получен сигнал {sig_name}")
    asyncio.create_task(shutdown.request(f"signal_{sig_name}"))


async def _init_components() -> Optional[Tuple[DatabaseManager, TelegramDownloader, YandexUploader, Compressor]]:
    """Инициализирует компоненты."""
    api_id: int = int(os.getenv("API_ID", 0))
    api_hash: str = os.getenv("API_HASH", "")
    ya_token: str = os.getenv("YA_DISK_TOKEN", "")
    session_string: str = os.getenv("STRING_SESSION", "")

    if not api_id or not api_hash:
        logger.error("❌ API_ID и API_HASH не загружены")
        return None
    if not ya_token:
        logger.error("❌ YA_DISK_TOKEN не задан")
        return None

    db: DatabaseManager = await get_db()
    download_dir: str = await db.get_download_dir()
    os.makedirs(download_dir, exist_ok=True)

    file_types: Dict[str, List[str]] = await db.get_file_types()

    tg: TelegramDownloader = TelegramDownloader({
        'api_id': api_id, 'api_hash': api_hash, 'session_string': session_string,
        'session_file': 'user_session', 'download_dir': download_dir,
        **await db.get_telegram_client_settings(), 'file_types': file_types})

    ya: YandexUploader = YandexUploader({'token': ya_token, **await db.get_upload_settings()})
    comp: Compressor = Compressor({**await db.get_compression_settings(), 'file_types': file_types})
    
    return db, tg, ya, comp


async def _connect_services(tg: TelegramDownloader, ya: YandexUploader) -> bool:
    """Подключает сервисы."""
    backoff: ExponentialBackoff = ExponentialBackoff(base_delay=2.0, max_delay=30.0, max_retries=5)
    while not await tg.connect():
        if not await backoff.wait():
            logger.error("❌ Не удалось подключиться к Telegram")
            return False
    
    backoff.reset()
    while not await ya.connect():
        if not await backoff.wait():
            logger.error("❌ Не удалось подключиться к Яндекс.Диску")
            await tg.disconnect()
            return False
    
    logger.info("✅ Все сервисы подключены")
    return True


async def main_async(scan_only: bool = False, full_scan: bool = False,
                     incremental: bool = False, chat_id: Optional[int] = None) -> int:
    """Главная асинхронная функция."""
    logger.info(f"🚀 Запуск оркестратора v{__version__}")
    write_pid()

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: signal_handler(s, None))

    components: Optional[Tuple] = await _init_components()
    if not components:
        remove_pid()
        return 1
    
    db, tg, ya, comp = components
    
    if not await _connect_services(tg, ya):
        remove_pid()
        return 1

    health_checker: HealthChecker = HealthChecker()
    health_checker.set_shutdown_manager(shutdown)
    queue_system: QueueSystem = QueueSystem(tg, ya, comp, db, await db.get_download_dir())
    queue_system.set_shutdown_manager(shutdown)

    shutdown.set_components(health_checker=health_checker, queue_system=queue_system, tg=tg, ya=ya, comp=comp)

    if scan_only:
        mode: str = 'full' if full_scan else ('incremental' if incremental else 'full')
        await tg.scan_all_chats(db, full=(mode == 'full'), chat_id=chat_id)
        await shutdown.request("scan_complete")
        remove_pid()
        return 0

    await health_checker.start(tg, ya)

    try:
        await queue_system.process_selected_files()
    except asyncio.CancelledError:
        logger.info("👋 Получен сигнал остановки")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
        await db.record_system_error("critical", str(e))
    finally:
        if not shutdown.is_requested():
            await shutdown.request("cleanup")
        await db.checkpoint()
        await db.close()
        remove_pid()

    logger.info("🏁 Работа завершена")
    return 0


def main() -> int:
    """Точка входа."""
    env_path: Path = Path(__file__).parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
        logger.info(f"✅ Загружен .env из {env_path}")
    else:
        logger.error(f"❌ Файл .env не найден в {env_path}")
        return 1
    
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description='Telegram Backup')
    parser.add_argument('--scan-only', action='store_true', help='Только сканирование')
    parser.add_argument('--full-scan', action='store_true', help='Полное сканирование')
    parser.add_argument('--incremental', action='store_true', help='Инкрементальное сканирование')
    parser.add_argument('--chat-id', type=int, help='ID чата для сканирования')
    args: argparse.Namespace = parser.parse_args()

    try:
        return asyncio.run(main_async(args.scan_only, args.full_scan, args.incremental, args.chat_id))
    except KeyboardInterrupt:
        logger.info("👋 Прервано пользователем")
        return 0
    except Exception as e:
        logger.error(f"❌ Необработанная ошибка: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
