#!/usr/bin/env python3
"""
Telegram MTProto Backup to Yandex Disk
Главный файл проекта с поддержкой StringSession
"""

import os
import sys
import json
import asyncio
import logging
import tempfile
import shutil
import subprocess
from pathlib import Path
from datetime import datetime

from telegram_client import TelegramDownloader
from compress import optimize_image, compress_video
from yandex_uploader import YandexUploader

# ==================== НАСТРОЙКИ ====================
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
PHONE_NUMBER = os.getenv("PHONE_NUMBER", "")
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", 0))
YA_DISK_TOKEN = os.getenv("YA_DISK_TOKEN", "")
YA_DISK_PATH = os.getenv("YA_DISK_PATH", "/mtproto_backup")
STRING_SESSION = os.getenv("STRING_SESSION", None)  # 👈 НОВОЕ!

MAX_FILES_PER_RUN = int(os.getenv("MAX_FILES_PER_RUN", "50"))
RATE_LIMIT_DELAY = float(os.getenv("RATE_LIMIT_DELAY", "1.0"))

# Файлы
PROGRESS_FILE = "progress.json"
TOPIC_CACHE_FILE = "topic_cache.json"

# ==================== ЛОГИРОВАНИЕ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('backup.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ==================== УТИЛИТЫ ====================
def sanitize_filename(name: str) -> str:
    """Безопасное имя файла"""
    if not name:
        return "unnamed"
    import re
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', name)
    name = re.sub(r'\.{2,}', '.', name)
    name = name.strip('. ')
    name = name[:200]
    name = name.replace(' ', '_')
    return name if name else "unnamed"

def sanitize_folder_name(name: str) -> str:
    """Безопасное имя папки"""
    if not name:
        return "general"
    import re
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = name.strip().replace(' ', '_')
    name = re.sub(r'_+', '_', name)
    return name[:100]

def load_json(filepath: str, default: dict) -> dict:
    """Загрузка JSON файла"""
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return default

def save_json(filepath: str, data: dict):
    """Сохранение JSON файла"""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# ==================== ОБРАБОТКА СООБЩЕНИЯ ====================
async def process_message(tg_client, message, yandex, topic_cache: dict) -> tuple[bool, int]:
    """Обработка одного сообщения"""
    temp_files = []
    
    try:
        # 1. Определяем тему
        topic_id = None
        if message.reply_to:
            topic_id = getattr(message.reply_to, 'reply_to_top_id', None) or message.reply_to.reply_to_msg_id
        
        topic_name = "general"
        
        if topic_id:
            topic_id_str = str(topic_id)
            if topic_id_str in topic_cache:
                topic_name = topic_cache[topic_id_str]
                logger.info(f"📁 Тема (кэш): {topic_name}")
            else:
                real_name = await tg_client.get_topic_name(message.chat_id, topic_id)
                if real_name:
                    topic_name = sanitize_folder_name(real_name)
                    topic_cache[topic_id_str] = topic_name
                    save_json(TOPIC_CACHE_FILE, topic_cache)
                    logger.info(f"📁 Тема: {real_name}")
                else:
                    topic_name = f"topic_{topic_id}"
                    logger.warning(f"⚠️ Тема не найдена, использую {topic_name}")
        
        # 2. Определяем тип файла и имя
        filename = None
        is_image = False
        is_video = False
        
        if message.photo:
            photo_date = message.date.strftime('%Y%m%d_%H%M%S')
            filename = f"photo_{photo_date}.jpg"
            is_image = True
            
        elif message.document:
            for attr in message.document.attributes:
                if hasattr(attr, 'file_name'):
                    filename = attr.file_name
                    break
            if not filename:
                ext = Path(message.document.mime_type or "").suffix or ".dat"
                filename = f"document_{message.id}{ext}"
            
            ext = Path(filename).suffix.lower()
            if ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']:
                is_image = True
            elif ext in ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.webm']:
                is_video = True
        
        elif message.video:
            if hasattr(message.video, 'file_name') and message.video.file_name:
                filename = message.video.file_name
            else:
                video_date = message.date.strftime('%Y%m%d_%H%M%S')
                filename = f"video_{video_date}.mp4"
            is_video = True
        
        if not filename:
            return False, 0
        
        # 3. Скачиваем (MTProto - нет лимита 20MB!)
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            temp_path = tmp.name
            temp_files.append(temp_path)
        
        logger.info(f"📥 Скачивание: {filename} ({topic_name})")
        await tg_client.download_media(message, temp_path)
        
        # 4. Сжимаем если нужно
        final_path = temp_path
        compress_info = ""
        
        if is_image:
            img_path = temp_path + ".compressed.jpg"
            temp_files.append(img_path)
            success, info = await optimize_image(temp_path, img_path)
            if success:
                final_path = img_path
                compress_info = info
        
        elif is_video:
            video_path = temp_path + ".compressed.mp4"
            temp_files.append(video_path)
            success, info = await compress_video(temp_path, video_path)
            if success:
                final_path = video_path
                compress_info = info
        
        # 5. Загружаем на Яндекс.Диск
        chat_title = getattr(message.chat, 'title', str(message.chat_id))
        chat_folder = sanitize_folder_name(chat_title)
        
        remote_dir = f"{yandex.base_path}/{chat_folder}/{topic_name}"
        safe_filename = sanitize_filename(filename)
        
        success = await yandex.upload(final_path, remote_dir, safe_filename)
        
        if success and compress_info:
            logger.info(f"   {compress_info}")
        
        return success, 1 if success else 0
        
    except Exception as e:
        logger.error(f"❌ Ошибка обработки: {e}")
        return False, 0
    
    finally:
        for f in temp_files:
            try:
                if os.path.exists(f):
                    os.unlink(f)
            except:
                pass

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
async def main():
    """Основная функция"""
    # Проверка настроек
    required = [API_ID, API_HASH, TARGET_CHAT_ID, YA_DISK_TOKEN]
    if not all(required):
        logger.error("❌ Не все переменные окружения установлены")
        logger.error("Нужны: API_ID, API_HASH, TARGET_CHAT_ID, YA_DISK_TOKEN")
        return 1
    
    # Проверяем наличие сессии (StringSession или телефон)
    if not STRING_SESSION and not PHONE_NUMBER:
        logger.error("❌ Нужна либо STRING_SESSION, либо PHONE_NUMBER")
        return 1
    
    logger.info("🚀 Запуск MTProto бэкапа")
    
    # Загружаем прогресс
    progress = load_json(PROGRESS_FILE, {"last_id": 0, "total": 0})
    topic_cache = load_json(TOPIC_CACHE_FILE, {})
    last_id = progress.get("last_id", 0)
    total = progress.get("total", 0)
    
    logger.info(f"📊 Прогресс: последний ID {last_id}, всего файлов {total}")
    
    # Подключение к Telegram
    tg_client = TelegramDownloader(
        api_id=API_ID, 
        api_hash=API_HASH,
        session_string=STRING_SESSION  # 👈 Передаём строку сессии
    )
    
    try:
        await tg_client.connect()
        
        chat = await tg_client.get_chat(TARGET_CHAT_ID)
        chat_title = getattr(chat, 'title', str(chat.id))
        logger.info(f"✅ Чат: {chat_title}")
        
        # Подключение к Яндекс.Диску
        async with YandexUploader(YA_DISK_TOKEN, YA_DISK_PATH) as yandex:
            
            processed = 0
            
            logger.info(f"📥 Поиск новых сообщений (с ID > {last_id})")
            
            # Получаем асинхронный итератор сообщений
            messages = await tg_client.get_messages(TARGET_CHAT_ID, min_id=last_id)
            
            async for message in messages:
                if message.id <= last_id:
                    continue
                
                if message.media:
                    logger.info(f"📨 Сообщение {message.id}")
                    success, count = await process_message(tg_client, message, yandex, topic_cache)
                    
                    if success:
                        processed += count
                        total += count
                        
                        # Сохраняем прогресс
                        progress["last_id"] = message.id
                        progress["total"] = total
                        save_json(PROGRESS_FILE, progress)
                        
                        if processed >= MAX_FILES_PER_RUN:
                            logger.info(f"⏸️ Достигнут лимит {MAX_FILES_PER_RUN} файлов")
                            break
                    
                    await asyncio.sleep(RATE_LIMIT_DELAY)
            
            logger.info(f"✅ Готово. Обработано: {processed}, всего: {total}")
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        await tg_client.disconnect()
    
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
