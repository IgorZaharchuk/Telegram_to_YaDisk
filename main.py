#!/usr/bin/env python3
"""
Telegram MTProto Backup to Yandex Disk
Главный файл проекта для Pyrogram с отладкой тем
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

# ==================== ЛОГИРОВАНИЕ ====================
logging.basicConfig(
    level=logging.DEBUG,  # Временно ставим DEBUG для отладки
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('backup.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ==================== НАСТРОЙКИ ====================
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
PHONE_NUMBER = os.getenv("PHONE_NUMBER", "")
YA_DISK_TOKEN = os.getenv("YA_DISK_TOKEN", "")
YA_DISK_PATH = os.getenv("YA_DISK_PATH", "/mtproto_backup")
STRING_SESSION = os.getenv("STRING_SESSION", None)

# Преобразуем TARGET_CHAT_ID в число
TARGET_CHAT_ID_STR = os.getenv("TARGET_CHAT_ID", "0")
try:
    TARGET_CHAT_ID = int(TARGET_CHAT_ID_STR)
    logger.info(f"✅ TARGET_CHAT_ID преобразован в число: {TARGET_CHAT_ID}")
except ValueError:
    logger.error(f"❌ TARGET_CHAT_ID должен быть числом, получено: {TARGET_CHAT_ID_STR}")
    TARGET_CHAT_ID = 0

MAX_FILES_PER_RUN = int(os.getenv("MAX_FILES_PER_RUN", "50"))
RATE_LIMIT_DELAY = float(os.getenv("RATE_LIMIT_DELAY", "1.0"))

# Файлы
PROGRESS_FILE = "progress.json"
TOPIC_CACHE_FILE = "topic_cache.json"

# Импорты после настроек
from telegram_client import TelegramDownloader
from compress import optimize_image, compress_video
from yandex_uploader import YandexUploader

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
    """Обработка одного сообщения с подробной отладкой"""
    temp_files = []
    
    try:
        # === ПОДРОБНАЯ ОТЛАДКА СООБЩЕНИЯ ===
        logger.debug("="*50)
        logger.debug(f"📨 Обработка сообщения ID: {message.id}")
        logger.debug(f"Тип сообщения: {type(message)}")
        logger.debug(f"Дата: {message.date}")
        
        # Проверяем все возможные атрибуты, связанные с темами
        logger.debug("--- Поиск атрибутов темы ---")
        
        # Список атрибутов для проверки
        topic_attrs = [
            'reply_to_top_id',
            'message_thread_id', 
            'forum_topic',
            'reply_to_msg_id',
            'reply_to_message_id',
            'reply_to'
        ]
        
        for attr in topic_attrs:
            if hasattr(message, attr):
                value = getattr(message, attr)
                logger.debug(f"✅ Найден атрибут {attr}: {value}")
            else:
                logger.debug(f"❌ Нет атрибута {attr}")
        
        # Если есть reply_to, проверяем его подробно
        if hasattr(message, 'reply_to') and message.reply_to:
            logger.debug("--- Детальный разбор reply_to ---")
            reply_to = message.reply_to
            logger.debug(f"Тип reply_to: {type(reply_to)}")
            for attr in dir(reply_to):
                if not attr.startswith('_'):
                    try:
                        value = getattr(reply_to, attr)
                        logger.debug(f"  reply_to.{attr}: {value}")
                    except:
                        pass
        
        # Проверяем структуру сообщения
        logger.debug("--- Основные атрибуты сообщения ---")
        important_attrs = ['id', 'chat_id', 'from_user', 'text', 'caption', 'media']
        for attr in important_attrs:
            if hasattr(message, attr):
                value = getattr(message, attr)
                logger.debug(f"  {attr}: {value}")
        
        # === ОПРЕДЕЛЕНИЕ ТЕМЫ ===
        topic_id = tg_client.get_topic_id_from_message(message)
        logger.debug(f"🎯 Определенный topic_id: {topic_id}")

        topic_name = "general"

        if topic_id:
            topic_id_str = str(topic_id)
            logger.info(f"🔍 Найдена тема ID: {topic_id}")

            # Проверяем кэш
            if topic_id_str in topic_cache:
                topic_name = topic_cache[topic_id_str]
                logger.info(f"📁 Тема из кэша: {topic_name} (ID: {topic_id})")
            else:
                # Пытаемся получить название через API
                logger.info(f"🔄 Запрашиваю название для темы ID: {topic_id}")
                real_name = await tg_client.get_topic_name(message.chat.id, topic_id)

                if real_name:
                    topic_name = sanitize_folder_name(real_name)
                    topic_cache[topic_id_str] = topic_name
                    save_json(TOPIC_CACHE_FILE, topic_cache)
                    logger.info(f"✅ Найдена тема: {real_name} (ID: {topic_id})")
                else:
                    # Если не удалось, используем ID
                    topic_name = f"topic_{topic_id}"
                    topic_cache[topic_id_str] = topic_name
                    save_json(TOPIC_CACHE_FILE, topic_cache)
                    logger.info(f"📁 Использую ID темы: {topic_name}")
        else:
            logger.warning("⚠️ Не удалось определить ID темы")
            logger.info("📁 Сообщение будет в папку general")
        
        # 2. Определяем тип файла и имя
        filename = None
        is_image = False
        is_video = False
        
        if message.photo:
            photo_date = message.date.strftime('%Y%m%d_%H%M%S')
            filename = f"photo_{photo_date}.jpg"
            is_image = True
            logger.debug(f"📸 Обнаружено фото")
            
        elif message.document:
            logger.debug(f"📄 Обнаружен документ")
            # Получаем имя файла из атрибутов документа
            if hasattr(message.document, 'file_name'):
                filename = message.document.file_name
                logger.debug(f"   Имя файла: {filename}")
            else:
                # Если нет имени, генерируем из mime_type
                ext = Path(message.document.mime_type or "").suffix or ".dat"
                filename = f"document_{message.id}{ext}"
                logger.debug(f"   Сгенерировано имя: {filename}")
            
            ext = Path(filename).suffix.lower()
            if ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']:
                is_image = True
                logger.debug(f"   Определено как изображение")
            elif ext in ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.webm']:
                is_video = True
                logger.debug(f"   Определено как видео")
        
        elif message.video:
            logger.debug(f"🎬 Обнаружено видео")
            if hasattr(message.video, 'file_name') and message.video.file_name:
                filename = message.video.file_name
                logger.debug(f"   Имя файла: {filename}")
            else:
                video_date = message.date.strftime('%Y%m%d_%H%M%S')
                filename = f"video_{video_date}.mp4"
                logger.debug(f"   Сгенерировано имя: {filename}")
            is_video = True
        
        if not filename:
            logger.debug("⏭️ Сообщение не содержит медиафайл")
            return False, 0
        
        # 3. Скачиваем
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            temp_path = tmp.name
            temp_files.append(temp_path)
        
        logger.info(f"📥 Скачивание: {filename} в папку {topic_name}")
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
                logger.debug(f"   {info}")
        
        elif is_video:
            video_path = temp_path + ".compressed.mp4"
            temp_files.append(video_path)
            success, info = await compress_video(temp_path, video_path)
            if success:
                final_path = video_path
                compress_info = info
                logger.debug(f"   {info}")
        
        # 5. Загружаем на Яндекс.Диск
        chat_title = getattr(message.chat, 'title', str(message.chat.id))
        chat_folder = sanitize_folder_name(chat_title)
        
        remote_dir = f"{yandex.base_path}/{chat_folder}/{topic_name}"
        safe_filename = sanitize_filename(filename)
        
        logger.debug(f"📤 Загрузка на Яндекс.Диск: {remote_dir}/{safe_filename}")
        success = await yandex.upload(final_path, remote_dir, safe_filename)
        
        if success and compress_info:
            logger.info(f"   {compress_info}")
        
        logger.info(f"✅ Загружено: {filename}")
        return success, 1 if success else 0
        
    except Exception as e:
        logger.error(f"❌ Ошибка обработки: {e}", exc_info=True)
        return False, 0
    
    finally:
        for f in temp_files:
            try:
                if os.path.exists(f):
                    os.unlink(f)
                    logger.debug(f"🗑️ Удален временный файл: {f}")
            except:
                pass
        logger.debug("="*50)

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
async def main():
    """Основная функция"""
    # Проверка настроек
    if not API_ID or not API_HASH or not TARGET_CHAT_ID or not YA_DISK_TOKEN:
        logger.error("❌ Не все переменные окружения установлены")
        logger.error(f"API_ID: {API_ID}")
        logger.error(f"API_HASH: {'есть' if API_HASH else 'нет'}")
        logger.error(f"TARGET_CHAT_ID: {TARGET_CHAT_ID}")
        logger.error(f"YA_DISK_TOKEN: {'есть' if YA_DISK_TOKEN else 'нет'}")
        return 1
    
    # Проверяем наличие сессии
    if not STRING_SESSION and not PHONE_NUMBER:
        logger.error("❌ Нужна либо STRING_SESSION, либо PHONE_NUMBER")
        return 1
    
    # Загружаем прогресс
    progress = load_json(PROGRESS_FILE, {"last_id": 0, "total": 0})
    topic_cache = load_json(TOPIC_CACHE_FILE, {})
    last_id = progress.get("last_id", 0)
    total = progress.get("total", 0)
    
    logger.info("🚀 Запуск MTProto бэкапа")
    logger.info(f"📊 Прогресс: последний ID {last_id}, всего файлов {total}")
    
    # Подключение к Telegram
    tg_client = TelegramDownloader(
        api_id=API_ID, 
        api_hash=API_HASH,
        session_string=STRING_SESSION
    )
    
    try:
        await tg_client.connect()
        
        # Получаем чат
        chat = await tg_client.get_chat(TARGET_CHAT_ID)
        chat_id = chat.id
        chat_title = getattr(chat, 'title', str(chat_id))
        logger.info(f"✅ Чат: {chat_title} (ID: {chat_id})")
        
        # Подключение к Яндекс.Диску
        async with YandexUploader(YA_DISK_TOKEN, YA_DISK_PATH) as yandex:
            
            processed = 0
            
            logger.info(f"📥 Поиск новых сообщений (с ID > {last_id})")
            
            # Получаем сообщения
            messages = await tg_client.get_messages(chat_id, min_id=last_id)
            
            for message in messages:
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
