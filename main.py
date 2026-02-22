#!/usr/bin/env python3
"""
Telegram MTProto Backup to Yandex Disk
Главный файл - только связующая логика
"""

import os
import sys
import asyncio
import logging

from telegram_client import TelegramDownloader
from compress import MediaCompressor
from yandex_uploader import YandexUploader
from progress import ProgressTracker

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('backup.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

async def process_media(download_result, compressor, yandex):
    """Обработка одного медиафайла - вся логика в одном месте"""
    # 1. Сжатие (компрессор сам решает)
    final_path, compress_info = await compressor.process(
        download_result.local_path,
        download_result.file_type
    )
    
    # 2. Загрузка (яндекс сам проверяет дубликаты)
    remote_dir = f"{yandex.base_path}/{download_result.chat_folder}/{download_result.topic_folder}"
    upload_result = await yandex.upload_file(
        local_path=final_path,
        remote_dir=remote_dir,
        filename=download_result.original_filename
    )
    
    # 3. Очистка временных файлов
    if final_path != download_result.local_path:
        try:
            os.unlink(download_result.local_path)
            os.unlink(final_path)
        except:
            pass
    
    return upload_result.status, compress_info

async def main():
    # Конфигурация из переменных окружения
    config = {
        'api_id': int(os.getenv("API_ID")),
        'api_hash': os.getenv("API_HASH"),
        'session_string': os.getenv("STRING_SESSION"),
        'token': os.getenv("YA_DISK_TOKEN"),
        'base_path': os.getenv("YA_DISK_PATH", "/mtproto_backup")
    }
    
    target_chat = int(os.getenv("TARGET_CHAT_ID"))
    max_files = int(os.getenv("MAX_FILES_PER_RUN", "50"))
    
    # Инициализация модулей
    tg = TelegramDownloader(config)
    yandex = YandexUploader(config)
    compressor = MediaCompressor()
    progress = ProgressTracker()
    
    # Подключение
    if not await tg.connect():
        logger.error("❌ Не удалось подключиться к Telegram")
        return 1
    
    if not await yandex.connect():
        logger.error("❌ Не удалось подключиться к Яндекс.Диску")
        await tg.disconnect()
        return 1
    
    try:
        # Поиск чата
        chat = await tg.find_chat(target_chat)
        logger.info(f"✅ Чат: {chat.title}")
        
        # Загрузка всех тем через raw API
        all_topics = await tg.load_all_topics(chat.id)
        logger.info(f"📚 Загружено {len(all_topics)} тем")
        
        # Получение новых сообщений
        messages = await tg.get_new_messages(chat.id, progress.progress.last_id, limit=200)
        logger.info(f"📨 Получено {len(messages)} новых сообщений")
        
        processed = 0
        for msg in messages:
            if processed >= max_files:
                break
            
            # Скачивание (телеграм сам определяет тему)
            download_result = await tg.download_media_with_topic(msg)
            if not download_result:
                # Если не удалось скачать - все равно обновляем прогресс
                progress.update(msg.id, 'error')
                logger.warning(f"⚠️ Не удалось скачать сообщение {msg.id}")
                continue
            
            # Обработка
            status, compress_info = await process_media(download_result, compressor, yandex)
            
            # Обновление прогресса (ВСЕГДА, даже для skipped)
            progress.update(msg.id, status)
            
            if compress_info and compress_info.info:
                logger.info(f"   {compress_info.info}")
            logger.info(f"{'✅' if status=='uploaded' else '⏭️'} {status}: {download_result.original_filename}")
            
            processed += 1
            await asyncio.sleep(float(os.getenv("RATE_LIMIT_DELAY", "1.0")))
        
        logger.info(f"🏁 Завершено. {progress.get_summary()}")
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        await tg.disconnect()
        await yandex.disconnect()
    
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
