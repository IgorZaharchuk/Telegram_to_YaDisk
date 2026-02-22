"""
Загрузка файлов на Яндекс.Диск
Только проверка через listdir() - никаких exists() для файлов
"""

import os
import yadisk
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class YandexUploader:
    def __init__(self, token: str, base_path: str = "/mtproto_backup"):
        self.client = yadisk.AsyncClient(token=token)
        self.base_path = base_path
    
    async def __aenter__(self):
        await self.client.__aenter__()
        if not await self.client.exists(self.base_path):
            await self.client.mkdir(self.base_path)
            logger.info(f"✅ Создана корневая папка {self.base_path}")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.__aexit__(exc_type, exc_val, exc_tb)
    
    async def ensure_dir_recursive(self, remote_dir: str) -> bool:
        """Создание всех вложенных папок"""
        try:
            path_parts = remote_dir.strip('/').split('/')
            current_path = ""
            
            for part in path_parts:
                if not part:
                    continue
                if current_path:
                    current_path = f"{current_path}/{part}"
                else:
                    current_path = f"/{part}"
                
                if not await self.client.exists(current_path):
                    await self.client.mkdir(current_path)
                    logger.debug(f"✅ Создана папка: {current_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка при создании папок: {e}")
            return False
    
    async def check_file_exists_listdir(self, remote_dir: str, filename: str) -> bool:
        """
        ЕДИНСТВЕННЫЙ метод проверки - через listdir()
        """
        try:
            # Проверяем, существует ли папка
            if not await self.client.exists(remote_dir):
                logger.debug(f"📁 Папки нет, файла точно нет")
                return False
            
            # Получаем список файлов в папке
            files = []
            async for item in self.client.listdir(remote_dir):
                if item['type'] == 'file':
                    files.append(item['name'])
            
            logger.debug(f"📋 Файлы в папке: {files}")
            exists = filename in files
            logger.debug(f"🔍 Файл {filename}: {'✅ ЕСТЬ' if exists else '❌ НЕТ'}")
            return exists
            
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке файла: {e}")
            return False
    
    async def upload(self, local_path: str, remote_dir: str, filename: str) -> bool:
        """
        Загрузка файла с проверкой ТОЛЬКО через listdir()
        """
        try:
            # Создаем структуру папок
            if not await self.ensure_dir_recursive(remote_dir):
                logger.error(f"❌ Не удалось создать папки {remote_dir}")
                return False
            
            # Проверяем через listdir
            if await self.check_file_exists_listdir(remote_dir, filename):
                logger.info(f"⏭️ Файл уже существует (по listdir), пропускаем: {remote_dir}/{filename}")
                return True
            
            # Загружаем файл
            remote_path = f"{remote_dir}/{filename}"
            logger.info(f"📤 Загрузка нового файла: {remote_path}")
            
            with open(local_path, 'rb') as f:
                await self.client.upload(f, remote_path, overwrite=False)
            
            logger.info(f"✅ Загружено: {remote_path}")
            return True
            
        except yadisk.exceptions.PathExistsError:
            # На случай, если API вернет ошибку
            logger.info(f"⏭️ Файл уже существует (PathExistsError), пропускаем: {remote_dir}/{filename}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки: {e}")
            return False
