"""
Загрузка файлов на Яндекс.Диск
Упрощенная версия - загружаем с overwrite=False
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
    
    async def upload(self, local_path: str, remote_dir: str, filename: str) -> bool:
        """
        Загрузка файла на Яндекс.Диск
        Просто создаем папки и загружаем с overwrite=False
        """
        try:
            # Создаем структуру папок
            if not await self.ensure_dir_recursive(remote_dir):
                logger.error(f"❌ Не удалось создать папки {remote_dir}")
                return False
            
            remote_path = f"{remote_dir}/{filename}"
            
            # Пытаемся загрузить с overwrite=False (не перезаписывать)
            with open(local_path, 'rb') as f:
                await self.client.upload(f, remote_path, overwrite=False)
            
            logger.info(f"✅ Загружено: {remote_path}")
            return True
            
        except yadisk.exceptions.PathExistsError:
            # Файл уже существует - это нормально, пропускаем
            logger.info(f"⏭️ Файл уже существует, пропускаем: {remote_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки: {e}")
            return False
