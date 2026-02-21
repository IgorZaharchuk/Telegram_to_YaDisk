"""
Загрузка файлов на Яндекс.Диск
Исправленная версия с правильным созданием папок
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
        
        # Создаём корневую папку если нет
        try:
            if not await self.client.exists(self.base_path):
                await self.client.mkdir(self.base_path)
                logger.info(f"✅ Создана корневая папка {self.base_path}")
        except Exception as e:
            logger.error(f"⚠️ Ошибка при создании корневой папки: {e}")
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.__aexit__(exc_type, exc_val, exc_tb)
    
    async def _create_folder_recursive(self, folder_path: str) -> bool:
        """Создание папки рекурсивно (все вложенные папки)"""
        try:
            # Разбиваем путь на части
            parts = folder_path.strip('/').split('/')
            current_path = ""
            
            for part in parts:
                if not part:
                    continue
                current_path = f"{current_path}/{part}" if current_path else f"/{part}"
                
                # Проверяем существование каждой папки
                try:
                    if not await self.client.exists(current_path):
                        await self.client.mkdir(current_path)
                        logger.debug(f"✅ Создана папка: {current_path}")
                except Exception as e:
                    # Если папка уже существует (конфликт), игнорируем
                    if "DiskPathPointsToExistentDirectoryError" in str(e):
                        continue
                    else:
                        logger.warning(f"⚠️ Ошибка при создании {current_path}: {e}")
            
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка при создании папок: {e}")
            return False
    
    async def upload(self, local_path: str, remote_dir: str, filename: str) -> bool:
        """
        Загрузка файла на Яндекс.Диск
        :param local_path: Путь к локальному файлу
        :param remote_dir: Удалённая папка (полный путь)
        :param filename: Имя файла
        :return: Успех загрузки
        """
        try:
            # Создаём все необходимые папки
            await self._create_folder_recursive(remote_dir)
            
            # Формируем полный путь к файлу
            remote_path = f"{remote_dir}/{filename}"
            
            # Проверяем, существует ли уже файл
            try:
                if await self.client.exists(remote_path):
                    # Если существует, добавляем счётчик
                    base, ext = os.path.splitext(filename)
                    counter = 1
                    while await self.client.exists(f"{remote_dir}/{base}_{counter}{ext}"):
                        counter += 1
                    remote_path = f"{remote_dir}/{base}_{counter}{ext}"
            except Exception:
                # Если ошибка проверки, пробуем загрузить как есть
                pass
            
            # Загружаем файл
            with open(local_path, 'rb') as f:
                await self.client.upload(f, remote_path)
            
            logger.info(f"✅ Загружено: {remote_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки: {e}")
            # Пробуем загрузить в корень как запасной вариант
            try:
                root_path = f"{self.base_path}/{filename}"
                with open(local_path, 'rb') as f:
                    await self.client.upload(f, root_path)
                logger.info(f"✅ Загружено в корень: {root_path}")
                return True
            except Exception as e2:
                logger.error(f"❌ Критическая ошибка загрузки: {e2}")
                return False
