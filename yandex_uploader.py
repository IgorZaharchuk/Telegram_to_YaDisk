"""
Загрузка файлов на Яндекс.Диск
С поддержкой проверки существования файлов (без создания дублей)
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
    
    async def check_file_exists(self, remote_dir: str, filename: str) -> bool:
        """
        Проверка существования файла на Яндекс.Диске
        :param remote_dir: Удалённая папка
        :param filename: Имя файла
        :return: True если файл существует
        """
        try:
            remote_path = f"{remote_dir}/{filename}"
            exists = await self.client.exists(remote_path)
            if exists:
                logger.debug(f"✅ Файл уже существует: {remote_path}")
            return exists
        except Exception as e:
            logger.debug(f"⚠️ Ошибка при проверке файла: {e}")
            return False
    
    async def upload(self, local_path: str, remote_dir: str, filename: str) -> bool:
        """
        Загрузка файла на Яндекс.Диск
        :param local_path: Путь к локальному файлу
        :param remote_dir: Удалённая папка
        :param filename: Имя файла
        :return: Успех загрузки
        """
        try:
            # Создаём папку если нужно
            if not await self.client.exists(remote_dir):
                await self.client.mkdir(remote_dir)
                logger.debug(f"✅ Создана папка: {remote_dir}")
            
            remote_path = f"{remote_dir}/{filename}"
            
            # Проверяем, существует ли уже файл (еще раз для надежности)
            if await self.client.exists(remote_path):
                logger.info(f"⏭️ Файл уже существует, пропускаем: {remote_path}")
                return True  # Считаем успешным, так как файл уже есть
            
            # Загружаем файл
            with open(local_path, 'rb') as f:
                await self.client.upload(f, remote_path)
            
            logger.info(f"✅ Загружено: {remote_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки: {e}")
            return False
