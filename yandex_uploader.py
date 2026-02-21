"""
Загрузка файлов на Яндекс.Диск
Полностью скопировано из первого рабочего проекта
"""

import os
import yadisk
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class YandexUploader:
    def __init__(self, token: str, base_path: str = "/mtproto_backup"):
        """
        Инициализация загрузчика на Яндекс.Диск
        :param token: Токен доступа к Яндекс.Диску
        :param base_path: Базовая папка на диске
        """
        self.client = yadisk.AsyncClient(token=token)
        self.base_path = base_path
    
    async def __aenter__(self):
        """Вход в контекстный менеджер"""
        await self.client.__aenter__()
        
        # Создаём корневую папку если нет
        if not await self.client.exists(self.base_path):
            await self.client.mkdir(self.base_path)
            logger.info(f"✅ Создана корневая папка {self.base_path}")
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Выход из контекстного менеджера"""
        await self.client.__aexit__(exc_type, exc_val, exc_tb)
    
    async def _get_unique_path(self, remote_dir: str, filename: str) -> str:
        """
        Получение уникального пути (если файл уже существует)
        :param remote_dir: Удалённая папка
        :param filename: Имя файла
        :return: Уникальный путь
        """
        remote_path = f"{remote_dir}/{filename}"
        
        if not await self.client.exists(remote_path):
            return remote_path
        
        base, ext = os.path.splitext(filename)
        counter = 1
        while await self.client.exists(f"{remote_dir}/{base}_{counter}{ext}"):
            counter += 1
            if counter > 1000:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                return f"{remote_dir}/{base}_{timestamp}{ext}"
        
        return f"{remote_dir}/{base}_{counter}{ext}"
    
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
                logger.info(f"✅ Создана папка: {remote_dir}")
            
            # Получаем уникальный путь
            remote_path = await self._get_unique_path(remote_dir, filename)
            
            # Загружаем файл
            with open(local_path, 'rb') as f:
                await self.client.upload(f, remote_path)
            
            logger.info(f"✅ Загружено: {os.path.basename(remote_path)}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки: {e}")
            return False
