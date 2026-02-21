"""
Загрузка файлов на Яндекс.Диск
Исправленная версия с правильной обработкой папок
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
        try:
            if not await self.client.exists(self.base_path):
                await self.client.mkdir(self.base_path)
                logger.info(f"✅ Создана корневая папка {self.base_path}")
        except Exception as e:
            logger.error(f"⚠️ Ошибка при создании корневой папки: {e}")
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Выход из контекстного менеджера"""
        await self.client.__aexit__(exc_type, exc_val, exc_tb)
    
    async def _ensure_dir(self, remote_dir: str) -> bool:
        """
        Убедиться, что папка существует (создать если нет)
        :param remote_dir: Путь к папке
        :return: Успех операции
        """
        try:
            if not await self.client.exists(remote_dir):
                await self.client.mkdir(remote_dir)
                logger.info(f"✅ Создана папка: {remote_dir}")
                return True
            return True
        except Exception as e:
            logger.error(f"❌ Не удалось создать папку {remote_dir}: {e}")
            return False
    
    async def _get_unique_path(self, remote_dir: str, filename: str) -> str:
        """
        Получение уникального пути (если файл уже существует)
        :param remote_dir: Удалённая папка
        :param filename: Имя файла
        :return: Уникальный путь
        """
        remote_path = f"{remote_dir}/{filename}"
        
        try:
            if not await self.client.exists(remote_path):
                return remote_path
        except Exception:
            return remote_path  # Если ошибка проверки, пробуем загрузить
        
        base, ext = os.path.splitext(filename)
        counter = 1
        while True:
            new_path = f"{remote_dir}/{base}_{counter}{ext}"
            try:
                if not await self.client.exists(new_path):
                    return new_path
            except Exception:
                return new_path
            counter += 1
            if counter > 1000:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                return f"{remote_dir}/{base}_{timestamp}{ext}"
    
    async def upload(self, local_path: str, remote_dir: str, filename: str) -> bool:
        """
        Загрузка файла на Яндекс.Диск
        :param local_path: Путь к локальному файлу
        :param remote_dir: Удалённая папка
        :param filename: Имя файла
        :return: Успех загрузки
        """
        try:
            # Убеждаемся, что папка существует
            if not await self._ensure_dir(remote_dir):
                # Если не удалось создать папку, пробуем загрузить напрямую
                logger.warning(f"⚠️ Не удалось создать папку, пробую загрузить в корень")
                remote_dir = self.base_path
            
            # Получаем уникальный путь для файла
            remote_path = await self._get_unique_path(remote_dir, filename)
            
            # Загружаем файл
            with open(local_path, 'rb') as f:
                await self.client.upload(f, remote_path)
            
            logger.info(f"✅ Загружено: {os.path.basename(remote_path)}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки: {e}")
            return False
