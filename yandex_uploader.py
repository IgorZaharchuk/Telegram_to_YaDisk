"""
Загрузка файлов на Яндекс.Диск
С правильной проверкой существования файлов и созданием папок
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
    
    async def ensure_dir_recursive(self, remote_dir: str) -> bool:
        """
        Создание папки рекурсивно (все вложенные папки)
        :param remote_dir: Полный путь к папке (например: /mtproto_backup/tg2ya/Весна_2022)
        :return: Успех операции
        """
        try:
            # Разбиваем путь на части
            path_parts = remote_dir.strip('/').split('/')
            current_path = ""
            
            for part in path_parts:
                if not part:
                    continue
                    
                # Формируем текущий путь
                if current_path:
                    current_path = f"{current_path}/{part}"
                else:
                    current_path = f"/{part}"
                
                # Создаем папку если её нет
                if not await self.client.exists(current_path):
                    await self.client.mkdir(current_path)
                    logger.debug(f"✅ Создана папка: {current_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка при создании папок: {e}")
            return False
    
    async def check_file_exists(self, remote_dir: str, filename: str) -> bool:
        """
        Проверка существования файла на Яндекс.Диске
        ВНИМАНИЕ: Только проверка, без создания папок!
        :param remote_dir: Удалённая папка (полный путь)
        :param filename: Имя файла
        :return: True если файл существует
        """
        try:
            remote_path = f"{remote_dir}/{filename}"
            
            # Проверяем существование файла (НЕ создаем папки!)
            exists = await self.client.exists(remote_path)
            
            if exists:
                logger.debug(f"✅ Файл уже существует: {remote_path}")
            else:
                logger.debug(f"📄 Файл не найден, будет загружен: {remote_path}")
                
            return exists
            
        except Exception as e:
            logger.debug(f"⚠️ Ошибка при проверке файла: {e}")
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
            remote_path = f"{remote_dir}/{filename}"
            
            # ШАГ 1: Проверяем существование файла (без создания папок!)
            if await self.client.exists(remote_path):
                logger.info(f"⏭️ Файл уже существует, пропускаем: {remote_path}")
                return True  # Считаем успешным, так как файл уже есть
            
            # ШАГ 2: Файла нет - создаем структуру папок
            logger.debug(f"📂 Файл не найден, создаю структуру папок: {remote_dir}")
            if not await self.ensure_dir_recursive(remote_dir):
                logger.error(f"❌ Не удалось создать структуру папок {remote_dir}")
                return False
            
            # ШАГ 3: Загружаем файл
            logger.info(f"📤 Загрузка нового файла: {remote_path}")
            with open(local_path, 'rb') as f:
                await self.client.upload(f, remote_path)
            
            logger.info(f"✅ Загружено: {remote_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки: {e}")
            return False
