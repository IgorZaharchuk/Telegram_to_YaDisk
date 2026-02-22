"""
Загрузка файлов на Яндекс.Диск
С правильной проверкой через listdir()
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
    
    async def file_exists(self, remote_dir: str, filename: str) -> bool:
        """
        Проверка существования файла на Яндекс.Диске через listdir()
        Более надежный метод, чем exists()
        :param remote_dir: Удалённая папка (полный путь)
        :param filename: Имя файла
        :return: True если файл существует
        """
        try:
            # Проверяем, существует ли папка
            if not await self.client.exists(remote_dir):
                logger.debug(f"📁 Папка не существует: {remote_dir}")
                return False
            
            # Получаем список файлов в папке
            files = []
            async for item in self.client.listdir(remote_dir):
                if item['type'] == 'file':
                    files.append(item['name'])
            
            logger.debug(f"📋 Файлы в папке: {files}")
            exists = filename in files
            logger.debug(f"🔍 Файл {filename}: {'НАЙДЕН' if exists else 'НЕ НАЙДЕН'}")
            return exists
            
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке файла: {e}")
            return False
    
    async def upload(self, local_path: str, remote_dir: str, filename: str) -> bool:
        """
        Загрузка файла на Яндекс.Диск с надежной проверкой
        :param local_path: Путь к локальному файлу
        :param remote_dir: Удалённая папка (полный путь)
        :param filename: Имя файла
        :return: Успех загрузки
        """
        try:
            # Создаем структуру папок
            if not await self.ensure_dir_recursive(remote_dir):
                logger.error(f"❌ Не удалось создать папки {remote_dir}")
                return False
            
            # Проверяем, есть ли уже такой файл
            if await self.file_exists(remote_dir, filename):
                logger.info(f"⏭️ Файл уже существует, пропускаем: {remote_dir}/{filename}")
                return True
            
            # Загружаем файл
            remote_path = f"{remote_dir}/{filename}"
            logger.info(f"📤 Загрузка нового файла: {remote_path}")
            
            with open(local_path, 'rb') as f:
                await self.client.upload(f, remote_path, overwrite=False)
            
            logger.info(f"✅ Загружено: {remote_path}")
            return True
            
        except yadisk.exceptions.PathExistsError:
            logger.info(f"⏭️ Файл уже существует (PathExistsError): {remote_dir}/{filename}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки: {e}")
            return False
