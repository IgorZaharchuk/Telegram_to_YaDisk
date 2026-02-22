"""
Модуль загрузки на Яндекс.Диск - самодостаточный
"""

import os
from dataclasses import dataclass
from typing import Optional
import yadisk
import logging

logger = logging.getLogger(__name__)

@dataclass
class UploadResult:
    """Результат загрузки на Яндекс.Диск"""
    success: bool
    status: str  # 'uploaded', 'skipped', 'error'
    remote_path: Optional[str]
    message: str

class YandexUploader:
    def __init__(self, config: dict):
        """
        :param config: Словарь с настройками:
            - token: str
            - base_path: str (default: '/mtproto_backup')
        """
        self.token = config['token']
        self.base_path = config.get('base_path', '/mtproto_backup')
        self.client = None
    
    async def connect(self) -> bool:
        """Подключение к Яндекс.Диску"""
        try:
            self.client = yadisk.AsyncClient(token=self.token)
            await self.client.__aenter__()
            
            # Создаем корневую папку
            if not await self.client.exists(self.base_path):
                await self.client.mkdir(self.base_path)
                logger.info(f"✅ Создана корневая папка {self.base_path}")
            
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к Яндекс.Диску: {e}")
            return False
    
    async def disconnect(self):
        """Отключение от Яндекс.Диска"""
        if self.client:
            await self.client.__aexit__(None, None, None)
            logger.info("🔒 Отключено от Яндекс.Диска")
    
    async def _ensure_dir_recursive(self, remote_dir: str) -> bool:
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
            logger.error(f"❌ Ошибка создания папок: {e}")
            return False
    
    async def _file_exists_listdir(self, remote_dir: str, filename: str) -> bool:
        """
        Проверка существования файла через listdir()
        Самый надежный метод
        """
        try:
            if not await self.client.exists(remote_dir):
                return False
            
            files = []
            async for item in self.client.listdir(remote_dir):
                if item['type'] == 'file':
                    files.append(item['name'])
            
            # Проверяем точное совпадение имени файла, нормализуя возможные варианты
            normalized_filename = filename.encode('utf-8').decode('utf-8')
            for existing_file in files:
                existing_normalized = existing_file.encode('utf-8').decode('utf-8')
                if normalized_filename == existing_normalized:
                    return True
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка проверки файла: {e}")
            return False
    
    async def upload_file(self, local_path: str, remote_dir: str, filename: str) -> UploadResult:
        """
        Загрузка файла на Яндекс.Диск с проверкой через listdir()
        Полностью самодостаточный метод
        """
        try:
            remote_path = f"{remote_dir}/{filename}"
            
            # 1. Создаем папки
            if not await self._ensure_dir_recursive(remote_dir):
                return UploadResult(
                    success=False,
                    status='error',
                    remote_path=None,
                    message='Не удалось создать папки'
                )
            
            # 2. Проверяем существование файла (только listdir!)
            if await self._file_exists_listdir(remote_dir, filename):
                logger.info(f"⏭️ Файл уже существует: {remote_path}")
                return UploadResult(
                    success=True,
                    status='skipped',
                    remote_path=remote_path,
                    message='Файл уже существует'
                )
            
            # 3. Загружаем файл
            logger.info(f"📤 Загрузка: {remote_path}")
            with open(local_path, 'rb') as f:
                await self.client.upload(f, remote_path, overwrite=False)
            
            logger.info(f"✅ Загружено: {remote_path}")
            return UploadResult(
                success=True,
                status='uploaded',
                remote_path=remote_path,
                message='Успешно загружено'
            )
            
        except yadisk.exceptions.PathExistsError:
            # На всякий случай, если API вернет ошибку
            logger.info(f"⏭️ Файл уже существует (PathExistsError): {remote_path}")
            return UploadResult(
                success=True,
                status='skipped',
                remote_path=remote_path,
                message='Файл уже существует'
            )
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки: {e}")
            return UploadResult(
                success=False,
                status='error',
                remote_path=None,
                message=str(e)
            )
