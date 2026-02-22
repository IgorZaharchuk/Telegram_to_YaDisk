"""
Модуль для работы с прогрессом
"""

import os
import json
from dataclasses import dataclass
from typing import Optional
import logging

logger = logging.getLogger(__name__)

@dataclass
class Progress:
    """Прогресс бэкапа"""
    last_id: int = 0
    total_files: int = 0
    uploaded: int = 0
    skipped: int = 0
    errors: int = 0

class ProgressTracker:
    def __init__(self, filepath: str = "progress.json"):
        self.filepath = filepath
        self.progress = self.load()
    
    def load(self) -> Progress:
        """Загрузка прогресса из файла"""
        try:
            if os.path.exists(self.filepath):
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return Progress(
                        last_id=data.get('last_id', 0),
                        total_files=data.get('total_files', 0),
                        uploaded=data.get('uploaded', 0),
                        skipped=data.get('skipped', 0),
                        errors=data.get('errors', 0)
                    )
        except Exception as e:
            logger.warning(f"⚠️ Ошибка загрузки прогресса: {e}")
        
        return Progress()
    
    def save(self):
        """Сохранение прогресса в файл"""
        try:
            # Сохраняем во временный файл
            temp_file = f"{self.filepath}.tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'last_id': self.progress.last_id,
                    'total_files': self.progress.total_files,
                    'uploaded': self.progress.uploaded,
                    'skipped': self.progress.skipped,
                    'errors': self.progress.errors
                }, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            
            # Атомарно заменяем старый файл
            os.replace(temp_file, self.filepath)
            logger.debug(f"💾 Прогресс сохранен: ID {self.progress.last_id}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения прогресса: {e}")
    
    def update(self, message_id: int, status: str):
        """
        Обновление прогресса после обработки сообщения
        ВАЖНО: last_id всегда увеличивается, даже если файл пропущен!
        """
        # Всегда обновляем last_id на максимальный обработанный ID
        if message_id > self.progress.last_id:
            self.progress.last_id = message_id
        
        self.progress.total_files += 1
        
        if status == 'uploaded':
            self.progress.uploaded += 1
        elif status == 'skipped':
            self.progress.skipped += 1
        elif status == 'error':
            self.progress.errors += 1
        
        self.save()
    
    def get_summary(self) -> str:
        """Получение сводки по прогрессу"""
        return (f"последний ID:{self.progress.last_id}, "
                f"всего:{self.progress.total_files}, "
                f"загружено:{self.progress.uploaded}, "
                f"пропущено:{self.progress.skipped}, "
                f"ошибок:{self.progress.errors}")
