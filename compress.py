"""
Модуль сжатия медиафайлов - самодостаточный
"""

import os
import shutil
import asyncio
import subprocess
from dataclasses import dataclass
from typing import Tuple, Optional
from PIL import Image
import logging

logger = logging.getLogger(__name__)

@dataclass
class CompressionResult:
    """Результат сжатия"""
    output_path: str
    info: str
    original_size: int
    compressed_size: int
    ratio: float

class MediaCompressor:
    def __init__(self, config: dict = None):
        """
        :param config: Словарь с настройками:
            - image_quality: int (default: 92)
            - max_image_size: int (default: 3840)
            - image_min_size_kb: int (default: 100)
            - video_crf: int (default: 22)
            - video_preset: str (default: 'fast')
            - video_min_size_mb: int (default: 15)
            - video_timeout: int (default: 300)
        """
        self.config = config or {}
        self.image_quality = self.config.get('image_quality', 92)
        self.max_image_size = self.config.get('max_image_size', 3840)
        self.image_min_size_kb = self.config.get('image_min_size_kb', 100)
        self.video_crf = self.config.get('video_crf', 22)
        self.video_preset = self.config.get('video_preset', 'fast')
        self.video_min_size_mb = self.config.get('video_min_size_mb', 15)
        self.video_timeout = self.config.get('video_timeout', 300)
        
        self.ffmpeg_path = shutil.which('ffmpeg')
        if not self.ffmpeg_path:
            logger.warning("⚠️ FFmpeg не найден, видео не будут сжиматься")
    
    async def process(self, input_path: str, file_type: str) -> Tuple[str, Optional[CompressionResult]]:
        """
        Обработка файла (сжатие если нужно)
        :return: (выходной_путь, результат_сжатия или None)
        """
        if file_type in ['photo', 'image']:
            return await self._compress_image(input_path)
        elif file_type == 'video':
            return await self._compress_video(input_path)
        else:
            # Для документов возвращаем оригинал
            return input_path, None
    
    async def _compress_image(self, input_path: str) -> Tuple[str, Optional[CompressionResult]]:
        """Сжатие изображения"""
        try:
            file_size_kb = os.path.getsize(input_path) / 1024
            
            # Маленькие файлы не сжимаем
            if file_size_kb < self.image_min_size_kb:
                return input_path, CompressionResult(
                    output_path=input_path,
                    info=f"Пропущено ({file_size_kb:.1f}KB)",
                    original_size=int(file_size_kb * 1024),
                    compressed_size=int(file_size_kb * 1024),
                    ratio=1.0
                )
            
            # Создаем временный файл для сжатого изображения
            output_path = input_path + ".compressed.jpg"
            
            loop = asyncio.get_event_loop()
            
            def _compress():
                with Image.open(input_path) as img:
                    # Конвертация в RGB
                    if img.mode in ('RGBA', 'P', 'LA'):
                        background = Image.new('RGB', img.size, (255, 255, 255))
                        if img.mode == 'P':
                            img = img.convert('RGBA')
                        if img.mode in ('RGBA', 'LA'):
                            background.paste(img, mask=img.split()[-1])
                        else:
                            background.paste(img)
                        img = background
                    elif img.mode != 'RGB':
                        img = img.convert('RGB')
                    
                    # Ресайз
                    if self.max_image_size > 0 and max(img.size) > self.max_image_size:
                        img.thumbnail((self.max_image_size, self.max_image_size), Image.Resampling.LANCZOS)
                    
                    # Сохранение
                    img.save(output_path, 'JPEG', quality=self.image_quality, 
                            optimize=True, progressive=True)
            
            await loop.run_in_executor(None, _compress)
            
            orig_size = os.path.getsize(input_path)
            comp_size = os.path.getsize(output_path)
            
            if comp_size >= orig_size:
                # Если сжатие не помогло, оставляем оригинал
                os.unlink(output_path)
                return input_path, CompressionResult(
                    output_path=input_path,
                    info=f"Оригинал ({file_size_kb:.1f}KB)",
                    original_size=orig_size,
                    compressed_size=orig_size,
                    ratio=1.0
                )
            
            saved = (orig_size - comp_size) / 1024
            return output_path, CompressionResult(
                output_path=output_path,
                info=f"Сжато: {orig_size/1024:.1f}KB → {comp_size/1024:.1f}KB (-{saved:.1f}KB)",
                original_size=orig_size,
                compressed_size=comp_size,
                ratio=comp_size / orig_size
            )
            
        except Exception as e:
            logger.error(f"Ошибка сжатия изображения: {e}")
            return input_path, None
    
    async def _compress_video(self, input_path: str) -> Tuple[str, Optional[CompressionResult]]:
        """Сжатие видео"""
        if not self.ffmpeg_path:
            return input_path, None
        
        try:
            file_size_mb = os.path.getsize(input_path) / (1024 * 1024)
            
            if file_size_mb < self.video_min_size_mb:
                return input_path, CompressionResult(
                    output_path=input_path,
                    info=f"Пропущено ({file_size_mb:.1f}MB)",
                    original_size=int(file_size_mb * 1024 * 1024),
                    compressed_size=int(file_size_mb * 1024 * 1024),
                    ratio=1.0
                )
            
            output_path = input_path + ".compressed.mp4"
            
            cmd = [
                self.ffmpeg_path, '-i', input_path,
                '-c:v', 'libx265', '-crf', str(self.video_crf), '-preset', self.video_preset,
                '-c:a', 'aac', '-b:a', '128k',
                '-movflags', '+faststart', '-y', output_path
            ]
            
            loop = asyncio.get_event_loop()
            
            def _compress():
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    stdout, stderr = process.communicate(timeout=self.video_timeout)
                    return process.returncode
                except subprocess.TimeoutExpired:
                    process.kill()
                    raise TimeoutError("FFmpeg timeout")
            
            try:
                return_code = await loop.run_in_executor(None, _compress)
                
                if return_code != 0:
                    return input_path, None
                
                orig_size = os.path.getsize(input_path)
                comp_size = os.path.getsize(output_path)
                
                if comp_size >= orig_size:
                    os.unlink(output_path)
                    return input_path, CompressionResult(
                        output_path=input_path,
                        info=f"Оригинал ({file_size_mb:.1f}MB)",
                        original_size=orig_size,
                        compressed_size=orig_size,
                        ratio=1.0
                    )
                
                saved = (orig_size - comp_size) / (1024 * 1024)
                return output_path, CompressionResult(
                    output_path=output_path,
                    info=f"Сжато: {orig_size/1024/1024:.1f}MB → {comp_size/1024/1024:.1f}MB (-{saved:.1f}MB)",
                    original_size=orig_size,
                    compressed_size=comp_size,
                    ratio=comp_size / orig_size
                )
                
            except TimeoutError:
                logger.warning("Таймаут сжатия видео")
                return input_path, None
                
        except Exception as e:
            logger.error(f"Ошибка сжатия видео: {e}")
            return input_path, None
