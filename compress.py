"""
Сжатие фото и видео
Полностью скопировано из первого рабочего проекта
"""

import os
import shutil
import asyncio
import subprocess
from PIL import Image
import logging

logger = logging.getLogger(__name__)

# Настройки сжатия
IMAGE_QUALITY = 92
MAX_IMAGE_SIZE = 3840
IMAGE_MIN_SIZE_KB = 100

VIDEO_CRF = 22
VIDEO_PRESET = 'fast'
VIDEO_MIN_SIZE_MB = 15
VIDEO_TIMEOUT = 300

async def optimize_image(input_path: str, output_path: str) -> tuple[bool, str]:
    """
    Оптимизация изображения
    :param input_path: Путь к исходному файлу
    :param output_path: Путь для сохранения результата
    :return: (успех, информация о сжатии)
    """
    try:
        file_size_kb = os.path.getsize(input_path) / 1024
        
        # Маленькие файлы не сжимаем
        if file_size_kb < IMAGE_MIN_SIZE_KB:
            shutil.copy2(input_path, output_path)
            return True, f"Пропущено ({file_size_kb:.1f}KB)"
        
        # Сжимаем в отдельном потоке
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
                
                # Ресайз если нужно
                if MAX_IMAGE_SIZE > 0 and max(img.size) > MAX_IMAGE_SIZE:
                    img.thumbnail((MAX_IMAGE_SIZE, MAX_IMAGE_SIZE), Image.Resampling.LANCZOS)
                
                # Сохранение
                img.save(output_path, 'JPEG', quality=IMAGE_QUALITY, 
                        optimize=True, progressive=True)
        
        await loop.run_in_executor(None, _compress)
        
        orig_size = os.path.getsize(input_path)
        opt_size = os.path.getsize(output_path)
        
        # Если сжатие не помогло, оставляем оригинал
        if opt_size >= orig_size:
            shutil.copy2(input_path, output_path)
            return True, f"Оригинал ({file_size_kb:.1f}KB)"
        
        saved = (orig_size - opt_size) / 1024
        return True, f"Сжато: {orig_size/1024:.1f}KB → {opt_size/1024:.1f}KB (-{saved:.1f}KB)"
        
    except Exception as e:
        logger.error(f"Ошибка сжатия изображения: {e}")
        shutil.copy2(input_path, output_path)
        return True, "Копия (ошибка)"

async def compress_video(input_path: str, output_path: str) -> tuple[bool, str]:
    """
    Сжатие видео через ffmpeg
    :param input_path: Путь к исходному файлу
    :param output_path: Путь для сохранения результата
    :return: (успех, информация о сжатии)
    """
    try:
        # Проверяем наличие ffmpeg
        ffmpeg = shutil.which('ffmpeg')
        if not ffmpeg:
            shutil.copy2(input_path, output_path)
            return True, "Копия (нет FFmpeg)"
        
        file_size_mb = os.path.getsize(input_path) / (1024 * 1024)
        
        # Маленькие видео не сжимаем
        if file_size_mb < VIDEO_MIN_SIZE_MB:
            shutil.copy2(input_path, output_path)
            return True, f"Пропущено ({file_size_mb:.1f}MB)"
        
        cmd = [
            ffmpeg, '-i', input_path,
            '-c:v', 'libx265', '-crf', str(VIDEO_CRF), '-preset', VIDEO_PRESET,
            '-c:a', 'aac', '-b:a', '128k',
            '-movflags', '+faststart', '-y', output_path
        ]
        
        # Запускаем ffmpeg в отдельном потоке
        loop = asyncio.get_event_loop()
        
        def _compress():
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                stdout, stderr = process.communicate(timeout=VIDEO_TIMEOUT)
                return process.returncode
            except subprocess.TimeoutExpired:
                process.kill()
                raise TimeoutError("FFmpeg timeout")
        
        try:
            return_code = await loop.run_in_executor(None, _compress)
            
            if return_code != 0:
                shutil.copy2(input_path, output_path)
                return True, "Копия (ошибка FFmpeg)"
            
            orig_size = os.path.getsize(input_path)
            comp_size = os.path.getsize(output_path)
            
            if comp_size >= orig_size:
                shutil.copy2(input_path, output_path)
                return True, f"Оригинал ({file_size_mb:.1f}MB)"
            
            saved = (orig_size - comp_size) / (1024 * 1024)
            return True, f"Сжато: {orig_size/1024/1024:.1f}MB → {comp_size/1024/1024:.1f}MB (-{saved:.1f}MB)"
            
        except TimeoutError:
            logger.warning("Таймаут сжатия видео")
            shutil.copy2(input_path, output_path)
            return True, "Таймаут (копия)"
            
    except Exception as e:
        logger.error(f"Ошибка сжатия видео: {e}")
        shutil.copy2(input_path, output_path)
        return True, "Копия (ошибка)"
