#!/usr/bin/env python3
"""
Модуль сжатия медиафайлов - ПОЛНОСТЬЮ АВТОНОМНЫЙ
ВЕРСИЯ 0.17.2 — ИСПРАВЛЕНИЯ: REGEX ДЛЯ FFMPEG, ДЕЛЕНИЕ НА НОЛЬ
"""

__version__ = "0.17.2"

import os
import asyncio
import logging
import subprocess
import time
import shutil
import json
import re
import multiprocessing
import signal
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List, Callable, Set
from dataclasses import dataclass
from PIL import Image, ImageFile

try:
    from concurrent.futures.process import BrokenProcessPool
except ImportError:
    class BrokenProcessPool(Exception):
        """Заглушка для BrokenProcessPool."""
        pass

logging.getLogger("PIL").setLevel(logging.WARNING)
ImageFile.LOAD_TRUNCATED_IMAGES = True

logger = logging.getLogger(__name__)

CPU_COUNT: int = multiprocessing.cpu_count()
_PHOTO_PROCESS_POOL: Optional[ProcessPoolExecutor] = None
_PHOTO_POOL_LOCK: asyncio.Lock = asyncio.Lock()


async def _get_photo_pool(max_workers: int) -> ProcessPoolExecutor:
    """Возвращает или создаёт пул процессов для сжатия фото."""
    global _PHOTO_PROCESS_POOL
    async with _PHOTO_POOL_LOCK:
        if _PHOTO_PROCESS_POOL is None:
            _PHOTO_PROCESS_POOL = ProcessPoolExecutor(max_workers=max_workers)
            logger.info(f"📸 Process pool для фото создан (workers={max_workers})")
        return _PHOTO_PROCESS_POOL


async def _recreate_photo_pool(max_workers: int) -> ProcessPoolExecutor:
    """Пересоздаёт пул процессов при BrokenProcessPool."""
    global _PHOTO_PROCESS_POOL
    async with _PHOTO_POOL_LOCK:
        if _PHOTO_PROCESS_POOL:
            logger.warning("🔄 Пересоздание process pool после ошибки...")
            try:
                await asyncio.to_thread(_PHOTO_PROCESS_POOL.shutdown, wait=True)
            except Exception:
                pass
            finally:
                _PHOTO_PROCESS_POOL = None
        
        _PHOTO_PROCESS_POOL = ProcessPoolExecutor(max_workers=max_workers)
        logger.info(f"📸 Process pool пересоздан (workers={max_workers})")
        return _PHOTO_PROCESS_POOL


async def _shutdown_photo_pool() -> None:
    """Закрывает пул процессов для фото."""
    global _PHOTO_PROCESS_POOL
    if _PHOTO_PROCESS_POOL:
        logger.info("🛑 Закрытие process pool для фото...")
        try:
            await asyncio.to_thread(_PHOTO_PROCESS_POOL.shutdown, wait=True, cancel_futures=True)
        except Exception as e:
            logger.error(f"Ошибка при закрытии пула: {e}")
        finally:
            _PHOTO_PROCESS_POOL = None


@dataclass
class VideoInfo:
    """Информация о видеофайле."""
    codec: str
    codec_long: str
    width: int
    height: int
    duration: float
    bitrate: int
    fps: float
    
    @property
    def is_efficient(self) -> bool:
        """Проверяет, является ли кодек эффективным."""
        return any(c in self.codec.lower() for c in ('hevc', 'h265', 'av1', 'vp9'))
    
    def to_dict(self) -> dict:
        """Сериализует в словарь."""
        return {
            'codec': self.codec, 'codec_long': self.codec_long,
            'width': self.width, 'height': self.height,
            'duration': self.duration, 'bitrate': self.bitrate,
            'fps': self.fps, 'is_efficient': self.is_efficient
        }


@dataclass
class CompressionResult:
    """Результат сжатия файла."""
    success: bool
    original_path: str
    compressed_path: str
    original_size: int
    compressed_size: int
    saved_bytes: int
    saved_percent: float
    compression_type: str
    decision: str
    duration_sec: float
    video_info: Optional[dict] = None
    error: str = ""
    was_compressed: bool = False
    should_compress: bool = True


class FFmpegRunner:
    """Управляет запуском и остановкой процессов ffmpeg."""
    
    def __init__(self) -> None:
        """Инициализирует раннер ffmpeg."""
        self._active_processes: Dict[int, asyncio.subprocess.Process] = {}
        self._lock: asyncio.Lock = asyncio.Lock()
        self._shutdown_requested: bool = False
    
    def request_shutdown(self) -> None:
        """Запрашивает завершение всех процессов."""
        self._shutdown_requested = True
    
    @property
    def shutdown_requested(self) -> bool:
        """Проверяет, запрошено ли завершение."""
        return self._shutdown_requested
    
    async def add_process(self, process: asyncio.subprocess.Process) -> None:
        """Добавляет процесс в отслеживание."""
        async with self._lock:
            self._active_processes[process.pid] = process
    
    async def remove_process(self, pid: int) -> None:
        """Удаляет процесс из отслеживания."""
        async with self._lock:
            self._active_processes.pop(pid, None)
    
    async def stop_all(self) -> None:
        """Останавливает все запущенные процессы ffmpeg."""
        self._shutdown_requested = True
        async with self._lock:
            processes: List[asyncio.subprocess.Process] = list(self._active_processes.values())
            self._active_processes.clear()
        
        if not processes:
            return
        
        logger.info(f"🛑 Остановка {len(processes)} процессов ffmpeg...")
        
        for p in processes:
            if p.returncode is None:
                try:
                    if os.name == 'posix':
                        try:
                            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                    else:
                        p.kill()
                except Exception:
                    pass
        
        try:
            subprocess.run(['pkill', '-9', '-f', 'ffmpeg'], timeout=5, capture_output=True)
            subprocess.run(['pkill', '-9', '-f', 'cpulimit'], timeout=5, capture_output=True)
            subprocess.run(['pkill', '-9', '-f', 'x265'], timeout=5, capture_output=True)
            logger.info("🔪 Радикально убиты все процессы ffmpeg/cpulimit/x265")
        except Exception:
            pass
        
        logger.info("✅ Все процессы ffmpeg остановлены")
    
    async def run(self, cmd: List[str], file_path: str, total_duration: float,
                  progress_callback: Optional[Callable] = None, timeout: Optional[float] = None) -> Tuple[int, str, str]:
        """Запускает ffmpeg и отслеживает прогресс."""
        if os.name == 'posix':
            process: asyncio.subprocess.Process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True)
        else:
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        
        await self.add_process(process)
        logger.debug(f"🎬 Запущен ffmpeg PID={process.pid} для {os.path.basename(file_path)}")
        
        safe_duration: float = max(total_duration, 0.1)
        stderr_lines: List[str] = []
        start_time: float = time.time()
        last_progress: float = 0.0
        
        async def read_stderr() -> None:
            nonlocal last_progress
            try:
                while True:
                    line: bytes = await process.stderr.readline()
                    if not line:
                        break
                    line_str: str = line.decode('utf-8', errors='ignore').strip()
                    stderr_lines.append(line_str)
                    
                    if progress_callback and (match := re.search(r'time=(\d+):(\d{2}):(\d{2}\.\d+)', line_str)):
                        try:
                            h: float = float(match.group(1))
                            m: float = float(match.group(2))
                            s: float = float(match.group(3))
                            current: float = h * 3600 + m * 60 + s
                            percent: float = max(0.0, min(100.0, (current / safe_duration) * 100.0))
                            
                            if percent - last_progress >= 5 or percent >= 99:
                                speed_match: Optional[re.Match] = re.search(r'speed=([\d.]+)x', line_str)
                                speed: float = float(speed_match.group(1)) if speed_match else 0.0
                                eta: Optional[float] = None
                                if speed > 0.01 and 0 < percent < 99:
                                    eta = (100.0 - percent) * ((time.time() - start_time) / percent)
                                    if eta and eta > 86400:
                                        eta = None
                                await progress_callback(percent, speed, eta)
                                last_progress = percent
                        except Exception:
                            pass
            except Exception as e:
                logger.error(f"Ошибка чтения stderr: {e}")
        
        stderr_task: asyncio.Task = asyncio.create_task(read_stderr())
        
        try:
            if timeout:
                await asyncio.wait_for(process.wait(), timeout=timeout)
            else:
                await process.wait()
            await stderr_task
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            stderr_task.cancel()
            raise
        except asyncio.CancelledError:
            logger.info(f"🛑 Принудительное завершение ffmpeg PID={process.pid}")
            try:
                if os.name == 'posix':
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                else:
                    process.kill()
            except Exception:
                pass
            await process.wait()
            stderr_task.cancel()
            raise
        finally:
            await self.remove_process(process.pid)
        
        try:
            stdout, _ = await asyncio.wait_for(process.communicate(), timeout=5.0)
        except Exception:
            stdout = b""
        
        return process.returncode, '\n'.join(stderr_lines), stdout.decode('utf-8', errors='ignore') if stdout else ""


class Compressor:
    """Класс для сжатия фото и видео."""
    
    def __init__(self, config: Optional[dict] = None) -> None:
        """Инициализирует компрессор."""
        self.config: dict = config or {}
        
        self.min_photo_size_kb: int = self.config.get('min_photo_size_kb', 500)
        self.image_quality: int = self.config.get('image_quality', 92)
        self.max_image_dimension: int = self.config.get('max_image_dimension', 3840)
        self.photo_processes: int = self.config.get('photo_processes', 4)
        
        self.min_video_size_mb: int = self.config.get('min_video_size_mb', 15)
        self.min_video_duration: int = self.config.get('min_video_duration', 10)
        self.video_crf: int = self.config.get('video_crf', 23)
        self.video_preset: str = self.config.get('video_preset', 'veryfast')
        self.video_threads: int = self.config.get('video_threads', 2)
        self.skip_efficient: bool = self.config.get('skip_efficient_codecs', True)
        
        self.video_timeout_base: int = self.config.get('video_timeout_base', 60)
        self.video_timeout_per_mb: int = self.config.get('video_timeout_per_mb', 10)
        self.video_timeout_min: int = self.config.get('video_timeout_min', 120)
        self.video_timeout_max: int = self.config.get('video_timeout_max', 3600)
        
        self.use_cpulimit: bool = self.config.get('use_cpulimit', True)
        self.video_cpu_limit: int = self.config.get('video_cpu_limit', 80)
        self.low_priority: bool = self.config.get('low_priority', True)
        
        self.convert_heic_enabled: bool = self.config.get('convert_heic', True)
        self.verbose: bool = self.config.get('verbose', False)
        
        file_types: Dict[str, List[str]] = self.config.get('file_types', {})
        self.photo_extensions: Set[str] = set(file_types.get('photo', []))
        self.video_extensions: Set[str] = set(file_types.get('video', []))
        
        self.ffmpeg: bool = shutil.which('ffmpeg') is not None
        self.ffprobe: bool = shutil.which('ffprobe') is not None
        self.heif_convert: bool = shutil.which('heif-convert') is not None
        
        self._ffmpeg_runner: FFmpegRunner = FFmpegRunner()
        
        if not self.ffmpeg:
            logger.warning("⚠️ ffmpeg не найден, сжатие видео недоступно")
        if not self.heif_convert and self.convert_heic_enabled:
            logger.warning("⚠️ heif-convert не найден, конвертация HEIC недоступна")
        
        self.stats: Dict[str, int] = {
            'analyzed': 0, 'compressed': 0, 'skipped_efficient': 0,
            'skipped_small': 0, 'skipped_short': 0, 'failed': 0,
            'total_saved_bytes': 0, 'total_original_bytes': 0, 'total_compressed_bytes': 0
        }
    
    def request_shutdown(self) -> None:
        """Запрашивает завершение всех процессов."""
        self._ffmpeg_runner.request_shutdown()
    
    async def stop_all_ffmpeg(self) -> None:
        """Останавливает все процессы ffmpeg."""
        await self._ffmpeg_runner.stop_all()
    
    @staticmethod
    def _compress_photo_sync(file_path: str, temp_path: str, max_dimension: int, quality: int) -> Tuple[bool, int]:
        """Синхронное сжатие фото."""
        try:
            with Image.open(file_path) as img:
                exif: Optional[bytes] = img.info.get('exif')
                
                if img.mode in ('RGBA', 'P', 'LA'):
                    bg: Image.Image = Image.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'P':
                        img = img.convert('RGBA')
                    bg.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
                    img = bg
                
                if max(img.size) > max_dimension:
                    ratio: float = max_dimension / max(img.size)
                    img = img.resize(tuple(int(d * ratio) for d in img.size), Image.Resampling.LANCZOS)
                
                save_kwargs: Dict[str, Any] = {'quality': quality, 'optimize': True, 'progressive': True}
                if exif:
                    save_kwargs['exif'] = exif
                img.save(temp_path, 'JPEG', **save_kwargs)
                return True, os.path.getsize(temp_path)
        except Exception as e:
            logger.error(f"Ошибка сжатия фото: {e}")
            return False, 0
    
    async def compress_photo(self, file_path: str, retry_count: int = 0) -> CompressionResult:
        """Сжимает фотографию."""
        start: float = time.time()
        original_size: int = os.path.getsize(file_path)
        
        if original_size < self.min_photo_size_kb * 1024:
            self.stats['skipped_small'] += 1
            return CompressionResult(
                success=True, original_path=file_path, compressed_path=file_path,
                original_size=original_size, compressed_size=original_size,
                saved_bytes=0, saved_percent=0, compression_type='none',
                decision=f"Фото меньше {self.min_photo_size_kb}KB",
                duration_sec=time.time() - start, was_compressed=False, should_compress=False)
        
        temp_path: str = file_path + '.compressed.jpg'
        
        try:
            pool: ProcessPoolExecutor = await _get_photo_pool(self.photo_processes)
            loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
            
            success: bool
            compressed_size: int
            success, compressed_size = await asyncio.wait_for(
                loop.run_in_executor(pool, self._compress_photo_sync,
                                     file_path, temp_path, self.max_image_dimension, self.image_quality),
                timeout=300)
            
            if not success or not os.path.exists(temp_path):
                raise Exception("Compression failed")
            
            saved: int = original_size - compressed_size
            percent: float = (saved / original_size * 100) if original_size > 0 else 0
            
            self.stats['total_original_bytes'] += original_size
            self.stats['total_compressed_bytes'] += compressed_size
            
            if percent >= 5:
                self.stats['compressed'] += 1
                self.stats['total_saved_bytes'] += saved
                
                final_path: str = os.path.splitext(file_path)[0] + '_compressed.jpg'
                if os.path.exists(final_path):
                    import uuid
                    final_path = f"{os.path.splitext(file_path)[0]}_{uuid.uuid4().hex[:8]}_compressed.jpg"
                shutil.move(temp_path, final_path)
                
                return CompressionResult(
                    success=True, original_path=file_path, compressed_path=final_path,
                    original_size=original_size, compressed_size=compressed_size,
                    saved_bytes=saved, saved_percent=percent, compression_type='photo',
                    decision=f"Сжато на {percent:.1f}%", duration_sec=time.time() - start,
                    was_compressed=True, should_compress=True)
            else:
                os.unlink(temp_path)
                self.stats['skipped_small'] += 1
                return CompressionResult(
                    success=True, original_path=file_path, compressed_path=file_path,
                    original_size=original_size, compressed_size=original_size,
                    saved_bytes=0, saved_percent=0, compression_type='none',
                    decision=f"Сжатие неэффективно ({percent:.1f}%)",
                    duration_sec=time.time() - start, was_compressed=False, should_compress=True)
                    
        except BrokenProcessPool as e:
            logger.error(f"❌ Process pool сломан: {e}")
            if retry_count < 3:
                logger.warning(f"🔄 Повторная попытка сжатия (попытка {retry_count + 1}/3)")
                await _recreate_photo_pool(self.photo_processes)
                await asyncio.sleep(1)
                return await self.compress_photo(file_path, retry_count + 1)
            else:
                self.stats['failed'] += 1
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                return CompressionResult(
                    success=False, original_path=file_path, compressed_path=file_path,
                    original_size=original_size, compressed_size=original_size,
                    saved_bytes=0, saved_percent=0, compression_type='none',
                    decision="Ошибка пула процессов после 3 попыток", duration_sec=time.time() - start,
                    error="Process pool broken after retries", was_compressed=False, should_compress=True)
        except asyncio.TimeoutError:
            logger.error(f"⏰ Таймаут сжатия фото {file_path} (5 минут)")
            self.stats['failed'] += 1
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            return CompressionResult(
                success=False, original_path=file_path, compressed_path=file_path,
                original_size=original_size, compressed_size=original_size,
                saved_bytes=0, saved_percent=0, compression_type='none',
                decision="Таймаут сжатия (5 минут)", duration_sec=time.time() - start,
                error="timeout", was_compressed=False, should_compress=True)
        except Exception as e:
            self.stats['failed'] += 1
            logger.error(f"❌ Ошибка сжатия фото: {e}")
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            return CompressionResult(
                success=False, original_path=file_path, compressed_path=file_path,
                original_size=original_size, compressed_size=original_size,
                saved_bytes=0, saved_percent=0, compression_type='none',
                decision="Ошибка сжатия", duration_sec=time.time() - start,
                error=str(e), was_compressed=False, should_compress=True)
    
    async def analyze(self, file_path: str) -> Optional[VideoInfo]:
        """Анализирует видеофайл через ffprobe."""
        if not self.ffprobe or not os.path.exists(file_path):
            return None
        
        self.stats['analyzed'] += 1
        
        try:
            cmd: List[str] = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_format', '-show_streams', file_path]
            process: asyncio.subprocess.Process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, _ = await process.communicate()
            
            if process.returncode != 0:
                return None
            
            data: dict = json.loads(stdout)
            video: Optional[dict] = next((s for s in data.get('streams', []) if s.get('codec_type') == 'video'), None)
            if not video:
                return None
            
            duration: float = float(video.get('duration', 0) or data.get('format', {}).get('duration', 0))
            if duration <= 0:
                file_size: int = int(data.get('format', {}).get('size', 0))
                duration = max(0.1, min(86400.0, file_size / (10 * 1024 * 1024) * 60.0 if file_size > 0 else 60.0))
            
            fps: float = 0.0
            r_frame_rate: str = video.get('r_frame_rate', '0/0')
            if '/' in r_frame_rate:
                try:
                    num, den = map(int, r_frame_rate.split('/'))
                    fps = num / den if den else 0.0
                except Exception:
                    pass
            
            return VideoInfo(
                codec=video.get('codec_name', 'unknown'),
                codec_long=video.get('codec_long_name', ''),
                width=int(video.get('width', 0)), height=int(video.get('height', 0)),
                duration=duration,
                bitrate=int(video.get('bit_rate', 0) or data.get('format', {}).get('bit_rate', 0)),
                fps=fps)
        except Exception as e:
            logger.debug(f"Ошибка анализа видео: {e}")
            return None
    
    async def convert_heic(self, file_path: str) -> CompressionResult:
        """Конвертирует HEIC в JPEG."""
        start: float = time.time()
        original_size: int = os.path.getsize(file_path)
        
        if not self.heif_convert:
            return CompressionResult(
                success=False, original_path=file_path, compressed_path=file_path,
                original_size=original_size, compressed_size=original_size,
                saved_bytes=0, saved_percent=0, compression_type='none',
                decision="heif-convert не найден", duration_sec=time.time() - start,
                error="heif-convert not available", was_compressed=False, should_compress=True)
        
        temp_path: str = file_path + '.converted.jpg'
        
        try:
            def convert() -> bool:
                result: subprocess.CompletedProcess = subprocess.run(['heif-convert', file_path, temp_path], capture_output=True, text=True, timeout=60)
                return result.returncode == 0 and os.path.exists(temp_path)
            
            if await asyncio.get_event_loop().run_in_executor(None, convert) and os.path.exists(temp_path):
                converted_size: int = os.path.getsize(temp_path)
                saved: int = original_size - converted_size
                percent: float = (saved / original_size * 100) if original_size > 0 else 0
                
                self.stats['compressed'] += 1
                self.stats['total_saved_bytes'] += saved
                self.stats['total_original_bytes'] += original_size
                self.stats['total_compressed_bytes'] += converted_size
                
                final_path: str = os.path.splitext(file_path)[0] + '.jpg'
                if os.path.exists(final_path):
                    import uuid
                    final_path = f"{os.path.splitext(file_path)[0]}_{uuid.uuid4().hex[:8]}.jpg"
                shutil.move(temp_path, final_path)
                
                return CompressionResult(
                    success=True, original_path=file_path, compressed_path=final_path,
                    original_size=original_size, compressed_size=converted_size,
                    saved_bytes=saved, saved_percent=percent, compression_type='heic',
                    decision=f"HEIC конвертирован, экономия {percent:.1f}%",
                    duration_sec=time.time() - start, was_compressed=True, should_compress=True)
            
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise Exception("Conversion failed")
            
        except subprocess.TimeoutExpired:
            self.stats['failed'] += 1
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            return CompressionResult(
                success=False, original_path=file_path, compressed_path=file_path,
                original_size=original_size, compressed_size=original_size,
                saved_bytes=0, saved_percent=0, compression_type='none',
                decision="Таймаут конвертации", duration_sec=time.time() - start,
                error="timeout", was_compressed=False, should_compress=True)
        except Exception as e:
            self.stats['failed'] += 1
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            return CompressionResult(
                success=False, original_path=file_path, compressed_path=file_path,
                original_size=original_size, compressed_size=original_size,
                saved_bytes=0, saved_percent=0, compression_type='none',
                decision="Ошибка конвертации", duration_sec=time.time() - start,
                error=str(e), was_compressed=False, should_compress=True)
    
    def _calculate_timeout(self, file_size_mb: float) -> int:
        """Вычисляет таймаут для сжатия видео."""
        return int(max(self.video_timeout_min,
                       min(self.video_timeout_max,
                           self.video_timeout_base + file_size_mb * self.video_timeout_per_mb)))
    
    def _build_ffmpeg_cmd(self, input_path: str, output_path: str) -> List[str]:
        """Строит команду ffmpeg для сжатия."""
        cmd: List[str] = []
        
        if self.use_cpulimit and self.video_cpu_limit < 100 and shutil.which('cpulimit'):
            cmd.extend(['cpulimit', '-l', str(min(self.video_cpu_limit * CPU_COUNT, 800)), '--'])
        
        if self.low_priority:
            if shutil.which('nice'):
                cmd.extend(['nice', '-n', '19'])
            if shutil.which('ionice'):
                cmd.extend(['ionice', '-c', '3'])
        
        cmd.extend([
            'ffmpeg', '-y', '-nostdin', '-i', input_path,
            '-c:v', 'libx265', '-preset', self.video_preset, '-crf', str(self.video_crf),
            '-threads', str(self.video_threads), '-c:a', 'copy',
            '-movflags', '+faststart', '-pix_fmt', 'yuv420p', '-progress', 'pipe:2', output_path])
        
        if self.video_threads > 1:
            cmd.extend(['-x265-params', f'frame-threads={min(self.video_threads, 3)}:slice-threads=1:no-wpp=1'])
        
        return cmd
    
    async def compress_video(self, file_path: str, progress_callback: Optional[Callable] = None) -> CompressionResult:
        """Сжимает видеофайл."""
        start: float = time.time()
        original_size: int = os.path.getsize(file_path)
        size_mb: float = original_size / (1024 * 1024)
        
        if self._ffmpeg_runner.shutdown_requested:
            return CompressionResult(
                success=False, original_path=file_path, compressed_path=file_path,
                original_size=original_size, compressed_size=original_size,
                saved_bytes=0, saved_percent=0, compression_type='none',
                decision="Shutdown requested", duration_sec=time.time() - start,
                error="Shutdown", was_compressed=False, should_compress=True)
        
        info: Optional[VideoInfo] = await self.analyze(file_path)
        filename: str = os.path.basename(file_path)
        
        if size_mb < self.min_video_size_mb:
            self.stats['skipped_small'] += 1
            return CompressionResult(
                success=True, original_path=file_path, compressed_path=file_path,
                original_size=original_size, compressed_size=original_size,
                saved_bytes=0, saved_percent=0, compression_type='none',
                decision=f"Видео меньше {self.min_video_size_mb}MB",
                duration_sec=time.time() - start, video_info=info.to_dict() if info else None,
                was_compressed=False, should_compress=False)
        
        if info and info.is_efficient and self.skip_efficient:
            self.stats['skipped_efficient'] += 1
            return CompressionResult(
                success=True, original_path=file_path, compressed_path=file_path,
                original_size=original_size, compressed_size=original_size,
                saved_bytes=0, saved_percent=0, compression_type='none',
                decision=f"Видео уже в эффективном кодеке ({info.codec})",
                duration_sec=time.time() - start, video_info=info.to_dict(),
                was_compressed=False, should_compress=False)
        
        if info and info.duration < self.min_video_duration:
            self.stats['skipped_short'] += 1
            return CompressionResult(
                success=True, original_path=file_path, compressed_path=file_path,
                original_size=original_size, compressed_size=original_size,
                saved_bytes=0, saved_percent=0, compression_type='none',
                decision=f"Видео слишком короткое ({info.duration:.1f}с)",
                duration_sec=time.time() - start, video_info=info.to_dict(),
                was_compressed=False, should_compress=False)
        
        if not self.ffmpeg:
            self.stats['failed'] += 1
            return CompressionResult(
                success=False, original_path=file_path, compressed_path=file_path,
                original_size=original_size, compressed_size=original_size,
                saved_bytes=0, saved_percent=0, compression_type='none',
                decision="ffmpeg не найден", duration_sec=time.time() - start,
                video_info=info.to_dict() if info else None, error="ffmpeg not available",
                was_compressed=False, should_compress=True)
        
        temp_path: str = file_path + '.compressed.mp4'
        cmd: List[str] = self._build_ffmpeg_cmd(file_path, temp_path)
        timeout: int = self._calculate_timeout(size_mb)
        
        try:
            returncode: int
            stderr: str
            stdout: str
            returncode, stderr, stdout = await self._ffmpeg_runner.run(
                cmd, file_path, info.duration if info else 60.0, progress_callback, timeout)
            
            if returncode == 0 and os.path.exists(temp_path):
                compressed_size: int = os.path.getsize(temp_path)
                saved: int = original_size - compressed_size
                percent: float = (saved / original_size * 100) if original_size > 0 else 0
                
                self.stats['total_original_bytes'] += original_size
                self.stats['total_compressed_bytes'] += compressed_size
                
                if percent >= 5:
                    self.stats['compressed'] += 1
                    self.stats['total_saved_bytes'] += saved
                    
                    final_path: str = os.path.splitext(file_path)[0] + '_compressed.mp4'
                    if os.path.exists(final_path):
                        import uuid
                        final_path = f"{os.path.splitext(file_path)[0]}_{uuid.uuid4().hex[:8]}_compressed.mp4"
                    shutil.move(temp_path, final_path)
                    
                    logger.info(f"🎬 [{filename}] ✅ Сжато: экономия {percent:.1f}%")
                    return CompressionResult(
                        success=True, original_path=file_path, compressed_path=final_path,
                        original_size=original_size, compressed_size=compressed_size,
                        saved_bytes=saved, saved_percent=percent, compression_type='video',
                        decision=f"Сжато на {percent:.1f}%", duration_sec=time.time() - start,
                        video_info=info.to_dict() if info else None, was_compressed=True, should_compress=True)
                else:
                    os.unlink(temp_path)
                    self.stats['skipped_small'] += 1
                    logger.info(f"🎬 [{filename}] ⏭️ Сжатие неэффективно ({percent:.1f}%)")
                    return CompressionResult(
                        success=True, original_path=file_path, compressed_path=file_path,
                        original_size=original_size, compressed_size=original_size,
                        saved_bytes=0, saved_percent=0, compression_type='none',
                        decision=f"Сжатие неэффективно ({percent:.1f}%)",
                        duration_sec=time.time() - start, video_info=info.to_dict() if info else None,
                        was_compressed=False, should_compress=True)
            else:
                self.stats['failed'] += 1
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                error_msg: str = stderr or stdout or f"FFmpeg exited with code {returncode}"
                logger.error(f"🎬 [{filename}] ❌ {error_msg[:300]}")
                return CompressionResult(
                    success=False, original_path=file_path, compressed_path=file_path,
                    original_size=original_size, compressed_size=original_size,
                    saved_bytes=0, saved_percent=0, compression_type='none',
                    decision=error_msg[:200], duration_sec=time.time() - start,
                    video_info=info.to_dict() if info else None, error=error_msg[:500],
                    was_compressed=False, should_compress=True)
                    
        except asyncio.TimeoutError:
            self.stats['failed'] += 1
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            logger.error(f"🎬 [{filename}] ⏰ Таймаут сжатия после {timeout}с")
            return CompressionResult(
                success=False, original_path=file_path, compressed_path=file_path,
                original_size=original_size, compressed_size=original_size,
                saved_bytes=0, saved_percent=0, compression_type='none',
                decision=f"Таймаут после {timeout}с", duration_sec=time.time() - start,
                video_info=info.to_dict() if info else None, error="timeout",
                was_compressed=False, should_compress=True)
        except asyncio.CancelledError:
            logger.info(f"🎬 [{filename}] Сжатие отменено")
            await self._ffmpeg_runner.stop_all()
            raise
        except Exception as e:
            self.stats['failed'] += 1
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            logger.error(f"🎬 [{filename}] ❌ Ошибка сжатия: {e}")
            return CompressionResult(
                success=False, original_path=file_path, compressed_path=file_path,
                original_size=original_size, compressed_size=original_size,
                saved_bytes=0, saved_percent=0, compression_type='none',
                decision="Ошибка сжатия", duration_sec=time.time() - start,
                video_info=info.to_dict() if info else None, error=str(e),
                was_compressed=False, should_compress=True)
    
    async def compress(self, file_path: str, progress_callback: Optional[Callable] = None) -> CompressionResult:
        """Универсальный метод сжатия."""
        if not os.path.exists(file_path):
            return CompressionResult(
                success=False, original_path=file_path, compressed_path=file_path,
                original_size=0, compressed_size=0, saved_bytes=0, saved_percent=0,
                compression_type='none', decision="Файл не найден", duration_sec=0,
                error="File not found", was_compressed=False, should_compress=True)
        
        if self._ffmpeg_runner.shutdown_requested:
            original_size: int = os.path.getsize(file_path)
            return CompressionResult(
                success=False, original_path=file_path, compressed_path=file_path,
                original_size=original_size, compressed_size=original_size,
                saved_bytes=0, saved_percent=0, compression_type='none',
                decision="Shutdown requested", duration_sec=0,
                error="Shutdown", was_compressed=False, should_compress=True)
        
        ext: str = os.path.splitext(file_path)[1].lower()
        
        if ext == '.heic' and self.convert_heic_enabled:
            return await self.convert_heic(file_path)
        if ext in self.photo_extensions:
            return await self.compress_photo(file_path)
        if ext in self.video_extensions:
            return await self.compress_video(file_path, progress_callback)
        
        size: int = os.path.getsize(file_path)
        return CompressionResult(
            success=True, original_path=file_path, compressed_path=file_path,
            original_size=size, compressed_size=size, saved_bytes=0, saved_percent=0,
            compression_type='none', decision=f"Неподдерживаемый тип {ext}",
            duration_sec=0, was_compressed=False, should_compress=False)
    
    async def shutdown(self) -> None:
        """Единая точка остановки — убивает ffmpeg и закрывает пулы."""
        await self._ffmpeg_runner.stop_all()
        logger.info("✅ Все процессы ffmpeg остановлены")
        await _shutdown_photo_pool()
        logger.info("✅ Process pool для фото закрыт")
    
    def get_stats(self) -> dict:
        """Возвращает статистику компрессора."""
        return self.stats.copy()
