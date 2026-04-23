#!/usr/bin/env python3
"""
Скрипт для быстрой проверки всех модулей проекта Telegram to YaDisk
Запуск: python util/check_project.py
"""

import importlib
import sys
import os
from pathlib import Path

# Цвета для красивого вывода (опционально)
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'
BOLD = '\033[1m'

def print_colored(text: str, color: str = '', bold: bool = False) -> None:
    """Цветной вывод (если поддерживается терминалом)"""
    try:
        if sys.stdout.isatty() and color:
            prefix = color + (BOLD if bold else '')
            print(f"{prefix}{text}{RESET}")
        else:
            print(text)
    except Exception:
        print(text)

def main() -> None:
    """Главная функция проверки"""
    try:
        # Определяем корневую папку проекта (на один уровень выше util/)
        script_path = Path(__file__).absolute()
        project_root = script_path.parent.parent.absolute()
        os.chdir(project_root)
        
        # Добавляем корневую папку в sys.path
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        print_colored("\n🔍 ПРОВЕРКА ПРОЕКТА TELEGRAM TO YADISK", BLUE, bold=True)
        print_colored("=" * 60, BLUE)
        
        print(f"\n📂 Корневая папка: {project_root}")
        print(f"📌 Python: {sys.version.split()[0]}")
        print(f"📌 Скрипт: {script_path}")
        
        # =========================================================
        # 1. Проверка структуры папок
        # =========================================================
        print_colored("\n📁 СТРУКТУРА ПАПОК:", YELLOW, bold=True)
        
        required_folders = [
            ('downloads', 'Временные файлы'),
            ('logs', 'Логи'),
            ('sessions', 'Сессии'),
            ('util', 'Утилиты')
        ]
        
        for folder, description in required_folders:
            try:
                path = Path(folder)
                if path.is_dir():
                    if folder != 'util':
                        files = list(path.glob('*'))
                        file_count = len(files)
                        size = sum(f.stat().st_size for f in files if f.is_file()) / 1024
                        print(f"  ✅ {folder:12} {description:15} ({file_count} файлов, {size:.1f} KB)")
                    else:
                        py_files = list(path.glob('*.py'))
                        sh_files = list(path.glob('*.sh'))
                        file_count = len(py_files) + len(sh_files)
                        size = sum(f.stat().st_size for f in py_files + sh_files if f.is_file()) / 1024
                        print(f"  ✅ {folder:12} {description:15} ({file_count} файлов, {size:.1f} KB)")
                else:
                    print(f"  ❌ {folder:12} {description:15} - не найдена")
                    path.mkdir(parents=True, exist_ok=True)
                    print(f"     ✅ Папка создана автоматически")
            except Exception as e:
                print(f"  ⚠️ {folder:12} - ошибка проверки: {e}")
        
        # =========================================================
        # 2. Проверка основных файлов проекта
        # =========================================================
        print_colored("\n📄 ОСНОВНЫЕ ФАЙЛЫ:", YELLOW, bold=True)
        
        required_files = [
            ('database.py', 'База данных'),
            ('queue_system.py', 'Система очередей'),
            ('telegram_client.py', 'Telegram клиент'),
            ('yandex_uploader.py', 'Яндекс.Диск'),
            ('compressor.py', 'Сжатие'),
            ('main.py', 'Основной скрипт'),
            ('telegram_bot.py', 'Telegram бот'),
            ('requirements.txt', 'Зависимости'),
            ('.env.example', 'Шаблон конфига'),
            ('.env', 'Конфигурация'),
            ('run_bot.sh', 'Скрипт запуска'),
            ('util/check_id.py', 'Проверка ID (util)'),
            ('util/check_project.py', 'Проверка проекта (util)'),
            ('util/pyro_session_maker.py', 'Создание сессии (util)'),
            ('util/run_bot.sh', 'Скрипт запуска (util)')
        ]
        
        for file, description in required_files:
            try:
                path = Path(file)
                if path.exists():
                    size = path.stat().st_size
                    if file == '.env':
                        with open(path, 'r') as f:
                            content = f.read()
                        has_api = 'API_ID' in content and 'API_HASH' in content
                        has_token = 'YA_DISK_TOKEN' in content or 'BOT_TOKEN' in content
                        if has_api and has_token:
                            status = f"✅ {file:30} {description:20} ({size:6} bytes) - настроен"
                        else:
                            status = f"⚠️ {file:30} {description:20} ({size:6} bytes) - требует настройки"
                    else:
                        status = f"✅ {file:30} {description:20} ({size:6} bytes)"
                    print(f"  {status}")
                else:
                    if file == '.env' and Path('.env.example').exists():
                        print(f"  ⚠️ {file:30} {description:20} - не найден (скопируйте .env.example)")
                    elif file == 'run_bot.sh' and Path('util/run_bot.sh').exists():
                        try:
                            os.symlink('util/run_bot.sh', 'run_bot.sh')
                            print(f"  ✅ {file:30} {description:20} (симлинк создан)")
                        except Exception:
                            print(f"  ⚠️ {file:30} {description:20} - не удалось создать симлинк")
                    else:
                        print(f"  ❌ {file:30} {description:20} - не найден")
            except Exception as e:
                print(f"  ⚠️ {file:30} - ошибка проверки: {e}")
        
        # =========================================================
        # 3. Проверка импорта модулей
        # =========================================================
        print_colored("\n📦 МОДУЛИ PYTHON:", YELLOW, bold=True)
        
        modules = [
            ('database', 'База данных'),
            ('queue_system', 'Система очередей'),
            ('telegram_client', 'Telegram клиент'),
            ('yandex_uploader', 'Яндекс.Диск'),
            ('compressor', 'Сжатие'),
            ('telegram_bot', 'Telegram бот'),
            ('main', 'Основной скрипт'),
            ('aiolimiter', 'Адаптивный лимитер'),
            ('pyrogram', 'Pyrogram'),
            ('yadisk', 'YaDisk API'),
            ('PIL', 'Pillow'),
            ('psutil', 'PSUtil'),
            ('dotenv', 'DotEnv'),
            ('aiosqlite', 'AioSQLite')
        ]
        
        all_ok = True
        for module, description in modules:
            try:
                mod = importlib.import_module(module)
                version = getattr(mod, '__version__', 'unknown')
                print(f"  ✅ {module:20} {description:20} v{version}")
            except ImportError as e:
                print(f"  ❌ {module:20} {description:20} - {str(e)}")
                all_ok = False
            except Exception as e:
                print(f"  ⚠️ {module:20} {description:20} - ошибка: {str(e)[:50]}")
                all_ok = False
        
        # =========================================================
        # 4. Проверка системных зависимостей
        # =========================================================
        print_colored("\n🛠️  СИСТЕМНЫЕ ЗАВИСИМОСТИ:", YELLOW, bold=True)
        
        try:
            import shutil
            import subprocess
            
            ffmpeg_path = shutil.which('ffmpeg')
            if ffmpeg_path:
                try:
                    result = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True, timeout=2)
                    version = result.stdout.split('\n')[0].split(' ')[2] if result.stdout else 'unknown'
                    print(f"  ✅ ffmpeg: {ffmpeg_path} (версия {version})")
                except Exception:
                    print(f"  ✅ ffmpeg: {ffmpeg_path}")
            else:
                print(f"  ❌ ffmpeg: не найден (нужен для сжатия видео)")
                all_ok = False
            
            ffprobe_path = shutil.which('ffprobe')
            if ffprobe_path:
                print(f"  ✅ ffprobe: {ffprobe_path}")
            else:
                print(f"  ⚠️ ffprobe: не найден (нужен для анализа видео)")
            
            heif_path = shutil.which('heif-convert')
            if heif_path:
                print(f"  ✅ heif-convert: {heif_path}")
            else:
                print(f"  ⚠️ heif-convert: не найден (HEIC конвертация недоступна)")
            
            cpulimit_path = shutil.which('cpulimit')
            if cpulimit_path:
                print(f"  ✅ cpulimit: {cpulimit_path}")
            else:
                print(f"  ℹ️ cpulimit: не найден (опционально)")
                
        except Exception as e:
            print(f"  ⚠️ Ошибка проверки системных зависимостей: {e}")
        
        # =========================================================
        # 5. Проверка совместимости Python
        # =========================================================
        print_colored("\n🐍 СОВМЕСТИМОСТЬ PYTHON:", YELLOW, bold=True)
        
        required_version = (3, 8)
        current_version = sys.version_info[:2]
        if current_version >= required_version:
            print(f"  ✅ Python {current_version[0]}.{current_version[1]} (требуется {required_version[0]}.{required_version[1]}+)")
        else:
            print(f"  ❌ Python {current_version[0]}.{current_version[1]} (требуется {required_version[0]}.{required_version[1]}+)")
            all_ok = False
        
        # =========================================================
        # 6. Проверка виртуального окружения
        # =========================================================
        print_colored("\n🔧 ВИРТУАЛЬНОЕ ОКРУЖЕНИЕ:", YELLOW, bold=True)
        
        in_venv = sys.prefix != sys.base_prefix
        if in_venv:
            print(f"  ✅ Активно: {sys.prefix}")
        else:
            print(f"  ⚠️ Виртуальное окружение не активно")
            print(f"     Рекомендуется: source venv/bin/activate")
        
        # =========================================================
        # 7. Проверка прав доступа
        # =========================================================
        print_colored("\n🔐 ПРАВА ДОСТУПА:", YELLOW, bold=True)
        
        try:
            env_path = Path('.env')
            if env_path.exists():
                mode = oct(env_path.stat().st_mode)[-3:]
                if mode in ['600', '640', '644']:
                    print(f"  ✅ .env: права {mode} (безопасно)")
                else:
                    print(f"  ⚠️ .env: права {mode} (рекомендуется 600)")
            else:
                print(f"  ℹ️ .env: файл не найден")
            
            run_path = Path('run_bot.sh')
            if run_path.exists():
                if os.access(run_path, os.X_OK):
                    print(f"  ✅ run_bot.sh: исполняемый")
                else:
                    print(f"  ⚠️ run_bot.sh: не исполняемый (chmod +x run_bot.sh)")
                    
            sessions_path = Path('sessions')
            if sessions_path.exists():
                mode = oct(sessions_path.stat().st_mode)[-3:]
                if mode == '700':
                    print(f"  ✅ sessions/: права 700 (безопасно)")
                else:
                    print(f"  ⚠️ sessions/: права {mode} (рекомендуется 700)")
                    
        except Exception as e:
            print(f"  ⚠️ Ошибка проверки прав: {e}")
        
        # =========================================================
        # 8. Краткая статистика
        # =========================================================
        print_colored("\n📊 КРАТКАЯ СТАТИСТИКА:", YELLOW, bold=True)
        
        py_files = [
            'database.py',
            'queue_system.py',
            'telegram_client.py',
            'yandex_uploader.py',
            'compressor.py',
            'main.py',
            'telegram_bot.py'
        ]
        
        total_lines = 0
        for file in py_files:
            try:
                path = Path(file)
                if path.exists():
                    with open(path, 'r', encoding='utf-8') as f:
                        lines = len(f.readlines())
                        total_lines += lines
                        print(f"  {file:20} {lines:6} строк")
                else:
                    print(f"  {file:20} ❌ не найден")
            except Exception as e:
                print(f"  {file:20} ⚠️ ошибка чтения: {e}")
        
        print(f"  {'='*28}")
        print(f"  {'ВСЕГО':20} {total_lines:6} строк")
        
        # =========================================================
        # 9. Проверка базы данных
        # =========================================================
        print_colored("\n🗄️ БАЗА ДАННЫХ:", YELLOW, bold=True)
        
        db_path = Path('backup.db')
        if db_path.exists():
            size = db_path.stat().st_size / (1024 * 1024)
            print(f"  ✅ backup.db: {size:.1f} MB")
            
            try:
                import sqlite3
                conn = sqlite3.connect(str(db_path))
                cursor = conn.cursor()
                
                # Количество файлов
                cursor.execute("SELECT COUNT(*) FROM files")
                files_count = cursor.fetchone()[0]
                print(f"     📄 Файлов в БД: {files_count}")
                
                # Количество чатов
                cursor.execute("SELECT COUNT(*) FROM chat_names")
                chats_count = cursor.fetchone()[0]
                print(f"     📁 Чатов: {chats_count}")
                
                # Статус очереди
                cursor.execute("SELECT COUNT(*) FROM queue_items WHERE status NOT IN ('completed', 'failed')")
                pending = cursor.fetchone()[0]
                if pending > 0:
                    print(f"     ⏳ В очереди: {pending} файлов")
                
                conn.close()
            except Exception as e:
                print(f"     ⚠️ Ошибка чтения БД: {e}")
        else:
            print(f"  ℹ️ backup.db: ещё не создана")
        
        # =========================================================
        # Итог
        # =========================================================
        print_colored("\n" + "=" * 60, BLUE)
        if all_ok:
            print_colored("✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ! Система готова к работе.", GREEN, bold=True)
        else:
            print_colored("⚠️ ОБНАРУЖЕНЫ ПРОБЛЕМЫ! Исправьте их перед запуском.", RED, bold=True)
        print_colored("=" * 60 + "\n", BLUE)
        
    except KeyboardInterrupt:
        print("\n\n❌ Проверка прервана пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()