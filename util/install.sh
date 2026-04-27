#!/bin/bash
# =============================================================================
# Telegram to YaDisk Backup - Установка (РЕЖИМ 1/2/3)
# =============================================================================
# Режим 1: Клонирование + установка
# Режим 2: Установка в текущую папку
# Режим 3: Управление systemd сервисом
#   - 3.1: Установка и настройка сервиса
#   - 3.2: Удаление сервиса
# =============================================================================
set -e

# ════════════════════════════════════════════════════════════
# Функции вывода
# ════════════════════════════════════════════════════════════
print_header() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════════════╗"
    echo "║          Telegram to YaDisk Backup - Установка системы           ║"
    echo "╚══════════════════════════════════════════════════════════════════╝"
    echo ""
}
print_step() { echo "▶ $1"; }
print_info() { echo "ℹ $1"; }
print_success() { echo "✅ $1"; }
print_warning() { echo "⚠️ $1"; }
print_error() { echo "❌ $1"; }
print_divider() { echo "────────────────────────────────────────────────────────"; }

# ════════════════════════════════════════════════════════════
# 1. ВЫБОР РЕЖИМА
# ════════════════════════════════════════════════════════════
choose_installation_mode() {
    print_step "Выбор режима установки..."
    echo ""
    echo "  1) Клонировать репозиторий в ~/Telegram_to_YaDisk (полная установка)"
    echo "  2) Установить в текущую папку: $(pwd) (полная установка)"
    echo "  3) Управление systemd сервисом"
    echo ""
    read -p "Ваш выбор (1/2/3): " INSTALL_MODE
    
    case "$INSTALL_MODE" in
        1)
            print_info "Режим 1: Клонирование в ~/Telegram_to_YaDisk"
            INSTALL_DIR="$HOME/Telegram_to_YaDisk"
            CLONE_REPO=true
            SETUP_SYSTEMD_LATER=true
            ;;
        2)
            print_info "Режим 2: Установка в текущую папку"
            INSTALL_DIR="$(pwd)"
            CLONE_REPO=false
            [[ ! -f "requirements.txt" ]] && { print_error "Нет requirements.txt!"; exit 1; }
            SETUP_SYSTEMD_LATER=true
            ;;
        3)
            print_info "Режим 3: Управление systemd сервисом"
            choose_systemd_action
            return
            ;;
        *)
            print_error "Неверный выбор"
            exit 1
            ;;
    esac
    echo ""
}

# ════════════════════════════════════════════════════════════
# 1a. ВЫБОР ДЕЙСТВИЯ С SYSTEMD (режим 3)
# ════════════════════════════════════════════════════════════
choose_systemd_action() {
    echo ""
    echo "  3.1) Установить и настроить systemd сервис"
    echo "  3.2) Удалить systemd сервис"
    echo "  3.3) Вернуться в главное меню"
    echo ""
    read -p "Ваш выбор (1/2/3): " SYSTEMD_ACTION
    
    case "$SYSTEMD_ACTION" in
        1)
            INSTALL_DIR="$HOME/Telegram_to_YaDisk"
            if [[ ! -d "$INSTALL_DIR" ]]; then
                print_error "Директория $INSTALL_DIR не найдена!"
                print_info "Сначала выполните установку в режиме 1 или 2"
                exit 1
            fi
            setup_systemd_service
            show_final_info_systemd
            ;;
        2)
            remove_systemd_service
            ;;
        3)
            choose_installation_mode
            return
            ;;
        *)
            print_error "Неверный выбор"
            exit 1
            ;;
    esac
}

# ════════════════════════════════════════════════════════════
# 2. Клонирование репозитория (только режим 1)
# ════════════════════════════════════════════════════════════
clone_repository() {
    [[ "$CLONE_REPO" != true ]] && { print_info "Используем существующие файлы"; return; }
    
    print_step "Клонирование репозитория..."
    
    if [[ -d "$INSTALL_DIR/.git" ]]; then
        print_warning "Репозиторий уже существует"
        cd "$INSTALL_DIR" && git pull && print_success "Обновлено" && return
    fi
    
    if git clone https://github.com/IgorZaharchuk/Telegram_to_YaDisk.git "$INSTALL_DIR" 2>/dev/null; then
        print_success "Репозиторий склонирован"
        return
    fi
    
    print_warning "git clone не сработал (проверьте интернет), скачиваю ZIP..."
    wget -q https://github.com/IgorZaharchuk/Telegram_to_YaDisk/archive/refs/heads/main.zip -O /tmp/tg.zip || {
        print_error "Не удалось скачать ZIP. Проверьте интернет-соединение."
        exit 1
    }
    unzip -q /tmp/tg.zip -d "$HOME"
    mv "$HOME/Telegram_to_YaDisk-main" "$INSTALL_DIR"
    rm /tmp/tg.zip
    print_success "ZIP распакован"
}

# ════════════════════════════════════════════════════════════
# 3. Переход в директорию проекта
# ════════════════════════════════════════════════════════════
enter_project_dir() {
    cd "$INSTALL_DIR" || exit 1
    print_info "Рабочая директория: $(pwd)"
}

# ════════════════════════════════════════════════════════════
# 4. Установка системных зависимостей (только режим 1/2)
# ════════════════════════════════════════════════════════════
install_dependencies() {
    [[ "$SETUP_SYSTEMD_LATER" != true ]] && return
    
    print_step "Установка системных зависимостей..."
    sudo apt-get update -qq
    sudo apt-get install -y -qq python3 python3-pip python3-venv git curl wget ffmpeg cpulimit jq --no-install-recommends
    
    # libheif-examples может называться по-разному в разных версиях Ubuntu
    if apt-cache show libheif-examples &>/dev/null; then
        sudo apt-get install -y -qq libheif-examples --no-install-recommends
    else
        print_warning "Пакет libheif-examples не найден в репозитории (конвертация HEIC будет недоступна)"
    fi
    
    print_success "Зависимости установлены"
}

# ════════════════════════════════════════════════════════════
# 5. Создание виртуального окружения (только режим 1/2)
# ════════════════════════════════════════════════════════════
create_venv() {
    [[ "$SETUP_SYSTEMD_LATER" != true ]] && return
    
    print_step "Создание виртуального окружения..."
    
    if ! command -v python3 &>/dev/null; then
        print_error "python3 не установлен!"
        exit 1
    fi
    
    [[ -d "venv" ]] && { print_warning "venv существует"; rm -rf venv; }
    python3 -m venv venv
    source "$(pwd)/venv/bin/activate"
    [[ -z "$VIRTUAL_ENV" ]] && { print_error "Не удалось активировать venv!"; exit 1; }
    print_success "✅ venv активирован: $VIRTUAL_ENV"
    pip install --upgrade pip -q
    pip install -r requirements.txt -q
    print_success "Python зависимости установлены"
}

# ════════════════════════════════════════════════════════════
# 6. Создание .env (только режим 1/2)
# ════════════════════════════════════════════════════════════
create_env_file() {
    [[ "$SETUP_SYSTEMD_LATER" != true ]] && return
    
    print_step "Настройка конфигурации..."
    if [[ -f ".env" ]]; then
        print_warning ".env уже существует"
        read -p "Перезаписать? (y/N): " -n 1 -r
        echo
        [[ ! $REPLY =~ ^[Yy]$ ]] && { print_info "Оставляем существующий"; return; }
    fi
    
    if [[ -f ".env.example" ]]; then
        cp .env.example .env
    else
        cat > .env << 'EOF'
API_ID=your_api_id
API_HASH=your_api_hash
BOT_TOKEN=your_bot_token
YA_DISK_TOKEN=your_yandex_token
ALLOWED_USERS=
STRING_SESSION=
EOF
    fi
    
    chmod 600 .env
    print_success ".env создан (права 600)"
}

# ════════════════════════════════════════════════════════════
# 7. Создание STRING_SESSION (только режим 1/2)
# ════════════════════════════════════════════════════════════
create_session() {
    [[ "$SETUP_SYSTEMD_LATER" != true ]] && return
    
    print_step "Создание STRING_SESSION..."
    local HAS_SESSION=false
    if [[ -f ".env" ]] && grep -q "STRING_SESSION=" .env; then
        SESSION_VAL=$(grep "STRING_SESSION=" .env | cut -d'=' -f2-)
        [[ -n "$SESSION_VAL" && ${#SESSION_VAL} -gt 20 ]] && HAS_SESSION=true
    fi
    
    echo ""
    if [[ "$HAS_SESSION" == true ]]; then
        read -p "Создать НОВУЮ сессию? (y/N): " -n 1 -r
    else
        read -p "Создать STRING_SESSION сейчас? (y/N): " -n 1 -r
    fi
    echo
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        [[ "$HAS_SESSION" == false ]] && print_warning "Не забудьте настроить STRING_SESSION!"
        return
    fi
    
    if [[ -f "util/pyro_session_maker.py" ]]; then
        source "$(pwd)/venv/bin/activate"
        python util/pyro_session_maker.py
        echo ""
        read -p "Скопировать сессию в .env? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            read -r NEW_SESSION
            if [[ -n "$NEW_SESSION" ]]; then
                python3 -c "
import re
s = open('.env').read()
s = re.sub(r'^STRING_SESSION=.*', 'STRING_SESSION=' + '''"'"'${NEW_SESSION}'"'"''', s, flags=re.M)
open('.env','w').write(s)
" 2>/dev/null || echo "STRING_SESSION=$NEW_SESSION" >> .env
            fi
            print_success "STRING_SESSION обновлена"
        fi
    else
        print_warning "util/pyro_session_maker.py не найден"
        print_info "Создайте через @StringSessionBot в Telegram"
    fi
}

# ════════════════════════════════════════════════════════════
# 8. Создание папок и права (только режим 1/2)
# ════════════════════════════════════════════════════════════
setup_permissions() {
    [[ "$SETUP_SYSTEMD_LATER" != true ]] && return
    
    print_step "Настройка папок и прав..."
    mkdir -p downloads logs sessions util
    chmod 600 .env 2>/dev/null || true
    chmod 700 sessions 2>/dev/null || true
    chmod 755 logs downloads 2>/dev/null || true
    print_success "Папки и права настроены"
}

# ════════════════════════════════════════════════════════════
# 9. Создание run_bot.sh (только режим 1/2)
# ════════════════════════════════════════════════════════════
create_run_script() {
    [[ "$SETUP_SYSTEMD_LATER" != true ]] && return
    
    print_step "Создание скрипта запуска..."
    
    cat > "run_bot.sh" << RUNEOF
#!/bin/bash
cd $HOME/Telegram_to_YaDisk || { echo "❌ Директория не найдена"; exit 1; }

# Убить старые процессы бота (кроме себя)
CURRENT_PID=\$\$
OLD_PIDS=\$(pgrep -f "python.*telegram_bot.py" 2>/dev/null | grep -v "\$CURRENT_PID" || true)
if [[ -n "\$OLD_PIDS" ]]; then
    echo "🛑 Завершение старых процессов бота: \$OLD_PIDS"
    kill \$OLD_PIDS 2>/dev/null || true
    sleep 2
    STILL=\$(pgrep -f "python.*telegram_bot.py" 2>/dev/null | grep -v "\$CURRENT_PID" || true)
    [[ -n "\$STILL" ]] && kill -9 \$STILL 2>/dev/null || true
fi

echo "🔄 Активация виртуального окружения..."
source venv/bin/activate || { echo "❌ Ошибка активации"; exit 1; }
echo "🚀 Запуск telegram_bot.py..."

# Цикл перезапуска при падении
while true; do
    python telegram_bot.py
    EXIT_CODE=\$?
    echo "⚠️ Бот упал с кодом \$EXIT_CODE, перезапуск через 5с..."
    sleep 5
done
RUNEOF
    
    chmod +x run_bot.sh
    print_success "run_bot.sh создан (автоперезапуск при падении, защита от двойного запуска)"
}

# ════════════════════════════════════════════════════════════
# 10. Проверка проекта (только режим 1/2)
# ════════════════════════════════════════════════════════════
run_project_check() {
    [[ "$SETUP_SYSTEMD_LATER" != true ]] && return
    
    print_step "Проверка проекта..."
    if [[ -f "util/check_project.py" ]]; then
        source "$(pwd)/venv/bin/activate" 2>/dev/null || true
        python3 util/check_project.py || true
        if [[ ${PIPESTATUS[0]} -eq 0 ]]; then
            print_success "✅ Все проверки пройдены!"
        else
            print_warning "⚠️ Есть проблемы (проверьте вывод выше)"
        fi
    else
        print_warning "util/check_project.py не найден"
    fi
}

# ════════════════════════════════════════════════════════════
# 11. НАСТРОЙКА SYSTEMD (режим 3.1 или вопрос в конце 1/2)
# ════════════════════════════════════════════════════════════
setup_systemd_service() {
    print_step "Настройка systemd сервиса..."
    echo ""
    echo "Systemd сервис позволяет запускать бота автоматически:"
    echo "  • При загрузке системы"
    echo "  • В фоновом режиме"
    echo "  • С автоматическим перезапуском при сбоях"
    echo ""
    
    if [[ ! -d "$HOME/Telegram_to_YaDisk" ]]; then
        print_warning "Директория ~/Telegram_to_YaDisk не найдена"
        print_info "Systemd сервис доступен только для установки в домашнюю директорию"
        return
    fi
    
    cd "$HOME/Telegram_to_YaDisk" || return
    
    # Пересоздаём run_bot.sh
    create_run_script
    
    # Создаём сервис с текущим пользователем
    print_step "Создание файла сервиса..."
    sudo tee /etc/systemd/system/tg2ya-bot.service > /dev/null << EOF
[Unit]
Description=Telegram to YaDisk Backup Bot
After=network.target network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$(whoami)
Group=$(whoami)
WorkingDirectory=$HOME/Telegram_to_YaDisk
Environment="HOME=$HOME"
Environment="USER=$(whoami)"
ExecStart=$HOME/Telegram_to_YaDisk/run_bot.sh
Restart=always
RestartSec=10
StandardOutput=append:$HOME/Telegram_to_YaDisk/logs/bot.log
StandardError=append:$HOME/Telegram_to_YaDisk/logs/bot_error.log
SyslogIdentifier=tg2ya-bot

KillMode=mixed
KillSignal=SIGTERM
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF
    
    sudo systemctl daemon-reload
    print_success "Файл сервиса создан"
    
    echo ""
    read -p "Включить и запустить сервис сейчас? (y/N): " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        sudo systemctl enable tg2ya-bot.service
        sudo systemctl start tg2ya-bot.service
        sleep 2
        if sudo systemctl is-active --quiet tg2ya-bot.service; then
            print_success "✅ Сервис запущен и работает"
        else
            print_warning "⚠️ Сервис не запустился (проверьте: sudo journalctl -u tg2ya-bot)"
            print_info "Отключаем автозапуск до исправления проблемы..."
            sudo systemctl disable tg2ya-bot.service 2>/dev/null || true
        fi
    else
        print_info "Сервис установлен но не запущен"
        print_info "Для запуска:"
        echo "  sudo systemctl enable tg2ya-bot.service"
        echo "  sudo systemctl start tg2ya-bot.service"
    fi
}

# ════════════════════════════════════════════════════════════
# 12. УДАЛЕНИЕ SYSTEMD СЕРВИСА (режим 3.2)
# ════════════════════════════════════════════════════════════
remove_systemd_service() {
    print_step "Удаление systemd сервиса..."
    echo ""
    
    if [[ ! -f "/etc/systemd/system/tg2ya-bot.service" ]]; then
        print_warning "Сервис tg2ya-bot.service не найден"
        return
    fi
    
    echo "Будет выполнено:"
    echo "  • sudo systemctl stop tg2ya-bot.service"
    echo "  • sudo systemctl disable tg2ya-bot.service"
    echo "  • sudo rm /etc/systemd/system/tg2ya-bot.service"
    echo "  • sudo systemctl daemon-reload"
    echo ""
    read -p "Продолжить? (y/N): " -n 1 -r
    echo
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Удаление отменено"
        return
    fi
    
    sudo systemctl stop tg2ya-bot.service 2>/dev/null || true
    sudo systemctl disable tg2ya-bot.service 2>/dev/null || true
    sudo rm -f /etc/systemd/system/tg2ya-bot.service
    sudo systemctl daemon-reload
    
    print_success "✅ Systemd сервис удалён"
}

# ════════════════════════════════════════════════════════════
# 13. Финальная информация (полная установка)
# ════════════════════════════════════════════════════════════
show_final_info() {
    print_divider
    print_success "🎉 Установка завершена!"
    print_divider
    echo ""
    echo "📋 Информация:"
    echo "  • Директория: $INSTALL_DIR"
    echo "  • Режим: $([ "$CLONE_REPO" == true ] && echo "клонирование" || echo "текущая папка")"
    echo ""
    echo "📂 Основные модули:"
    echo "  • database.py - база данных"
    echo "  • queue_system.py - система очередей"
    echo "  • telegram_client.py - клиент Telegram"
    echo "  • yandex_uploader.py - загрузка на Диск"
    echo "  • compressor.py - сжатие медиа"
    echo "  • telegram_bot.py - бот управления"
    echo "  • main.py - оркестратор"
    echo ""
    echo "🚀 Запуск бота:"
    echo "  ./run_bot.sh"
    echo ""
    echo "📝 Конфигурация:"
    echo "  nano .env"
    echo ""
    echo "🔍 Проверка:"
    echo "  python3 util/check_project.py"
    echo ""
}

# ════════════════════════════════════════════════════════════
# 14. Финальная информация (systemd)
# ════════════════════════════════════════════════════════════
show_final_info_systemd() {
    print_divider
    print_success "🎉 Systemd сервис настроен!"
    print_divider
    echo ""
    echo "🔧 Управление сервисом:"
    echo "  sudo systemctl status tg2ya-bot      # Статус"
    echo "  sudo systemctl stop tg2ya-bot        # Остановить"
    echo "  sudo systemctl restart tg2ya-bot     # Перезапустить"
    echo "  sudo journalctl -u tg2ya-bot -f      # Логи"
    echo ""
}

# ════════════════════════════════════════════════════════════
# ГЛАВНАЯ ФУНКЦИЯ
# ════════════════════════════════════════════════════════════
main() {
    print_header
    
    choose_installation_mode
    
    if [[ "$INSTALL_MODE" == "3" ]]; then
        exit 0
    fi
    
    if [[ "$SETUP_SYSTEMD_LATER" == true ]]; then
        clone_repository
        enter_project_dir
        install_dependencies
        create_venv
        create_env_file
        create_session
        setup_permissions
        create_run_script
        run_project_check
        
        echo ""
        read -p "Настроить systemd сервис сейчас? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            setup_systemd_service
            show_final_info_systemd
        else
            show_final_info
        fi
    fi
}

trap 'echo ""; print_error "Установка прервана"; exit 1' INT

main "$@"