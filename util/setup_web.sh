#!/bin/bash
# Настройка веб-интерфейса Telegram Backup
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_step() { echo -e "${GREEN}▶ $1${NC}"; }
print_success() { echo -e "${GREEN}✅ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠️ $1${NC}"; }
print_error() { echo -e "${RED}❌ $1${NC}"; }

echo "╔══════════════════════════════════════════════════╗"
echo "║     Настройка веб-интерфейса Telegram Backup     ║"
echo "╚══════════════════════════════════════════════════╝"

if [[ ! -d "$HOME/Telegram_to_YaDisk" ]]; then
    print_error "Директория ~/Telegram_to_YaDisk не найдена!"
    exit 1
fi

cd "$HOME/Telegram_to_YaDisk"
source venv/bin/activate 2>/dev/null || { print_error "venv не найден!"; exit 1; }

# Python зависимости
print_step "Установка Python-зависимостей..."
pip install flask gunicorn python-dotenv requests -q
print_success "Готово"

# Nginx
print_step "Настройка Nginx..."
sudo apt-get install -y -qq nginx apache2-utils 2>/dev/null

read -p "Домен (например, tg2ya.ru): " DOMAIN
read -p "Логин [admin]: " WEB_USER
WEB_USER=${WEB_USER:-admin}
read -sp "Пароль: " WEB_PASS
echo

if [[ -n "$WEB_PASS" ]]; then
    sudo htpasswd -bc /etc/nginx/.htpasswd "$WEB_USER" "$WEB_PASS"
    print_success "Пользователь $WEB_USER создан"
fi

if [[ ! -f "/etc/lighttpd/server.pem" ]]; then
    sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
        -keyout /etc/lighttpd/server.pem -out /etc/lighttpd/server.pem \
        -subj "/CN=$DOMAIN" 2>/dev/null
    print_success "SSL сертификат создан"
fi

sudo tee /etc/nginx/sites-available/tg2ya > /dev/null << NGINXEOF
server {
    listen 443 ssl;
    server_name $DOMAIN;
    ssl_certificate /etc/lighttpd/server.pem;
    ssl_certificate_key /etc/lighttpd/server.pem;
    location / { root /var/www/html; try_files /index.html =404; }
    location /backup {
        auth_basic "Access Restricted";
        auth_basic_user_file /etc/nginx/.htpasswd;
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host \$host;
        proxy_buffering off;
        proxy_cache off;
    }
}
server {
    listen 80;
    server_name $DOMAIN;
    return 301 https://\$server_name\$request_uri;
}
NGINXEOF

sudo ln -sf /etc/nginx/sites-available/tg2ya /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl reload nginx
print_success "Nginx настроен"

# Systemd сервис
print_step "Создание systemd сервиса..."
sudo tee /etc/systemd/system/tg2ya-web.service > /dev/null << EOFWEB
[Unit]
Description=Telegram Backup Web Interface
After=network.target
[Service]
User=$(whoami)
WorkingDirectory=$HOME/Telegram_to_YaDisk
ExecStart=$HOME/Telegram_to_YaDisk/venv/bin/gunicorn -w 2 -b 127.0.0.1:5000 web.server:app --timeout 10
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
EOWEB

sudo systemctl daemon-reload

read -p "Запустить веб-интерфейс сейчас? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    sudo systemctl enable tg2ya-web.service
    sudo systemctl start tg2ya-web.service
    print_success "Веб-интерфейс запущен: https://$DOMAIN/backup"
fi

# Главная страница
sudo tee /var/www/html/index.html > /dev/null << HTMLEOF
<!DOCTYPE html><html lang="ru"><head><meta charset="UTF-8"><title>$DOMAIN</title>
<style>body{font-family:Arial;background:#1a1a2e;color:#eee;text-align:center;padding:50px}a{color:#74b9ff;font-size:20px;text-decoration:none}</style></head>
<body><h1>☁️ $DOMAIN</h1><p><a href="/backup">📊 Панель управления</a></p></body></html>
HTMLEOF

print_success "Готово! Доступ: https://$DOMAIN/backup"
echo "Логин: $WEB_USER"
