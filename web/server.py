import requests
from dotenv import load_dotenv
load_dotenv()
#!/usr/bin/env python3
"""
Веб-интерфейс Telegram Backup — ПОЛНЫЙ ФУНКЦИОНАЛ
Автообновление, прогресс-бары, сессионные счётчики
"""

import os, sys, json, time, sqlite3, subprocess, signal, re
from pathlib import Path
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, Blueprint, jsonify

app = Flask(__name__, static_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates/static'), static_url_path='/backup/static')
backup_bp = Blueprint('backup', __name__, url_prefix='/backup')

# Константы
DB_PATH = '/home/zip/Telegram_to_YaDisk/backup.db'
PID_FILE = '/home/zip/Telegram_to_YaDisk/backup.pid'
LOG_FILE = '/home/zip/Telegram_to_YaDisk/logs/backup.log'
WEB_LOG_FILE = '/home/zip/Telegram_to_YaDisk/logs/web.log'
PROJECT_DIR = '/home/zip/Telegram_to_YaDisk'
PYTHON = f'{PROJECT_DIR}/venv/bin/python'
YA_TOKEN = os.getenv("YA_DISK_TOKEN", "")
YA_BASE_PATH = os.getenv("YA_DISK_PATH", "/tg_backup")

def find_disk_path(chat_name, topic_name=None, filename=None):
    """Находит реальный путь на Яндекс.Диске."""
    if not YA_TOKEN:
        return None
    
    headers = {'Authorization': f'OAuth {YA_TOKEN}'}
    
    # Ищем чат
    chat_path = None
    try:
        r = requests.get(f'https://cloud-api.yandex.net/v1/disk/resources?path={YA_BASE_PATH}&limit=100', headers=headers, timeout=10)
        if r.status_code == 200:
            for item in r.json().get('_embedded', {}).get('items', []):
                if item['type'] == 'dir':
                    # Нормализуем имена для сравнения
                    item_name = item['name']
                    db_name = chat_name.replace(' ', '_')
                    if item_name == db_name or item_name == chat_name:
                        chat_path = item['path']
                        break
    except:
        pass
    
    if not chat_path or not topic_name:
        return chat_path
    
    # Ищем тему внутри чата
    topic_path = None
    try:
        r = requests.get(f'https://cloud-api.yandex.net/v1/disk/resources?path={chat_path}&limit=200', headers=headers, timeout=10)
        if r.status_code == 200:
            for item in r.json().get('_embedded', {}).get('items', []):
                if item['type'] == 'dir':
                    item_name = item['name']
                    db_name = topic_name.replace(' ', '_')
                    if item_name == db_name or item_name == topic_name:
                        topic_path = item['path']
                        break
    except:
        pass
    
    if not topic_path or not filename:
        return topic_path
    
    return f"{topic_path}/{filename}"
MAIN_PY = f'{PROJECT_DIR}/main.py'
DOWNLOAD_DIR = 'downloads'

STATE_NEW = 0
STATE_SELECTED = 1
STATE_UPLOADED = 2
STATE_SKIPPED = 3
STATE_ERROR = 4
STATE_UNLOADED = 5

STATUS_PENDING_CHECK = "pending_check"
STATUS_PENDING_DOWNLOAD = "pending_download"
STATUS_PENDING_COMPRESS = "pending_compress"
STATUS_PENDING_UPLOAD = "pending_upload"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"

FILE_EMOJI = {'photo': '📸', 'video': '🎬', 'audio': '🎵', 'document': '📄', 'archive': '🗜️', 'other': '📎'}
TYPE_NAMES = {'all': 'Все', 'photo': 'Фото', 'video': 'Видео', 'audio': 'Аудио', 'document': 'Документы', 'archive': 'Архивы'}
STATUS_NAMES = {'all': 'Все', 'uploaded': 'Загружено', 'unuploaded': 'Не скачано', 'new': 'Новые', 'selected': 'Выбрано', 'error': 'Ошибки', 'skipped': 'Пропущено'}

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=1000")
    return conn

def fmt_size(b):
    if not b: return "0B"
    b = int(b)
    if b < 1024: return f"{b}B"
    if b < 1024**2: return f"{b/1024:.1f}KB"
    if b < 1024**3: return f"{b/(1024**2):.1f}MB"
    return f"{b/(1024**3):.1f}GB"

def fmt_bar(p, width=13):
    """Прогресс-бар: ▰▰▰▱▱▱ 45%"""
    if p is None: p = 0
    try: p = float(p)
    except: p = 0
    p = max(0, min(100, p))
    filled = int(width * p / 100)
    return '▰' * filled + '▱' * (width - filled), p

def is_running():
    if not os.path.exists(PID_FILE): return False
    try:
        with open(PID_FILE) as f:
            os.kill(int(f.read().strip()), 0)
        return True
    except: return False

def get_heartbeat_age():
    """Возраст последнего heartbeat из active_progress/queue_processing."""
    try:
        conn = get_db()
        row = conn.execute("SELECT MAX(updated_at) FROM (SELECT updated_at FROM queue_processing WHERE started_at > ? UNION ALL SELECT updated_at FROM active_progress WHERE updated_at > ?)", (time.time()-3600, time.time()-3600)).fetchone()
        conn.close()
        if row and row[0]:
            return max(0, time.time() - row[0])
    except: pass
    return 0

def read_log(path, lines=500):
    if not os.path.exists(path): return []
    with open(path, 'r', errors='ignore') as f:
        return f.readlines()[-lines:]

def get_session_stats():
    """Сессионные счётчики из app_state."""
    conn = get_db()
    row = conn.execute("SELECT value FROM app_state WHERE key='session_stats'").fetchone()
    conn.close()
    if row:
        try: return json.loads(row['value'])
        except: pass
    return {'uploaded': 0, 'downloaded': 0, 'compressed': 0, 'checked': 0, 'skipped': 0}

def get_chat_ids_from_settings():
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key='chat_ids'").fetchone()
    conn.close()
    if row:
        try: return json.loads(row['value'])
        except: pass
    return []

# ==================== API для автообновления ====================

@backup_bp.route('/api/status')
def api_status():
    conn = get_db()
    c = conn.cursor()
    
    running = is_running()
    age = get_heartbeat_age() if running else 0
    
    # Только основные цифры
    total = c.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    uploaded = c.execute("SELECT COUNT(*) FROM files WHERE state=?", (STATE_UPLOADED,)).fetchone()[0]
    new_files = c.execute("SELECT COUNT(*) FROM files WHERE state=?", (STATE_NEW,)).fetchone()[0]
    errors = c.execute("SELECT COUNT(*) FROM files WHERE state=?", (STATE_ERROR,)).fetchone()[0]
    selected = c.execute("SELECT COUNT(*) FROM files WHERE state=?", (STATE_SELECTED,)).fetchone()[0]
    skipped = c.execute("SELECT COUNT(*) FROM files WHERE state=?", (STATE_SKIPPED,)).fetchone()[0]
    unloaded = c.execute("SELECT COUNT(*) FROM files WHERE state=?", (STATE_UNLOADED,)).fetchone()[0]
    
    queue_counts = {}
    for row in c.execute("SELECT status, COUNT(*) as cnt FROM queue_items GROUP BY status"):
        queue_counts[row['status']] = row['cnt']
    pending = sum(queue_counts.get(s, 0) for s in (STATUS_PENDING_CHECK, STATUS_PENDING_DOWNLOAD, STATUS_PENDING_COMPRESS, STATUS_PENDING_UPLOAD))
    
    active = []
    for row in c.execute("SELECT qi.filename, qi.file_size, qp.worker_type, ap.progress, ap.speed, ap.eta FROM queue_items qi JOIN queue_processing qp ON qi.key = qp.key LEFT JOIN active_progress ap ON qi.key = ap.key"):
        active.append(dict(row))
    
    downloading = [a for a in active if a['worker_type'] == 'download']
    compressing = [a for a in active if a['worker_type'] in ('compress_photo', 'compress_video')]
    uploading = [a for a in active if a['worker_type'] == 'upload']
    
    cs = c.execute("SELECT * FROM stage_stats WHERE stage='compress'").fetchone()
    saved_bytes = cs['saved_bytes'] if cs else 0
    compressed_count = cs['processed'] if cs else 0
    
    session = get_session_stats()
    
    scan_progress = {}
    for row in c.execute("SELECT * FROM scan_progress WHERE completed=0"):
        scan_progress[str(row['chat_id'])] = dict(row)
    
    file_err_count = c.execute("SELECT COUNT(*) FROM file_errors").fetchone()[0]
    sys_err_count = c.execute("SELECT COUNT(*) FROM system_errors").fetchone()[0]
    
    chat_stats = []
    for row in c.execute("SELECT cs.*, cn.name FROM chat_stats cs LEFT JOIN chat_names cn ON cs.chat_id = cn.chat_id ORDER BY cs.uploaded DESC LIMIT 10"):
        chat_stats.append(dict(row))
    
    # Системная информация
    import shutil
    disk = shutil.disk_usage(PROJECT_DIR)
    mem_total = mem_avail = 0
    try:
        with open('/proc/meminfo') as f:
            for line in f:
                if 'MemTotal' in line: mem_total = int(line.split()[1]) * 1024
                if 'MemAvailable' in line: mem_avail = int(line.split()[1]) * 1024
    except: pass
    load = ['0','0','0']
    try:
        with open('/proc/loadavg') as f:
            load = f.read().split()[:3]
    except: pass
    net_rx = net_tx = 0
    try:
        with open('/proc/net/dev') as f:
            for line in f:
                if 'eth0' in line or 'ens' in line or 'enp' in line:
                    parts = line.split()
                    net_rx += int(parts[1])
                    net_tx += int(parts[9])
    except: pass
    
    conn.close()
    
    # Кэш системных данных на 5 секунд
    global _sys_cache
    now = time.time()
    if '_sys_cache' not in dir():
        _sys_cache = {'data': None, 'time': 0}
    if _sys_cache['data'] and now - _sys_cache['time'] < 5:
        sys_data = _sys_cache['data']
    else:
        sys_data = {
            'disk_free': disk.free,
            'mem_avail': mem_avail,
            'mem_total': mem_total,
            'load': [float(x) for x in load],
            'net_rx': net_rx,
            'net_tx': net_tx
        }
        _sys_cache = {'data': sys_data, 'time': now}
    
    return jsonify({
        'running': running,
        'heartbeat_age': round(age, 1),
        'stats': {'total': total, 'uploaded': uploaded, 'new_files': new_files, 'errors': errors, 'selected': selected, 'skipped': skipped, 'unloaded': unloaded, 'pending': pending},
        'session': session,
        'queue_counts': queue_counts,
        'active': active,
        'downloading': downloading,
        'compressing': compressing,
        'uploading': uploading,
        'compressed_count': compressed_count,
        'saved_bytes': saved_bytes,
        'file_err_count': file_err_count,
        'sys_err_count': sys_err_count,
        'scan_progress': scan_progress,
        'chat_stats': chat_stats,
        'system': sys_data
    })

@backup_bp.route('/')
def index():
    return redirect(url_for('backup.dashboard'))

@backup_bp.route('/dashboard')
def dashboard():
    conn = get_db()
    c = conn.cursor()
    
    running = is_running()
    age = get_heartbeat_age() if running else 0
    
    total = c.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    uploaded = c.execute("SELECT COUNT(*) FROM files WHERE state=?", (STATE_UPLOADED,)).fetchone()[0]
    new_files = c.execute("SELECT COUNT(*) FROM files WHERE state=?", (STATE_NEW,)).fetchone()[0]
    errors = c.execute("SELECT COUNT(*) FROM files WHERE state=?", (STATE_ERROR,)).fetchone()[0]
    selected = c.execute("SELECT COUNT(*) FROM files WHERE state=?", (STATE_SELECTED,)).fetchone()[0]
    skipped = c.execute("SELECT COUNT(*) FROM files WHERE state=?", (STATE_SKIPPED,)).fetchone()[0]
    unloaded = c.execute("SELECT COUNT(*) FROM files WHERE state=?", (STATE_UNLOADED,)).fetchone()[0]
    
    queue_counts = {}
    for row in c.execute("SELECT status, COUNT(*) as cnt FROM queue_items GROUP BY status"):
        queue_counts[row['status']] = row['cnt']
    pending = sum(queue_counts.get(s, 0) for s in (STATUS_PENDING_CHECK, STATUS_PENDING_DOWNLOAD, STATUS_PENDING_COMPRESS, STATUS_PENDING_UPLOAD))
    
    active = []
    for row in c.execute("SELECT qi.filename, qi.file_size, qp.worker_type, ap.progress, ap.speed, ap.eta FROM queue_items qi JOIN queue_processing qp ON qi.key = qp.key LEFT JOIN active_progress ap ON qi.key = ap.key"):
        active.append(dict(row))
    
    cs = c.execute("SELECT * FROM stage_stats WHERE stage='compress'").fetchone()
    saved_bytes = cs['saved_bytes'] if cs else 0
    compressed_count = cs['processed'] if cs else 0
    
    session = get_session_stats()
    
    scan_progress = {}
    for row in c.execute("SELECT * FROM scan_progress WHERE completed=0"):
        scan_progress[str(row['chat_id'])] = dict(row)
    
    file_err_count = c.execute("SELECT COUNT(*) FROM file_errors").fetchone()[0]
    sys_err_count = c.execute("SELECT COUNT(*) FROM system_errors").fetchone()[0]
    
    chat_stats = []
    for row in c.execute("SELECT cs.*, cn.name FROM chat_stats cs LEFT JOIN chat_names cn ON cs.chat_id = cn.chat_id ORDER BY cs.uploaded DESC"):
        chat_stats.append(dict(row))
    
    conn.close()
    
    return render_template('dashboard.html',
        running=running, heartbeat_age=age,
        stats={'total': total, 'uploaded': uploaded, 'new_files': new_files, 'errors': errors, 'selected': selected, 'skipped': skipped, 'unloaded': unloaded, 'pending': pending},
        session=session, active=active, scan_progress=scan_progress,
        chat_stats=chat_stats, compressed_count=compressed_count,
        saved_bytes=saved_bytes, file_err_count=file_err_count,
        sys_err_count=sys_err_count, fmt_size=fmt_size, fmt_bar=fmt_bar, FILE_EMOJI=FILE_EMOJI)

@backup_bp.route('/queue')
def queue():
    conn = get_db()
    c = conn.cursor()
    
    page = request.args.get('page', 1, type=int)
    per_page = 50
    offset = (page - 1) * per_page
    
    total = c.execute("SELECT COUNT(*) FROM queue_items").fetchone()[0]
    
    items = []
    for row in c.execute("""
        SELECT qi.*, qp.worker_type, ap.progress, ap.speed, ap.stage,
               cn.name as chat_name, t.topic_name
        FROM queue_items qi
        LEFT JOIN queue_processing qp ON qi.key = qp.key
        LEFT JOIN active_progress ap ON qi.key = ap.key
        LEFT JOIN chat_names cn ON qi.chat_id = cn.chat_id
        LEFT JOIN topics t ON qi.chat_id = t.chat_id AND qi.topic_id = t.topic_id
        ORDER BY qi.created_at DESC
        LIMIT ? OFFSET ?
    """, (per_page, offset)):
        items.append(dict(row))
    
    status_counts = {}
    for row in c.execute("SELECT status, COUNT(*) as cnt FROM queue_items GROUP BY status"):
        status_counts[row['status']] = row['cnt']
    
    conn.close()
    total_pages = max(1, (total + per_page - 1) // per_page)
    
    return render_template('queue.html',
        items=items, status_counts=status_counts,
        page=page, total_pages=total_pages, total=total,
        fmt_size=fmt_size)

@backup_bp.route('/history')
def history():
    conn = get_db()
    c = conn.cursor()
    
    page = request.args.get('page', 1, type=int)
    per_page = 50
    offset = (page - 1) * per_page
    
    total = c.execute("SELECT COUNT(*) FROM history").fetchone()[0]
    
    # Группируем по message_id
    rows = c.execute("SELECT * FROM history ORDER BY timestamp DESC LIMIT ? OFFSET ?", (per_page * 3, offset)).fetchall()
    
    grouped = {}
    for row in rows:
        item = dict(row)
        key = f"{item['chat_id']}_{item['message_id']}"
        ts = item.get('timestamp')
        if ts and ts > 1000000000:
            from datetime import datetime
            item['date_str'] = datetime.fromtimestamp(ts).strftime('%d.%m.%Y %H:%M:%S')
        else:
            item['date_str'] = str(ts)
        
        if key not in grouped:
            item['statuses'] = [item['status']]
            grouped[key] = item
        else:
            if item['status'] not in grouped[key]['statuses']:
                grouped[key]['statuses'].append(item['status'])
                # Сортируем в порядке: queued -> downloaded -> compressed -> uploaded
                order = {'queued': 0, 'downloaded': 1, 'compressed': 2, 'uploaded': 3, 'skipped': 4, 'error': 5}
                grouped[key]['statuses'].sort(key=lambda x: order.get(x, 99))
            # Берём compressed_size из compressed, остальное — свежее
            if item['status'] == 'compressed' and item.get('compressed_size', 0) > 0:
                grouped[key]['compressed_size'] = item['compressed_size']
            if item['timestamp'] > grouped[key]['timestamp']:
                for field in ['date_str', 'stage', 'error']:
                    grouped[key][field] = item[field]
            # size всегда из downloaded/queued (оригинал), compressed_size из compressed
            if item['status'] in ('downloaded', 'queued') and item.get('size', 0) > 0:
                grouped[key]['size'] = item['size']
    
    # Сортируем по времени
    items = sorted(grouped.values(), key=lambda x: x['timestamp'], reverse=True)[:per_page]
    
    conn.close()
    total_pages = max(1, (total + per_page - 1) // per_page)
    
    return render_template('history.html',
        items=items, page=page, total_pages=total_pages,
        fmt_size=fmt_size)

@backup_bp.route('/logs')
def logs():
    log_type = request.args.get('type', 'backup')
    page = request.args.get('page', 1, type=int)
    per_page = 100
    
    if log_type == 'web':
        all_lines = read_log(WEB_LOG_FILE, 2000)
    else:
        all_lines = read_log(LOG_FILE, 5000)
    
    total_lines = len(all_lines)
    total_pages = max(1, (total_lines + per_page - 1) // per_page)
    # page 1 = самые свежие (конец), page N = старее
    start = total_lines - (page * per_page)
    if start < 0:
        start = 0
    end = start + per_page
    if end > total_lines:
        end = total_lines
    content = all_lines[start:end]
    
    return render_template('logs.html',
        content=content, log_type=log_type,
        page=page, total_pages=total_pages)

@backup_bp.route('/errors')
def errors_view():
    conn = get_db()
    c = conn.cursor()
    file_rows = c.execute("SELECT * FROM file_errors ORDER BY timestamp DESC LIMIT 300").fetchall()
    sys_errs = [dict(r) for r in c.execute("SELECT * FROM system_errors ORDER BY timestamp DESC LIMIT 100")]
    
    # Группируем ошибки файлов, убираем дубликаты
    from datetime import datetime
    grouped_files = {}
    for row in file_rows:
        item = dict(row)
        key = f"{item['chat_id']}_{item['message_id']}"
        ts = item.get('timestamp')
        item['date_str'] = datetime.fromtimestamp(ts).strftime('%d.%m.%Y %H:%M:%S') if ts and ts > 1000000000 else str(ts)
        err_tuple = (item['stage'], item['error'][:100])  # ключ уникальности
        if key not in grouped_files:
            item['errors'] = [{'stage': item['stage'], 'error': item['error']}]
            item['_seen'] = {err_tuple}
            grouped_files[key] = item
        else:
            if err_tuple not in grouped_files[key]['_seen']:
                grouped_files[key]['_seen'].add(err_tuple)
                grouped_files[key]['errors'].append({'stage': item['stage'], 'error': item['error']})
    
    file_errs = sorted(grouped_files.values(), key=lambda x: x['timestamp'], reverse=True)[:100]
    for f in file_errs: f.pop('_seen', None)
    conn.close()
    return render_template('errors.html', file_errs=file_errs, sys_errs=sys_errs)

@backup_bp.route('/chats')
def chats():
    conn = get_db()
    c = conn.cursor()
    chat_ids = get_chat_ids_from_settings()
    
    chat_data = []
    for cid in chat_ids:
        name_row = c.execute("SELECT name FROM chat_names WHERE chat_id=?", (cid,)).fetchone()
        name = name_row['name'] if name_row else f"Chat {cid}"
        stats = dict(c.execute("SELECT * FROM chat_stats WHERE chat_id=?", (cid,)).fetchone() or {})
        
        topic_rows = list(c.execute("SELECT * FROM topics WHERE chat_id=? ORDER BY topic_name", (cid,)))
        topics = []
        for t in topic_rows:
            t = dict(t)
            tid = t['topic_id']
            t['files_count'] = c.execute("SELECT COUNT(*) FROM files WHERE chat_id=? AND topic_id=?", (cid, tid)).fetchone()[0]
            t['uploaded_count'] = c.execute("SELECT COUNT(*) FROM files WHERE chat_id=? AND topic_id=? AND state=?", (cid, tid, STATE_UPLOADED)).fetchone()[0]
            t['selected_count'] = c.execute("SELECT COUNT(*) FROM files WHERE chat_id=? AND topic_id=? AND state=?", (cid, tid, STATE_SELECTED)).fetchone()[0]
            t['error_count'] = c.execute("SELECT COUNT(*) FROM files WHERE chat_id=? AND topic_id=? AND state=?", (cid, tid, STATE_ERROR)).fetchone()[0]
            topics.append(t)
        
        scan = dict(c.execute("SELECT * FROM scan_progress WHERE chat_id=?", (cid,)).fetchone() or {})
        
        chat_data.append({
            'id': cid, 'name': name, 'stats': stats, 'topics_count': c.execute('SELECT COUNT(*) FROM topics WHERE chat_id=?', (cid,)).fetchone()[0], 'scan': scan
        })
    
    conn.close()
    return render_template('chats.html', chats=chat_data, fmt_size=fmt_size)

@backup_bp.route('/chat/<path:chat_id>')
def chat_detail(chat_id):
    chat_id = int(chat_id)
    conn = get_db()
    c = conn.cursor()
    
    name_row = c.execute("SELECT name FROM chat_names WHERE chat_id=?", (chat_id,)).fetchone()
    name = name_row['name'] if name_row else f"Chat {chat_id}"
    stats = dict(c.execute("SELECT * FROM chat_stats WHERE chat_id=?", (chat_id,)).fetchone() or {})
    
    topic_rows = list(c.execute("SELECT * FROM topics WHERE chat_id=? ORDER BY topic_name", (chat_id,)))
    topics = []
    for t in topic_rows:
        t = dict(t)
        tid = t['topic_id']
        t['files_count'] = c.execute("SELECT COUNT(*) FROM files WHERE chat_id=? AND topic_id=?", (chat_id, tid)).fetchone()[0]
        t['uploaded_count'] = c.execute("SELECT COUNT(*) FROM files WHERE chat_id=? AND topic_id=? AND state=?", (chat_id, tid, STATE_UPLOADED)).fetchone()[0]
        t['selected_count'] = c.execute("SELECT COUNT(*) FROM files WHERE chat_id=? AND topic_id=? AND state=?", (chat_id, tid, STATE_SELECTED)).fetchone()[0]
        t['error_count'] = c.execute("SELECT COUNT(*) FROM files WHERE chat_id=? AND topic_id=? AND state=?", (chat_id, tid, STATE_ERROR)).fetchone()[0]
        topics.append(t)
    
    scan = dict(c.execute("SELECT * FROM scan_progress WHERE chat_id=?", (chat_id,)).fetchone() or {})
    
    conn.close()
    return render_template('chat_detail.html',
        chat_id=chat_id, name=name, stats=stats, topics=topics,
        scan=scan, fmt_size=fmt_size)

@backup_bp.route('/chat/<path:chat_id>/topic/<int:topic_id>')
def topic_files(chat_id, topic_id):
    chat_id = int(chat_id)
    conn = get_db()
    c = conn.cursor()
    
    name_row = c.execute("SELECT name FROM chat_names WHERE chat_id=?", (chat_id,)).fetchone()
    chat_name = name_row['name'] if name_row else f"Chat {chat_id}"
    topic = dict(c.execute("SELECT * FROM topics WHERE chat_id=? AND topic_id=?", (chat_id, topic_id)).fetchone() or {'topic_name': f'Topic {topic_id}', 'is_selected': False})
    
    type_filter = request.form.get('type', request.args.get('type', 'all'))
    status_filter = request.form.get('status', request.args.get('status', 'all'))
    sort_by = request.form.get('sort', request.args.get('sort', 'date'))
    sort_order = request.form.get('order', request.args.get('order', 'desc'))
    page = request.args.get('page', 1, type=int)
    per_page = 30
    
    query = "SELECT * FROM files WHERE chat_id=? AND topic_id=?"
    params = [chat_id, topic_id]
    
    if type_filter != 'all':
        query += " AND file_type=?"
        params.append(type_filter)
    
    state_map = {'uploaded': STATE_UPLOADED, 'new': STATE_NEW, 'selected': STATE_SELECTED, 'error': STATE_ERROR, 'skipped': STATE_SKIPPED, 'unloaded': STATE_UNLOADED}
    if status_filter in state_map:
        query += " AND state=?"
        params.append(state_map[status_filter])
    elif status_filter == 'unuploaded':
        query += " AND state IN (?,?,?)"
        params.extend([STATE_UNLOADED, STATE_NEW, STATE_ERROR])
    
    sort_map = {'date': 'timestamp', 'name': 'filename', 'size': 'size'}
    order = 'DESC' if sort_order == 'desc' else 'ASC'
    query += f" ORDER BY {sort_map.get(sort_by, 'timestamp')} {order}"
    
    total = c.execute(f"SELECT COUNT(*) FROM ({query})", params).fetchone()[0]
    total_pages = max(1, (total + per_page - 1) // per_page)
    offset = (page - 1) * per_page
    
    files = [dict(row) for row in c.execute(query + " LIMIT ? OFFSET ?", params + [per_page, offset])]
    
    stats = {
        'total': c.execute("SELECT COUNT(*) FROM files WHERE chat_id=? AND topic_id=?", (chat_id, topic_id)).fetchone()[0],
        'uploaded': c.execute("SELECT COUNT(*) FROM files WHERE chat_id=? AND topic_id=? AND state=?", (chat_id, topic_id, STATE_UPLOADED)).fetchone()[0],
        'selected': c.execute("SELECT COUNT(*) FROM files WHERE chat_id=? AND topic_id=? AND state=?", (chat_id, topic_id, STATE_SELECTED)).fetchone()[0],
        'error': c.execute("SELECT COUNT(*) FROM files WHERE chat_id=? AND topic_id=? AND state=?", (chat_id, topic_id, STATE_ERROR)).fetchone()[0],
        'new': c.execute("SELECT COUNT(*) FROM files WHERE chat_id=? AND topic_id=? AND state=?", (chat_id, topic_id, STATE_NEW)).fetchone()[0],
        'unloaded': c.execute("SELECT COUNT(*) FROM files WHERE chat_id=? AND topic_id=? AND state=?", (chat_id, topic_id, STATE_UNLOADED)).fetchone()[0],
    }
    
    conn.close()
    
    # Ищем реальный путь на диске
    sanitized_chat = chat_name.replace(' ', '_')
    sanitized_topic = topic['topic_name'].replace(' ', '_') if topic.get('topic_name') else ''
    disk_path = f'/tg_backup/{sanitized_chat}/{sanitized_topic}'
    
    return render_template('topic_files.html',
        chat_id=chat_id, topic_id=topic_id,
        chat_name=chat_name, topic=topic, files=files,
        stats=stats, type_filter=type_filter, status_filter=status_filter,
        sort_by=sort_by, sort_order=sort_order,
        page=page, total_pages=total_pages,
        fmt_size=fmt_size, FILE_EMOJI=FILE_EMOJI,
        disk_path=disk_path)

@backup_bp.route('/settings')
def settings():
    conn = get_db()
    c = conn.cursor()
    windows = []
    auto = False
    settings_dict = {}
    try:
        for row in c.execute("SELECT key, value FROM settings"):
            try:
                settings_dict[row['key']] = json.loads(row['value'])
            except:
                settings_dict[row['key']] = row['value']
        windows = settings_dict.get('windows', [])
        auto = settings_dict.get('auto_backup_enabled', False)
    except: pass
    conn.close()
    return render_template('settings.html',
        windows=windows, auto=auto, settings=settings_dict)

# ==================== ДЕЙСТВИЯ ====================

@backup_bp.route('/start', methods=['POST'])
def start_backup():
    if not is_running():
        time.sleep(60)
        subprocess.Popen([PYTHON, MAIN_PY], cwd=PROJECT_DIR, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    return redirect(url_for('backup.dashboard'))

@backup_bp.route('/stop', methods=['POST'])
def stop_backup():
    if os.path.exists(PID_FILE):
        time.sleep(60)
        with open(PID_FILE) as f:
            try: os.kill(int(f.read().strip()), signal.SIGTERM)
            except: pass
    return redirect(url_for('backup.dashboard'))

@backup_bp.route('/force_kill', methods=['POST'])
def force_kill():
    if os.path.exists(PID_FILE):
        with open(PID_FILE) as f:
            try:
                pid = int(f.read().strip())
                os.kill(pid, signal.SIGKILL)
            except: pass
        try: os.remove(PID_FILE)
        except: pass
    return redirect(url_for('backup.dashboard'))

@backup_bp.route('/clear_queue', methods=['POST'])
def clear_queue():
    conn = get_db()
    conn.execute("DELETE FROM queue_items")
    conn.execute("DELETE FROM queue_processing")
    conn.execute("DELETE FROM queue_retry")
    conn.execute("DELETE FROM active_progress")
    conn.commit()
    conn.close()
    return redirect(url_for('backup.dashboard'))

@backup_bp.route('/cleanup_temp', methods=['POST'])
def cleanup_temp():
    """Очищает временные файлы (.tmp, _compressed.*)."""
    deleted = 0
    download_dir = os.path.join(PROJECT_DIR, DOWNLOAD_DIR)
    if os.path.exists(download_dir):
        for root, dirs, files in os.walk(download_dir):
            for f in files:
                if '.tmp' in f or '_compressed.' in f or '.compressed.' in f:
                    try:
                        os.unlink(os.path.join(root, f))
                        deleted += 1
                    except: pass
    # Flash message через редирект с параметром
    return redirect(url_for('backup.dashboard', msg=f'Удалено {deleted} временных файлов'))

@backup_bp.route('/clear_errors', methods=['POST'])
def clear_errors():
    conn = get_db()
    conn.execute("DELETE FROM file_errors")
    conn.execute("DELETE FROM system_errors")
    conn.commit()
    conn.close()
    return redirect(url_for('backup.errors_view'))

@backup_bp.route('/reset_all_stats', methods=['POST'])
def reset_all_stats():
    conn = get_db()
    # Очищаем все таблицы
    tables = ['chat_names', 'topics', 'files', 'chat_stats', 'scan_progress', 'history', 'file_errors', 'system_errors', 'stage_stats', 'queue_items', 'queue_processing', 'queue_retry', 'active_progress']
    for t in tables:
        conn.execute(f"DELETE FROM {t}")
    conn.execute("UPDATE settings SET value='[]', updated_at=? WHERE key='chat_ids'", (time.time(),))
    conn.execute("INSERT OR REPLACE INTO app_state (key, value, updated_at) VALUES ('selected_snapshot', '0', ?)", (time.time(),))
    conn.commit()
    conn.close()
    return redirect(url_for('backup.dashboard'))

@backup_bp.route('/chat/<path:chat_id>/delete', methods=['POST'])
def delete_chat(chat_id):
    chat_id = int(chat_id)
    conn = get_db()
    for t in ['queue_items', 'files', 'topics', 'chat_names', 'chat_stats', 'scan_progress', 'history', 'file_errors']:
        conn.execute(f"DELETE FROM {t} WHERE chat_id=?", (chat_id,))
    conn.commit()
    row = conn.execute("SELECT value FROM settings WHERE key='chat_ids'").fetchone()
    if row:
        chat_ids = json.loads(row['value'])
        if chat_id in chat_ids:
            chat_ids.remove(chat_id)
            conn.execute("UPDATE settings SET value=?, updated_at=? WHERE key='chat_ids'", (json.dumps(chat_ids), time.time()))
            conn.commit()
    conn.close()
    return redirect(url_for('backup.chats'))

@backup_bp.route('/chat/<path:chat_id>/reset_errors', methods=['POST'])
def reset_chat_errors(chat_id):
    chat_id = int(chat_id)
    conn = get_db()
    conn.execute("UPDATE files SET state=?, attempts=0, last_error=NULL WHERE chat_id=? AND state=?", (STATE_UNLOADED, chat_id, STATE_ERROR))
    conn.execute("UPDATE chat_stats SET errors=0 WHERE chat_id=?", (chat_id,))
    conn.commit()
    conn.close()
    return redirect(url_for('backup.chat_detail', chat_id=chat_id))

@backup_bp.route('/chat/<path:chat_id>/refresh_cache', methods=['POST'])
def refresh_cache(chat_id):
    chat_id = int(chat_id)
    subprocess.Popen([PYTHON, MAIN_PY, '--scan-only', '--full-scan', f'--chat-id={chat_id}'],
                    cwd=PROJECT_DIR, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    return redirect(url_for('backup.chat_detail', chat_id=chat_id))

@backup_bp.route('/refresh_all_cache', methods=['POST'])
def refresh_all_cache():
    subprocess.Popen([PYTHON, MAIN_PY, '--scan-only', '--full-scan'],
                    cwd=PROJECT_DIR, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    return redirect(url_for('backup.chats'))

@backup_bp.route('/topic/<path:chat_id>/<int:topic_id>/select', methods=['POST'])
def select_topic(chat_id, topic_id):
    chat_id = int(chat_id)
    conn = get_db()
    conn.execute("UPDATE topics SET is_selected=1 WHERE chat_id=? AND topic_id=?", (chat_id, topic_id))
    conn.execute("UPDATE files SET state=? WHERE chat_id=? AND topic_id=? AND state NOT IN (?,?,?)", (STATE_SELECTED, chat_id, topic_id, STATE_UPLOADED, STATE_SKIPPED, STATE_ERROR))
    conn.commit()
    conn.close()
    return redirect(url_for('backup.chat_detail', chat_id=chat_id))

@backup_bp.route('/topic/<path:chat_id>/<int:topic_id>/deselect', methods=['POST'])
def deselect_topic(chat_id, topic_id):
    chat_id = int(chat_id)
    conn = get_db()
    conn.execute("UPDATE topics SET is_selected=0 WHERE chat_id=? AND topic_id=?", (chat_id, topic_id))
    conn.execute("UPDATE files SET state=? WHERE chat_id=? AND topic_id=? AND state=?", (STATE_UNLOADED, chat_id, topic_id, STATE_SELECTED))
    conn.commit()
    conn.close()
    return redirect(url_for('backup.chat_detail', chat_id=chat_id))

@backup_bp.route('/file/<path:chat_id>/<int:message_id>/toggle', methods=['POST'])
def toggle_file(chat_id, message_id):
    chat_id = int(chat_id)
    topic_id = request.args.get('topic_id', 0, type=int)
    conn = get_db()
    row = conn.execute("SELECT state FROM files WHERE chat_id=? AND message_id=?", (chat_id, message_id)).fetchone()
    if row:
        if row['state'] in (STATE_UNLOADED, STATE_NEW, STATE_ERROR, STATE_UPLOADED):
            conn.execute("UPDATE files SET state=? WHERE chat_id=? AND message_id=?", (STATE_SELECTED, chat_id, message_id))
        elif row['state'] == STATE_SELECTED:
            conn.execute("UPDATE files SET state=? WHERE chat_id=? AND message_id=?", (STATE_UNLOADED, chat_id, message_id))
        conn.commit()
    conn.close()
    return redirect(request.referrer or url_for('backup.topic_files', chat_id=chat_id, topic_id=topic_id))

@backup_bp.route('/topic/<path:chat_id>/<int:topic_id>/deselect_all', methods=['POST'])
def deselect_all_files(chat_id, topic_id):
    chat_id = int(chat_id)
    conn = get_db()
    conn.execute("UPDATE files SET state=? WHERE chat_id=? AND topic_id=? AND state=?", (STATE_UNLOADED, chat_id, topic_id, STATE_SELECTED))
    conn.commit()
    conn.close()
    type_filter = request.form.get('type', request.args.get('type', 'all'))
    status_filter = request.form.get('status', request.args.get('status', 'all'))
    sort_by = request.form.get('sort', request.args.get('sort', 'date'))
    sort_order = request.form.get('order', request.args.get('order', 'desc'))
    page = request.form.get('page', request.args.get('page', 1))
    return redirect(url_for('backup.topic_files', chat_id=chat_id, topic_id=topic_id, type=type_filter, status=status_filter, sort=sort_by, order=sort_order, page=page))

@backup_bp.route('/topic/<path:chat_id>/<int:topic_id>/select_new', methods=['POST'])
def select_new_files(chat_id, topic_id):
    chat_id = int(chat_id)
    conn = get_db()
    conn.execute("UPDATE files SET state=? WHERE chat_id=? AND topic_id=? AND state=?", (STATE_SELECTED, chat_id, topic_id, STATE_NEW))
    conn.commit()
    conn.close()
    type_filter = request.form.get('type', request.args.get('type', 'all'))
    status_filter = request.form.get('status', request.args.get('status', 'all'))
    sort_by = request.form.get('sort', request.args.get('sort', 'date'))
    sort_order = request.form.get('order', request.args.get('order', 'desc'))
    page = request.form.get('page', request.args.get('page', 1))
    return redirect(url_for('backup.topic_files', chat_id=chat_id, topic_id=topic_id, type=type_filter, status=status_filter, sort=sort_by, order=sort_order, page=page))

@backup_bp.route('/topic/<path:chat_id>/<int:topic_id>/select_unuploaded', methods=['POST'])
def select_unuploaded_files(chat_id, topic_id):
    chat_id = int(chat_id)
    conn = get_db()
    conn.execute("UPDATE files SET state=? WHERE chat_id=? AND topic_id=? AND state IN (?,?,?)", (STATE_SELECTED, chat_id, topic_id, STATE_UNLOADED, STATE_NEW, STATE_ERROR))
    conn.commit()
    conn.close()
    type_filter = request.form.get('type', request.args.get('type', 'all'))
    status_filter = request.form.get('status', request.args.get('status', 'all'))
    sort_by = request.form.get('sort', request.args.get('sort', 'date'))
    sort_order = request.form.get('order', request.args.get('order', 'desc'))
    page = request.form.get('page', request.args.get('page', 1))
    return redirect(url_for('backup.topic_files', chat_id=chat_id, topic_id=topic_id, type=type_filter, status=status_filter, sort=sort_by, order=sort_order, page=page))

@backup_bp.route('/topic/<path:chat_id>/<int:topic_id>/reset_errors', methods=['POST'])
def reset_topic_errors(chat_id, topic_id):
    chat_id = int(chat_id)
    conn = get_db()
    conn.execute("UPDATE files SET state=?, attempts=0, last_error=NULL WHERE chat_id=? AND topic_id=? AND state=?", (STATE_UNLOADED, chat_id, topic_id, STATE_ERROR))
    conn.commit()
    conn.close()
    type_filter = request.form.get('type', request.args.get('type', 'all'))
    status_filter = request.form.get('status', request.args.get('status', 'all'))
    sort_by = request.form.get('sort', request.args.get('sort', 'date'))
    sort_order = request.form.get('order', request.args.get('order', 'desc'))
    page = request.form.get('page', request.args.get('page', 1))
    return redirect(url_for('backup.topic_files', chat_id=chat_id, topic_id=topic_id, type=type_filter, status=status_filter, sort=sort_by, order=sort_order, page=page))

@backup_bp.route('/settings/add_window', methods=['POST'])
def add_window():
    start = request.form.get('start', '').strip()
    end = request.form.get('end', '').strip()
    if start and end:
        conn = get_db()
        row = conn.execute("SELECT value FROM settings WHERE key='windows'").fetchone()
        windows = json.loads(row['value']) if row else []
        windows.append({'start': start, 'end': end})
        conn.execute("INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES ('windows', ?, ?)", (json.dumps(windows), time.time()))
        conn.commit()
        conn.close()
    return redirect(url_for('backup.settings'))

@backup_bp.route('/settings/remove_window/<int:idx>', methods=['POST'])
def remove_window(idx):
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key='windows'").fetchone()
    if row:
        windows = json.loads(row['value'])
        if 0 <= idx < len(windows):
            windows.pop(idx)
            conn.execute("UPDATE settings SET value=?, updated_at=? WHERE key='windows'", (json.dumps(windows), time.time()))
            conn.commit()
    conn.close()
    return redirect(url_for('backup.settings'))

@backup_bp.route('/settings/clear_windows', methods=['POST'])
def clear_windows():
    conn = get_db()
    conn.execute("UPDATE settings SET value='[]', updated_at=? WHERE key='windows'", (time.time(),))
    conn.commit()
    conn.close()
    return redirect(url_for('backup.settings'))

@backup_bp.route('/settings/toggle_auto', methods=['POST'])
def toggle_auto():
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key='auto_backup_enabled'").fetchone()
    current = json.loads(row['value']) if row else False
    conn.execute("INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES ('auto_backup_enabled', ?, ?)", (json.dumps(not current), time.time()))
    conn.commit()
    conn.close()
    return redirect(url_for('backup.settings'))

@backup_bp.route('/settings/add_chat', methods=['POST'])
def add_chat():
    chat_id = request.form.get('chat_id', '').strip()
    if chat_id:
        try:
            cid = int(chat_id)
            conn = get_db()
            row = conn.execute("SELECT value FROM settings WHERE key='chat_ids'").fetchone()
            chat_ids = json.loads(row['value']) if row else []
            if cid not in chat_ids:
                chat_ids.append(cid)
                conn.execute("INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES ('chat_ids', ?, ?)", (json.dumps(chat_ids), time.time()))
                conn.commit()
            conn.close()
        except ValueError: pass
    return redirect(url_for('backup.settings'))


@backup_bp.route("/yadisk")
def yadisk_browse():
    path = request.args.get("path", YA_BASE_PATH)
    if not YA_TOKEN:
        return render_template("yadisk.html", error="Токен Яндекс.Диска не настроен", files=[], path=path)
    headers = {"Authorization": f"OAuth {YA_TOKEN}"}
    api_url = f"https://cloud-api.yandex.net/v1/disk/resources?path={path}&limit=100"
    try:
        r = requests.get(api_url, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()
            items = data.get("_embedded", {}).get("items", [])
            items.sort(key=lambda x: (x["type"] != "dir", x["name"].lower()))
            return render_template("yadisk.html", files=items, path=path, error=None)
        else:
            error = f"Ошибка API: {r.status_code}"
            if r.status_code == 401:
                error = "Ошибка авторизации. Проверьте токен Яндекс.Диска"
            elif r.status_code == 404:
                error = f"Путь не найден: {path}"
            return render_template("yadisk.html", error=error, files=[], path=path)
    except Exception as e:
        return render_template("yadisk.html", error=f"Ошибка подключения: {str(e)}", files=[], path=path)

# Кэш прямых ссылок (путь -> (url, expires))
_disk_link_cache = {}

@backup_bp.route('/yadisk/file')
def yadisk_file():
    """Потоковое проксирование файла с Яндекс.Диска."""
    path = request.args.get('path', '')
    if not path or not YA_TOKEN:
        return "No path or token", 400
    
    headers = {'Authorization': f'OAuth {YA_TOKEN}'}
    try:
        # Получаем ссылку (с кэшем)
        now = time.time()
        cached = _disk_link_cache.get(path)
        if cached and cached[1] > now:
            download_url = cached[0]
        else:
            r = requests.get(f'https://cloud-api.yandex.net/v1/disk/resources/download?path={path}', headers=headers, timeout=10)
            if r.status_code != 200:
                return f"Файл не найден: {path}", 404
            download_url = r.json().get('href', '')
            if not download_url:
                return "Ссылка не получена", 400
            _disk_link_cache[path] = (download_url, now + 600)
        
        # Проксируем потоково
        from flask import Response
        file_r = requests.get(download_url, stream=True, timeout=120)
        filename = path.split('/')[-1]
        content_type = file_r.headers.get('Content-Type', 'application/octet-stream')
        content_length = file_r.headers.get('Content-Length', '')
        
        headers_out = {'Content-Disposition': f'inline; filename="{filename}"'}
        if content_length:
            headers_out['Content-Length'] = content_length
        
        return Response(
            file_r.iter_content(chunk_size=131072),
            content_type=content_type,
            headers=headers_out,
            direct_passthrough=True
        )
    except Exception as e:
        return f"Ошибка: {str(e)}", 500


@backup_bp.route('/yadisk/links')
def yadisk_links():
    """Возвращает прямые ссылки для списка файлов."""
    paths = request.args.get('paths', '').split(',')
    if not paths or not YA_TOKEN:
        import shutil
    disk = shutil.disk_usage(PROJECT_DIR)
    mem_total = mem_avail = 0
    try:
        with open('/proc/meminfo') as f:
            for line in f:
                if 'MemTotal' in line: mem_total = int(line.split()[1]) * 1024
                if 'MemAvailable' in line: mem_avail = int(line.split()[1]) * 1024
    except: pass
    load = ['0','0','0']
    try:
        with open('/proc/loadavg') as f:
            load = f.read().split()[:3]
    except: pass
    net_rx = net_tx = 0
    try:
        with open('/proc/net/dev') as f:
            for line in f:
                if 'eth0' in line or 'ens' in line or 'enp' in line:
                    parts = line.split()
                    net_rx += int(parts[1])
                    net_tx += int(parts[9])
    except: pass
    
    return jsonify({})
    
    headers = {'Authorization': f'OAuth {YA_TOKEN}'}
    result = {}
    now = time.time()
    
    for path in paths:
        if not path: continue
        # Проверяем кэш
        cached = _disk_link_cache.get(path)
        if cached and cached[1] > now:
            result[path] = cached[0]
            continue
        try:
            r = requests.get(f'https://cloud-api.yandex.net/v1/disk/resources/download?path={path}', headers=headers, timeout=5)
            if r.status_code == 200:
                url = r.json().get('href', '')
                if url:
                    _disk_link_cache[path] = (url, now + 600)
                    result[path] = url
        except:
            pass
    
    return jsonify(result)

@backup_bp.route('/dashboard/controls')
def dashboard_controls():
    conn = get_db()
    c = conn.cursor()
    running = is_running()
    age = get_heartbeat_age() if running else 0
    
    active = []
    for row in c.execute("SELECT qi.filename, qi.file_size, qp.worker_type, ap.progress, ap.speed, ap.eta FROM queue_items qi JOIN queue_processing qp ON qi.key = qp.key LEFT JOIN active_progress ap ON qi.key = ap.key"):
        active.append(dict(row))
    
    scan_progress = {}
    for row in c.execute("SELECT * FROM scan_progress WHERE completed=0"):
        scan_progress[str(row['chat_id'])] = dict(row)
    
    conn.close()
    return render_template('controls.html', running=running, heartbeat_age=age, active=active, scan_progress=scan_progress, fmt_size=fmt_size)

app.register_blueprint(backup_bp)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=False)
