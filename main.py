from flask import Flask, render_template, request, jsonify, send_from_directory
import sqlite3
import os
import logging
from datetime import datetime
import threading
import time
from contextlib import contextmanager
from typing import Iterator
import shutil

import online_history

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 1. 获取当前文件 (main.py) 所在的目录的绝对路径
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
SRC_DB_PATH = os.environ.get('TRIMMEDIA_DB') or os.path.join(BASE_DIR, 'database', 'trimmedia.db')
TMP_DB_PATH = os.path.join(BASE_DIR, 'trimmedia_tmp.db')
ACTIVITY_SRC_PATH = os.path.join(os.path.dirname(SRC_DB_PATH), 'trimactivity.db')
ACTIVITY_TMP_PATH = os.path.join(BASE_DIR, 'trimactivity_tmp.db')

# ### NEW: Configuration for lazy, atomic copy ###
DB_EXPIRATION_SECONDS = 60  # 数据库副本的过期时间（60秒）
_last_copy_time = 0.0       # 上次拷贝成功的时间戳，初始化为0以强制首次拷贝
_db_copy_lock = threading.Lock() # 确保只有一个线程执行拷贝操作

def _sqlite_ro_uri(path):
    """只读 URI。Windows 路径里的反斜杠不能直接放进 file: URI。"""
    return "file:" + os.path.abspath(path).replace("\\", "/") + "?mode=ro"


def _remove_copy(path):
    try:
        if os.path.lexists(path):
            os.remove(path)
    except OSError as exc:
        logger.warning(f"清理副本文件失败 {path}: {exc}")


def _atomic_copy_one(src_path, dst_path):
    """只读源文件的字节，先写入临时文件再替换。不在源目录创建任何文件。"""
    atomic_tmp_path = dst_path + ".new"
    try:
        if not os.path.isfile(src_path):
            return False
        if not os.access(src_path, os.R_OK):
            logger.warning(f"数据库文件无法读取: {src_path}")
            return False
        shutil.copy2(src_path, atomic_tmp_path)
        os.replace(atomic_tmp_path, dst_path)
        return True
    except Exception as e:
        logger.error(f"数据库拷贝失败 {src_path}: {e}")
        _remove_copy(atomic_tmp_path)
        return False


def _atomic_copy_file(src_path, dst_path):
    """拷贝数据库。WAL 模式下一并拷贝 -wal，shm 留在副本旁边由 SQLite 重建。"""
    if not _atomic_copy_one(src_path, dst_path):
        return False
    wal_src = src_path + "-wal"
    wal_dst = dst_path + "-wal"
    if os.path.isfile(wal_src):
        if not _atomic_copy_one(wal_src, wal_dst):
            _remove_copy(wal_dst)
    else:
        _remove_copy(wal_dst)
    # 源目录里的 shm 是影视进程的共享内存，拷走没有意义，也不能在源目录新建。
    _remove_copy(dst_path + "-shm")
    return True


def _atomic_copy_database():
    """
    ### NEW ###
    Performs an atomic copy of the database from the source to the temporary path.
    This is done by copying to a new file first, then renaming it.
    登录会话库 trimactivity.db 和媒体库放在同一目录，用来向影视服务查询在线片库进度。
    """
    global _last_copy_time
    logger.info("开始执行数据库原子化拷贝...")

    if not os.path.exists(SRC_DB_PATH):
        logger.warning(f"源数据库文件不存在: {SRC_DB_PATH}")
        return

    if _atomic_copy_file(SRC_DB_PATH, TMP_DB_PATH):
        _last_copy_time = time.time()
        logger.info(f"数据库原子化拷贝成功: {SRC_DB_PATH} -> {TMP_DB_PATH}")
    if os.path.isfile(ACTIVITY_SRC_PATH):
        if _atomic_copy_file(ACTIVITY_SRC_PATH, ACTIVITY_TMP_PATH):
            wal = "，含 wal" if os.path.isfile(ACTIVITY_TMP_PATH + "-wal") else ""
            logger.info(f"登录会话库拷贝成功: {ACTIVITY_SRC_PATH} -> {ACTIVITY_TMP_PATH}{wal}")

@contextmanager
def get_db_connection() -> Iterator[sqlite3.Connection]:
    """
    ### CHANGED: Implemented lazy loading with expiration and thread safety ###
    
    为每个请求创建一个新的只读数据库连接。
    在连接前，会检查临时数据库是否已过期或不存在。如果需要，会触发一次
    线程安全的、原子化的数据库拷贝。
    """
    
    # 检查数据库副本是否过期或不存在
    is_expired = (time.time() - _last_copy_time) > DB_EXPIRATION_SECONDS
    if not os.path.exists(TMP_DB_PATH) or is_expired:
        # 使用锁来防止多个请求同时触发拷贝 (Race Condition)
        with _db_copy_lock:
            # 双重检查：在获取锁后，再次检查是否需要拷贝
            # 因为可能在等待锁的时候，已经有另一个线程完成了拷贝
            is_still_expired = (time.time() - _last_copy_time) > DB_EXPIRATION_SECONDS
            if not os.path.exists(TMP_DB_PATH) or is_still_expired:
                if is_still_expired:
                    logger.info("数据库副本已过期，触发更新。")
                else:
                    logger.info("数据库副本不存在，触发更新。")
                _atomic_copy_database()

    # --- 以下为原始的连接逻辑 ---
    conn = None
    max_retries = 10
    base_delay = 0.05  # 50毫秒

    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(_sqlite_ro_uri(TMP_DB_PATH), uri=True, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            break
        except sqlite3.OperationalError:
            if attempt < max_retries - 1:
                time.sleep(base_delay)
            else:
                logger.error("多次重试后数据库仍然被锁定。")
                raise
    
    if not conn:
        raise sqlite3.OperationalError("无法建立数据库连接。")

    try:
        yield conn
    finally:
        if conn:
            conn.close()

# ===============================================================
# Flask 应用 (Flask Application)
# ===============================================================

app = Flask(__name__)

def get_item_hierarchy(conn, item_guid, cache=None):
    """获取项目的完整层级信息（带缓存）"""
    if cache is None:
        cache = {}
    
    if item_guid in cache:
        return cache[item_guid]
    
    # 一次性查询所有可能的父级项目
    hierarchy_query = '''
        WITH RECURSIVE item_hierarchy(guid, title, original_title, parent_guid, level) AS (
            -- 起始项目
            SELECT guid, title, original_title, parent_guid, 0 as level
            FROM item 
            WHERE guid = ?
            
            UNION ALL
            
            -- 递归查找父级
            SELECT i.guid, i.title, i.original_title, i.parent_guid, ih.level + 1
            FROM item i
            INNER JOIN item_hierarchy ih ON i.guid = ih.parent_guid
            WHERE ih.level < 10 AND i.guid IS NOT NULL  -- 增加递归深度并确保不为NULL
        )
        SELECT * FROM item_hierarchy ORDER BY level ASC
    '''
    
    items = conn.execute(hierarchy_query, (item_guid,)).fetchall()
    
    hierarchy = []
    for item in items:
        hierarchy.append({
            'guid': item['guid'],
            'title': item['title'],
            'original_title': item['original_title'],
            'parent_guid': item['parent_guid'],
            'level': item['level']
        })
    
    # 只缓存当前查询的项目层级信息
    cache[item_guid] = hierarchy
    
    return hierarchy

def format_timestamp(timestamp):
    """将时间戳转换为可读格式"""
    if timestamp:
        return datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
    return ''

def format_duration(seconds):
    """将秒数转换为时分秒格式"""
    if not seconds:
        return '00:00:00'
    
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"

def _parse_filter_time(value, label):
    if not value:
        return None
    try:
        return int(datetime.strptime(value, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
    except ValueError:
        logger.warning(f"无效的{label}格式: {value}")
        return None

def _activity_db_path():
    # 只打开容器目录里的副本。打开影视目录里的库会在那边创建 shm。
    if os.path.isfile(ACTIVITY_TMP_PATH):
        return ACTIVITY_TMP_PATH
    return None

def _refresh_online():
    try:
        online_history.refresh(_activity_db_path())
    except Exception as exc:
        logger.error(f"加载在线片库播放记录失败: {exc}")

def _online_extras(conn):
    """在线片库里、本地 item_user_play 还没有的记录。"""
    _refresh_online()
    taken = {
        (row['user_guid'], row['item_guid'])
        for row in conn.execute(
            'SELECT user_guid, item_guid FROM item_user_play WHERE visible = 1'
        )
    }
    names = {
        row['guid']: row['username']
        for row in conn.execute('SELECT guid, username FROM user')
    }
    extras = []
    for record in online_history.records():
        if (record['user_guid'], record['item_guid']) in taken:
            continue
        record = dict(record)
        record['username'] = names.get(record['user_guid']) or '未识别账号'
        extras.append(record)
    return extras

def _online_matches(record, user_guid, search_title, start_ms, end_ms):
    if user_guid and record.get('user_guid') != user_guid:
        return False
    stamp = record.get('sort_ts') or 0
    if start_ms is not None and stamp < start_ms:
        return False
    if end_ms is not None and (not stamp or stamp > end_ms):
        return False
    if search_title:
        meta = record.get('meta') or {}
        blob = ' '.join([
            online_history.compose_title(meta),
            meta.get('title') or '',
            meta.get('series') or '',
            meta.get('overview') or '',
        ]).lower()
        if search_title.lower() not in blob:
            return False
    return True

def format_local_record(conn, record, cache):
    """把本地媒体库的一行播放记录格式化成页面卡片。"""
    hierarchy = get_item_hierarchy(conn, record['item_guid'], cache)
    display_title = record['title']
    series_info = ""

    if len(hierarchy) > 1:
        root_item = hierarchy[-1]
        series_info = root_item['title']
        if record['season_number'] and record['episode_number']:
            display_title = f"{series_info} - S{record['season_number']:02d}E{record['episode_number']:02d} - {record['title']}"
        elif record['title'] != series_info:
            display_title = f"{series_info} - {record['title']}"
    elif record['season_number'] and record['episode_number']:
        display_title = f"S{record['season_number']:02d}E{record['episode_number']:02d} - {record['title']}"

    runtime_seconds = record['runtime'] * 60 if record['runtime'] else 0
    progress = 0
    if runtime_seconds and record['position'] and runtime_seconds > 0:
        progress = min(100, (record['position'] / runtime_seconds) * 100)

    is_episode = record['season_number'] is not None and record['episode_number'] is not None
    return {
        'item_guid': record['item_guid'],
        'user_guid': record['user_guid'],
        'username': record['username'],
        'title': display_title,
        'original_title': record['original_title'],
        'overview': record['overview'],
        'type': record['item_type'],
        'play_type': record['play_type'],
        'source': 'local',
        'season_number': record['season_number'],
        'episode_number': record['episode_number'],
        'series_title': series_info,
        'hierarchy': [{'title': h['title'], 'level': h['level']} for h in hierarchy],
        'position': record['position'],
        'position_formatted': format_duration(record['position']),
        'runtime': runtime_seconds,
        'runtime_formatted': format_duration(runtime_seconds),
        'progress': round(progress, 1),
        'watched': bool(record['watched']),
        'resolution': record['resolution'],
        'create_time': format_timestamp(record['create_time']),
        'update_time': format_timestamp(record['update_time']),
        'release_date': record['release_date'],
        'is_episode': is_episode
    }

def format_online_record(record):
    """在线片库记录使用同一张卡片，多一个「在线片库」标记。"""
    meta = record.get('meta') or {}
    runtime_seconds = record.get('duration') or 0
    position = record.get('position') or 0
    progress = 0
    if runtime_seconds and position and runtime_seconds > 0:
        progress = min(100, (position / runtime_seconds) * 100)
    watched = bool(record.get('watched'))
    if runtime_seconds and position >= runtime_seconds * 0.95:
        watched = True
    season = meta.get('season')
    episode = meta.get('episode')
    named = bool((meta.get('title') or '').strip() or (meta.get('series') or '').strip())
    return {
        'item_guid': record.get('item_guid'),
        'user_guid': record.get('user_guid'),
        'username': record.get('username') or '未识别账号',
        'title': online_history.compose_title(meta) if named else '在线片库',
        'original_title': '',
        'overview': meta.get('overview') or '',
        'type': meta.get('item_type') or '在线片库',
        'play_type': '在线片库',
        'source': 'online',
        'season_number': season,
        'episode_number': episode,
        'series_title': meta.get('series') or '',
        'hierarchy': [],
        'position': position,
        'position_formatted': format_duration(position),
        'runtime': runtime_seconds,
        'runtime_formatted': format_duration(runtime_seconds),
        'progress': round(progress, 1),
        'watched': watched,
        'resolution': record.get('resolution') or '',
        'create_time': format_timestamp(record.get('create_ts')),
        'update_time': format_timestamp(record.get('sort_ts')),
        'release_date': meta.get('release_date') or '',
        'is_episode': bool(season and episode)
    }

def _orphan_entry(record):
    """item_user_play 里有进度，但 item 表没有这条片子，多半是在线片库。"""
    meta = online_history.meta_for(record['item_guid']) or {}
    return {
        'user_guid': record['user_guid'],
        'item_guid': record['item_guid'],
        'username': record['username'],
        'sort_ts': record['update_time'] or 0,
        'create_ts': record['create_time'] or 0,
        'position': record['position'] or 0,
        'duration': meta.get('duration') or 0,
        'resolution': record['resolution'] or meta.get('resolution') or '',
        'watched': bool(record['watched']),
        'meta': meta,
    }

@app.route('/favicon.ico')
def favicon():
    """浏览器标签图标，使用应用图标。"""
    return send_from_directory(os.path.join(BASE_DIR, 'static'), 'icon.png', mimetype='image/png')

@app.route('/')
def index():
    """主页面"""
    return render_template('index.html')

@app.route('/api/users')
def get_users():
    """获取所有用户列表"""
    with get_db_connection() as conn:
        users = conn.execute('''
            SELECT guid, username, last_login_time, is_admin, status
            FROM user 
            WHERE status = 1 AND guid != 'default-user-template'
            ORDER BY username
        ''').fetchall()
        
        user_list = []
        for user in users:
            user_list.append({
                'guid': user['guid'],
                'username': user['username'],
                'last_login': format_timestamp(user['last_login_time']),
                'is_admin': user['is_admin'],
                'status': user['status']
            })
        
        return jsonify(user_list)

_HISTORY_COLUMNS = '''
    iup.item_guid,
    iup.user_guid,
    iup.ts as position,
    iup.watched,
    iup.create_time,
    iup.update_time,
    iup.type as play_type,
    iup.resolution,
    u.username,
    i.title,
    i.original_title,
    i.overview,
    i.type as item_type,
    i.season_number,
    i.episode_number,
    i.parent_guid,
    i.runtime,
    i.release_date
'''

@app.route('/api/play_history')
def get_play_history():
    """获取播放历史记录，本地媒体库和在线片库合在一起按时间排序。"""
    user_guid = request.args.get('user_guid', '')
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    search_title = request.args.get('search_title', '').strip()
    start_time = request.args.get('start_time', '')
    end_time = request.args.get('end_time', '')
    start_ms = _parse_filter_time(start_time, '开始时间')
    end_ms = _parse_filter_time(end_time, '结束时间')

    with get_db_connection() as conn:
        _refresh_online()
        where_clause = "WHERE iup.visible = 1"
        params = []

        if user_guid:
            where_clause += " AND iup.user_guid = ?"
            params.append(user_guid)

        if search_title:
            logger.info(f"搜索关键词: {search_title}")
            where_clause += """
                AND EXISTS (
                    WITH RECURSIVE item_hierarchy(guid, title, original_title, parent_guid, level) AS (
                        SELECT guid, title, original_title, parent_guid, 0 as level
                        FROM item
                        WHERE guid = i.guid

                        UNION ALL

                        SELECT parent.guid, parent.title, parent.original_title, parent.parent_guid, ih.level + 1
                        FROM item parent
                        INNER JOIN item_hierarchy ih ON parent.guid = ih.parent_guid
                        WHERE ih.level < 10 AND parent.guid IS NOT NULL
                    )
                    SELECT 1 FROM item_hierarchy
                    WHERE title LIKE ? OR original_title LIKE ?
                )
            """
            search_param = f"%{search_title}%"
            params.extend([search_param, search_param])

        if start_ms is not None:
            where_clause += " AND iup.update_time >= ?"
            params.append(start_ms)
        if end_ms is not None:
            where_clause += " AND iup.update_time <= ?"
            params.append(end_ms)

        local_rows = conn.execute(f'''
            SELECT {_HISTORY_COLUMNS}
            FROM item_user_play iup
            JOIN user u ON iup.user_guid = u.guid
            JOIN item i ON iup.item_guid = i.guid
            {where_clause}
        ''', params).fetchall()

        orphan_where = "WHERE iup.visible = 1 AND i.guid IS NULL"
        orphan_params = []
        if user_guid:
            orphan_where += " AND iup.user_guid = ?"
            orphan_params.append(user_guid)
        if start_ms is not None:
            orphan_where += " AND iup.update_time >= ?"
            orphan_params.append(start_ms)
        if end_ms is not None:
            orphan_where += " AND iup.update_time <= ?"
            orphan_params.append(end_ms)
        orphan_rows = conn.execute(f'''
            SELECT
                iup.item_guid,
                iup.user_guid,
                iup.ts as position,
                iup.watched,
                iup.create_time,
                iup.update_time,
                iup.resolution,
                u.username
            FROM item_user_play iup
            JOIN user u ON iup.user_guid = u.guid
            LEFT JOIN item i ON iup.item_guid = i.guid
            {orphan_where}
        ''', orphan_params).fetchall()

        candidates = [('local', row['update_time'] or 0, row) for row in local_rows]
        for row in orphan_rows:
            entry = _orphan_entry(row)
            if _online_matches(entry, user_guid, search_title, start_ms, end_ms):
                candidates.append(('online', entry['sort_ts'], entry))
        for entry in _online_extras(conn):
            if _online_matches(entry, user_guid, search_title, start_ms, end_ms):
                candidates.append(('online', entry.get('sort_ts') or 0, entry))

        candidates.sort(key=lambda item: item[1], reverse=True)
        total = len(candidates)
        offset = (page - 1) * per_page
        hierarchy_cache = {}
        history_list = []
        for kind, _stamp, payload in candidates[offset:offset + per_page]:
            if kind == 'local':
                history_list.append(format_local_record(conn, payload, hierarchy_cache))
            else:
                history_list.append(format_online_record(payload))

        return jsonify({
            'total': total,
            'page': page,
            'per_page': per_page,
            'pages': (total + per_page - 1) // per_page if per_page else 0,
            'data': history_list
        })

@app.route('/api/stats')
def get_stats():
    """获取统计数据"""
    with get_db_connection() as conn:
        # 总用户数
        total_users = conn.execute('''
            SELECT COUNT(*) as count 
            FROM user 
            WHERE status = 1 AND guid != 'default-user-template'
        ''').fetchone()['count']
        
        # 本地库和只写在播放表里的在线记录都算。文件/接口里多出来的在线记录再补上。
        total_plays = conn.execute('''
            SELECT COUNT(*) as count
            FROM item_user_play iup
            JOIN user u ON iup.user_guid = u.guid
            WHERE iup.visible = 1
        ''').fetchone()['count']

        extras = _online_extras(conn)
        total_plays += len(extras)

        active_ids = {
            row['user_guid']
            for row in conn.execute('''
                SELECT DISTINCT user_guid
                FROM item_user_play
                WHERE visible = 1
            ''')
        }
        active_ids.update(row['user_guid'] for row in extras if row.get('user_guid'))
        active_users = len(active_ids)

        latest_play = conn.execute('''
            SELECT MAX(update_time) as latest
            FROM item_user_play
            WHERE visible = 1
        ''').fetchone()['latest'] or 0
        if extras:
            latest_play = max(latest_play, max(row.get('sort_ts') or 0 for row in extras))

        today_start = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        today_plays = conn.execute('''
            SELECT COUNT(*) as count
            FROM item_user_play iup
            JOIN user u ON iup.user_guid = u.guid
            WHERE iup.visible = 1 AND iup.update_time >= ?
        ''', (today_start,)).fetchone()['count']
        today_plays += sum(1 for row in extras if (row.get('sort_ts') or 0) >= today_start)
        
        return jsonify({
            'total_users': total_users,
            'total_plays': total_plays,
            'active_users': active_users,
            'today_plays': today_plays,
            'latest_play': format_timestamp(latest_play)
        })

@app.route('/api/user_activity')
def get_user_activity():
    """获取用户活动数据"""
    with get_db_connection() as conn:
        # 用户播放次数统计
        user_stats = conn.execute('''
            SELECT
                u.username,
                u.guid,
                COUNT(iup.item_guid) as play_count,
                SUM(iup.ts) as total_seconds,
                MAX(iup.update_time) as last_play
            FROM user u
            LEFT JOIN item_user_play iup ON u.guid = iup.user_guid AND iup.visible = 1
            WHERE u.status = 1 AND u.guid != 'default-user-template'
            GROUP BY u.guid, u.username
        ''').fetchall()

        extra_by_user = {}
        for record in _online_extras(conn):
            bucket = extra_by_user.setdefault(record['user_guid'], {
                'count': 0,
                'seconds': 0,
                'last': 0,
                'username': record.get('username') or '未识别账号',
            })
            bucket['count'] += 1
            bucket['seconds'] += record.get('position') or 0
            bucket['last'] = max(bucket['last'], record.get('sort_ts') or 0)

        activity_data = []
        seen = set()
        for user in user_stats:
            seen.add(user['guid'])
            extra = extra_by_user.get(user['guid']) or {'count': 0, 'seconds': 0, 'last': 0}
            last_play = max(user['last_play'] or 0, extra['last'])
            activity_data.append({
                'username': user['username'],
                'play_count': (user['play_count'] or 0) + extra['count'],
                'total_hours': round(((user['total_seconds'] or 0) + extra['seconds']) / 3600, 1),
                'last_play': format_timestamp(last_play)
            })
        for guid, extra in extra_by_user.items():
            if guid in seen:
                continue
            activity_data.append({
                'username': extra['username'],
                'play_count': extra['count'],
                'total_hours': round(extra['seconds'] / 3600, 1),
                'last_play': format_timestamp(extra['last'])
            })
        activity_data.sort(key=lambda row: row['play_count'], reverse=True)
        return jsonify(activity_data[:10])

if __name__ == '__main__':
    logger.info("=" * 50)
    logger.info("启动飞牛影视观看历史管理系统")
    logger.info("=" * 50)
    logger.info("访问地址: http://127.0.0.1:5000")
    logger.info("Flask 运行模式: 串行处理 (单线程)")
    
    logger.info("所有组件已启动，服务运行中...")
    logger.info("=" * 50)
    
    app.run(host='0.0.0.0', port=5000)