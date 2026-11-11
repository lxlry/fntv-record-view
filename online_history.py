"""在线片库播放记录。

本地媒体库的进度在 trimmedia.db 的 item_user_play，并且能 JOIN 到 item。
在线片库不进这张 item 表，进度在另外两处：

- 影视服务自己的「继续观看」：GET /v/api/v1/play/list。
  用拷到容器里的 trimactivity.db 副本中的登录 token 去拉。官方在线片库的进度只在这个接口里。
  优先连已经存在的 /var/run/trim_media.sock（容器里挂的是整个 /var/run，
  不会在 socket 路径上建目录）。连不上再按 TRIM_MEDIA_URL 和常见端口走 HTTP。
- 第三方在线源（如 fnmedia-ext）写在自己的 state/history.json，片名在同目录 titles.json。

两边都只读。读不到时返回空列表，不影响本地记录。
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import socket
import stat
import threading
import time
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

# 与飞牛影视网页端相同的本机接口签名，只用来读自己 NAS 上的播放列表。
_SECRET = "NDzZTVxnRKP8Z0jXg1VAMonaG8akvh"
_API_KEY = "16CCEB3D-AB42-077D-36A1-F355324E4237"
_PLAY_LIST_PATH = "/v/api/v1/play/list"

_DEFAULT_STATE_DIRS = (
    "/app/online_state",
    "/usr/local/apps/@appdata/fnmedia-ext/state",
    "/vol1/@appdata/fnmedia-ext/state",
    "/vol2/@appdata/fnmedia-ext/state",
)

_lock = threading.Lock()
_cache = {"at": 0.0, "records": [], "metas": {}}


def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _coerce_ms(value) -> int:
    """秒或毫秒时间戳统一成毫秒。无法识别时返回 0。"""
    try:
        number = int(float(value or 0))
    except (TypeError, ValueError):
        return 0
    if number <= 0:
        return 0
    if number < 10 ** 11:
        return number * 1000
    return number


def _as_int(value) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def compose_title(meta: dict | None) -> str:
    """拼成和本地记录一样的「剧名 - S01E01 - 集名」。"""
    if not meta:
        return "在线片库"
    title = (meta.get("title") or "").strip()
    series = (meta.get("series") or "").strip()
    season = _as_int(meta.get("season"))
    episode = _as_int(meta.get("episode"))
    if series and season and episode:
        base = f"{series} - S{season:02d}E{episode:02d}"
        if title and title != series:
            return f"{base} - {title}"
        return base
    if series and title and series != title:
        return f"{series} - {title}"
    return title or series or "在线片库"


def state_dirs() -> list[str]:
    found = []
    seen = set()
    raw = os.environ.get("ONLINE_STATE_DIR", "")
    parts = []
    for chunk in raw.replace(",", os.pathsep).split(os.pathsep):
        chunk = chunk.strip()
        if chunk:
            parts.append(chunk)
    parts.extend(_DEFAULT_STATE_DIRS)
    # 第三方在线源的进度在自己的安装目录，不在影视数据库里。
    for root in ("/host/apps", "/usr/local/apps"):
        for rel in (
            "fnmedia-ext/app/repo/state",
            "fnmedia-ext/state",
            "@appdata/fnmedia-ext/state",
            "@appdata/fnmedia-ext/app/repo/state",
        ):
            parts.append(os.path.join(root, rel))
    for path in parts:
        norm = os.path.abspath(path)
        if norm in seen:
            continue
        seen.add(norm)
        history = os.path.join(path, "history.json")
        if os.path.isfile(history):
            found.append(path)
    return found


def _episode_list(brief: dict) -> list:
    lines = brief.get("lines") or {}
    if not isinstance(lines, dict):
        return []
    best = []
    for episodes in lines.values():
        if isinstance(episodes, list) and len(episodes) > len(best):
            best = episodes
    return [ep for ep in best if isinstance(ep, dict)]


def _is_series(brief: dict, episode_count: int) -> bool:
    label = f"{brief.get('type_name') or ''}{brief.get('type') or ''}"
    # 单集电影、剧场版、综艺特辑按一部片子展示，不编造成 S01E01。
    if episode_count <= 1 and any(word in label for word in ("电影", "影片", "Movie", "综艺", "动漫", "动画", "番")):
        return False
    if any(word in label for word in ("剧", "综艺", "动漫", "动画", "TV")):
        return episode_count > 0
    return episode_count > 1


def _blank_meta() -> dict:
    return {
        "title": "",
        "series": "",
        "overview": "",
        "item_type": "Movie",
        "season": None,
        "episode": None,
        "release_date": "",
        "duration": 0,
        "resolution": "",
    }


def _meta_from_brief(brief: dict, *, title: str, series: str, item_type: str,
                     season, episode, duration: int = 0) -> dict:
    meta = _blank_meta()
    meta.update({
        "title": title,
        "series": series,
        "overview": (brief.get("overview") or "")[:500],
        "item_type": item_type,
        "season": season,
        "episode": episode,
        "release_date": str(brief.get("year") or brief.get("release_date") or ""),
        "duration": duration,
    })
    return meta


def _index_titles(titles: dict, metas: dict) -> None:
    if not isinstance(titles, dict):
        return
    for guid, brief in titles.items():
        if not isinstance(brief, dict) or not guid:
            continue
        name = (brief.get("title") or "").strip()
        episodes = _episode_list(brief)
        if not _is_series(brief, len(episodes)):
            movie = _meta_from_brief(
                brief, title=name, series="", item_type="Movie",
                season=None, episode=None,
            )
            metas[guid] = movie
            if len(episodes) == 1:
                metas[_md5(f"ep:{guid}:0")] = movie
            continue
        metas[guid] = _meta_from_brief(
            brief, title=name, series=name, item_type="TV",
            season=None, episode=None,
        )
        for index, episode in enumerate(episodes):
            episode_title = (
                episode.get("ep_title") or episode.get("title") or f"第{index + 1}集"
            )
            season = _as_int(episode.get("season") or 1) or 1
            number = _as_int(episode.get("episode_number") or index + 1) or (index + 1)
            meta = _meta_from_brief(
                brief, title=episode_title, series=name, item_type="Episode",
                season=season, episode=number,
            )
            episode_guid = _md5(f"ep:{guid}:{index}")
            metas[episode_guid] = meta
            for key_name in ("episode_key", "key"):
                key = episode.get(key_name)
                if key:
                    metas[_md5(f"t4ep:{guid}:{key}")] = meta


def _resolution_of(payload: dict) -> str:
    if payload.get("resolution"):
        return str(payload["resolution"])
    stream = payload.get("media_stream") or {}
    if not isinstance(stream, dict):
        return ""
    resolutions = stream.get("resolutions")
    if isinstance(resolutions, list) and resolutions:
        return str(resolutions[0])
    if isinstance(resolutions, str):
        return resolutions
    return ""


def _library_label(value: str) -> bool:
    """媒体库名字（「在线资源」一类）不是剧名。"""
    text = (value or "").strip()
    if not text or text in {"在线资源", "在线片库"}:
        return True
    return text.startswith("🌐")


def _meta_from_card(card: dict) -> dict:
    title = (card.get("title") or "").strip()
    series = (card.get("tv_title") or "").strip()
    if not series:
        ancestor = (card.get("ancestor_name") or "").strip()
        if ancestor and not _library_label(ancestor):
            series = ancestor
    season = _as_int(card.get("season_number"))
    episode = _as_int(card.get("episode_number"))
    raw_type = card.get("type") or ""
    if season and episode:
        item_type = "Episode"
    elif raw_type in {"TV", "Season"}:
        item_type = "TV"
        series = series or title
    elif raw_type == "Episode":
        item_type = "Episode"
    else:
        item_type = raw_type or "Movie"
        if series == title:
            series = ""
    duration = _as_int(card.get("duration"))
    if not duration:
        duration = _as_int(card.get("runtime")) * 60
    meta = _blank_meta()
    meta.update({
        "title": title,
        "series": series,
        "overview": (card.get("overview") or "")[:500],
        "item_type": item_type,
        "season": season or None,
        "episode": episode or None,
        "release_date": str(card.get("release_date") or ""),
        "duration": duration,
        "resolution": _resolution_of(card),
    })
    return meta


def _guess_card_time(card: dict) -> int:
    for key in ("update_time", "watched_ts", "create_time", "played_at", "last_play_time"):
        stamp = _coerce_ms(card.get(key))
        if stamp >= 1577836800000:  # 2020-01-01，避开把播放秒数误当成时间
            return stamp
    return 0


def _record(user_guid: str, item_guid: str, position: int, duration: int,
            sort_ts: int, resolution: str, watched: bool, meta: dict) -> dict:
    finished = bool(duration) and position >= duration * 0.95
    return {
        "user_guid": user_guid or "",
        "item_guid": item_guid,
        "sort_ts": sort_ts,
        "create_ts": 0,
        "position": position,
        "duration": duration or _as_int(meta.get("duration")),
        "resolution": resolution or meta.get("resolution") or "",
        "watched": bool(watched) or finished,
        "meta": meta,
    }


def _prefer(current: dict, incoming: dict) -> dict:
    """同一用户同一条目保留进度更完整、时间更新的那条，片名互相补齐。"""
    def score(row: dict):
        meta = row.get("meta") or {}
        named = bool((meta.get("title") or "").strip() or (meta.get("series") or "").strip())
        return (
            1 if row.get("position") else 0,
            1 if row.get("duration") else 0,
            1 if named else 0,
            row.get("sort_ts") or 0,
        )

    winner, loser = (incoming, current) if score(incoming) >= score(current) else (current, incoming)
    merged_meta = dict(loser.get("meta") or {})
    for key, value in (winner.get("meta") or {}).items():
        if value not in (None, "", 0):
            merged_meta[key] = value
    chosen = dict(winner)
    chosen["meta"] = merged_meta
    if not chosen.get("duration"):
        chosen["duration"] = loser.get("duration") or 0
    if not chosen.get("resolution"):
        chosen["resolution"] = loser.get("resolution") or ""
    if (chosen.get("sort_ts") or 0) < (loser.get("sort_ts") or 0):
        chosen["sort_ts"] = loser["sort_ts"]
    return chosen


def _load_history_file(path: str, metas: dict, bucket: dict) -> int:
    try:
        with open(path, encoding="utf-8") as handle:
            payload = json.loads(handle.read())
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("读取在线播放记录失败 %s: %s", path, exc)
        return 0
    if not isinstance(payload, dict):
        return 0
    users = payload.get("users")
    if isinstance(users, dict):
        grouped = users
    else:
        legacy = payload.get("records")
        grouped = {"": legacy} if isinstance(legacy, dict) else {}
    count = 0
    for user_guid, records in grouped.items():
        if not isinstance(records, dict) or str(user_guid).startswith("__"):
            continue
        for item_guid, raw in records.items():
            if not isinstance(raw, dict):
                continue
            guid = raw.get("item_guid") or item_guid
            if not guid:
                continue
            meta = dict(metas.get(guid) or _blank_meta())
            position = _as_int(raw.get("ts"))
            duration = _as_int(raw.get("duration")) or meta.get("duration") or 0
            if duration and not meta.get("duration"):
                meta["duration"] = duration
            resolution = raw.get("resolution") or meta.get("resolution") or ""
            meta["resolution"] = resolution
            metas[guid] = meta
            row = _record(
                str(user_guid or ""), guid, position, duration,
                _coerce_ms(raw.get("updated_at")), str(resolution),
                False, meta,
            )
            key = (row["user_guid"], guid)
            bucket[key] = _prefer(bucket[key], row) if key in bucket else row
            count += 1
    return count


def _cards_of(payload) -> list:
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, list):
        return [card for card in data if isinstance(card, dict)]
    if isinstance(data, dict):
        for key in ("list", "items", "rows"):
            rows = data.get(key)
            if isinstance(rows, list):
                return [card for card in rows if isinstance(card, dict)]
    return []


def _make_authx(path: str) -> str:
    nonce = str(random.randint(100000, 999999))
    timestamp = str(int(time.time() * 1000))
    digest = _md5("")
    raw = "_".join([_SECRET, path, nonce, timestamp, digest, _API_KEY])
    return f"nonce={nonce}&timestamp={timestamp}&sign={_md5(raw)}"


def _decode_http(raw: bytes) -> dict | None:
    head, _, body = raw.partition(b"\r\n\r\n")
    if b"chunked" in head.lower():
        decoded = b""
        rest = body
        while True:
            newline = rest.find(b"\r\n")
            if newline < 0:
                break
            try:
                size = int(rest[:newline].split(b";")[0], 16)
            except ValueError:
                break
            if size == 0:
                break
            decoded += rest[newline + 2:newline + 2 + size]
            rest = rest[newline + 2 + size + 2:]
        body = decoded
    try:
        parsed = json.loads(body.decode("utf-8", "replace"))
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _http_request(url: str, token: str) -> dict | None:
    request = urllib.request.Request(
        url,
        headers={
            "authx": _make_authx(_PLAY_LIST_PATH),
            "Authorization": token,
            "Content-Type": "application/json",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            parsed = json.loads(response.read().decode("utf-8", "replace"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
        logger.warning("请求在线片库播放列表失败: %s", exc)
        return None
    return parsed if isinstance(parsed, dict) else None


def _socket_request(sock_path: str, token: str) -> dict | None:
    authx = _make_authx(_PLAY_LIST_PATH)
    header = (
        f"GET {_PLAY_LIST_PATH} HTTP/1.1\r\n"
        "Host: localhost\r\n"
        "Connection: close\r\n"
        f"authx: {authx}\r\n"
        f"Authorization: {token}\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: 0\r\n\r\n"
    )
    client = socket.socket(socket.AF_UNIX)
    client.settimeout(3)
    try:
        client.connect(sock_path)
        client.sendall(header.encode("utf-8"))
        chunks = b""
        while True:
            piece = client.recv(65536)
            if not piece:
                break
            chunks += piece
    except (OSError, socket.timeout) as exc:
        logger.warning("连接影视服务失败 %s: %s", sock_path, exc)
        return None
    finally:
        client.close()
    return _decode_http(chunks)


def _is_socket(path: str) -> bool:
    try:
        return bool(path) and stat.S_ISSOCK(os.stat(path).st_mode)
    except OSError:
        return False


def _socket_candidates() -> list[str]:
    """只连接已经存在的 socket，调用方不会创建这个路径。"""
    found = []
    explicit = (os.environ.get("TRIM_MEDIA_SOCK") or "").strip()
    if _is_socket(explicit):
        found.append(os.path.abspath(explicit))
    for root in ("/host/run", "/var/run", "/run"):
        for name in ("trim_media.sock", "mediasrv.socket"):
            path = os.path.join(root, name)
            if _is_socket(path) and path not in found:
                found.append(path)
    return found


def _http_bases() -> list[str]:
    bases = []
    explicit = (os.environ.get("TRIM_MEDIA_URL") or "").strip().rstrip("/")
    if explicit:
        bases.append(explicit)
    for extra in (
        "http://127.0.0.1:5666",
        "http://host.docker.internal:5666",
        "http://172.17.0.1:5666",
    ):
        if extra not in bases:
            bases.append(extra)
    return bases


def _accepted(payload, source: str, errors: list[str]) -> list | None:
    """返回卡片列表。来源不可用时返回 None，并记下原因。"""
    if not isinstance(payload, dict):
        errors.append(f"{source} 无响应")
        return None
    code = payload.get("code")
    if code not in (0, None, "0"):
        errors.append(f"{source} code={code} {payload.get('msg') or ''}".strip())
        return None
    return _cards_of(payload)


def _play_list(token: str) -> list:
    errors = []
    for sock in _socket_candidates():
        cards = _accepted(_socket_request(sock, token), f"socket {sock}", errors)
        if cards is not None:
            logger.info("已从 %s 读到继续观看 %s 条", sock, len(cards))
            return cards
    for base in _http_bases():
        cards = _accepted(_http_request(base + _PLAY_LIST_PATH, token), base, errors)
        if cards is not None:
            logger.info("已从 %s 读到继续观看 %s 条", base, len(cards))
            return cards
    if errors:
        logger.warning("拉取在线片库继续观看失败: %s", "; ".join(errors[:4]))
    return []


def _tokens(activity_db: str | None) -> list[tuple[str, str]]:
    if not activity_db or not os.path.isfile(activity_db):
        return []
    import sqlite3
    uri = "file:" + os.path.abspath(activity_db).replace("\\", "/") + "?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=2)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT user_guid, token FROM user_token WHERE status = 1 "
            "ORDER BY update_time DESC"
        ).fetchall()
        conn.close()
    except sqlite3.Error as exc:
        logger.warning("读取登录会话失败: %s", exc)
        return []
    chosen = []
    seen = set()
    for row in rows:
        user_guid = row["user_guid"]
        token = row["token"]
        if not user_guid or not token or user_guid in seen:
            continue
        seen.add(user_guid)
        chosen.append((user_guid, token))
    return chosen


def _load_play_lists(activity_db: str | None, metas: dict, bucket: dict) -> int:
    tokens = _tokens(activity_db)
    if not tokens:
        logger.warning(
            "没有可用的影视登录会话，在线片库继续观看无法读取 (%s)",
            activity_db or "缺少 trimactivity.db",
        )
        return 0
    count = 0
    for user_guid, token in tokens:
        for card in _play_list(token):
            guid = str(card.get("guid") or card.get("item_guid") or "").strip()
            if not guid:
                continue
            position = _as_int(card.get("ts")) or _as_int(card.get("position"))
            meta = _meta_from_card(card)
            previous = metas.get(guid)
            if previous and compose_title(previous) != "在线片库":
                meta = _prefer(
                    {"meta": previous, "position": 0, "duration": previous.get("duration") or 0,
                     "resolution": previous.get("resolution") or "", "sort_ts": 0},
                    {"meta": meta, "position": position,
                     "duration": meta.get("duration") or 0,
                     "resolution": meta.get("resolution") or "", "sort_ts": 1},
                )["meta"]
            metas[guid] = meta
            row = _record(
                user_guid, guid, position,
                meta.get("duration") or 0, _guess_card_time(card),
                meta.get("resolution") or "", bool(card.get("watched")), meta,
            )
            key = (user_guid, guid)
            bucket[key] = _prefer(bucket[key], row) if key in bucket else row
            count += 1
    return count


def _load(activity_db: str | None) -> tuple[list, dict]:
    metas: dict = {}
    bucket: dict = {}
    file_count = 0
    for directory in state_dirs():
        title_path = os.path.join(directory, "titles.json")
        if os.path.isfile(title_path):
            try:
                with open(title_path, encoding="utf-8") as handle:
                    parsed = json.loads(handle.read())
            except (OSError, json.JSONDecodeError) as exc:
                logger.warning("读取在线片名缓存失败 %s: %s", title_path, exc)
                parsed = {}
            _index_titles(parsed.get("titles") if isinstance(parsed, dict) else {}, metas)
        file_count += _load_history_file(os.path.join(directory, "history.json"), metas, bucket)
    api_count = _load_play_lists(activity_db, metas, bucket)
    if file_count or api_count:
        logger.info("在线片库记录: 文件 %s 条, 接口 %s 条", file_count, api_count)
    return list(bucket.values()), metas


def refresh(activity_db: str | None) -> None:
    ttl = float(os.environ.get("ONLINE_CACHE_SECONDS", "30") or 30)
    now = time.time()
    with _lock:
        if _cache["at"] and now - _cache["at"] < ttl:
            return
        try:
            records, metas = _load(activity_db)
        except Exception as exc:
            logger.error("加载在线片库播放记录失败: %s", exc)
            records, metas = [], {}
        _cache["records"] = records
        _cache["metas"] = metas
        _cache["at"] = now


def records() -> list[dict]:
    with _lock:
        return [dict(row, meta=dict(row.get("meta") or {})) for row in _cache["records"]]


def meta_for(guid: str) -> dict | None:
    with _lock:
        meta = _cache["metas"].get(guid)
        return dict(meta) if meta else None
