
import asyncio
import json
import aiohttp
import aiosqlite
import html
import re
import time
import random
import os
from urllib.parse import urlsplit, urlunsplit
from collections import defaultdict

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest, TelegramForbiddenError

from config import API_TOKEN as _API_TOKEN, LZT_API_KEY as _LZT_API_KEY

# ====================== ENV ======================
API_TOKEN = os.getenv("API_TOKEN") or _API_TOKEN
LZT_API_KEY = os.getenv("LZT_API_KEY") or _LZT_API_KEY

bot: Bot | None = None
dp = Dispatcher()

# ====================== OWNER / ACCESS ======================
OWNER_ID = 1377985336
OWNER_IDS = {OWNER_ID}

# ====================== НАСТРОЙКИ ======================
HUNTER_INTERVAL_BASE = 0.20
FETCH_TIMEOUT = 5.40
BUY_TIMEOUT = 6
RETRY_MAX = 2
RETRY_BASE_DELAY = 0.30

SHORT_CARD_MAX = 950
ERROR_REPORT_INTERVAL = 3600

MAX_URLS_PER_USER_DEFAULT = 50
MAX_URLS_PER_USER_LIMITED = 3

MAX_CONCURRENT_REQUESTS = 10
LIMITED_EXTRA_DELAY = 3.0

DB_FILE = "bot_data.sqlite"

LZT_SECRET_WORD = (os.getenv("LZT_SECRET_WORD") or "Мазда").strip()

URL_PAGE_SIZE = 12
USER_PAGE_SIZE = 14

TG_SEND_DELAY = 0.07
AUTOBUY_RETRY_ATTEMPTS = 4
AUTOBUY_RETRY_MIN_DELAY = 1.0
AUTOBUY_RETRY_MAX_DELAY = 2.0

SEEN_RETENTION_SECONDS = 48 * 3600
CLEANUP_INTERVAL = 3600
BACKUP_INTERVAL = 60
DATA_BACKUP_FILE = "bot_state_backup.json"

# ====================== LOGGING ======================
AUTOBUY_LOG_FILE = os.getenv("AUTOBUY_LOG_FILE") or "autobuy.log"
LOG_MAX_BYTES = 15 * 1024 * 1024
LOG_ROTATE_KEEP = 2


def _ts_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def _safe_compact(s: str, n: int = 400) -> str:
    s = (s or "").replace("\n", "\\n").replace("\r", "\\r")
    if len(s) <= n:
        return s
    return s[: n - 20] + f"...(len={len(s)})"


def _rotate_log_if_needed():
    try:
        if not os.path.exists(AUTOBUY_LOG_FILE):
            return
        if os.path.getsize(AUTOBUY_LOG_FILE) < LOG_MAX_BYTES:
            return

        for i in range(LOG_ROTATE_KEEP, 0, -1):
            src = f"{AUTOBUY_LOG_FILE}.{i}"
            dst = f"{AUTOBUY_LOG_FILE}.{i+1}"
            if os.path.exists(src):
                if i == LOG_ROTATE_KEEP:
                    try:
                        os.remove(src)
                    except Exception:
                        pass
                else:
                    try:
                        os.replace(src, dst)
                    except Exception:
                        pass

        try:
            os.replace(AUTOBUY_LOG_FILE, f"{AUTOBUY_LOG_FILE}.1")
        except Exception:
            pass
    except Exception:
        pass


def log_autobuy(line: str):
    try:
        _rotate_log_if_needed()
        with open(AUTOBUY_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{_ts_str()}] {line}\n")
    except Exception:
        pass


# ====================== START MESSAGES ======================
START_MSG_1 = (
    "🤖 Parsing Bot 🤖\n"
    "😶‍🌫️Отслеживание новых лотов по вашим URL в один клик😶‍🌫️\n\n"
    "🔗 Полезные ссылки, обязательно подписаться 🔗\n"
    "• Канал поддержки: https://t.me/+wHlSL7Ij2rpjYmFi\n"
    "• Создатель: https://t.me/StaliNusshhAaaaaa😶‍🌫️"
)

START_MSG_2 = (
    "🧭 Меню\n\n"
    "• ✨ Проверка лотов — показать до 10 лотов\n"
    "• 📚 Мои URL — управление источниками (+ 🛒 автобай)\n"
    "• 🚀 Старт охотника — мониторинг\n"
    "• ♻️ Сбросить историю — чтобы снова считать лоты новыми"
)

DENIED_TEXT = (
    "⛔️ Доступ к боту закрыт по умолчанию.\n\n"
    "Нажми кнопку ниже, чтобы отправить запрос владельцу."
)


# ====================== UI: KEYBOARDS ======================
def kb_request() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🔓 Запрос на бота")]],
        resize_keyboard=True,
    )


def kb_main(user_id: int) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="🚀 Старт охотника"), KeyboardButton(text="🛑 Стоп охотника")],
        [KeyboardButton(text="✨ Проверка лотов"), KeyboardButton(text="📊 Статус")],
        [KeyboardButton(text="📚 Мои URL"), KeyboardButton(text="♻️ Сбросить историю")],
        [KeyboardButton(text="🧪 Диагностика"), KeyboardButton(text="ℹ️ Инфо")],
    ]
    if user_id in OWNER_IDS:
        rows.insert(3, [KeyboardButton(text="👥 Пользователи")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def kb_urls_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📄 Список URL"), KeyboardButton(text="➕ Добавить URL")],
            [KeyboardButton(text="✏️ Переименовать URL"), KeyboardButton(text="🗑 Удалить URL")],
            [KeyboardButton(text="🔁 Вкл/Выкл URL"), KeyboardButton(text="🛒 Автобай URL")],
            [KeyboardButton(text="✅ Тест URL"), KeyboardButton(text="⬅️ Назад")],
        ],
        resize_keyboard=True,
    )


# ====================== HELPERS ======================
def has_valid_telegram_token(token: str) -> bool:
    if not token:
        return False
    return bool(re.match(r"^\d{6,12}:[A-Za-z0-9_-]{20,}$", token))


async def safe_delete(message: types.Message):
    try:
        await message.delete()
    except Exception:
        pass


send_locks: dict[int, asyncio.Lock] = {}


def get_send_lock(chat_id: int) -> asyncio.Lock:
    lock = send_locks.get(chat_id)
    if lock is None:
        lock = asyncio.Lock()
        send_locks[chat_id] = lock
    return lock


async def send_bot_message(chat_id: int, text: str, **kwargs):
    if bot is None:
        raise RuntimeError("Bot не инициализирован")

    lock = get_send_lock(chat_id)
    async with lock:
        for attempt in range(3):
            try:
                msg = await bot.send_message(chat_id, text, **kwargs)
                if TG_SEND_DELAY > 0:
                    await asyncio.sleep(TG_SEND_DELAY)
                return msg
            except TelegramRetryAfter as e:
                await asyncio.sleep(float(getattr(e, "retry_after", 1.5)) + 0.2)
            except (TelegramBadRequest, TelegramForbiddenError):
                raise
            except Exception:
                if attempt >= 2:
                    raise
                await asyncio.sleep(0.6 + attempt * 0.5)


def make_item_key(item: dict) -> str:
    iid = item.get("item_id") or item.get("id")
    if iid is not None:
        return f"id::{str(iid).strip()}"
    title = str(item.get("title") or "").strip()
    price = str(item.get("price") or "")
    return f"noid::{title}::{price}"


def parse_index_from_button(text: str) -> int | None:
    m = re.match(r"^\s*(\d+)\)", text or "")
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


# ====================== STATE ======================
user_search_active = defaultdict(lambda: False)
user_seen_items = defaultdict(set)
user_buy_attempted = defaultdict(set)
user_hunter_tasks: dict[int, asyncio.Task] = {}
user_hunter_start_locks: dict[int, asyncio.Lock] = {}

user_modes = defaultdict(lambda: None)
user_started = set()
user_urls = defaultdict(list)
user_api_errors = defaultdict(int)

user_last_screen_msg_id = defaultdict(lambda: None)
user_pending_url = defaultdict(lambda: None)
user_pending_rename_url = defaultdict(lambda: None)
user_page_state = defaultdict(lambda: {"ctx": None, "page": 0})
user_runtime_stats = defaultdict(lambda: {
    "autobuy_ok": 0,
    "autobuy_fail": 0,
    "last_autobuy_error": "",
    "last_autobuy_ok_ts": 0,
    "last_autobuy_item": "",
})

autobuy_endpoint_cache: dict[str, list[str]] = {}
buy_locks: dict[str, asyncio.Lock] = {}
buy_semaphore = asyncio.Semaphore(3)


def get_buy_lock(item_key: str) -> asyncio.Lock:
    lock = buy_locks.get(item_key)
    if lock is None:
        lock = asyncio.Lock()
        buy_locks[item_key] = lock
    return lock


def get_user_hunter_start_lock(user_id: int) -> asyncio.Lock:
    lock = user_hunter_start_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        user_hunter_start_locks[user_id] = lock
    return lock


async def delete_last_screen(chat_id: int, user_id: int):
    mid = user_last_screen_msg_id.get(user_id)
    if not mid or bot is None:
        return
    try:
        await bot.delete_message(chat_id, mid)
    except Exception:
        pass
    user_last_screen_msg_id[user_id] = None


async def send_screen(chat_id: int, user_id: int, text: str, reply_markup: ReplyKeyboardMarkup | None = None, parse_mode: str | None = None):
    await delete_last_screen(chat_id, user_id)
    msg = await send_bot_message(
        chat_id,
        text,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
        disable_web_page_preview=True,
    )
    user_last_screen_msg_id[user_id] = msg.message_id
    return msg


def build_urls_picker_kb(sources: list[dict], page: int, back_text: str = "⬅️ Назад") -> ReplyKeyboardMarkup:
    total = len(sources)
    if total <= 0:
        return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=back_text)]], resize_keyboard=True)

    total_pages = (total + URL_PAGE_SIZE - 1) // URL_PAGE_SIZE
    page = max(0, min(page, total_pages - 1))

    start = page * URL_PAGE_SIZE
    end = min(total, start + URL_PAGE_SIZE)
    chunk = sources[start:end]

    rows: list[list[KeyboardButton]] = []
    row: list[KeyboardButton] = []
    for src in chunk:
        idx = src["idx"]
        name = src.get("name") or f"URL #{idx}"
        row.append(KeyboardButton(text=f"{idx}) {name}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if total_pages > 1:
        rows.append([
            KeyboardButton(text="◀️ Назад страница"),
            KeyboardButton(text=f"📄 {page+1}/{total_pages}"),
            KeyboardButton(text="▶️ Далее"),
        ])

    rows.append([KeyboardButton(text=back_text)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def build_users_picker_kb(users: list[tuple[int, int, str]], page: int) -> ReplyKeyboardMarkup:
    total = len(users)
    if total <= 0:
        return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ Назад")]], resize_keyboard=True)

    total_pages = (total + USER_PAGE_SIZE - 1) // USER_PAGE_SIZE
    page = max(0, min(page, total_pages - 1))

    start = page * USER_PAGE_SIZE
    end = min(total, start + USER_PAGE_SIZE)
    chunk = users[start:end]

    rows: list[list[KeyboardButton]] = []
    for uid, allowed, _role in chunk:
        icon = "✅" if allowed else "⛔️"
        rows.append([KeyboardButton(text=f"{icon} {uid}")])

    if total_pages > 1:
        rows.append([
            KeyboardButton(text="◀️ Назад страница"),
            KeyboardButton(text=f"📄 {page+1}/{total_pages}"),
            KeyboardButton(text="▶️ Далее"),
        ])

    rows.append([KeyboardButton(text="⬅️ Назад")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def parse_user_id_from_button(text: str) -> int | None:
    m = re.search(r"(\d{5,})", text or "")
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


# ====================== DB ======================
_db: aiosqlite.Connection | None = None
_db_lock = asyncio.Lock()


async def db_conn() -> aiosqlite.Connection:
    global _db
    if _db is None:
        _db = await aiosqlite.connect(DB_FILE)
        await _db.execute("PRAGMA journal_mode=WAL")
        await _db.execute("PRAGMA synchronous=NORMAL")
        await _db.execute("PRAGMA foreign_keys=ON")
    return _db


async def db_close():
    global _db
    if _db is not None:
        await _db.close()
        _db = None


async def db_execute(query: str, params: tuple = (), commit: bool = False):
    db = await db_conn()
    async with _db_lock:
        cur = await db.execute(query, params)
        if commit:
            await db.commit()
        return cur


async def db_executemany(query: str, params_seq, commit: bool = False):
    db = await db_conn()
    async with _db_lock:
        cur = await db.executemany(query, params_seq)
        if commit:
            await db.commit()
        return cur


async def db_fetchone(query: str, params: tuple = ()):
    db = await db_conn()
    async with _db_lock:
        cur = await db.execute(query, params)
        row = await cur.fetchone()
        await cur.close()
        return row


async def db_fetchall(query: str, params: tuple = ()):
    db = await db_conn()
    async with _db_lock:
        cur = await db.execute(query, params)
        rows = await cur.fetchall()
        await cur.close()
        return rows


async def init_db():
    await db_execute("""
        CREATE TABLE IF NOT EXISTS urls (
            user_id INTEGER,
            url TEXT,
            name TEXT DEFAULT '',
            added_at INTEGER,
            enabled INTEGER DEFAULT 1,
            autobuy INTEGER DEFAULT 0,
            PRIMARY KEY(user_id, url)
        )
    """, commit=True)

    cols = [row[1] for row in await db_fetchall("PRAGMA table_info(urls)")]
    if "enabled" not in cols:
        await db_execute("ALTER TABLE urls ADD COLUMN enabled INTEGER DEFAULT 1", commit=True)
    if "autobuy" not in cols:
        await db_execute("ALTER TABLE urls ADD COLUMN autobuy INTEGER DEFAULT 0", commit=True)
    if "name" not in cols:
        await db_execute("ALTER TABLE urls ADD COLUMN name TEXT DEFAULT ''", commit=True)

    await db_execute("""
        CREATE TABLE IF NOT EXISTS seen (
            user_id INTEGER,
            item_key TEXT,
            seen_at INTEGER,
            PRIMARY KEY(user_id, item_key)
        )
    """, commit=True)

    await db_execute("""
        CREATE TABLE IF NOT EXISTS buy_attempted (
            user_id INTEGER,
            item_key TEXT,
            attempted_at INTEGER,
            PRIMARY KEY(user_id, item_key)
        )
    """, commit=True)

    await db_execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            role TEXT DEFAULT 'unknown',
            allowed INTEGER DEFAULT 0,
            last_error_report INTEGER DEFAULT 0,
            last_request_ts INTEGER DEFAULT 0
        )
    """, commit=True)

    await db_execute("""
        CREATE TABLE IF NOT EXISTS user_stats (
            user_id INTEGER PRIMARY KEY,
            autobuy_ok INTEGER DEFAULT 0,
            autobuy_fail INTEGER DEFAULT 0,
            last_autobuy_error TEXT DEFAULT '',
            last_autobuy_ok_ts INTEGER DEFAULT 0,
            last_autobuy_item TEXT DEFAULT ''
        )
    """, commit=True)

    ucols = [row[1] for row in await db_fetchall("PRAGMA table_info(users)")]
    if "allowed" not in ucols:
        await db_execute("ALTER TABLE users ADD COLUMN allowed INTEGER DEFAULT 0", commit=True)
    if "last_request_ts" not in ucols:
        await db_execute("ALTER TABLE users ADD COLUMN last_request_ts INTEGER DEFAULT 0", commit=True)
    if "last_error_report" not in ucols:
        await db_execute("ALTER TABLE users ADD COLUMN last_error_report INTEGER DEFAULT 0", commit=True)


async def db_ensure_user(user_id: int):
    await db_execute(
        "INSERT OR IGNORE INTO users(user_id, role, allowed, last_error_report, last_request_ts) VALUES (?, ?, ?, ?, ?)",
        (user_id, "unknown", 1 if user_id in OWNER_IDS else 0, 0, 0),
        commit=True,
    )
    if user_id in OWNER_IDS:
        await db_execute("UPDATE users SET allowed=1 WHERE user_id=?", (user_id,), commit=True)
    await db_execute(
        "INSERT OR IGNORE INTO user_stats(user_id, autobuy_ok, autobuy_fail, last_autobuy_error, last_autobuy_ok_ts, last_autobuy_item) VALUES (?, 0, 0, '', 0, '')",
        (user_id,),
        commit=True,
    )


async def db_is_allowed(user_id: int) -> bool:
    if user_id in OWNER_IDS:
        return True
    row = await db_fetchone("SELECT allowed FROM users WHERE user_id=?", (user_id,))
    return bool(row[0]) if row else False


async def db_toggle_allowed(target_user_id: int) -> bool:
    if target_user_id in OWNER_IDS:
        return True
    await db_execute(
        "UPDATE users SET allowed = CASE WHEN COALESCE(allowed,0)=1 THEN 0 ELSE 1 END WHERE user_id=?",
        (target_user_id,),
        commit=True,
    )
    row = await db_fetchone("SELECT allowed FROM users WHERE user_id=?", (target_user_id,))
    return bool(row[0]) if row else False


async def db_list_users(limit: int, offset: int):
    rows = await db_fetchall(
        "SELECT user_id, allowed, role FROM users ORDER BY user_id LIMIT ? OFFSET ?",
        (limit, offset),
    )
    return [(int(r[0]), int(r[1] or 0), str(r[2] or "unknown")) for r in rows]


async def db_count_users() -> int:
    row = await db_fetchone("SELECT COUNT(1) FROM users")
    return int(row[0]) if row and row[0] is not None else 0


async def db_get_last_request_ts(user_id: int) -> int:
    row = await db_fetchone("SELECT last_request_ts FROM users WHERE user_id=?", (user_id,))
    return int(row[0]) if row and row[0] is not None else 0


async def db_set_last_request_ts(user_id: int, ts: int):
    await db_execute("UPDATE users SET last_request_ts=? WHERE user_id=?", (ts, user_id), commit=True)


async def db_get_role(user_id: int) -> str:
    row = await db_fetchone("SELECT role FROM users WHERE user_id=?", (user_id,))
    return row[0] if row else "unknown"


async def db_get_last_report(user_id: int) -> int:
    row = await db_fetchone("SELECT last_error_report FROM users WHERE user_id=?", (user_id,))
    return int(row[0]) if row and row[0] is not None else 0


async def db_set_last_report(user_id: int, ts: int):
    await db_execute("UPDATE users SET last_error_report=? WHERE user_id=?", (ts, user_id), commit=True)


async def db_get_urls(user_id: int):
    rows = await db_fetchall(
        "SELECT url, name, enabled, autobuy FROM urls WHERE user_id=? ORDER BY added_at, url",
        (user_id,),
    )
    return [{"url": url, "name": name or "", "enabled": bool(enabled), "autobuy": bool(autobuy)} for url, name, enabled, autobuy in rows]


async def db_add_url(user_id: int, url: str, name: str):
    await db_execute(
        "INSERT OR IGNORE INTO urls(user_id, url, name, added_at, enabled, autobuy) VALUES (?, ?, ?, ?, 1, 0)",
        (user_id, url, name or "", int(time.time())),
        commit=True,
    )
    await db_execute("UPDATE urls SET name=? WHERE user_id=? AND url=?", (name or "", user_id, url), commit=True)


async def db_set_url_name(user_id: int, url: str, name: str):
    await db_execute("UPDATE urls SET name=? WHERE user_id=? AND url=?", (name or "", user_id, url), commit=True)


async def db_remove_url(user_id: int, url: str):
    await db_execute("DELETE FROM urls WHERE user_id=? AND url=?", (user_id, url), commit=True)


async def db_set_url_enabled(user_id: int, url: str, enabled: bool):
    await db_execute("UPDATE urls SET enabled=? WHERE user_id=? AND url=?", (1 if enabled else 0, user_id, url), commit=True)


async def db_set_url_autobuy(user_id: int, url: str, autobuy: bool):
    await db_execute("UPDATE urls SET autobuy=? WHERE user_id=? AND url=?", (1 if autobuy else 0, user_id, url), commit=True)


async def db_mark_seen_batch(user_id: int, keys: list[str]):
    if not keys:
        return
    now = int(time.time())
    rows = [(user_id, k, now) for k in keys]
    await db_executemany("INSERT OR IGNORE INTO seen(user_id, item_key, seen_at) VALUES (?, ?, ?)", rows, commit=True)


async def db_load_seen(user_id: int):
    rows = await db_fetchall("SELECT item_key FROM seen WHERE user_id=?", (user_id,))
    return {r[0] for r in rows}


async def db_clear_seen(user_id: int):
    await db_execute("DELETE FROM seen WHERE user_id=?", (user_id,), commit=True)


async def db_mark_buy_attempted(user_id: int, key: str):
    await db_execute(
        "INSERT OR IGNORE INTO buy_attempted(user_id, item_key, attempted_at) VALUES (?, ?, ?)",
        (user_id, key, int(time.time())),
        commit=True,
    )


async def db_load_buy_attempted(user_id: int):
    rows = await db_fetchall("SELECT item_key FROM buy_attempted WHERE user_id=?", (user_id,))
    return {r[0] for r in rows}


async def db_clear_buy_attempted(user_id: int):
    await db_execute("DELETE FROM buy_attempted WHERE user_id=?", (user_id,), commit=True)


async def db_get_user_stats(user_id: int):
    row = await db_fetchone(
        "SELECT autobuy_ok, autobuy_fail, last_autobuy_error, last_autobuy_ok_ts, last_autobuy_item FROM user_stats WHERE user_id=?",
        (user_id,),
    )
    if not row:
        return {"autobuy_ok": 0, "autobuy_fail": 0, "last_autobuy_error": "", "last_autobuy_ok_ts": 0, "last_autobuy_item": ""}
    return {
        "autobuy_ok": int(row[0] or 0),
        "autobuy_fail": int(row[1] or 0),
        "last_autobuy_error": str(row[2] or ""),
        "last_autobuy_ok_ts": int(row[3] or 0),
        "last_autobuy_item": str(row[4] or ""),
    }


async def db_record_autobuy_result(user_id: int, ok: bool, item_desc: str, error_text: str = ""):
    await db_ensure_user(user_id)
    if ok:
        await db_execute(
            """
            INSERT INTO user_stats(user_id, autobuy_ok, autobuy_fail, last_autobuy_error, last_autobuy_ok_ts, last_autobuy_item)
            VALUES (?, 1, 0, '', ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                autobuy_ok = COALESCE(autobuy_ok, 0) + 1,
                last_autobuy_ok_ts = excluded.last_autobuy_ok_ts,
                last_autobuy_item = excluded.last_autobuy_item
            """,
            (user_id, int(time.time()), item_desc[:255]),
            commit=True,
        )
    else:
        await db_execute(
            """
            INSERT INTO user_stats(user_id, autobuy_ok, autobuy_fail, last_autobuy_error, last_autobuy_ok_ts, last_autobuy_item)
            VALUES (?, 0, 1, ?, 0, '')
            ON CONFLICT(user_id) DO UPDATE SET
                autobuy_fail = COALESCE(autobuy_fail, 0) + 1,
                last_autobuy_error = excluded.last_autobuy_error
            """,
            (user_id, error_text[:1000]),
            commit=True,
        )


async def db_cleanup_old_history(retention_seconds: int = SEEN_RETENTION_SECONDS):
    cutoff = int(time.time()) - int(retention_seconds)
    await db_execute("DELETE FROM seen WHERE seen_at < ?", (cutoff,), commit=True)
    await db_execute("DELETE FROM buy_attempted WHERE attempted_at < ?", (cutoff,), commit=True)


async def db_counts_snapshot():
    users = await db_fetchone("SELECT COUNT(1) FROM users")
    urls = await db_fetchone("SELECT COUNT(1) FROM urls")
    seen = await db_fetchone("SELECT COUNT(1) FROM seen")
    buy = await db_fetchone("SELECT COUNT(1) FROM buy_attempted")
    return {
        "users": int(users[0] or 0) if users else 0,
        "urls": int(urls[0] or 0) if urls else 0,
        "seen": int(seen[0] or 0) if seen else 0,
        "buy_attempted": int(buy[0] or 0) if buy else 0,
    }


async def write_backup_snapshot():
    db = await db_counts_snapshot()
    payload = {
        "saved_at": int(time.time()),
        "db_counts": db,
        "users": [
            {
                "user_id": int(r[0]),
                "role": str(r[1] or "unknown"),
                "allowed": int(r[2] or 0),
                "last_error_report": int(r[3] or 0),
                "last_request_ts": int(r[4] or 0),
            }
            for r in await db_fetchall("SELECT user_id, role, allowed, last_error_report, last_request_ts FROM users ORDER BY user_id")
        ],
        "urls": [
            {
                "user_id": int(r[0]),
                "url": str(r[1]),
                "name": str(r[2] or ""),
                "added_at": int(r[3] or 0),
                "enabled": int(r[4] or 0),
                "autobuy": int(r[5] or 0),
            }
            for r in await db_fetchall("SELECT user_id, url, name, added_at, enabled, autobuy FROM urls ORDER BY user_id, added_at, url")
        ],
        "seen": [
            {"user_id": int(r[0]), "item_key": str(r[1]), "seen_at": int(r[2] or 0)}
            for r in await db_fetchall("SELECT user_id, item_key, seen_at FROM seen")
        ],
        "buy_attempted": [
            {"user_id": int(r[0]), "item_key": str(r[1]), "attempted_at": int(r[2] or 0)}
            for r in await db_fetchall("SELECT user_id, item_key, attempted_at FROM buy_attempted")
        ],
        "user_stats": [
            {
                "user_id": int(r[0]),
                "autobuy_ok": int(r[1] or 0),
                "autobuy_fail": int(r[2] or 0),
                "last_autobuy_error": str(r[3] or ""),
                "last_autobuy_ok_ts": int(r[4] or 0),
                "last_autobuy_item": str(r[5] or ""),
            }
            for r in await db_fetchall("SELECT user_id, autobuy_ok, autobuy_fail, last_autobuy_error, last_autobuy_ok_ts, last_autobuy_item FROM user_stats")
        ],
    }
    tmp = DATA_BACKUP_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, DATA_BACKUP_FILE)


async def restore_from_backup_if_needed():
    counts = await db_counts_snapshot()
    if sum(counts.values()) > 0:
        return False
    if not os.path.exists(DATA_BACKUP_FILE):
        return False

    with open(DATA_BACKUP_FILE, "r", encoding="utf-8") as f:
        payload = json.load(f)

    users = payload.get("users") or []
    urls = payload.get("urls") or []
    seen_rows = payload.get("seen") or []
    buy_rows = payload.get("buy_attempted") or []
    stats_rows = payload.get("user_stats") or []

    if users:
        await db_executemany(
            "INSERT OR REPLACE INTO users(user_id, role, allowed, last_error_report, last_request_ts) VALUES (?, ?, ?, ?, ?)",
            [
                (int(u["user_id"]), str(u.get("role") or "unknown"), int(u.get("allowed") or 0), int(u.get("last_error_report") or 0), int(u.get("last_request_ts") or 0))
                for u in users
            ],
            commit=True,
        )
    if urls:
        await db_executemany(
            "INSERT OR REPLACE INTO urls(user_id, url, name, added_at, enabled, autobuy) VALUES (?, ?, ?, ?, ?, ?)",
            [
                (int(r["user_id"]), str(r["url"]), str(r.get("name") or ""), int(r.get("added_at") or 0), int(r.get("enabled") or 0), int(r.get("autobuy") or 0))
                for r in urls
            ],
            commit=True,
        )
    if seen_rows:
        await db_executemany(
            "INSERT OR REPLACE INTO seen(user_id, item_key, seen_at) VALUES (?, ?, ?)",
            [(int(r["user_id"]), str(r["item_key"]), int(r.get("seen_at") or 0)) for r in seen_rows],
            commit=True,
        )
    if buy_rows:
        await db_executemany(
            "INSERT OR REPLACE INTO buy_attempted(user_id, item_key, attempted_at) VALUES (?, ?, ?)",
            [(int(r["user_id"]), str(r["item_key"]), int(r.get("attempted_at") or 0)) for r in buy_rows],
            commit=True,
        )
    if stats_rows:
        await db_executemany(
            "INSERT OR REPLACE INTO user_stats(user_id, autobuy_ok, autobuy_fail, last_autobuy_error, last_autobuy_ok_ts, last_autobuy_item) VALUES (?, ?, ?, ?, ?, ?)",
            [
                (int(r["user_id"]), int(r.get("autobuy_ok") or 0), int(r.get("autobuy_fail") or 0), str(r.get("last_autobuy_error") or ""), int(r.get("last_autobuy_ok_ts") or 0), str(r.get("last_autobuy_item") or ""))
                for r in stats_rows
            ],
            commit=True,
        )
    return True


# ====================== LOAD USER DATA ======================
async def load_user_data(user_id: int, force: bool = False):
    if user_id in user_started and not force:
        return
    await db_ensure_user(user_id)
    user_urls[user_id] = await db_get_urls(user_id)
    user_seen_items[user_id] = await db_load_seen(user_id)
    user_buy_attempted[user_id] = await db_load_buy_attempted(user_id)
    user_runtime_stats[user_id] = await db_get_user_stats(user_id)
    user_started.add(user_id)


async def get_user_role(user_id: int) -> str | None:
    await load_user_data(user_id)
    role = await db_get_role(user_id)
    return None if role == "unknown" else role


async def user_url_limit(user_id: int) -> int:
    role = await get_user_role(user_id)
    return MAX_URLS_PER_USER_LIMITED if role == "limited" else MAX_URLS_PER_USER_DEFAULT


async def user_hunter_interval(user_id: int) -> float:
    role = await get_user_role(user_id)
    extra = LIMITED_EXTRA_DELAY if role == "limited" else 0.0
    return HUNTER_INTERVAL_BASE + extra


def fmt_ts(ts: int | float | None) -> str:
    try:
        if ts and float(ts) > 0:
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(ts)))
    except Exception:
        pass
    return "—"


async def build_health_text(user_id: int) -> str:
    await load_user_data(user_id)
    task = user_hunter_tasks.get(user_id)
    counts = await db_counts_snapshot()
    backup_exists = os.path.exists(DATA_BACKUP_FILE)
    backup_mtime = fmt_ts(os.path.getmtime(DATA_BACKUP_FILE)) if backup_exists else "—"
    backup_size = os.path.getsize(DATA_BACKUP_FILE) if backup_exists else 0
    db_size = os.path.getsize(DB_FILE) if os.path.exists(DB_FILE) else 0
    session_state = "open" if _global_session is not None and not _global_session.closed else "closed"
    active_sources = await get_all_sources(user_id, enabled_only=True)
    all_sources = await get_all_sources(user_id, enabled_only=False)

    return (
        "<b>🧪 Диагностика</b>\n"
        f"• DB файл: <code>{html.escape(DB_FILE)}</code> ({db_size} bytes)\n"
        f"• Backup: <code>{html.escape(DATA_BACKUP_FILE)}</code> | {'есть' if backup_exists else 'нет'} | {backup_size} bytes\n"
        f"• Backup обновлён: <b>{html.escape(backup_mtime)}</b>\n"
        f"• HTTP session: <b>{session_state}</b>\n"
        f"• Hunter task: <b>{'alive' if task and not task.done() else 'stopped'}</b>\n"
        f"• Охотник флаг: <b>{'ВКЛ' if user_search_active[user_id] else 'ВЫКЛ'}</b>\n"
        f"• Активных URL: <b>{len(active_sources)}</b> / всего: <b>{len(all_sources)}</b>\n"
        f"• Записей users/urls/seen/buy: <b>{counts['users']}/{counts['urls']}/{counts['seen']}/{counts['buy_attempted']}</b>\n"
        f"• Seen в памяти: <b>{len(user_seen_items[user_id])}</b> | buy_attempted в памяти: <b>{len(user_buy_attempted[user_id])}</b>\n"
        f"• API ошибок в памяти: <b>{user_api_errors.get(user_id, 0)}</b>"
    )


# ====================== URL VALIDATION/NORMALIZATION ======================
VALID_API_HOSTS = {"api.lzt.market", "prod-api.lzt.market", "api.lolz.live"}


def validate_market_url(url: str):
    try:
        parts = urlsplit((url or "").strip())
    except Exception:
        return False, "❌ Это не похоже на URL."

    if parts.scheme not in ("http", "https") or not parts.netloc:
        return False, "❌ Это не похоже на URL."

    host = parts.netloc.lower()
    if host not in VALID_API_HOSTS:
        return False, "❌ Нужна API-ссылка LZT: prod-api.lzt.market / api.lzt.market / api.lolz.live."

    return True, None


def normalize_url(url: str) -> str:
    if not url:
        return url

    s = (url or "").strip().replace(" ", "").replace("\t", "").replace("\n", "")
    parts = urlsplit(s)

    scheme = parts.scheme or "https"
    netloc = (parts.netloc or "").lower()
    path = parts.path or ""
    query = parts.query or ""

    alias_map = {
        "lzt.market": "api.lzt.market",
        "www.lzt.market": "api.lzt.market",
        "api.lolz.guru": "api.lzt.market",
    }
    netloc = alias_map.get(netloc, netloc)

    query = query.replace("genshinlevelmin", "genshin_level_min")
    query = query.replace("genshinlevel_min", "genshin_level_min")
    query = query.replace("genshin_levelmin", "genshin_level_min")
    query = query.replace("brawl_cupmin", "brawl_cup_min")
    query = query.replace("clash_cupmin", "clash_cup_min")
    query = query.replace("clashcupmin", "clash_cup_min")
    query = query.replace("clashcupmax", "clash_cup_max")
    query = query.replace("clash_cupmax", "clash_cup_max")
    query = query.replace("orderby", "order_by")
    query = query.replace("order_by=pdate_to_down_upoad", "order_by=pdate_to_down_upload")
    query = query.replace("order_by=pdate_to_down_up", "order_by=pdate_to_down_upload")
    query = query.replace("order_by=pdate_to_downupload", "order_by=pdate_to_down_upload")

    return urlunsplit((scheme, netloc, path, query, ""))


# ====================== HTTP / API ======================
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
_global_session: aiohttp.ClientSession | None = None


async def get_session():
    global _global_session
    if _global_session is None or _global_session.closed:
        timeout = aiohttp.ClientTimeout(total=FETCH_TIMEOUT, connect=3, sock_connect=3, sock_read=FETCH_TIMEOUT)
        connector = aiohttp.TCPConnector(limit=32, limit_per_host=16, ttl_dns_cache=300, enable_cleanup_closed=True)
        _global_session = aiohttp.ClientSession(timeout=timeout, connector=connector)
    return _global_session


async def close_session():
    global _global_session
    if _global_session:
        await _global_session.close()
        _global_session = None


async def fetch_items_raw(url: str):
    headers = {"Authorization": f"Bearer {LZT_API_KEY}"} if LZT_API_KEY else {}
    try:
        session = await get_session()
        async with session.get(url, headers=headers, timeout=FETCH_TIMEOUT) as resp:
            text = await resp.text()

            if resp.status in (400, 401, 403, 404):
                return None, f"HTTP {resp.status}: {text[:300]}", resp.status

            try:
                data = json.loads(text)
            except Exception:
                return None, f"❌ API вернул не JSON:\n{text[:300]}", resp.status

            items = data.get("items")
            if not isinstance(items, list):
                return None, "⚠ API не вернул список items", resp.status

            return items, None, resp.status

    except asyncio.TimeoutError:
        return None, "❌ Таймаут запроса", 0
    except aiohttp.ClientError as e:
        return None, f"❌ Ошибка сети: {e}", 0
    except Exception as e:
        return None, f"❌ Ошибка: {e}", 0


async def fetch_with_retry(url: str, max_retries: int = RETRY_MAX):
    attempt = 0
    delay = RETRY_BASE_DELAY

    while attempt < max_retries:
        attempt += 1
        try:
            async with semaphore:
                items, err, status = await fetch_items_raw(url)
        except Exception as e:
            items, err, status = None, f"❌ Ошибка: {e}", 0

        if err is None:
            return items, None

        if status in (400, 401, 403, 404):
            return [], err

        if attempt >= max_retries:
            return [], err

        jitter = random.uniform(0, delay * 0.2)
        await asyncio.sleep(delay + jitter)
        delay *= 2

    return [], "❌ Не удалось получить ответ"


# ====================== SOURCES ======================
async def get_all_sources(user_id: int, enabled_only: bool = False):
    await load_user_data(user_id)

    deduped = []
    seen = set()
    for src in user_urls[user_id]:
        u = src.get("url")
        if not u or u in seen:
            continue
        seen.add(u)
        deduped.append(src)
    user_urls[user_id] = deduped

    out = []
    for i, s in enumerate(user_urls[user_id], start=1):
        if enabled_only and not s.get("enabled", True):
            continue
        out.append({**s, "idx": i})
    return out


async def fetch_all_sources(user_id: int):
    sources = await get_all_sources(user_id, enabled_only=True)
    if not sources:
        return [], []

    async def _fetch_one(src: dict):
        url = src["url"]
        label = src.get("name") or f"URL #{src['idx']}"
        source_info = {"idx": src["idx"], "url": url, "name": label, "enabled": src.get("enabled", True), "autobuy": src.get("autobuy", False)}
        items, err = await fetch_with_retry(url)
        return source_info, items, err

    tasks = [asyncio.create_task(_fetch_one(s)) for s in sources]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    items_with_sources = []
    errors = []
    for res in results:
        if isinstance(res, Exception):
            errors.append(("UNKNOWN", "UNKNOWN", str(res)))
            continue
        source_info, items, err = res
        if err:
            errors.append((source_info["name"], source_info["url"], err))
            continue
        for it in items:
            items_with_sources.append((it, source_info))

    return items_with_sources, errors


async def iter_sources_results(user_id: int):
    sources = await get_all_sources(user_id, enabled_only=True)
    if not sources:
        return

    async def _fetch_one(src: dict):
        url = src["url"]
        label = src.get("name") or f"URL #{src['idx']}"
        source_info = {"idx": src["idx"], "url": url, "name": label, "enabled": src.get("enabled", True), "autobuy": src.get("autobuy", False)}
        items, err = await fetch_with_retry(url)
        return source_info, items, err

    tasks = [asyncio.create_task(_fetch_one(s)) for s in sources]
    try:
        for fut in asyncio.as_completed(tasks):
            try:
                yield await fut
            except Exception as e:
                yield {"idx": -1, "url": "UNKNOWN", "name": "UNKNOWN", "enabled": True, "autobuy": False}, [], str(e)
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()


# ====================== DISPLAY ======================
def make_card(item: dict, source_name: str) -> str:
    title = str(item.get("title", "Без названия"))
    price = item.get("price", None)
    item_id = item.get("item_id") or item.get("id")

    trophies = item.get("trophies") or item.get("cups") or item.get("brawl_cup") or None
    level = item.get("level") or item.get("lvl") or item.get("user_level") or None
    townhall = item.get("townhall") or item.get("th") or None
    phone_bound = item.get("phone_bound")
    if phone_bound is None:
        phone_bound = item.get("phone")

    seller_id = item.get("seller_id") or item.get("owner_id") or item.get("user_id")
    category = item.get("category") or item.get("category_name") or item.get("game") or item.get("type")
    published_at = item.get("published_at") or item.get("created_at") or item.get("date") or item.get("time")
    views = item.get("views") or item.get("view_count")
    likes = item.get("likes") or item.get("favorites") or item.get("fav_count")

    desc = item.get("description") or item.get("desc") or ""
    if isinstance(desc, str):
        desc = html.unescape(desc).strip()
    else:
        desc = ""

    direct_url = item.get("url") or item.get("link") or None

    def _fmt_int(x):
        try:
            return f"{int(x):,}".replace(",", " ")
        except Exception:
            return str(x)

    def _fmt_time(x):
        try:
            if isinstance(x, (int, float)) and x > 0:
                return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(x)))
        except Exception:
            pass
        return str(x) if x is not None else None

    src = html.escape(str(source_name or "Источник"))
    ttl = html.escape(title)
    iid = html.escape(str(item_id)) if item_id is not None else "—"

    lines = [
        "━━━━━━━━━━━━━━━━━━━━",
        f"🔎 <b>{src}</b>",
        f"🎮 <b>{ttl}</b>",
        f"🆔 <code>{iid}</code>",
    ]

    meta = []
    if category:
        meta.append(f"🏷 {html.escape(str(category))}")
    if seller_id is not None:
        meta.append(f"👤 seller: <code>{html.escape(str(seller_id))}</code>")
    if views is not None:
        meta.append(f"👁 {html.escape(_fmt_int(views))}")
    if likes is not None:
        meta.append(f"⭐ {html.escape(_fmt_int(likes))}")
    if published_at is not None:
        meta.append(f"🕒 {html.escape(_fmt_time(published_at))}")
    if meta:
        lines.append(" • ".join(meta))

    if level is not None:
        lines.append(f"🔼 Уровень: <b>{html.escape(_fmt_int(level))}</b>")
    if trophies is not None:
        lines.append(f"🏆 Кубков: <b>{html.escape(_fmt_int(trophies))}</b>")
    if townhall is not None:
        lines.append(f"🏰 Ратуша: <b>{html.escape(_fmt_int(townhall))}</b>")
    if phone_bound is not None:
        lines.append(f"📱 Телефон привязан: <b>{'Да' if phone_bound else 'Нет'}</b>")

    if price is not None and price != "—":
        lines.append(f"💰 Цена: <b>{html.escape(_fmt_int(price))} ₽</b>")
    else:
        lines.append("💰 Цена: <b>—</b>")

    if direct_url:
        lines.append(f"🔗 {html.escape(str(direct_url))}")
    elif item_id is not None:
        lines.append(f"🔗 https://lzt.market/{html.escape(str(item_id))}")

    if desc:
        clean = re.sub(r"\s{3,}", "  ", desc).strip()
        if len(clean) > 800:
            clean = clean[:780] + "…"
        lines.extend(["", "📝 <b>Описание</b>", html.escape(clean)])

    lines.append("━━━━━━━━━━━━━━━━━━━━")
    card = "\n".join(lines)
    if len(card) > SHORT_CARD_MAX:
        return card[: SHORT_CARD_MAX - 80] + "\n… <i>(обрезано)</i>\n━━━━━━━━━━━━━━━━━━━━"
    return card


# ====================== AUTOBUY ======================
def _autobuy_payload_variants(item: dict):
    price = item.get("price")
    payload = {}
    if price is not None:
        payload.update({"price": price, "item_price": price, "amount": price})

    if LZT_SECRET_WORD:
        payload.update({
            "secret_answer": LZT_SECRET_WORD,
            "secret_word": LZT_SECRET_WORD,
            "secretWord": LZT_SECRET_WORD,
            "qa_answer": LZT_SECRET_WORD,
            "answer": LZT_SECRET_WORD,
        })

    variants = [
        payload,
        {**payload, "confirm": 1, "is_confirmed": True},
        {k: v for k, v in payload.items() if k not in {"price", "item_price", "amount"}},
    ]

    dedup = []
    seen = set()
    for var in variants:
        frozen = tuple(sorted(var.items()))
        if frozen in seen:
            continue
        seen.add(frozen)
        dedup.append(var)
    return dedup


def _autobuy_buy_urls(source_url: str, item_id: int):
    source_url = (source_url or "").strip()
    source_base = ""
    try:
        parts = urlsplit(source_url)
        if parts.scheme and parts.netloc:
            source_base = f"{parts.scheme}://{parts.netloc}"
    except Exception:
        source_base = ""

    base_hosts = ["https://prod-api.lzt.market"]
    if "api.lolz.live" in source_url.lower():
        base_hosts.append("https://api.lolz.live")
    if source_base:
        base_hosts.append(source_base)
    base_hosts.extend(["https://api.lzt.market", "https://api.lolz.live"])

    dedup_bases = []
    seen_bases = set()
    for base in base_hosts:
        if base in seen_bases:
            continue
        seen_bases.add(base)
        dedup_bases.append(base)

    fast_paths = ["{id}/fast-buy", "{id}/buy", "item/{id}/fast-buy", "item/{id}/buy"]
    slow_paths = [
        "{id}/purchase",
        "market/{id}/fast-buy",
        "market/{id}/buy",
        "market/{id}/purchase",
        "item/{id}/purchase",
        "items/{id}/buy",
        "items/{id}/fast-buy",
        "items/{id}/purchase",
    ]

    urls = []
    seen = set()
    for path_list in (fast_paths, slow_paths):
        for base in dedup_bases:
            for tpl in path_list:
                url = f"{base}/{tpl.format(id=item_id)}"
                if url in seen:
                    continue
                seen.add(url)
                urls.append(url)
    return urls


def _autobuy_cache_key(source_url: str) -> str:
    try:
        p = urlsplit(source_url or "")
        if p.netloc:
            return f"{p.scheme}://{p.netloc}"
    except Exception:
        pass
    return (source_url or "").strip().lower() or "default"


def _autobuy_prioritized_urls(source_url: str, item_id: int):
    all_urls = _autobuy_buy_urls(source_url, item_id)
    cache_key = _autobuy_cache_key(source_url)
    preferred = autobuy_endpoint_cache.get(cache_key, [])
    if preferred:
        pref_item_urls = [tpl.format(id=item_id) for tpl in preferred]
        ordered = []
        seen = set()
        for u in pref_item_urls + all_urls:
            if u in seen:
                continue
            seen.add(u)
            ordered.append(u)
        return ordered
    return all_urls


def _remember_autobuy_endpoint(source_url: str, used_url: str):
    cache_key = _autobuy_cache_key(source_url)
    try:
        parts = urlsplit(used_url)
        path = parts.path.lstrip("/")
        item_id_match = re.search(r"/(\d+)(?:/|$)", "/" + path)
        if not item_id_match:
            return
        item_id_str = item_id_match.group(1)
        template_path = path.replace(item_id_str, "{id}", 1)
        template_url = f"{parts.scheme}://{parts.netloc}/{template_path}"
        current = autobuy_endpoint_cache.get(cache_key, [])
        current = [template_url] + [u for u in current if u != template_url]
        autobuy_endpoint_cache[cache_key] = current[:3]
    except Exception:
        pass


def _autobuy_classify_response(status: int, text: str):
    raw = html.unescape(text or "")
    lower = raw.lower()
    try:
        data = json.loads(raw)
        joined = json.dumps(data, ensure_ascii=False).lower()
    except Exception:
        joined = lower

    success_markers = ("success", "ok", "purchased", "purchase complete", "already bought", "уже куп")
    terminal_error_markers = (
        "insufficient", "not enough", "недостаточно", "уже продан", "already sold",
        "already purchased", "already bought", "цена изменилась", "нельзя купить",
        "forbidden", "access denied",
    )

    if status in (404, 405):
        return "retry", raw[:220], False
    if status in (200, 201, 202):
        return "success", raw[:220], False
    if status in (401, 403):
        return "auth", raw[:220], False
    if status == 415:
        return "retry", raw[:220], True
    if status == 400 and any(x in joined for x in ("invalid json", "unsupported media", "content-type")):
        return "retry", raw[:220], True

    if "secret" in joined or "answer" in joined or "секрет" in joined:
        return "secret", raw[:220], False
    if any(marker in joined for marker in success_markers):
        return "success", raw[:220], False
    if any(marker in joined for marker in terminal_error_markers):
        return "terminal", raw[:220], False
    return "retry", raw[:220], False


async def _try_autobuy_once(source: dict, item: dict, found_perf: float | None = None):
    if not LZT_API_KEY:
        return False, "LZT_API_KEY не задан"

    item_id = item.get("item_id") or item.get("id")
    if not item_id:
        return False, "missing_item_id"

    try:
        item_id = int(item_id)
    except (TypeError, ValueError):
        return False, f"invalid_item_id={item_id}"

    t0 = time.perf_counter()
    source_name = (source.get("name") or "UNKNOWN").strip()
    source_url = (source.get("url") or "").strip()
    headers_json = {"Authorization": f"Bearer {LZT_API_KEY}", "Accept": "application/json", "Content-Type": "application/json"}
    headers_form = {"Authorization": f"Bearer {LZT_API_KEY}", "Accept": "application/json"}
    payload_variants = _autobuy_payload_variants(item)
    buy_urls = _autobuy_prioritized_urls(source_url, item_id)

    since_found_ms = None
    if found_perf is not None:
        since_found_ms = int((t0 - found_perf) * 1000)

    log_autobuy(
        f"BUY_START item_id={item_id} src='{_safe_compact(source_name,120)}' "
        f"since_found_ms={since_found_ms} urls={len(buy_urls)} payloads={len(payload_variants)}"
    )

    last_err = "unknown"
    session = await get_session()

    async with buy_semaphore:
        for buy_url in buy_urls:
            for payload_idx, payload in enumerate(payload_variants, start=1):
                need_form_retry = False
                try:
                    async with session.post(buy_url, headers=headers_json, json=payload, timeout=BUY_TIMEOUT) as resp:
                        body = await resp.text()
                        state, info, retry_as_form = _autobuy_classify_response(resp.status, body)

                        if resp.status not in (404, 405):
                            log_autobuy(
                                f"BUY_TRY item_id={item_id} payload={payload_idx} status={resp.status} "
                                f"state={state} url={buy_url} info='{_safe_compact(info,220)}'"
                            )

                        if state == "success":
                            _remember_autobuy_endpoint(source_url, buy_url)
                            return True, f"{buy_url} -> {info}"
                        if state == "auth":
                            return False, f"{buy_url} -> HTTP {resp.status}: проверьте API ключ и scope market ({info})"
                        if state == "secret":
                            return False, f"{buy_url} -> нужен/неверный ответ на секретный вопрос ({info})"
                        if state == "terminal":
                            _remember_autobuy_endpoint(source_url, buy_url)
                            return False, f"{buy_url} -> {info}"

                        last_err = f"{buy_url} -> HTTP {resp.status}: {info}"
                        need_form_retry = retry_as_form

                except asyncio.TimeoutError:
                    last_err = f"{buy_url} -> buy_timeout"
                    continue
                except Exception as e:
                    last_err = f"{buy_url} -> {e}"
                    continue

                if not need_form_retry:
                    continue

                try:
                    async with session.post(buy_url, headers=headers_form, data=payload, timeout=BUY_TIMEOUT) as form_resp:
                        form_body = await form_resp.text()
                        state, info, _ = _autobuy_classify_response(form_resp.status, form_body)

                        if form_resp.status not in (404, 405):
                            log_autobuy(
                                f"BUY_TRY_FORM item_id={item_id} payload={payload_idx} status={form_resp.status} "
                                f"state={state} url={buy_url} info='{_safe_compact(info,220)}'"
                            )

                        if state == "success":
                            _remember_autobuy_endpoint(source_url, buy_url)
                            return True, f"{buy_url} (form) -> {info}"
                        if state == "auth":
                            return False, f"{buy_url} (form) -> HTTP {form_resp.status}: проверьте API ключ и scope market ({info})"
                        if state == "secret":
                            return False, f"{buy_url} (form) -> нужен/неверный ответ на секретный вопрос ({info})"
                        if state == "terminal":
                            _remember_autobuy_endpoint(source_url, buy_url)
                            return False, f"{buy_url} (form) -> {info}"

                        last_err = f"{buy_url} (form) -> HTTP {form_resp.status}: {info}"

                except asyncio.TimeoutError:
                    last_err = f"{buy_url} (form) -> buy_timeout"
                    continue
                except Exception as e:
                    last_err = f"{buy_url} (form) -> {e}"
                    continue

    return False, last_err


async def try_autobuy_item(source: dict, item: dict, found_perf: float | None = None):
    item_key = make_item_key(item)
    lock = get_buy_lock(item_key)

    async with lock:
        last_result = (False, "unknown")
        for attempt in range(1, AUTOBUY_RETRY_ATTEMPTS + 1):
            bought, info = await _try_autobuy_once(source, item, found_perf=found_perf)
            last_result = (bought, info)

            if bought:
                return True, f"attempt={attempt}/{AUTOBUY_RETRY_ATTEMPTS} | {info}"

            low = (info or "").lower()
            terminal = any(x in low for x in [
                "недостаточно", "already sold", "already purchased", "already bought",
                "уже продан", "нельзя купить", "secret", "auth", "401", "403",
            ])
            if terminal:
                return False, f"attempt={attempt}/{AUTOBUY_RETRY_ATTEMPTS} | {info}"

            if attempt < AUTOBUY_RETRY_ATTEMPTS:
                delay = random.uniform(AUTOBUY_RETRY_MIN_DELAY, AUTOBUY_RETRY_MAX_DELAY)
                log_autobuy(f"BUY_RETRY_WAIT item_key={item_key} attempt={attempt} sleep={delay:.2f}s")
                await asyncio.sleep(delay)

        return last_result[0], f"attempt={AUTOBUY_RETRY_ATTEMPTS}/{AUTOBUY_RETRY_ATTEMPTS} | {last_result[1]}"


# ====================== REPORTER ======================
async def error_reporter_loop():
    while True:
        try:
            await asyncio.sleep(ERROR_REPORT_INTERVAL)
            now = int(time.time())
            for uid in list(user_started):
                count = user_api_errors.get(uid, 0)
                last = await db_get_last_report(uid)
                if count and (now - last >= ERROR_REPORT_INTERVAL):
                    try:
                        await send_bot_message(uid, f"⚠️ За последний час ошибок API: <b>{count}</b>", parse_mode="HTML")
                    except Exception:
                        pass
                    user_api_errors[uid] = 0
                    await db_set_last_report(uid, now)
        except Exception:
            await asyncio.sleep(ERROR_REPORT_INTERVAL)


async def cleanup_loop():
    while True:
        try:
            await asyncio.sleep(CLEANUP_INTERVAL)
            await db_cleanup_old_history(SEEN_RETENTION_SECONDS)
            for uid in list(user_started):
                user_seen_items[uid] = await db_load_seen(uid)
                user_buy_attempted[uid] = await db_load_buy_attempted(uid)
        except Exception as e:
            log_autobuy(f"CLEANUP_LOOP_EXC err='{_safe_compact(str(e),300)}'")
            await asyncio.sleep(10)


async def backup_loop():
    while True:
        try:
            await asyncio.sleep(BACKUP_INTERVAL)
            await write_backup_snapshot()
        except Exception as e:
            log_autobuy(f"BACKUP_LOOP_EXC err='{_safe_compact(str(e),300)}'")
            await asyncio.sleep(10)


# ====================== ACTIONS ======================
async def show_denied(user_id: int, chat_id: int):
    await send_screen(chat_id, user_id, DENIED_TEXT, reply_markup=kb_request())


async def show_status(user_id: int, chat_id: int):
    await load_user_data(user_id)
    role = await get_user_role(user_id) or "not set"
    active = user_search_active[user_id]
    all_sources = await get_all_sources(user_id, enabled_only=False)
    enabled_sources = [s for s in all_sources if s.get("enabled", True)]
    ab = sum(1 for s in all_sources if s.get("autobuy", False))
    stats = await db_get_user_stats(user_id)
    user_runtime_stats[user_id] = stats

    text = (
        "<b>📊 Статус</b>\n"
        f"• Роль: <b>{html.escape(role)}</b>\n"
        f"• Охотник: <b>{'ВКЛ' if active else 'ВЫКЛ'}</b>\n"
        f"• URL: <b>{len(enabled_sources)}/{len(all_sources)}</b> (автобай: <b>{ab}</b>)\n"
        f"• Увидено: <b>{len(user_seen_items[user_id])}</b>\n"
        f"• Попыток автобая: <b>{len(user_buy_attempted[user_id])}</b>\n"
        f"• Успешных автобаев: <b>{stats['autobuy_ok']}</b>\n"
        f"• Неуспешных автобаев: <b>{stats['autobuy_fail']}</b>\n"
        f"• Последний успешный лот: <b>{html.escape(stats['last_autobuy_item'] or '—')}</b>\n"
        f"• Последний успех: <b>{html.escape(fmt_ts(stats['last_autobuy_ok_ts']))}</b>\n"
        f"• Последняя ошибка автобая: <b>{html.escape((stats['last_autobuy_error'] or '—')[:180])}</b>\n"
        f"• Ошибок API: <b>{user_api_errors.get(user_id, 0)}</b>\n"
        f"• Backup: <code>{html.escape(DATA_BACKUP_FILE)}</code>\n"
        f"• Лог: <code>{html.escape(AUTOBUY_LOG_FILE)}</code>"
    )
    await send_screen(chat_id, user_id, text, reply_markup=kb_main(user_id), parse_mode="HTML")

async def show_urls_list_screen(user_id: int, chat_id: int, page: int = 0):
    await load_user_data(user_id)
    sources = await get_all_sources(user_id, enabled_only=False)

    if not sources:
        await send_screen(chat_id, user_id, "📚 Список URL пуст.\nНажми ➕ Добавить URL", reply_markup=kb_urls_menu())
        return

    lines = ["📚 <b>Мои URL</b>\n(жми на кнопку снизу, чтобы посмотреть детали/состояние)"]
    for s in sources:
        idx = s["idx"]
        name = s.get("name") or f"URL #{idx}"
        st = "🟢" if s.get("enabled", True) else "🔴"
        ab = "🛒" if s.get("autobuy", False) else "—"
        lines.append(f"{st} {ab} <b>{idx}.</b> {html.escape(name)}")

    kb = build_urls_picker_kb(sources, page=page, back_text="⬅️ Назад")
    user_modes[user_id] = "pick_list"
    user_page_state[user_id] = {"ctx": "pick_list", "page": page}
    await send_screen(chat_id, user_id, "\n".join(lines), reply_markup=kb, parse_mode="HTML")


async def show_users_screen(owner_id: int, chat_id: int, page: int = 0):
    rows = await db_list_users(limit=5000, offset=0)
    total = len(rows)

    rows_sorted = []
    for uid, allowed, role in rows:
        if uid in OWNER_IDS:
            allowed = 1
        rows_sorted.append((uid, allowed, role))
    rows_sorted.sort(key=lambda x: (0 if x[0] in OWNER_IDS else 1, x[0]))

    kb = build_users_picker_kb(rows_sorted, page=page)
    user_modes[owner_id] = "users_pick"
    user_page_state[owner_id] = {"ctx": "users_pick", "page": page}

    txt = (
        f"👥 <b>Пользователи</b>\n"
        f"Всего в базе: <b>{total}</b>\n\n"
        "Нажми на пользователя, чтобы переключить доступ ✅/⛔️"
    )
    await send_screen(chat_id, owner_id, txt, reply_markup=kb, parse_mode="HTML")


async def send_compact_10_for_user(user_id: int, chat_id: int):
    items_with_sources, errors = await fetch_all_sources(user_id)

    if errors:
        user_api_errors[user_id] += len(errors)
        chunks = []
        for label, url, err in errors[:10]:
            chunks.append(f"• <b>{html.escape(label)}</b>\n<code>{html.escape(url)}</code>\n{html.escape(str(err))}")
        await send_bot_message(chat_id, "❗ <b>Ошибки URL</b>\n\n" + "\n\n".join(chunks), parse_mode="HTML")

    if not items_with_sources:
        await send_screen(chat_id, user_id, "❗ Ничего не найдено по активным URL.", reply_markup=kb_main(user_id))
        return

    aggregated = {}
    for item, source in items_with_sources:
        key = make_item_key(item)
        if key not in aggregated:
            aggregated[key] = (item, source)

    items_list = list(aggregated.values())[:10]
    await send_screen(chat_id, user_id, f"✅ <b>Проверка лотов</b>\n• Показано: <b>{len(items_list)}</b>", reply_markup=kb_main(user_id), parse_mode="HTML")

    for item, source in items_list:
        await send_bot_message(chat_id, make_card(item, source["name"]), parse_mode="HTML", disable_web_page_preview=True)


async def send_test_for_single_url(user_id: int, chat_id: int, src: dict):
    url = src["url"]
    label = src.get("name") or f"URL #{src.get('idx', '?')}"
    items, err = await fetch_with_retry(url, max_retries=2)

    if err:
        await send_screen(chat_id, user_id, f"❗ Ошибка теста <b>{html.escape(label)}</b>\n{html.escape(str(err))}", reply_markup=kb_urls_menu(), parse_mode="HTML")
        return
    if not items:
        await send_screen(chat_id, user_id, f"⚠️ <b>{html.escape(label)}</b>: пусто.", reply_markup=kb_urls_menu(), parse_mode="HTML")
        return

    aggregated = {}
    for it in items:
        aggregated.setdefault(make_item_key(it), it)

    limited = list(aggregated.values())[:10]
    await send_screen(
        chat_id,
        user_id,
        f"✅ Тест: <b>{html.escape(label)}</b>\n• Уникальных: <b>{len(aggregated)}</b>\n• Показано: <b>{len(limited)}</b>",
        reply_markup=kb_urls_menu(),
        parse_mode="HTML",
    )

    for it in limited:
        await send_bot_message(chat_id, make_card(it, label), parse_mode="HTML", disable_web_page_preview=True)


async def seed_existing_without_notifications(user_id: int):
    items_with_sources, _ = await fetch_all_sources(user_id)
    aggregated = {}
    for item, source in items_with_sources:
        aggregated.setdefault(make_item_key(item), (item, source))

    seen_batch = []
    buy_batch = []
    for item, _source in aggregated.values():
        key = make_item_key(item)
        if key not in user_seen_items[user_id]:
            user_seen_items[user_id].add(key)
            seen_batch.append(key)
        if key not in user_buy_attempted[user_id]:
            user_buy_attempted[user_id].add(key)
            buy_batch.append(key)

    await db_mark_seen_batch(user_id, seen_batch)
    for key in buy_batch:
        await db_mark_buy_attempted(user_id, key)


async def hunter_loop_for_user(user_id: int, chat_id: int):
    await load_user_data(user_id)

    while user_search_active[user_id]:
        seen_batch = []
        try:
            async for source, items, err in iter_sources_results(user_id):
                if err:
                    user_api_errors[user_id] += 1
                    continue
                if not items:
                    continue

                try:
                    items = sorted(items, key=lambda x: int(x.get("item_id") or x.get("id") or 0), reverse=True)
                except Exception:
                    pass

                for item in items:
                    key = make_item_key(item)
                    if key in user_seen_items[user_id]:
                        continue

                    found_perf = time.perf_counter()
                    item_id = item.get("item_id") or item.get("id")
                    src_name = source.get("name") or "UNKNOWN"

                    buy_result_text = None
                    if source.get("autobuy", False) and key not in user_buy_attempted[user_id]:
                        bought, buy_info = await try_autobuy_item(source, item, found_perf=found_perf)
                        user_buy_attempted[user_id].add(key)
                        await db_mark_buy_attempted(user_id, key)

                        item_desc = f"{src_name}#{item_id}"
                        if bought:
                            await db_record_autobuy_result(user_id, True, item_desc=item_desc, error_text="")
                            user_runtime_stats[user_id]["autobuy_ok"] += 1
                            user_runtime_stats[user_id]["last_autobuy_ok_ts"] = int(time.time())
                            user_runtime_stats[user_id]["last_autobuy_item"] = item_desc
                            dur_ms = int((time.perf_counter() - found_perf) * 1000)
                            buy_result_text = (
                                f"🛒 <b>Автобай</b> ✅ [{html.escape(src_name)}] "
                                f"item_id=<code>{html.escape(str(item_id))}</code> "
                                f"⏱ <b>{dur_ms}ms</b>\n{html.escape(str(buy_info))}"
                            )
                        else:
                            buy_result_text = (
                                f"🛒 <b>Автобай</b> ❌ [{html.escape(src_name)}] "
                                f"item_id=<code>{html.escape(str(item_id))}</code>\n{html.escape(str(buy_info))}"
                            )

                    user_seen_items[user_id].add(key)
                    seen_batch.append(key)

                    await send_bot_message(chat_id, make_card(item, src_name), parse_mode="HTML", disable_web_page_preview=True)
                    if buy_result_text:
                        await send_bot_message(chat_id, buy_result_text, parse_mode="HTML")

            await db_mark_seen_batch(user_id, seen_batch)
            await asyncio.sleep(await user_hunter_interval(user_id))

        except asyncio.CancelledError:
            break
        except Exception as e:
            if seen_batch:
                try:
                    await db_mark_seen_batch(user_id, seen_batch)
                except Exception:
                    pass
            user_api_errors[user_id] += 1
            log_autobuy(f"HUNTER_EXC user_id={user_id} err='{_safe_compact(str(e),400)}'")
            await asyncio.sleep(max(await user_hunter_interval(user_id), 0.8))


# ====================== HANDLERS ======================
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = message.from_user.id
    await load_user_data(user_id, force=True)

    await send_bot_message(message.chat.id, START_MSG_1, disable_web_page_preview=True)
    allowed = await db_is_allowed(user_id)

    if allowed:
        await send_bot_message(message.chat.id, START_MSG_2, reply_markup=kb_main(user_id), disable_web_page_preview=True)
    else:
        await send_bot_message(message.chat.id, START_MSG_2, disable_web_page_preview=True)
        await show_denied(user_id, message.chat.id)

    user_last_screen_msg_id[user_id] = None
    await safe_delete(message)


@dp.message(Command("health"))
async def health_cmd(message: types.Message):
    user_id = message.from_user.id
    await load_user_data(user_id, force=True)
    allowed = await db_is_allowed(user_id)
    if not allowed and user_id not in OWNER_IDS:
        await show_denied(user_id, message.chat.id)
        return await safe_delete(message)

    await send_screen(message.chat.id, user_id, await build_health_text(user_id), reply_markup=kb_main(user_id), parse_mode="HTML")
    await safe_delete(message)


@dp.message()
async def buttons_handler(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    await load_user_data(user_id)

    text = (message.text or "").strip()
    mode = user_modes[user_id]

    allowed = await db_is_allowed(user_id)
    if not allowed and user_id not in OWNER_IDS:
        if text == "🔓 Запрос на бота":
            now = int(time.time())
            last = await db_get_last_request_ts(user_id)
            if now - last < 60:
                await send_screen(chat_id, user_id, "⏳ Запрос уже отправлен. Подожди немного.", reply_markup=kb_request())
                return await safe_delete(message)

            await db_set_last_request_ts(user_id, now)
            try:
                await send_bot_message(
                    OWNER_ID,
                    f"🔔 <b>Запрос доступа</b>\nПользователь: <code>{user_id}</code>\n\nОткрой 👥 Пользователи и нажми на него, чтобы разрешить.",
                    parse_mode="HTML",
                )
            except Exception:
                pass

            await send_screen(chat_id, user_id, "✅ Запрос отправлен. Жди разрешения.", reply_markup=kb_request())
            return await safe_delete(message)

        await show_denied(user_id, chat_id)
        return await safe_delete(message)

    try:
        if text in ("◀️ Назад страница", "▶️ Далее") and user_page_state[user_id].get("ctx"):
            ctx = user_page_state[user_id]["ctx"]
            page = int(user_page_state[user_id]["page"])

            if ctx == "users_pick":
                total = await db_count_users()
                total_pages = (total + USER_PAGE_SIZE - 1) // USER_PAGE_SIZE if total else 1
                page = max(0, min(page + (-1 if text == "◀️ Назад страница" else 1), total_pages - 1))
                await show_users_screen(user_id, chat_id, page=page)
                return await safe_delete(message)

            sources = await get_all_sources(user_id, enabled_only=False)
            total_pages = (len(sources) + URL_PAGE_SIZE - 1) // URL_PAGE_SIZE if sources else 1
            page = max(0, min(page + (-1 if text == "◀️ Назад страница" else 1), total_pages - 1))
            user_page_state[user_id] = {"ctx": ctx, "page": page}

            if ctx == "pick_list":
                await show_urls_list_screen(user_id, chat_id, page=page)
                return await safe_delete(message)

            kb = build_urls_picker_kb(sources, page=page, back_text="⬅️ Назад")
            title = {
                "pick_autobuy": "🛒 Выбери URL для переключения автобая",
                "pick_toggle": "🔁 Выбери URL для ВКЛ/ВЫКЛ",
                "pick_delete": "🗑 Выбери URL для удаления",
                "pick_rename": "✏️ Выбери URL для переименования",
                "pick_test": "✅ Выбери URL для теста",
            }.get(ctx, "Выбери URL")
            user_modes[user_id] = ctx
            await send_screen(chat_id, user_id, title, reply_markup=kb)
            return await safe_delete(message)

        if mode == "users_pick" and user_id in OWNER_IDS:
            if text == "⬅️ Назад":
                user_modes[user_id] = None
                user_page_state[user_id] = {"ctx": None, "page": 0}
                await send_screen(chat_id, user_id, "🧭 Меню", reply_markup=kb_main(user_id))
                return await safe_delete(message)

            target_uid = parse_user_id_from_button(text)
            if target_uid is None:
                return await safe_delete(message)

            await db_ensure_user(target_uid)
            new_allowed = await db_toggle_allowed(target_uid)

            try:
                if new_allowed:
                    await send_bot_message(target_uid, "✅ Доступ к боту разрешён владельцем.\nНажми /start")
                else:
                    await send_bot_message(target_uid, "⛔️ Доступ к боту отключён владельцем.\nЧтобы запросить снова — нажми /start и кнопку запроса.")
            except Exception:
                pass

            page = int(user_page_state[user_id].get("page", 0))
            await show_users_screen(user_id, chat_id, page=page)
            return await safe_delete(message)

        if mode == "add_url_url":
            user_modes[user_id] = None
            url = normalize_url(text)
            ok, err = validate_market_url(url)
            if not ok:
                await send_screen(chat_id, user_id, err, reply_markup=kb_urls_menu())
                return await safe_delete(message)

            limit = await user_url_limit(user_id)
            if len(await get_all_sources(user_id, enabled_only=False)) >= limit:
                await send_screen(chat_id, user_id, f"❌ Достигнут лимит URL: {limit}", reply_markup=kb_urls_menu())
                return await safe_delete(message)

            _items, api_err = await fetch_with_retry(url, max_retries=2)
            if api_err:
                await send_screen(chat_id, user_id, f"❌ API ошибка: {api_err}", reply_markup=kb_urls_menu())
                return await safe_delete(message)

            user_pending_url[user_id] = url
            user_modes[user_id] = "add_url_name"
            await send_screen(chat_id, user_id, "✏️ Введи название для этого URL:", reply_markup=kb_urls_menu())
            return await safe_delete(message)

        if mode == "add_url_name":
            name = (text or "").strip()
            url = user_pending_url.get(user_id)
            user_pending_url[user_id] = None
            user_modes[user_id] = None

            if not url:
                await send_screen(chat_id, user_id, "⚠️ Не нашёл ожидаемый URL. Нажми ➕ Добавить URL ещё раз.", reply_markup=kb_urls_menu())
                return await safe_delete(message)

            if not name:
                name = f"URL {int(time.time())}"

            await db_add_url(user_id, url, name)
            user_urls[user_id] = await db_get_urls(user_id)
            await send_screen(chat_id, user_id, f"✅ URL добавлен: <b>{html.escape(name)}</b>", reply_markup=kb_urls_menu(), parse_mode="HTML")
            return await safe_delete(message)

        if mode == "rename_url_name":
            new_name = (text or "").strip()
            user_modes[user_id] = None
            url = user_pending_rename_url.get(user_id)
            user_pending_rename_url[user_id] = None

            if not url:
                await send_screen(chat_id, user_id, "⚠️ Не нашёл URL для переименования. Повтори ✏️ Переименовать URL.", reply_markup=kb_urls_menu())
                return await safe_delete(message)
            if not new_name:
                await send_screen(chat_id, user_id, "❌ Название не может быть пустым.", reply_markup=kb_urls_menu())
                return await safe_delete(message)

            await db_set_url_name(user_id, url, new_name)
            user_urls[user_id] = await db_get_urls(user_id)
            await send_screen(chat_id, user_id, f"✅ Переименовано в: <b>{html.escape(new_name)}</b>", reply_markup=kb_urls_menu(), parse_mode="HTML")
            return await safe_delete(message)

        if mode and mode.startswith("pick_"):
            if text == "⬅️ Назад":
                user_modes[user_id] = None
                user_page_state[user_id] = {"ctx": None, "page": 0}
                await send_screen(chat_id, user_id, "📚 Меню URL", reply_markup=kb_urls_menu())
                return await safe_delete(message)

            idx = parse_index_from_button(text)
            if idx is None:
                return await safe_delete(message)

            sources = await get_all_sources(user_id, enabled_only=False)
            src = next((s for s in sources if s["idx"] == idx), None)
            if not src:
                await send_screen(chat_id, user_id, "❌ Не нашёл этот URL. Открой список заново.", reply_markup=kb_urls_menu())
                return await safe_delete(message)

            name = src.get("name") or f"URL #{idx}"

            if mode == "pick_list":
                detail = (
                    f"<b>{html.escape(name)}</b>\n"
                    f"• Статус: {'🟢 ВКЛ' if src.get('enabled', True) else '🔴 ВЫКЛ'}\n"
                    f"• Автобай: {'🛒 ВКЛ' if src.get('autobuy', False) else '— ВЫКЛ'}\n"
                    f"• API URL:\n<code>{html.escape(src['url'])}</code>"
                )
                page = user_page_state[user_id].get("page", 0)
                kb = build_urls_picker_kb(sources, page=page, back_text="⬅️ Назад")
                await send_screen(chat_id, user_id, detail, reply_markup=kb, parse_mode="HTML")
                return await safe_delete(message)

            if mode == "pick_autobuy":
                new_ab = not src.get("autobuy", False)
                await db_set_url_autobuy(user_id, src["url"], new_ab)
                user_urls[user_id] = await db_get_urls(user_id)
                user_modes[user_id] = None
                user_page_state[user_id] = {"ctx": None, "page": 0}
                await send_screen(chat_id, user_id, f"🛒 {html.escape(name)}: {'ВКЛ' if new_ab else 'ВЫКЛ'}", reply_markup=kb_urls_menu())
                return await safe_delete(message)

            if mode == "pick_toggle":
                new_enabled = not src.get("enabled", True)
                await db_set_url_enabled(user_id, src["url"], new_enabled)
                user_urls[user_id] = await db_get_urls(user_id)
                user_modes[user_id] = None
                user_page_state[user_id] = {"ctx": None, "page": 0}
                await send_screen(chat_id, user_id, f"🔁 {html.escape(name)}: {'ВКЛ' if new_enabled else 'ВЫКЛ'}", reply_markup=kb_urls_menu())
                return await safe_delete(message)

            if mode == "pick_delete":
                await db_remove_url(user_id, src["url"])
                user_urls[user_id] = await db_get_urls(user_id)
                user_modes[user_id] = None
                user_page_state[user_id] = {"ctx": None, "page": 0}
                await send_screen(chat_id, user_id, f"🗑 Удалено: <b>{html.escape(name)}</b>", reply_markup=kb_urls_menu(), parse_mode="HTML")
                return await safe_delete(message)

            if mode == "pick_test":
                user_modes[user_id] = None
                user_page_state[user_id] = {"ctx": None, "page": 0}
                await send_test_for_single_url(user_id, chat_id, src)
                return await safe_delete(message)

            if mode == "pick_rename":
                user_pending_rename_url[user_id] = src["url"]
                user_modes[user_id] = "rename_url_name"
                user_page_state[user_id] = {"ctx": None, "page": 0}
                await send_screen(chat_id, user_id, f"✏️ Новое название для <b>{html.escape(name)}</b>:", reply_markup=kb_urls_menu(), parse_mode="HTML")
                return await safe_delete(message)

        if text == "👥 Пользователи" and user_id in OWNER_IDS:
            await show_users_screen(user_id, chat_id, page=0)
            return await safe_delete(message)

        if text == "ℹ️ Инфо":
            await send_screen(
                chat_id,
                user_id,
                "ℹ️ Управление только нижними кнопками.\n"
                "📚 Мои URL → управление источниками.\n"
                "🚀 Старт охотника → уведомления о новых лотах.\n"
                "🛒 Автобай включается по конкретному URL.\n"
                "🧪 Диагностика или /health → быстрый техстатус.\n\n"
                f"🧾 Лог автобая: {AUTOBUY_LOG_FILE}",
                reply_markup=kb_main(user_id),
            )
            return await safe_delete(message)

        if text == "🧪 Диагностика":
            await send_screen(chat_id, user_id, await build_health_text(user_id), reply_markup=kb_main(user_id), parse_mode="HTML")
            return await safe_delete(message)

        if text == "📊 Статус":
            await show_status(user_id, chat_id)
            return await safe_delete(message)

        if text == "✨ Проверка лотов":
            await send_compact_10_for_user(user_id, chat_id)
            return await safe_delete(message)

        if text == "♻️ Сбросить историю":
            user_seen_items[user_id].clear()
            user_buy_attempted[user_id].clear()
            await db_clear_seen(user_id)
            await db_clear_buy_attempted(user_id)
            await send_screen(chat_id, user_id, "♻️ История сброшена. Теперь лоты снова считаются новыми.", reply_markup=kb_main(user_id))
            return await safe_delete(message)

        if text == "🚀 Старт охотника":
            lock = get_user_hunter_start_lock(user_id)
            async with lock:
                active_sources = await get_all_sources(user_id, enabled_only=True)
                if not active_sources:
                    await send_screen(chat_id, user_id, "❌ Нет активных URL. Зайди в 📚 Мои URL и добавь источник.", reply_markup=kb_main(user_id))
                    return await safe_delete(message)

                task = user_hunter_tasks.get(user_id)
                if task and not task.done():
                    await send_screen(chat_id, user_id, "⚠️ Охотник уже запущен.", reply_markup=kb_main(user_id))
                    return await safe_delete(message)

                user_search_active[user_id] = True
                user_seen_items[user_id] = await db_load_seen(user_id)
                user_buy_attempted[user_id] = await db_load_buy_attempted(user_id)

                if not user_seen_items[user_id]:
                    await seed_existing_without_notifications(user_id)

                task = asyncio.create_task(hunter_loop_for_user(user_id, chat_id))
                user_hunter_tasks[user_id] = task

                log_autobuy(f"HUNTER_START user_id={user_id} active_urls={len(active_sources)}")
                await send_screen(chat_id, user_id, f"🚀 Охотник запущен! Активных URL: {len(active_sources)}", reply_markup=kb_main(user_id))
                return await safe_delete(message)

        if text == "🛑 Стоп охотника":
            user_search_active[user_id] = False
            task = user_hunter_tasks.get(user_id)
            if task:
                task.cancel()
                user_hunter_tasks.pop(user_id, None)
            log_autobuy(f"HUNTER_STOP user_id={user_id}")
            await send_screen(chat_id, user_id, "🛑 Охотник остановлен.", reply_markup=kb_main(user_id))
            return await safe_delete(message)

        if text == "📚 Мои URL":
            user_modes[user_id] = None
            user_page_state[user_id] = {"ctx": None, "page": 0}
            await send_screen(chat_id, user_id, "📚 Меню URL", reply_markup=kb_urls_menu())
            return await safe_delete(message)

        if text == "⬅️ Назад":
            user_modes[user_id] = None
            user_page_state[user_id] = {"ctx": None, "page": 0}
            await send_screen(chat_id, user_id, "🧭 Меню", reply_markup=kb_main(user_id))
            return await safe_delete(message)

        if text == "📄 Список URL":
            await show_urls_list_screen(user_id, chat_id, page=0)
            return await safe_delete(message)

        if text == "➕ Добавить URL":
            user_modes[user_id] = "add_url_url"
            user_page_state[user_id] = {"ctx": None, "page": 0}
            await send_screen(chat_id, user_id, "Вставь API URL (prod-api.lzt.market / api.lzt.market / api.lolz.live):", reply_markup=kb_urls_menu())
            return await safe_delete(message)

        if text == "🛒 Автобай URL":
            sources = await get_all_sources(user_id, enabled_only=False)
            if not sources:
                await send_screen(chat_id, user_id, "URL пуст. Добавь источник.", reply_markup=kb_urls_menu())
                return await safe_delete(message)
            user_modes[user_id] = "pick_autobuy"
            user_page_state[user_id] = {"ctx": "pick_autobuy", "page": 0}
            await send_screen(chat_id, user_id, "🛒 Выбери URL для переключения автобая:", reply_markup=build_urls_picker_kb(sources, page=0, back_text="⬅️ Назад"))
            return await safe_delete(message)

        if text == "✏️ Переименовать URL":
            sources = await get_all_sources(user_id, enabled_only=False)
            if not sources:
                await send_screen(chat_id, user_id, "URL пуст. Добавь источник.", reply_markup=kb_urls_menu())
                return await safe_delete(message)
            user_modes[user_id] = "pick_rename"
            user_page_state[user_id] = {"ctx": "pick_rename", "page": 0}
            await send_screen(chat_id, user_id, "✏️ Выбери URL для переименования:", reply_markup=build_urls_picker_kb(sources, page=0, back_text="⬅️ Назад"))
            return await safe_delete(message)

        if text == "🗑 Удалить URL":
            sources = await get_all_sources(user_id, enabled_only=False)
            if not sources:
                await send_screen(chat_id, user_id, "URL пуст. Добавь источник.", reply_markup=kb_urls_menu())
                return await safe_delete(message)
            user_modes[user_id] = "pick_delete"
            user_page_state[user_id] = {"ctx": "pick_delete", "page": 0}
            await send_screen(chat_id, user_id, "🗑 Выбери URL для удаления:", reply_markup=build_urls_picker_kb(sources, page=0, back_text="⬅️ Назад"))
            return await safe_delete(message)

        if text == "🔁 Вкл/Выкл URL":
            sources = await get_all_sources(user_id, enabled_only=False)
            if not sources:
                await send_screen(chat_id, user_id, "URL пуст. Добавь источник.", reply_markup=kb_urls_menu())
                return await safe_delete(message)
            user_modes[user_id] = "pick_toggle"
            user_page_state[user_id] = {"ctx": "pick_toggle", "page": 0}
            await send_screen(chat_id, user_id, "🔁 Выбери URL для ВКЛ/ВЫКЛ:", reply_markup=build_urls_picker_kb(sources, page=0, back_text="⬅️ Назад"))
            return await safe_delete(message)

        if text == "✅ Тест URL":
            sources = await get_all_sources(user_id, enabled_only=False)
            if not sources:
                await send_screen(chat_id, user_id, "URL пуст. Добавь источник.", reply_markup=kb_urls_menu())
                return await safe_delete(message)
            user_modes[user_id] = "pick_test"
            user_page_state[user_id] = {"ctx": "pick_test", "page": 0}
            await send_screen(chat_id, user_id, "✅ Выбери URL для теста:", reply_markup=build_urls_picker_kb(sources, page=0, back_text="⬅️ Назад"))
            return await safe_delete(message)

        if text and not text.startswith("/"):
            await asyncio.sleep(0.12)
            await safe_delete(message)

    except Exception as e:
        try:
            await send_bot_message(chat_id, f"❌ Ошибка: {html.escape(str(e))}", parse_mode="HTML")
        except Exception:
            pass
        await safe_delete(message)


# ====================== RUN ======================
async def main():
    global bot
    print("[BOT] Start: fixed hunter + rate-limit safe send + db lock + autobuy retries")

    if not has_valid_telegram_token(API_TOKEN):
        raise RuntimeError("Некорректный API_TOKEN: бот не может быть запущен")

    bot = Bot(token=API_TOKEN)

    await init_db()
    restored = await restore_from_backup_if_needed()
    if restored:
        log_autobuy("BACKUP_RESTORE_OK")
    await write_backup_snapshot()

    asyncio.create_task(error_reporter_loop())
    asyncio.create_task(cleanup_loop())
    asyncio.create_task(backup_loop())

    try:
        await dp.start_polling(bot)
    finally:
        try:
            await write_backup_snapshot()
        except Exception:
            pass
        await close_session()
        await db_close()
        if bot is not None and getattr(bot, "session", None) is not None and not bot.session.closed:
            await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
