
import asyncio
import json
import aiohttp
import aiosqlite
import html
import re
import time
import random
import os
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from collections import defaultdict

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest, TelegramForbiddenError

from config import API_TOKEN as _API_TOKEN, LZT_API_KEY as _LZT_API_KEY

# ====================== ENV ======================
API_TOKEN = os.getenv("API_TOKEN") or _API_TOKEN
LZT_API_KEY = os.getenv("LZT_API_KEY") or _LZT_API_KEY
LZT_BALANCE_ID = int((os.getenv("LZT_BALANCE_ID") or "20212").strip())

bot: Bot | None = None
dp = Dispatcher()

# balance cache
user_balance_cache = defaultdict(lambda: {"text": "—", "ts": 0})
BALANCE_CACHE_TTL = 60

# ====================== OWNER / ACCESS ======================
OWNER_ID = 1377985336
OWNER_IDS = {OWNER_ID}

# ====================== НАСТРОЙКИ ======================
HUNTER_INTERVAL_BASE = float((os.getenv("HUNTER_INTERVAL_BASE") or "0.02").strip())
FETCH_TIMEOUT = float((os.getenv("FETCH_TIMEOUT") or "0.70").strip())
BUY_TIMEOUT = float((os.getenv("BUY_TIMEOUT") or "0.32").strip())
RETRY_MAX = int((os.getenv("RETRY_MAX") or "1").strip())
RETRY_BASE_DELAY = float((os.getenv("RETRY_BASE_DELAY") or "0.01").strip())

SHORT_CARD_MAX = 3200
ERROR_REPORT_INTERVAL = 3600

MAX_URLS_PER_USER_DEFAULT = 50
MAX_URLS_PER_USER_LIMITED = 3

MAX_CONCURRENT_REQUESTS = int((os.getenv("MAX_CONCURRENT_REQUESTS") or "512").strip())
LIMITED_EXTRA_DELAY = 0.0
MAX_NEW_ITEMS_PER_CYCLE = int((os.getenv("MAX_NEW_ITEMS_PER_CYCLE") or "1000").strip())
SEARCH_MIN_REQUEST_INTERVAL = float((os.getenv("SEARCH_MIN_REQUEST_INTERVAL") or "0.0").strip())
OTHER_MIN_REQUEST_INTERVAL = float((os.getenv("OTHER_MIN_REQUEST_INTERVAL") or "0.0").strip())
BUY_MIN_REQUEST_INTERVAL = float((os.getenv("BUY_MIN_REQUEST_INTERVAL") or "0.0").strip())
NON_AUTOBUY_CYCLE_EVERY = int((os.getenv("NON_AUTOBUY_CYCLE_EVERY") or "5").strip())

DB_FILE = (os.getenv("DB_FILE") or ("/data/bot_data.sqlite" if os.path.isdir("/data") else "bot_data.sqlite")).strip()

LZT_SECRET_WORD = (os.getenv("LZT_SECRET_WORD") or "Мазда").strip()
SEED_URLS_JSON = (os.getenv("SEED_URLS_JSON") or "").strip()

URL_PAGE_SIZE = 12
USER_PAGE_SIZE = 14
MAX_URL_NAME_LEN = 64

TG_SEND_DELAY = float((os.getenv("TG_SEND_DELAY") or "0.01").strip())
AUTOBUY_RETRY_ATTEMPTS = int((os.getenv("AUTOBUY_RETRY_ATTEMPTS") or "2").strip())
AUTOBUY_RETRY_MIN_DELAY = float((os.getenv("AUTOBUY_RETRY_MIN_DELAY") or "0.0").strip())
AUTOBUY_RETRY_MAX_DELAY = float((os.getenv("AUTOBUY_RETRY_MAX_DELAY") or "0.0").strip())
AUTOBUY_QUEUE_RETRY_MIN_DELAY = float((os.getenv("AUTOBUY_QUEUE_RETRY_MIN_DELAY") or "0.0").strip())
AUTOBUY_QUEUE_RETRY_MAX_DELAY = float((os.getenv("AUTOBUY_QUEUE_RETRY_MAX_DELAY") or "0.0").strip())
FAST_AUTOBUY_TIMEOUT = float((os.getenv("FAST_AUTOBUY_TIMEOUT") or "0.18").strip())
AUTOBUY_URL_LIMIT = int((os.getenv("AUTOBUY_URL_LIMIT") or "10").strip())
AUTOBUY_MAX_HTTP_ATTEMPTS = int((os.getenv("AUTOBUY_MAX_HTTP_ATTEMPTS") or "10").strip())
AUTOBUY_MAX_DURATION_SEC = float((os.getenv("AUTOBUY_MAX_DURATION_SEC") or "1.60").strip())
MAX_ITEMS_PER_SOURCE_SCAN = int((os.getenv("MAX_ITEMS_PER_SOURCE_SCAN") or "200").strip())

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
    "🧭 Главное меню\n\n"
    "• ✨ Проверка лотов — быстрый просмотр до 10 свежих карточек\n"
    "• 📚 Мои URL — управление источниками, тестом и автобаем\n"
    "• 📊 Статус — сводка по работе, балансу и ошибкам API\n"
    "• 🚀 Старт охотника — непрерывный мониторинг новых лотов\n"
    "• ♻️ Сбросить историю — считать все лоты снова новыми"
)

WELCOME_STICKERS = [
    "CAACAgIAAxkBAAIBQmYkJ4hB5lL0QwABJvY5S4UuTxR1xAACZQADwDZPE9xKkS4L5N5eNgQ",
    "CAACAgIAAxkBAAIBQ2YkJ5ILV0M5mD9Vpq3nP8a3m2qvAALgAAPANk8Tq5Y-X_7h3xQ2BA",
    "CAACAgIAAxkBAAIBRGYkJ53xVv9wNR8d3lNn2s9y4C9fAALiAAPANk8TG8rJkYdM3MM2BA",
]

DENIED_TEXT = (
    "⛔️ Доступ к боту закрыт по умолчанию.\n\n"
    "Нажми кнопку ниже, чтобы отправить запрос владельцу."
)


# ====================== UI: KEYBOARDS ======================
def kb_button(text: str, style: str | None = None) -> KeyboardButton:
    if style:
        try:
            return KeyboardButton(text=text, style=style)
        except Exception:
            pass
    return KeyboardButton(text=text)


def kb_request() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[kb_button("🔓 Запрос на бота", "primary")]],
        resize_keyboard=True,
    )


def kb_main(user_id: int) -> ReplyKeyboardMarkup:
    rows = [
        [kb_button("🚀 Старт охотника", "success"), kb_button("🛑 Стоп охотника")],
        [kb_button("✨ Проверка лотов", "primary"), kb_button("📊 Статус")],
        [kb_button("📚 Мои URL", "primary"), kb_button("♻️ Сбросить историю")],
        [kb_button("ℹ️ Инфо")],
    ]
    if user_id in OWNER_IDS:
        rows.insert(4, [kb_button("👥 Пользователи", "primary")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def kb_urls_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [kb_button("➕ Добавить URL", "success"), kb_button("📄 Список URL")],
            [kb_button("🔁 Вкл/Выкл URL"), kb_button("🛒 Автобай URL", "primary")],
            [kb_button("✏️ Переименовать URL"), kb_button("🗑 Удалить URL", "danger")],
            [kb_button("✅ Тест URL"), kb_button("⬅️ Назад")],
        ],
        resize_keyboard=True,
    )




def _to_bool_label(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "Да" if value else "Нет"
    low = str(value).strip().lower()
    if low in {"1", "true", "yes", "on", "enabled", "да"}:
        return "Да"
    if low in {"0", "false", "no", "off", "disabled", "нет"}:
        return "Нет"
    return None


def _format_value(v, limit: int = 140) -> str:
    if v is None:
        return "—"
    if isinstance(v, (int, float)):
        if isinstance(v, float) and not v.is_integer():
            return f"{v:.2f}".rstrip("0").rstrip(".")
        return f"{int(v):,}".replace(",", " ")
    s = str(v).strip()
    s = re.sub(r"\s+", " ", s)
    if len(s) > limit:
        return s[: limit - 1] + "…"
    return s


def sanitize_url_name(raw_name: str | None, fallback: str | None = None) -> str:
    name = (raw_name or "").strip()
    name = re.sub(r"[\x00-\x1f\x7f]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip()

    if not name:
        name = (fallback or "").strip()
    if not name:
        name = f"URL {int(time.time())}"

    if len(name) > MAX_URL_NAME_LEN:
        name = name[:MAX_URL_NAME_LEN].rstrip()

    return name


def _pick_first(item: dict, keys: list[str]):
    for k in keys:
        if k in item and item.get(k) not in (None, ""):
            return item.get(k)
    return None


def _collect_item_specs(item: dict) -> list[str]:
    known_specs = [
        ("🏆 Трофеи", ["trophies", "cups", "brawl_cup", "clash_cup", "rating"]),
        ("🔼 Уровень", ["level", "lvl", "user_level", "genshin_level"]),
        ("🏰 TownHall", ["townhall", "th"]),
        ("🧩 Ранг", ["rank", "elo", "mmr"]),
        ("🎖 Прайм", ["prime", "premium", "vip"]),
        ("📱 Привязка телефона", ["phone_bound", "phone"]),
        ("📧 Привязка почты", ["email_bound", "email"]),
        ("📨 Доступ к почте", ["mail_access", "email_access"]),
        ("🔐 2FA", ["twofa", "2fa", "ga", "guard"]),
        ("🌍 Регион", ["region", "country", "locale", "server"]),
        ("🧭 Платформа", ["platform", "device", "os"]),
        ("🧱 Инвентарь", ["inventory", "inv_value", "skin_count", "items_count"]),
    ]

    specs: list[str] = []
    used: set[str] = set()
    for label, keys in known_specs:
        raw = _pick_first(item, keys)
        if raw is None:
            continue
        for k in keys:
            if k in item:
                used.add(k)

        bool_label = _to_bool_label(raw)
        value = bool_label if bool_label is not None else _format_value(raw)
        specs.append(f"• {label}: <b>{html.escape(value)}</b>")

    ignored = {
        "title", "price", "old_price", "discount", "item_id", "id", "url", "link", "description", "desc",
        "category", "category_name", "game", "type", "seller_id", "owner_id", "user_id", "views", "view_count",
        "likes", "favorites", "fav_count", "published_at", "created_at", "date", "time", "updated_at", "edited_at",
    }
    extras_added = 0
    for k, v in item.items():
        if extras_added >= 8:
            break
        if k in ignored or k in used:
            continue
        if v in (None, "", [], {}):
            continue
        if isinstance(v, (dict, list, tuple, set)):
            continue
        human_key = k.replace("_", " ").strip().title()
        specs.append(f"• {html.escape(human_key)}: <b>{html.escape(_format_value(v, limit=90))}</b>")
        extras_added += 1
    return specs


# ====================== HELPERS ======================
def has_valid_telegram_token(token: str) -> bool:
    if not token:
        return False
    return bool(re.match(r"^\d{6,12}:[A-Za-z0-9_-]{20,}$", token))


async def send_welcome_sticker(chat_id: int):
    if bot is None:
        return
    for st in WELCOME_STICKERS:
        try:
            await bot.send_sticker(chat_id, st)
            return
        except Exception:
            continue


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
                await asyncio.sleep(0.01)


def _get_notify_queue(user_id: int) -> asyncio.Queue:
    q = user_notify_queues.get(user_id)
    if q is None:
        q = asyncio.Queue(maxsize=1500)
        user_notify_queues[user_id] = q
    return q


async def _notify_worker_loop(user_id: int):
    q = _get_notify_queue(user_id)
    while user_search_active[user_id] or not q.empty():
        try:
            chat_id, text, kwargs = await asyncio.wait_for(q.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        try:
            await send_bot_message(chat_id, text, **kwargs)
        except Exception as e:
            log_autobuy(f"NOTIFY_SEND_ERR user_id={user_id} err='{_safe_compact(str(e),240)}'")
        finally:
            q.task_done()


def ensure_notify_worker(user_id: int):
    task = user_notify_workers.get(user_id)
    if task and not task.done():
        return
    user_notify_workers[user_id] = asyncio.create_task(_notify_worker_loop(user_id))


def enqueue_hunter_notification(user_id: int, chat_id: int, text: str, **kwargs):
    q = _get_notify_queue(user_id)
    payload = (chat_id, text, kwargs)
    try:
        q.put_nowait(payload)
        return
    except asyncio.QueueFull:
        pass

    dropped = 0
    while q.full() and not q.empty() and dropped < 200:
        try:
            q.get_nowait()
            q.task_done()
            dropped += 1
        except Exception:
            break
    try:
        q.put_nowait(payload)
    except Exception:
        pass
    if dropped:
        log_autobuy(f"NOTIFY_QUEUE_DROP user_id={user_id} dropped={dropped}")


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
user_hunter_mode = defaultdict(lambda: "off")  # off/classic
user_seen_items = defaultdict(set)
user_buy_attempted = defaultdict(set)
user_buy_inflight = defaultdict(set)
user_hunter_tasks: dict[int, asyncio.Task] = {}
user_hunter_start_locks: dict[int, asyncio.Lock] = {}
user_history_reset_pending = defaultdict(lambda: False)

user_notify_queues: dict[int, asyncio.Queue] = {}
user_notify_workers: dict[int, asyncio.Task] = {}

user_modes = defaultdict(lambda: None)
user_started = set()
user_urls = defaultdict(list)
user_api_errors = defaultdict(int)
user_roles = defaultdict(lambda: "unknown")

user_last_screen_msg_id = defaultdict(lambda: None)
user_no_lots_msg_id = defaultdict(lambda: None)
user_pending_url = defaultdict(lambda: None)
user_pending_rename_url = defaultdict(lambda: None)
user_page_state = defaultdict(lambda: {"ctx": None, "page": 0})

autobuy_endpoint_cache: dict[str, list[str]] = {}
buy_locks: dict[str, asyncio.Lock] = {}
buy_semaphore = asyncio.Semaphore(int((os.getenv("BUY_SEMAPHORE") or "128").strip()))


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


async def upsert_no_lots_message(chat_id: int, user_id: int, text: str):
    if bot is None:
        return

    mid = user_no_lots_msg_id.get(user_id)
    if mid:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=mid,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            return
        except TelegramBadRequest:
            pass
        except Exception:
            pass

    try:
        msg = await send_bot_message(chat_id, text, parse_mode="HTML", disable_web_page_preview=True)
        user_no_lots_msg_id[user_id] = msg.message_id
    except Exception:
        pass


def reset_no_lots_message(user_id: int):
    user_no_lots_msg_id[user_id] = None


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

    ucols = [row[1] for row in await db_fetchall("PRAGMA table_info(users)")]
    if "allowed" not in ucols:
        await db_execute("ALTER TABLE users ADD COLUMN allowed INTEGER DEFAULT 0", commit=True)
    if "last_request_ts" not in ucols:
        await db_execute("ALTER TABLE users ADD COLUMN last_request_ts INTEGER DEFAULT 0", commit=True)
    if "last_error_report" not in ucols:
        await db_execute("ALTER TABLE users ADD COLUMN last_error_report INTEGER DEFAULT 0", commit=True)

    await db_execute(
        "CREATE INDEX IF NOT EXISTS idx_urls_user_added ON urls(user_id, added_at, url)",
        commit=True,
    )


async def db_ensure_user(user_id: int):
    await db_execute(
        "INSERT OR IGNORE INTO users(user_id, role, allowed, last_error_report, last_request_ts) VALUES (?, ?, ?, ?, ?)",
        (user_id, "unknown", 1 if user_id in OWNER_IDS else 0, 0, 0),
        commit=True,
    )
    if user_id in OWNER_IDS:
        await db_execute("UPDATE users SET allowed=1 WHERE user_id=?", (user_id,), commit=True)


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


def _load_seed_urls() -> list[tuple[str, str]]:
    if not SEED_URLS_JSON:
        return []

    out: list[tuple[str, str]] = []
    try:
        data = json.loads(SEED_URLS_JSON)
    except Exception:
        return out

    if not isinstance(data, list):
        return out

    for i, row in enumerate(data, start=1):
        if isinstance(row, dict):
            raw_url = str(row.get("url") or "").strip()
            raw_name = str(row.get("name") or f"SEED #{i}").strip()
        else:
            raw_url = str(row or "").strip()
            raw_name = f"SEED #{i}"

        if not raw_url:
            continue
        normalized = normalize_url(raw_url)
        ok, _ = validate_market_url(normalized)
        if not ok:
            continue
        out.append((normalized, raw_name))
    return out


async def db_seed_urls_if_empty(user_id: int):
    existing = await db_get_urls(user_id)
    if existing:
        return
    for url, name in _load_seed_urls():
        await db_add_url(user_id, url, name)


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


async def db_mark_buy_attempted_batch(user_id: int, keys: list[str]):
    if not keys:
        return
    ts = int(time.time())
    rows = [(user_id, k, ts) for k in keys]
    await db_executemany("INSERT OR IGNORE INTO buy_attempted(user_id, item_key, attempted_at) VALUES (?, ?, ?)", rows, commit=True)


async def db_load_buy_attempted(user_id: int):
    rows = await db_fetchall("SELECT item_key FROM buy_attempted WHERE user_id=?", (user_id,))
    return {r[0] for r in rows}


async def db_clear_buy_attempted(user_id: int):
    await db_execute("DELETE FROM buy_attempted WHERE user_id=?", (user_id,), commit=True)


# ====================== LOAD USER DATA ======================
async def load_user_data(user_id: int, force: bool = False):
    if user_id in user_started and not force:
        return
    await db_ensure_user(user_id)
    await db_seed_urls_if_empty(user_id)
    user_urls[user_id] = await db_get_urls(user_id)
    user_seen_items[user_id] = await db_load_seen(user_id)
    user_buy_attempted[user_id] = await db_load_buy_attempted(user_id)
    user_roles[user_id] = await db_get_role(user_id)
    user_started.add(user_id)


async def get_user_role(user_id: int) -> str | None:
    await load_user_data(user_id)
    role = user_roles.get(user_id, "unknown")
    return None if role == "unknown" else role


async def user_url_limit(user_id: int) -> int:
    role = await get_user_role(user_id)
    return MAX_URLS_PER_USER_LIMITED if role == "limited" else MAX_URLS_PER_USER_DEFAULT


async def user_hunter_interval(user_id: int) -> float:
    role = await get_user_role(user_id)
    extra = LIMITED_EXTRA_DELAY if role == "limited" else 0.0
    return HUNTER_INTERVAL_BASE + extra


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

    try:
        query_pairs = parse_qsl(query, keep_blank_values=True)
        qmap = {k: v for k, v in query_pairs}
        if "order_by" not in qmap or not str(qmap.get("order_by", "")).strip():
            query_pairs = [(k, v) for k, v in query_pairs if k != "order_by"]
            query_pairs.append(("order_by", "pdate_to_down_upload"))
            query = urlencode(query_pairs)
    except Exception:
        pass

    return urlunsplit((scheme, netloc, path, query, ""))


def _item_sort_key(item: dict) -> tuple[int, int]:
    published_at = item.get("published_at") or item.get("created_at") or item.get("date") or item.get("time")
    try:
        ts = int(float(published_at))
    except Exception:
        ts = 0

    item_id = item.get("item_id") or item.get("id")
    try:
        iid = int(item_id)
    except Exception:
        iid = 0

    return ts, iid


# ====================== HTTP / API ======================
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
_global_session: aiohttp.ClientSession | None = None


class RequestRateLimiter:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._next_allowed_at: dict[str, float] = {}

    async def wait(self, bucket: str, min_interval: float):
        if min_interval <= 0:
            return

        while True:
            async with self._lock:
                now = time.monotonic()
                allowed_at = self._next_allowed_at.get(bucket, 0.0)
                wait_for = allowed_at - now
                if wait_for <= 0:
                    self._next_allowed_at[bucket] = now + min_interval
                    return
            await asyncio.sleep(min(wait_for, min_interval))


request_rate_limiter = RequestRateLimiter()


def _is_search_endpoint(url: str) -> bool:
    try:
        path = (urlsplit(url).path or "").strip().lower()
    except Exception:
        return False

    if path.startswith("/category/"):
        return True
    if path in ("/steam", "/fortnite", "/valorant", "/mihoyo", "/epicgames"):
        return True
    return False


def _is_buy_endpoint(url: str) -> bool:
    try:
        path = (urlsplit(url).path or "").strip().lower()
    except Exception:
        return False
    return "buy" in path


def _api_limit_bucket(method: str, url: str) -> tuple[str, float]:
    if method.upper() == "POST" and _is_buy_endpoint(url):
        return "buy-global", BUY_MIN_REQUEST_INTERVAL
    if method.upper() == "GET" and _is_search_endpoint(url):
        return "search-global", SEARCH_MIN_REQUEST_INTERVAL
    return "other-global", OTHER_MIN_REQUEST_INTERVAL


def _default_api_headers() -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; ParsingBot/1.0; +https://api.lzt.market/)",
        "Referer": "https://zelenka.guru/",
    }
    if LZT_API_KEY:
        headers["Authorization"] = f"Bearer {LZT_API_KEY}"
    return headers


async def get_session():
    global _global_session
    if _global_session is None or _global_session.closed:
        timeout = aiohttp.ClientTimeout(total=FETCH_TIMEOUT, connect=3, sock_connect=3, sock_read=FETCH_TIMEOUT)
        connector = aiohttp.TCPConnector(limit=256, limit_per_host=128, ttl_dns_cache=300, enable_cleanup_closed=True)
        _global_session = aiohttp.ClientSession(timeout=timeout, connector=connector)
    return _global_session


async def close_session():
    global _global_session
    if _global_session:
        await _global_session.close()
        _global_session = None


async def fetch_items_raw(url: str):
    bucket, min_interval = _api_limit_bucket("GET", url)
    await request_rate_limiter.wait(bucket, min_interval)
    headers = _default_api_headers()
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




def _format_money(v) -> str:
    try:
        return f"{float(v):,.2f}".replace(",", " ").replace(".00", "")
    except Exception:
        try:
            return f"{int(v):,}".replace(",", " ")
        except Exception:
            return str(v)


def _extract_account_buy_balance_text(data) -> str | None:
    candidates = []

    def walk(obj):
        if isinstance(obj, dict):
            title = str(
                obj.get("title")
                or obj.get("name")
                or obj.get("label")
                or obj.get("description")
                or ""
            ).strip()
            oid = obj.get("id")
            amount = obj.get("amount")
            balance = obj.get("balance")
            value = amount if amount is not None else balance
            if title:
                candidates.append((title, oid, value))
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for x in obj:
                walk(x)

    walk(data)

    for title, oid, value in candidates:
        low = title.lower()
        if "баланс для покупки аккаунтов" in low or "buy account" in low or "purchase account" in low:
            parts = [title]
            if oid is not None:
                parts.append(f"ID {oid}")
            if value is not None:
                parts.append(f"{_format_money(value)} ₽")
            return " • ".join(parts)

    for title, oid, value in candidates:
        if oid == LZT_BALANCE_ID:
            parts = [title]
            if oid is not None:
                parts.append(f"ID {oid}")
            if value is not None:
                parts.append(f"{_format_money(value)} ₽")
            return " • ".join(parts)

    return None


async def get_account_buy_balance_text(force: bool = False) -> str:
    cache = user_balance_cache[0]
    now = time.time()
    if not force and cache["text"] != "—" and now - cache["ts"] < BALANCE_CACHE_TTL:
        return cache["text"]

    if not LZT_API_KEY:
        return "—"

    headers = _default_api_headers()
    urls = [
        "https://prod-api.lzt.market/balance/exchange",
        "https://api.lzt.market/balance/exchange",
    ]

    session = await get_session()
    for url in urls:
        try:
            async with session.get(url, headers=headers, timeout=FETCH_TIMEOUT) as resp:
                text = await resp.text()
                if resp.status != 200:
                    continue
                try:
                    data = json.loads(text)
                except Exception:
                    continue
                parsed = _extract_account_buy_balance_text(data)
                if parsed:
                    user_balance_cache[0] = {"text": parsed, "ts": now}
                    return parsed
        except Exception:
            continue

    return cache["text"] if cache["text"] != "—" else "—"

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


def _build_source_info(src: dict) -> dict:
    url = src["url"]
    return {
        "idx": src["idx"],
        "url": url,
        "name": src.get("name") or f"URL #{src['idx']}",
        "enabled": src.get("enabled", True),
        "autobuy": src.get("autobuy", False),
    }


async def _fetch_source_items(src: dict):
    source_info = _build_source_info(src)
    items, err = await fetch_with_retry(source_info["url"])
    return source_info, items, err


async def fetch_all_sources(user_id: int):
    sources = await get_all_sources(user_id, enabled_only=True)
    if not sources:
        return [], []

    # Для минимальной задержки автобая сначала запускаем опрос URL с включённым автобаем.
    sources.sort(key=lambda s: (not bool(s.get("autobuy", False)), s.get("idx", 0)))

    tasks = [asyncio.create_task(_fetch_source_items(s)) for s in sources]
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

    # Для минимальной задержки автобая сначала запускаем опрос URL с включённым автобаем.
    sources.sort(key=lambda s: (not bool(s.get("autobuy", False)), s.get("idx", 0)))

    tasks = [asyncio.create_task(_fetch_source_items(s)) for s in sources]
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


async def iter_sources_results_split(user_id: int, include_non_autobuy: bool):
    sources = await get_all_sources(user_id, enabled_only=True)
    if not sources:
        return

    autobuy_sources = [s for s in sources if s.get("autobuy", False)]
    plain_sources = [s for s in sources if not s.get("autobuy", False)]

    if not autobuy_sources and not (include_non_autobuy and plain_sources):
        return

    async def _run_group(group_sources: list[dict]):
        if not group_sources:
            return

        tasks = [asyncio.create_task(_fetch_source_items(s)) for s in group_sources]
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

    # В первую очередь опрашиваем URL с автобаем, чтобы не ставить покупку
    # в очередь за обычными источниками на глобальном rate-limit bucket.
    async for result in _run_group(autobuy_sources):
        yield result

    if include_non_autobuy:
        async for result in _run_group(plain_sources):
            yield result


# ====================== DISPLAY ======================
def make_card(item: dict, source_name: str) -> str:
    title = str(item.get("title", "Без названия"))
    price = item.get("price", None)
    old_price = item.get("old_price") or item.get("original_price")
    discount = item.get("discount")
    item_id = item.get("item_id") or item.get("id")

    seller_id = item.get("seller_id") or item.get("owner_id") or item.get("user_id")
    category = item.get("category") or item.get("category_name") or item.get("game") or item.get("type")
    published_at = item.get("published_at") or item.get("created_at") or item.get("date") or item.get("time")
    updated_at = item.get("updated_at") or item.get("edited_at")
    views = item.get("views") or item.get("view_count")
    likes = item.get("likes") or item.get("favorites") or item.get("fav_count")

    desc = item.get("description") or item.get("desc") or ""
    if isinstance(desc, str):
        desc = html.unescape(desc).strip()
    else:
        desc = ""

    direct_url = item.get("url") or item.get("link") or None

    def _fmt_time(x):
        try:
            if isinstance(x, (int, float)) and x > 0:
                return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(x)))
        except Exception:
            pass
        return str(x) if x is not None else None

    link = direct_url or (f"https://lzt.market/{item_id}" if item_id is not None else None)

    lines = []
    lines.append("╔══════ 🎐 Карточка лота 🎐 ══════╗")
    lines.append(f"🎯 <b>{html.escape(title)}</b>")
    lines.append(f"📦 Источник: <b>{html.escape(str(source_name or 'Источник'))}</b>")

    pricing = []
    if price is not None and price != "—":
        pricing.append(f"💰 Цена: <b>{html.escape(_format_value(price))} ₽</b>")
    if old_price not in (None, ""):
        pricing.append(f"🏷 Старая цена: <b>{html.escape(_format_value(old_price))} ₽</b>")
    if discount not in (None, ""):
        pricing.append(f"📉 Скидка: <b>{html.escape(_format_value(discount))}</b>")
    if pricing:
        lines.extend(pricing)

    main_meta = []
    if category:
        main_meta.append(f"🎮 Категория: <b>{html.escape(str(category))}</b>")
    if item_id is not None:
        main_meta.append(f"🆔 Лот: <code>{html.escape(str(item_id))}</code>")
    if seller_id is not None:
        main_meta.append(f"👤 Продавец: <code>{html.escape(str(seller_id))}</code>")
    if views is not None:
        main_meta.append(f"👁 Просмотры: <b>{html.escape(_format_value(views))}</b>")
    if likes is not None:
        main_meta.append(f"⭐ Избранное: <b>{html.escape(_format_value(likes))}</b>")
    lines.extend(main_meta)

    timing = []
    if published_at is not None:
        timing.append(f"🕒 Опубликован: <b>{html.escape(_fmt_time(published_at))}</b>")
    if updated_at is not None:
        timing.append(f"♻️ Обновлён: <b>{html.escape(_fmt_time(updated_at))}</b>")
    if timing:
        lines.extend(timing)

    specs = _collect_item_specs(item)
    if specs:
        lines.append("")
        lines.append("🧾 <b>Подробности:</b>")
        lines.extend(specs)

    if link:
        lines.append("")
        lines.append(f"🔗 Ссылка: {html.escape(link)}")

    if desc:
        clean = re.sub(r"\s{3,}", "  ", desc).strip()
        if len(clean) > 1200:
            clean = clean[:1200] + "…"
        lines.append("")
        lines.append("📝 <b>Описание:</b>")
        lines.append(html.escape(clean))
    lines.append("╚══════════════════════════════════╝")

    card = "\n".join(lines)
    if len(card) > SHORT_CARD_MAX:
        return card[: SHORT_CARD_MAX - 120] + "\n… <i>(часть текста скрыта из-за лимита Telegram)</i>\n╚══════════════════════════════════╝"
    return card


# ====================== AUTOBUY ======================
def _autobuy_buy_urls(source_url: str, item_id: int):
    source_url = (source_url or "").strip()
    source_base = ""
    try:
        parts = urlsplit(source_url)
        if parts.scheme and parts.netloc:
            source_base = f"{parts.scheme}://{parts.netloc}"
    except Exception:
        source_base = ""

    base_hosts = ["https://prod-api.lzt.market", "https://api.lzt.market", "https://api.lolz.live"]

    source_low = source_url.lower()
    source_is_api = source_base and any(marker in source_low for marker in ("api.", "prod-api."))
    if source_base and source_is_api:
        base_hosts.insert(0, source_base)
    elif source_base:
        # Для web-URL (например, https://lzt.market/...) API-эндпоинты приоритетнее.
        # Иначе AUTOBUY_URL_LIMIT может обрезать список до web-путей с 404.
        base_hosts.append(source_base)

    dedup_bases = []
    seen_bases = set()
    for base in base_hosts:
        if base in seen_bases:
            continue
        seen_bases.add(base)
        dedup_bases.append(base)

    # Важно: первые URL используются в fast-режиме и ограничиваются AUTOBUY_URL_LIMIT.
    # Поэтому в приоритете оставляем API-пути, которые реально встречаются в маркет-API,
    # а web-путь item/{id}/buy убираем из ранних попыток (он часто 404).
    fast_paths = [
        "{id}/confirm-buy",
        "market/{id}/confirm-buy",
    ]
    slow_paths = [
        "item/{id}/confirm-buy",
        "items/{id}/confirm-buy",
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
        "forbidden", "access denied", "аккаунт продан",
    )
    queue_markers = (
        "в очереди", "queue", "queued", "попробуйте повторить позднее",
    )
    auth_error_markers = (
        "api key", "scope", "token", "unauthorized", "authorization", "bearer",
        "неверный ключ", "доступ запрещен", "доступ запрещён",
    )

    if status in (404, 405):
        return "retry", raw[:220], False
    if status in (200, 201, 202):
        if any(marker in joined for marker in auth_error_markers):
            return "auth", raw[:220], False
        if "secret" in joined or "answer" in joined or "секрет" in joined:
            return "secret", raw[:220], False
        if any(marker in joined for marker in queue_markers):
            return "queue", raw[:220], False
        if any(marker in joined for marker in terminal_error_markers):
            return "terminal", raw[:220], False
        return "success", raw[:220], False
    if status == 401:
        return "auth", raw[:220], False
    if status == 415:
        return "retry", raw[:220], True
    if status == 400 and any(x in joined for x in ("invalid json", "unsupported media", "content-type")):
        return "retry", raw[:220], True

    if "secret" in joined or "answer" in joined or "секрет" in joined:
        return "secret", raw[:220], False
    if any(marker in joined for marker in queue_markers):
        return "queue", raw[:220], False
    if any(marker in joined for marker in success_markers):
        return "success", raw[:220], False
    if any(marker in joined for marker in terminal_error_markers):
        return "terminal", raw[:220], False
    if status == 403:
        if any(marker in joined for marker in auth_error_markers):
            return "auth", raw[:220], False
        return "retry", raw[:220], False
    return "retry", raw[:220], False


def _sanitize_buy_info_for_user(info: str) -> str:
    s = str(info or "")
    s = re.sub(r"https?://\S+", "[api-endpoint]", s)
    return s


def _autobuy_is_terminal_failure(state: str, status: int, info: str) -> bool:
    if state in {"auth", "secret", "terminal", "success"}:
        return True
    if status == 401:
        return True
    if status == 403 and state in {"auth", "secret", "terminal"}:
        return True
    if status == 400 and (state in {"auth", "secret", "terminal"} or "invalid balance" in (info or "").lower()):
        return True
    low = (info or "").lower()
    if any(x in low for x in (
        "already sold", "already purchased", "already bought", "уже продан",
        "нельзя купить", "недостаточно", "insufficient", "аккаунт продан",
    )):
        return True
    return False


def _autobuy_should_mark_attempt(bought: bool, info: str) -> bool:
    if bought:
        return True
    low = (info or "").lower()
    terminal_markers = (
        "недостаточно", "insufficient", "already sold", "already purchased", "already bought",
        "уже продан", "нельзя купить", "секрет", "secret", "ошибка авторизации", "auth",
        "аккаунт продан", "access denied", "forbidden", "unauthorized", "invalid balance",
    )
    return any(x in low for x in terminal_markers)


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
    buy_urls = _autobuy_prioritized_urls(source_url, item_id)
    if AUTOBUY_URL_LIMIT > 0:
        buy_urls = buy_urls[:AUTOBUY_URL_LIMIT]

    buy_url = buy_urls[0] if buy_urls else None
    if not buy_url:
        return False, "buy_url_not_found"

    payload = {"balance_id": LZT_BALANCE_ID}
    if LZT_SECRET_WORD:
        payload["secret_answer"] = LZT_SECRET_WORD

    since_found_ms = None
    if found_perf is not None:
        since_found_ms = int((t0 - found_perf) * 1000)

    log_autobuy(
        f"BUY_START item_id={item_id} src='{_safe_compact(source_name,120)}' "
        f"since_found_ms={since_found_ms} direct_mode=1 url={buy_url}"
    )

    session = await get_session()
    common_headers = _default_api_headers()
    headers_json = {**common_headers, "Content-Type": "application/json"}

    async with buy_semaphore:
        try:
            bucket, min_interval = _api_limit_bucket("POST", buy_url)
            await request_rate_limiter.wait(bucket, min_interval)
            async with session.post(buy_url, headers=headers_json, json=payload, timeout=FAST_AUTOBUY_TIMEOUT) as resp:
                body = await resp.text()
                state, info, _ = _autobuy_classify_response(resp.status, body)
                log_autobuy(
                    f"BUY_DIRECT item_id={item_id} status={resp.status} state={state} "
                    f"url={buy_url} info='{_safe_compact(info,220)}'"
                )

                if state == "success":
                    _remember_autobuy_endpoint(source_url, buy_url)
                    return True, f"{buy_url} -> {info}"
                if state == "auth":
                    return False, f"{buy_url} -> HTTP {resp.status}: ошибка авторизации API ({info})"
                if state == "secret":
                    return False, f"{buy_url} -> нужен/неверный ответ на секретный вопрос ({info})"
                if state == "terminal":
                    _remember_autobuy_endpoint(source_url, buy_url)
                    return False, f"{buy_url} -> {info}"
                if state == "queue":
                    return False, f"{buy_url} -> queue: {info}"

                return False, f"{buy_url} -> HTTP {resp.status}: {info}"
        except asyncio.TimeoutError:
            return False, f"{buy_url} -> buy_timeout"
        except Exception as e:
            return False, f"{buy_url} -> {e}"


async def try_autobuy_item(source: dict, item: dict, found_perf: float | None = None):
    item_key = make_item_key(item)
    lock = get_buy_lock(item_key)

    async with lock:
        bought, info = await _try_autobuy_once(source, item, found_perf=found_perf)
        return bought, f"attempt=1/1 | {info}"


async def _run_autobuy_and_notify(user_id: int, chat_id: int, source: dict, item: dict, found_perf: float):
    item_id = item.get("item_id") or item.get("id")
    item_key = make_item_key(item)
    src_name = source.get("name") or "UNKNOWN"
    bought = False
    buy_info = "autobuy_not_started"
    should_mark_attempt = False
    try:
        bought, buy_info = await try_autobuy_item(source, item, found_perf=found_perf)
        should_mark_attempt = _autobuy_should_mark_attempt(bought, str(buy_info))
        user_buy_inflight[user_id].discard(item_key)
        if should_mark_attempt and item_key not in user_buy_attempted[user_id]:
            user_buy_attempted[user_id].add(item_key)
            await db_mark_buy_attempted(user_id, item_key)
    except Exception as e:
        user_buy_inflight[user_id].discard(item_key)
        buy_info = f"autobuy_runtime_error: {e}"
        log_autobuy(f"BUY_MARK_ERR user_id={user_id} item_key={item_key} err='{_safe_compact(str(e),220)}'")

    if bought:
        dur_ms = int((time.perf_counter() - found_perf) * 1000)
        bought_link = item.get("url") or item.get("link") or (f"https://lzt.market/{item_id}" if item_id is not None else "")
        buy_result_text = (
            f"🛒 <b>Автобай</b> ✅ [{html.escape(src_name)}] "
            f"item_id=<code>{html.escape(str(item_id))}</code> "
            f"⏱ <b>{dur_ms}ms</b>\n"
            f"🔗 {html.escape(str(bought_link))}\n"
            f"{html.escape(_sanitize_buy_info_for_user(str(buy_info)))}"
        )
    else:
        buy_result_text = (
            f"🛒 <b>Автобай</b> ❌ [{html.escape(src_name)}] "
            f"item_id=<code>{html.escape(str(item_id))}</code>\n{html.escape(_sanitize_buy_info_for_user(str(buy_info)))}"
        )

    log_autobuy(
        f"BUY_RESULT user_id={user_id} item_key={item_key} bought={int(bool(bought))} "
        f"persist_attempt={int(bool(should_mark_attempt))} info='{_safe_compact(str(buy_info),240)}'"
    )

    enqueue_hunter_notification(user_id, chat_id, buy_result_text, parse_mode="HTML", disable_web_page_preview=True)


async def hunter_loop_for_user(user_id: int, chat_id: int):
    await load_user_data(user_id)
    user_buy_inflight[user_id].clear()
    ensure_notify_worker(user_id)
    no_lots_streak = 0
    cycle_num = 0
    pending_autobuy_tasks: set[asyncio.Task] = set()

    def _track_task(task: asyncio.Task):
        pending_autobuy_tasks.add(task)

        def _on_done(t: asyncio.Task):
            pending_autobuy_tasks.discard(t)
            try:
                t.result()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log_autobuy(f"AUTOBUY_TASK_ERR user_id={user_id} err='{_safe_compact(str(e),320)}'")

        task.add_done_callback(_on_done)

    while user_search_active[user_id]:
        cycle_num += 1
        include_non_autobuy = NON_AUTOBUY_CYCLE_EVERY <= 1 or (cycle_num % NON_AUTOBUY_CYCLE_EVERY == 0)
        seen_batch = []
        new_items_processed = 0
        try:
            async for source, items, err in iter_sources_results_split(user_id, include_non_autobuy=include_non_autobuy):
                if err:
                    user_api_errors[user_id] += 1
                    continue
                if not items:
                    continue

                items = sorted(items, key=_item_sort_key, reverse=True)
                if MAX_ITEMS_PER_SOURCE_SCAN > 0:
                    items = items[:MAX_ITEMS_PER_SOURCE_SCAN]

                for item in items:
                    if MAX_NEW_ITEMS_PER_CYCLE > 0 and new_items_processed >= MAX_NEW_ITEMS_PER_CYCLE:
                        break

                    key = make_item_key(item)
                    if key in user_seen_items[user_id]:
                        continue

                    found_perf = time.perf_counter()
                    src_name = source.get("name") or "UNKNOWN"

                    if source.get("autobuy", False) and key not in user_buy_attempted[user_id] and key not in user_buy_inflight[user_id]:
                        user_buy_inflight[user_id].add(key)
                        t = asyncio.create_task(_run_autobuy_and_notify(user_id, chat_id, source, item, found_perf))
                        _track_task(t)

                    user_seen_items[user_id].add(key)
                    seen_batch.append(key)
                    new_items_processed += 1

                    try:
                        await send_bot_message(chat_id, make_card(item, src_name), parse_mode="HTML", disable_web_page_preview=True)
                    except Exception as e:
                        log_autobuy(f"LOT_NOTIFY_SEND_ERR user_id={user_id} err='{_safe_compact(str(e),240)}'")

                if MAX_NEW_ITEMS_PER_CYCLE > 0 and new_items_processed >= MAX_NEW_ITEMS_PER_CYCLE:
                    break

            if new_items_processed == 0:
                no_lots_streak += 1
                ts = time.strftime("%H:%M:%S", time.localtime())
                await upsert_no_lots_message(
                    chat_id,
                    user_id,
                    (
                        "ℹ️ <b>Новых лотов пока нет</b>\n"
                        f"• Обновлено: <b>{ts}</b>\n"
                        f"• Пустых циклов подряд: <b>{no_lots_streak}</b>"
                    ),
                )
            else:
                no_lots_streak = 0
                reset_no_lots_message(user_id)

            await db_mark_seen_batch(user_id, seen_batch)
            await asyncio.sleep(await user_hunter_interval(user_id))

        except asyncio.CancelledError:
            user_hunter_mode[user_id] = "off"
            break
        except Exception as e:
            if seen_batch:
                try:
                    await db_mark_seen_batch(user_id, seen_batch)
                except Exception:
                    pass
            user_api_errors[user_id] += 1
            log_autobuy(f"HUNTER_EXC user_id={user_id} err='{_safe_compact(str(e),400)}'")
            await asyncio.sleep(max(await user_hunter_interval(user_id), 0.01))

    if pending_autobuy_tasks:
        for task in list(pending_autobuy_tasks):
            task.cancel()
    user_buy_inflight[user_id].clear()


# ====================== HANDLERS ======================
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = message.from_user.id
    await load_user_data(user_id, force=True)

    await send_welcome_sticker(message.chat.id)
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
    chat_id = message.chat.id
    await load_user_data(user_id)
    user_balance_cache[0] = {"text": "—", "ts": 0}
    await show_status(user_id, chat_id)
    await safe_delete(message)


@dp.message()
async def buttons_handler(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    await load_user_data(user_id)

    text = (message.text or "").strip()
    mode = user_modes[user_id]

    def _btn_eq(*labels: str) -> bool:
        if not text:
            return False
        normalized = re.sub(r"^[^0-9A-Za-zА-Яа-яЁё]+", "", text).strip().lower()
        for label in labels:
            if text == label:
                return True
            label_norm = re.sub(r"^[^0-9A-Za-zА-Яа-яЁё]+", "", label).strip().lower()
            if normalized == label_norm:
                return True
        return False

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
            name = sanitize_url_name(text)
            url = user_pending_url.get(user_id)
            user_pending_url[user_id] = None
            user_modes[user_id] = None

            if not url:
                await send_screen(chat_id, user_id, "⚠️ Не нашёл ожидаемый URL. Нажми ➕ Добавить URL ещё раз.", reply_markup=kb_urls_menu())
                return await safe_delete(message)

            await db_add_url(user_id, url, name)
            user_urls[user_id] = await db_get_urls(user_id)
            await send_screen(chat_id, user_id, f"✅ URL добавлен: <b>{html.escape(name)}</b>", reply_markup=kb_urls_menu(), parse_mode="HTML")
            return await safe_delete(message)

        if mode == "rename_url_name":
            new_name = sanitize_url_name(text)
            user_modes[user_id] = None
            url = user_pending_rename_url.get(user_id)
            user_pending_rename_url[user_id] = None

            if not url:
                await send_screen(chat_id, user_id, "⚠️ Не нашёл URL для переименования. Повтори ✏️ Переименовать URL.", reply_markup=kb_urls_menu())
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
                    f"• API URL:\n<code>{html.escape(src['url'])}</code>\n"
                    f"• Рекомендация: {'✅ Готов к охоте' if src.get('enabled', True) else '⚠️ Выключен, новые лоты не придут'}"
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
                await send_screen(chat_id, user_id, f"🛒 <b>{html.escape(name)}</b>: {'ВКЛ' if new_ab else 'ВЫКЛ'}", reply_markup=kb_urls_menu(), parse_mode="HTML")
                return await safe_delete(message)

            if mode == "pick_toggle":
                new_enabled = not src.get("enabled", True)
                await db_set_url_enabled(user_id, src["url"], new_enabled)
                user_urls[user_id] = await db_get_urls(user_id)
                user_modes[user_id] = None
                user_page_state[user_id] = {"ctx": None, "page": 0}
                await send_screen(chat_id, user_id, f"🔁 <b>{html.escape(name)}</b>: {'ВКЛ' if new_enabled else 'ВЫКЛ'}", reply_markup=kb_urls_menu(), parse_mode="HTML")
                return await safe_delete(message)

            if mode == "pick_delete":
                await db_remove_url(user_id, src["url"])
                user_urls[user_id] = await db_get_urls(user_id)
                exists_after = any(x.get("url") == src["url"] for x in user_urls[user_id])
                user_modes[user_id] = None
                user_page_state[user_id] = {"ctx": None, "page": 0}
                if exists_after:
                    await send_screen(chat_id, user_id, f"❌ Не удалось удалить: <b>{html.escape(name)}</b>", reply_markup=kb_urls_menu(), parse_mode="HTML")
                else:
                    await send_screen(chat_id, user_id, f"🗑 Удалено: <b>{html.escape(name)}</b>\nОсталось URL: <b>{len(user_urls[user_id])}</b>", reply_markup=kb_urls_menu(), parse_mode="HTML")
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
                "🚀 Старт охотника → максимально быстрый классический режим.\n"
                "🛒 Обычный автобай включается по конкретному URL.\n\n"
                f"✏️ Названия URL автоматически очищаются и ограничены {MAX_URL_NAME_LEN} символами.\n"
                f"🧾 Лог автобая: {AUTOBUY_LOG_FILE}",
                reply_markup=kb_main(user_id),
                parse_mode="HTML",
            )
            return await safe_delete(message)

        if text == "📊 Статус":
            await show_status(user_id, chat_id)
            return await safe_delete(message)

        if text == "✨ Проверка лотов":
            await send_compact_10_for_user(user_id, chat_id)
            return await safe_delete(message)

        if _btn_eq("♻️ Сбросить историю", "🍀 Сбросить историю", "Сбросить историю"):
            user_seen_items[user_id].clear()
            user_buy_attempted[user_id].clear()
            user_history_reset_pending[user_id] = True
            await db_clear_seen(user_id)
            await db_clear_buy_attempted(user_id)
            await send_screen(chat_id, user_id, "♻️ История сброшена. Следующий запуск охотника обработает все лоты как новые (включая автобай по URL, где он активен).", reply_markup=kb_main(user_id))
            return await safe_delete(message)

        if _btn_eq("🚀 Старт охотника", "▶️ Старт охотника", "Старт охотника"):
            requested_mode = "classic"
            lock = get_user_hunter_start_lock(user_id)
            async with lock:
                active_sources = await get_all_sources(user_id, enabled_only=True)
                if not active_sources:
                    await send_screen(chat_id, user_id, "❌ Нет активных URL. Зайди в 📚 Мои URL и добавь источник.", reply_markup=kb_main(user_id))
                    return await safe_delete(message)

                task = user_hunter_tasks.get(user_id)
                if task and not task.done():
                    await send_screen(chat_id, user_id, "⚠️ Охотник уже запущен. Сначала останови его.", reply_markup=kb_main(user_id))
                    return await safe_delete(message)

                user_search_active[user_id] = True
                user_hunter_mode[user_id] = requested_mode
                user_seen_items[user_id] = await db_load_seen(user_id)
                user_buy_attempted[user_id] = await db_load_buy_attempted(user_id)

                if not user_seen_items[user_id] and user_history_reset_pending[user_id]:
                    log_autobuy(f"HUNTER_RESET_MODE user_id={user_id} mode={requested_mode} treat_all_as_new=1")

                user_history_reset_pending[user_id] = False

                task = asyncio.create_task(hunter_loop_for_user(user_id, chat_id))
                user_hunter_tasks[user_id] = task

                log_autobuy(f"HUNTER_START user_id={user_id} active_urls={len(active_sources)} interval={HUNTER_INTERVAL_BASE}")
                await send_screen(chat_id, user_id, f"🚀 Охотник запущен! Активных URL: {len(active_sources)}\nИнтервал цикла: {HUNTER_INTERVAL_BASE:.2f} сек", reply_markup=kb_main(user_id))
                return await safe_delete(message)

        if _btn_eq("🛑 Стоп охотника", "🔴 Стоп охотника", "Стоп охотника"):
            user_search_active[user_id] = False
            user_hunter_mode[user_id] = "off"
            task = user_hunter_tasks.get(user_id)
            if task:
                task.cancel()
                user_hunter_tasks.pop(user_id, None)
            log_autobuy(f"HUNTER_STOP user_id={user_id}")
            await send_screen(chat_id, user_id, "🛑 Охотник остановлен.", reply_markup=kb_main(user_id))
            return await safe_delete(message)

        if _btn_eq("📚 Мои URL", "Мои URL"):
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
            await asyncio.sleep(0.01)
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
    print(f"[BOT] Start: classic hunter + balance_id={LZT_BALANCE_ID}")

    if not has_valid_telegram_token(API_TOKEN):
        raise RuntimeError("Некорректный API_TOKEN: бот не может быть запущен")

    bot = Bot(token=API_TOKEN)

    await init_db()
    asyncio.create_task(error_reporter_loop())

    try:
        await dp.start_polling(bot)
    finally:
        await close_session()
        await db_close()
        if bot is not None and getattr(bot, "session", None) is not None and not bot.session.closed:
            await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
