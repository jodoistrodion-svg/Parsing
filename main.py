import asyncio
import json
import aiohttp
import aiosqlite
import html
import re
import time
import random
import os
from urllib.parse import urlsplit
from collections import defaultdict

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

# config.py как у тебя. Env-поддержка НЕ ломает старое: если env нет — берётся из config.py
from config import API_TOKEN as _API_TOKEN, LZT_API_KEY as _LZT_API_KEY

API_TOKEN = os.getenv("API_TOKEN") or _API_TOKEN
LZT_API_KEY = os.getenv("LZT_API_KEY") or _LZT_API_KEY

bot: Bot | None = None
dp = Dispatcher()

# ---------------------- НАСТРОЙКИ ----------------------
HUNTER_INTERVAL_BASE = 1.0
SHORT_CARD_MAX = 950
ERROR_REPORT_INTERVAL = 3600

MAX_URLS_PER_USER_DEFAULT = 50
MAX_URLS_PER_USER_LIMITED = 3

MAX_CONCURRENT_REQUESTS = 6
FETCH_TIMEOUT = 12
RETRY_MAX = 4
RETRY_BASE_DELAY = 1.0

ADMIN_PASSWORD = "1303"
LIMITED_EXTRA_DELAY = 3.0

DB_FILE = "bot_data.sqlite"

LZT_SECRET_WORD = (os.getenv("LZT_SECRET_WORD") or "Мазда").strip()

# ---------------------- START MESSAGES (как просил) ----------------------
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

# ---------------------- UI: НИЖНИЕ ПАНЕЛИ ----------------------
def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚀 Старт охотника"), KeyboardButton(text="🛑 Стоп охотника")],
            [KeyboardButton(text="✨ Проверка лотов"), KeyboardButton(text="📊 Статус")],
            [KeyboardButton(text="📚 Мои URL"), KeyboardButton(text="♻️ Сбросить историю")],
            [KeyboardButton(text="ℹ️ Инфо")],
        ],
        resize_keyboard=True,
    )


def kb_urls() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📄 Список URL"), KeyboardButton(text="➕ Добавить URL")],
            [KeyboardButton(text="✏️ Переименовать URL"), KeyboardButton(text="🗑 Удалить URL")],
            [KeyboardButton(text="🔁 Вкл/Выкл URL"), KeyboardButton(text="🛒 Автобай URL")],
            [KeyboardButton(text="✅ Тест URL"), KeyboardButton(text="⬅️ Назад")],
        ],
        resize_keyboard=True,
    )


# ---------------------- ВСПОМОГАТЕЛИ ----------------------
def has_valid_telegram_token(token: str) -> bool:
    if not token:
        return False
    return bool(re.match(r"^\d{6,12}:[A-Za-z0-9_-]{20,}$", token))


async def safe_delete(message: types.Message):
    try:
        await message.delete()
    except Exception:
        pass


async def send_bot_message(chat_id: int, text: str, **kwargs):
    if bot is None:
        raise RuntimeError("Bot не инициализирован")
    return await bot.send_message(chat_id, text, **kwargs)


def make_item_key(item: dict) -> str:
    iid = item.get("item_id") or item.get("id")
    if iid is not None:
        return f"id::{iid}"
    return f"noid::{item.get('title')}_{item.get('price')}"


# ---------------------- ПАМЯТЬ (по юзеру) ----------------------
user_filters = defaultdict(lambda: {"title": None})
user_search_active = defaultdict(lambda: False)

user_seen_items = defaultdict(set)         # из БД
user_buy_attempted = defaultdict(set)      # из БД

user_hunter_tasks: dict[int, asyncio.Task] = {}
# modes:
# None
# add_url_url -> add_url_name
# del_url
# tog_url
# ab_url
# test_url
# rename_url_idx -> rename_url_name
user_modes = defaultdict(lambda: None)

user_started = set()
user_urls = defaultdict(list)  # [{"url":..., "name":..., "enabled":..., "autobuy":...}]
user_api_errors = defaultdict(int)

# чтобы удалять прошлый “экран”
user_last_screen_msg_id = defaultdict(lambda: None)

# промежуточное состояние
user_pending_url = defaultdict(lambda: None)
user_pending_rename_idx = defaultdict(lambda: None)


async def delete_last_screen(chat_id: int, user_id: int):
    mid = user_last_screen_msg_id.get(user_id)
    if not mid:
        return
    try:
        await bot.delete_message(chat_id, mid)
    except Exception:
        pass
    user_last_screen_msg_id[user_id] = None


async def send_screen(
    chat_id: int,
    user_id: int,
    text: str,
    reply_markup: ReplyKeyboardMarkup | None = None,
    parse_mode: str | None = None,
):
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


# ---------------------- БД ----------------------
async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS urls (
            user_id INTEGER,
            url TEXT,
            name TEXT DEFAULT '',
            added_at INTEGER,
            enabled INTEGER DEFAULT 1,
            autobuy INTEGER DEFAULT 0,
            PRIMARY KEY(user_id, url)
        )
        """)
        # миграции
        cur = await db.execute("PRAGMA table_info(urls)")
        cols = [row[1] for row in await cur.fetchall()]
        if "enabled" not in cols:
            await db.execute("ALTER TABLE urls ADD COLUMN enabled INTEGER DEFAULT 1")
        if "autobuy" not in cols:
            await db.execute("ALTER TABLE urls ADD COLUMN autobuy INTEGER DEFAULT 0")
        if "name" not in cols:
            await db.execute("ALTER TABLE urls ADD COLUMN name TEXT DEFAULT ''")

        await db.execute("""
        CREATE TABLE IF NOT EXISTS seen (
            user_id INTEGER,
            item_key TEXT,
            seen_at INTEGER,
            PRIMARY KEY(user_id, item_key)
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS buy_attempted (
            user_id INTEGER,
            item_key TEXT,
            attempted_at INTEGER,
            PRIMARY KEY(user_id, item_key)
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            role TEXT DEFAULT 'unknown',
            last_error_report INTEGER DEFAULT 0
        )
        """)
        await db.commit()


async def db_ensure_user(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users(user_id, role, last_error_report) VALUES (?, ?, ?)",
            (user_id, "unknown", 0),
        )
        await db.commit()


async def db_get_role(user_id: int) -> str:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT role FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return row[0] if row else "unknown"


async def db_get_last_report(user_id: int) -> int:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT last_error_report FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0


async def db_set_last_report(user_id: int, ts: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE users SET last_error_report=? WHERE user_id=?", (ts, user_id))
        await db.commit()


async def db_get_urls(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT url, name, enabled, autobuy FROM urls WHERE user_id=? ORDER BY added_at",
            (user_id,),
        )
        rows = await cur.fetchall()
        out = []
        for url, name, enabled, autobuy in rows:
            out.append({"url": url, "name": name or "", "enabled": bool(enabled), "autobuy": bool(autobuy)})
        return out


async def db_add_url(user_id: int, url: str, name: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR IGNORE INTO urls(user_id, url, name, added_at, enabled, autobuy) VALUES (?, ?, ?, ?, 1, 0)",
            (user_id, url, name or "", int(time.time())),
        )
        await db.execute("UPDATE urls SET name=? WHERE user_id=? AND url=?", (name or "", user_id, url))
        await db.commit()


async def db_set_url_name(user_id: int, url: str, name: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE urls SET name=? WHERE user_id=? AND url=?", (name or "", user_id, url))
        await db.commit()


async def db_remove_url(user_id: int, url: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM urls WHERE user_id=? AND url=?", (user_id, url))
        await db.commit()


async def db_set_url_enabled(user_id: int, url: str, enabled: bool):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE urls SET enabled=? WHERE user_id=? AND url=?", (1 if enabled else 0, user_id, url))
        await db.commit()


async def db_set_url_autobuy(user_id: int, url: str, autobuy: bool):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE urls SET autobuy=? WHERE user_id=? AND url=?", (1 if autobuy else 0, user_id, url))
        await db.commit()


async def db_mark_seen(user_id: int, key: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR IGNORE INTO seen(user_id, item_key, seen_at) VALUES (?, ?, ?)",
            (user_id, key, int(time.time())),
        )
        await db.commit()


async def db_load_seen(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT item_key FROM seen WHERE user_id=?", (user_id,))
        rows = await cur.fetchall()
        return {r[0] for r in rows}


async def db_clear_seen(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM seen WHERE user_id=?", (user_id,))
        await db.commit()


async def db_mark_buy_attempted(user_id: int, key: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR IGNORE INTO buy_attempted(user_id, item_key, attempted_at) VALUES (?, ?, ?)",
            (user_id, key, int(time.time())),
        )
        await db.commit()


async def db_load_buy_attempted(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT item_key FROM buy_attempted WHERE user_id=?", (user_id,))
        rows = await cur.fetchall()
        return {r[0] for r in rows}


async def db_clear_buy_attempted(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM buy_attempted WHERE user_id=?", (user_id,))
        await db.commit()


# ---------------------- LOAD USER DATA ----------------------
async def load_user_data(user_id: int, force: bool = False):
    if user_id in user_started and not force:
        return
    await db_ensure_user(user_id)
    user_urls[user_id] = await db_get_urls(user_id)
    user_seen_items[user_id] = await db_load_seen(user_id)
    user_buy_attempted[user_id] = await db_load_buy_attempted(user_id)
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


# ---------------------- URL VALIDATION/NORMALIZATION ----------------------
def validate_market_url(url: str):
    if not url.startswith(("http://", "https://")):
        return False, "❌ Это не похоже на URL."
    lower = url.lower()
    if not ("api.lzt.market/" in lower or "api.lolz.live/" in lower or "prod-api.lzt.market/" in lower):
        return False, "❌ Нужна API-ссылка LZT: prod-api.lzt.market / api.lzt.market / api.lolz.live."
    return True, None


def normalize_url(url: str) -> str:
    if not url:
        return url
    s = url.strip().replace(" ", "").replace("\t", "").replace("\n", "")

    # если явно prod-api — не ломаем
    if "prod-api.lzt.market" not in s.lower():
        s = re.sub(r"https?://api.*?\.market", "https://api.lzt.market", s)
        s = re.sub(r"https?://api\.lolz\.guru", "https://api.lzt.market", s)
        s = s.replace("://lzt.market", "://api.lzt.market")
        s = s.replace("://www.lzt.market", "://api.lzt.market")

    # фиксы параметров (как у тебя)
    s = s.replace("genshinlevelmin", "genshin_level_min")
    s = s.replace("genshinlevel_min", "genshin_level_min")
    s = s.replace("genshin_levelmin", "genshin_level_min")
    s = s.replace("brawl_cupmin", "brawl_cup_min")
    s = s.replace("clash_cupmin", "clash_cup_min")
    s = s.replace("clashcupmin", "clash_cup_min")
    s = s.replace("clashcupmax", "clash_cup_max")
    s = s.replace("clash_cupmax", "clash_cup_max")
    s = s.replace("orderby", "order_by")
    s = s.replace("order_by=pdate_to_down_upoad", "order_by=pdate_to_down_upload")
    s = s.replace("order_by=pdate_to_down_up", "order_by=pdate_to_down_upload")
    s = s.replace("order_by=pdate_to_downupload", "order_by=pdate_to_down_upload")

    return s


# ---------------------- HTTP / API ----------------------
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
_global_session: aiohttp.ClientSession | None = None


async def get_session():
    global _global_session
    if _global_session is None or _global_session.closed:
        _global_session = aiohttp.ClientSession()
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
            try:
                data = json.loads(text)
            except Exception:
                return None, f"❌ API вернул не JSON:\n{text[:300]}"
            items = data.get("items")
            if not isinstance(items, list):
                return None, "⚠ API не вернул список items"
            return items, None
    except asyncio.TimeoutError:
        return None, "❌ Таймаут запроса"
    except aiohttp.ClientError as e:
        return None, f"❌ Ошибка сети: {e}"
    except Exception as e:
        return None, f"❌ Ошибка: {e}"


async def fetch_with_retry(url: str, max_retries: int = RETRY_MAX):
    attempt = 0
    delay = RETRY_BASE_DELAY
    while attempt < max_retries:
        attempt += 1
        try:
            async with semaphore:
                items, err = await fetch_items_raw(url)
        except Exception as e:
            items, err = None, f"❌ Ошибка: {e}"

        if err is None:
            return items, None

        if attempt >= max_retries:
            return [], err

        jitter = random.uniform(0, delay * 0.3)
        await asyncio.sleep(delay + jitter)
        delay *= 2

    return [], "❌ Не удалось получить ответ"


# ---------------------- SOURCES ----------------------
async def get_all_sources(user_id: int, enabled_only: bool = False):
    await load_user_data(user_id)

    # дедуп
    deduped = []
    seen = set()
    for src in user_urls[user_id]:
        u = src.get("url")
        if not u or u in seen:
            continue
        seen.add(u)
        deduped.append(src)
    user_urls[user_id] = deduped

    if enabled_only:
        return [s for s in user_urls[user_id] if s.get("enabled", True)]
    return user_urls[user_id]


async def fetch_all_sources(user_id: int):
    sources = await get_all_sources(user_id, enabled_only=True)
    results = []
    errors = []
    for idx, source in enumerate(sources):
        url = source["url"]
        label = source.get("name") or f"URL #{idx+1}"
        source_info = {
            "idx": idx + 1,
            "url": url,
            "name": label,
            "enabled": source.get("enabled", True),
            "autobuy": source.get("autobuy", False),
        }
        items, err = await fetch_with_retry(url)
        if err:
            errors.append((label, url, err))
            continue
        for it in items:
            results.append((it, source_info))
    return results, errors


# ---------------------- FILTERS ----------------------
def passes_filters(item: dict, user_id: int) -> bool:
    f = user_filters[user_id]
    if f["title"]:
        title = (item.get("title") or "").lower()
        if f["title"].lower() not in title:
            return False
    return True


# ---------------------- DISPLAY ----------------------
def make_card(item: dict, source_name: str) -> str:
    title = item.get("title", "Без названия")
    price = item.get("price", "—")
    item_id = item.get("item_id", item.get("id", "—"))

    trophies = item.get("trophies") or item.get("cups") or item.get("brawl_cup") or None
    level = item.get("level") or item.get("lvl") or item.get("user_level") or None
    townhall = item.get("townhall") or item.get("th") or None
    phone_bound = item.get("phone_bound") or item.get("phone")

    lines = [
        "━━━━━━━━━━━━━━━━━━━━",
        f"🔎 <b>{html.escape(str(source_name))}</b>",
        f"🎮 <b>{html.escape(str(title))}</b>",
    ]
    if level:
        lines.append(f"🔼 Уровень: <b>{html.escape(str(level))}</b>")
    if trophies:
        lines.append(f"🏆 Кубков: <b>{html.escape(str(trophies))}</b>")
    if townhall:
        lines.append(f"🏰 Ратуша: <b>{html.escape(str(townhall))}</b>")
    if phone_bound is not None:
        lines.append(f"📱 Телефон: <b>{'Да' if phone_bound else 'Нет'}</b>")

    if price != "—":
        lines.append(f"💰 <b>{html.escape(str(price))} ₽</b>")
    else:
        lines.append("💰 —")

    lines.append(f"🆔 <code>{html.escape(str(item_id))}</code>")

    # без inline кнопок: просто ссылка (кликабельно в Telegram)
    if item_id != "—":
        lines.append(f"🔗 https://lzt.market/{html.escape(str(item_id))}")

    lines.append("━━━━━━━━━━━━━━━━━━━━")

    card = "\n".join(lines)
    if len(card) > SHORT_CARD_MAX:
        return card[:SHORT_CARD_MAX - 120] + "\n… <i>(обрезано)</i>"
    return card


# ---------------------- AUTOBUY (ЛОГИКУ НЕ ЛОМАЕМ) ----------------------
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
        {**payload, "fast_buy": 1, "instant_buy": 1},
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

    base_hosts = []
    # prod api first
    base_hosts.append("https://prod-api.lzt.market")

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

    paths = [
        "{id}/fast-buy",
        "{id}/buy",
        "{id}/purchase",
        "market/{id}/fast-buy",
        "market/{id}/buy",
        "market/{id}/purchase",
        "item/{id}/fast-buy",
        "item/{id}/buy",
        "item/{id}/purchase",
        "items/{id}/buy",
        "items/{id}/fast-buy",
        "items/{id}/purchase",
    ]

    urls = []
    seen = set()
    for base in dedup_bases:
        for tpl in paths:
            url = f"{base}/{tpl.format(id=item_id)}"
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)
    return urls


def _autobuy_classify_response(status: int, text: str):
    text = html.unescape(text or "")
    lower = text.lower()

    success_markers = (
        "success", "ok", "purchased", "purchase complete", "already bought", "уже куп"
    )
    terminal_error_markers = (
        "insufficient", "not enough", "недостаточно", "уже продан", "already sold",
        "already purchased", "already bought", "цена изменилась", "нельзя купить",
        "forbidden", "access denied"
    )

    # 404/405 по неверному пути покупки: продолжаем перебор
    if status in (404, 405):
        return "retry", text[:220]

    if status in (200, 201, 202):
        return "success", text[:220]
    if status in (401, 403):
        return "auth", text[:220]
    if "secret" in lower or "answer" in lower or "секрет" in lower:
        return "secret", text[:220]
    if any(marker in lower for marker in success_markers):
        return "success", text[:220]
    if any(marker in lower for marker in terminal_error_markers):
        return "terminal", text[:220]
    return "retry", text[:220]


async def try_autobuy_item(source: dict, item: dict):
    if not LZT_API_KEY:
        return False, "LZT_API_KEY не задан"

    item_id = item.get("item_id") or item.get("id")
    if not item_id:
        return False, "missing_item_id"

    try:
        item_id = int(item_id)
    except (TypeError, ValueError):
        return False, f"invalid_item_id={item_id}"

    headers = {
        "Authorization": f"Bearer {LZT_API_KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    payload_variants = _autobuy_payload_variants(item)
    buy_urls = _autobuy_buy_urls(source.get("url") or "", item_id)

    last_err = "unknown"
    try:
        session = await get_session()
        for buy_url in buy_urls:
            for payload in payload_variants:
                async with session.post(buy_url, headers=headers, json=payload, timeout=FETCH_TIMEOUT) as resp:
                    body = await resp.text()
                    state, info = _autobuy_classify_response(resp.status, body)
                    if state == "success":
                        return True, f"{buy_url} -> {info}"
                    if state == "auth":
                        return False, f"{buy_url} -> HTTP {resp.status}: проверьте API ключ и scope market ({info})"
                    if state == "secret":
                        last_err = f"{buy_url} -> нужен/неверный ответ на секретный вопрос ({info})"
                        continue
                    if state == "terminal":
                        return False, f"{buy_url} -> {info}"
                    last_err = f"{buy_url} -> HTTP {resp.status}: {info}"

                # fallback: form
                form_headers = {k: v for k, v in headers.items() if k.lower() != "content-type"}
                async with session.post(buy_url, headers=form_headers, data=payload, timeout=FETCH_TIMEOUT) as form_resp:
                    form_body = await form_resp.text()
                    state, info = _autobuy_classify_response(form_resp.status, form_body)
                    if state == "success":
                        return True, f"{buy_url} (form) -> {info}"
                    if state == "auth":
                        return False, f"{buy_url} (form) -> HTTP {form_resp.status}: проверьте API ключ и scope market ({info})"
                    if state == "secret":
                        last_err = f"{buy_url} (form) -> нужен/неверный ответ на секретный вопрос ({info})"
                        continue
                    if state == "terminal":
                        return False, f"{buy_url} (form) -> {info}"
                    last_err = f"{buy_url} (form) -> HTTP {form_resp.status}: {info}"

        return False, last_err
    except Exception as e:
        return False, str(e)


# ---------------------- REPORTER ----------------------
async def error_reporter_loop():
    while True:
        try:
            await asyncio.sleep(ERROR_REPORT_INTERVAL)
            now = int(time.time())
            users = list(user_started)
            for uid in users:
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


# ---------------------- ACTIONS ----------------------
async def show_status(user_id: int, chat_id: int):
    await load_user_data(user_id)
    role = await get_user_role(user_id) or "not set"
    active = user_search_active[user_id]
    total = len(await get_all_sources(user_id))
    enabled = len(await get_all_sources(user_id, enabled_only=True))
    ab = sum(1 for s in await get_all_sources(user_id) if s.get("autobuy", False))
    f = user_filters[user_id]["title"]

    text = (
        "<b>📊 Статус</b>\n"
        f"• Роль: <b>{html.escape(role)}</b>\n"
        f"• Охотник: <b>{'ВКЛ' if active else 'ВЫКЛ'}</b>\n"
        f"• URL: <b>{enabled}/{total}</b> (автобай: <b>{ab}</b>)\n"
        f"• Увидено: <b>{len(user_seen_items[user_id])}</b>\n"
        f"• Фильтр: <b>{html.escape(f) if f else 'нет'}</b>\n"
        f"• Ошибок API: <b>{user_api_errors.get(user_id, 0)}</b>"
    )
    await send_screen(chat_id, user_id, text, reply_markup=kb_main(), parse_mode="HTML")


async def show_urls_list(user_id: int, chat_id: int):
    await load_user_data(user_id)
    sources = await get_all_sources(user_id)
    if not sources:
        await send_screen(chat_id, user_id, "📚 Список URL пуст.\nНажми ➕ Добавить URL", reply_markup=kb_urls())
        return

    lines = ["📚 <b>Мои URL</b>\n(показываю названия; URL внутри хранится)"]
    for i, s in enumerate(sources, start=1):
        name = s.get("name") or f"URL #{i}"
        st = "🟢" if s.get("enabled", True) else "🔴"
        ab = "🛒" if s.get("autobuy", False) else "—"
        lines.append(f"{st} {ab} <b>{i}.</b> {html.escape(name)}")
    lines.append("\nПодсказка: для действий используй кнопки ниже.")
    await send_screen(chat_id, user_id, "\n".join(lines), reply_markup=kb_urls(), parse_mode="HTML")


async def send_compact_10_for_user(user_id: int, chat_id: int):
    items_with_sources, errors = await fetch_all_sources(user_id)

    if errors:
        for label, url, err in errors:
            user_api_errors[user_id] += 1
            await send_bot_message(
                chat_id,
                f"❗ <b>Ошибка</b> [{html.escape(label)}]\n<code>{html.escape(url)}</code>\n{html.escape(str(err))}",
                parse_mode="HTML",
            )

    if not items_with_sources:
        await send_screen(chat_id, user_id, "❗ Ничего не найдено по активным URL.", reply_markup=kb_main())
        return

    aggregated = {}
    for item, source in items_with_sources:
        key = make_item_key(item)
        if key not in aggregated:
            aggregated[key] = (item, source)

    items_list = list(aggregated.values())[:10]

    await send_screen(
        chat_id,
        user_id,
        f"✅ <b>Проверка лотов</b>\n• Показано: <b>{len(items_list)}</b>",
        reply_markup=kb_main(),
        parse_mode="HTML",
    )

    for item, source in items_list:
        if not passes_filters(item, user_id):
            continue
        await send_bot_message(chat_id, make_card(item, source["name"]), parse_mode="HTML", disable_web_page_preview=True)
        await asyncio.sleep(0.15)


async def send_test_for_single_url(user_id: int, chat_id: int, src: dict, idx: int):
    url = src["url"]
    label = src.get("name") or f"URL #{idx}"
    items, err = await fetch_with_retry(url, max_retries=2)
    if err:
        await send_screen(chat_id, user_id, f"❗ Ошибка теста <b>{html.escape(label)}</b>\n{html.escape(str(err))}", reply_markup=kb_urls(), parse_mode="HTML")
        return
    if not items:
        await send_screen(chat_id, user_id, f"⚠️ <b>{html.escape(label)}</b>: пусто.", reply_markup=kb_urls(), parse_mode="HTML")
        return

    aggregated = {}
    for it in items:
        k = make_item_key(it)
        if k not in aggregated:
            aggregated[k] = it
    limited = list(aggregated.values())[:10]

    await send_screen(
        chat_id,
        user_id,
        f"✅ Тест: <b>{html.escape(label)}</b>\n• Уникальных: <b>{len(aggregated)}</b>\n• Показано: <b>{len(limited)}</b>",
        reply_markup=kb_urls(),
        parse_mode="HTML",
    )

    for it in limited:
        if not passes_filters(it, user_id):
            continue
        await send_bot_message(chat_id, make_card(it, label), parse_mode="HTML", disable_web_page_preview=True)
        await asyncio.sleep(0.15)


async def autobuy_sweep_existing(user_id: int, chat_id: int):
    items_with_sources, _ = await fetch_all_sources(user_id)

    aggregated = {}
    for item, source in items_with_sources:
        key = make_item_key(item)
        if key not in aggregated:
            aggregated[key] = (item, source)

    for item, source in aggregated.values():
        key = make_item_key(item)

        if source.get("autobuy", False) and key not in user_buy_attempted[user_id]:
            user_buy_attempted[user_id].add(key)
            await db_mark_buy_attempted(user_id, key)

            bought, _info = await try_autobuy_item(source, item)
            if bought:
                await send_bot_message(
                    chat_id,
                    f"🛒 <b>Автобай</b> ✅ [{html.escape(source['name'])}] item_id=<code>{html.escape(str(item.get('item_id') or item.get('id')))}</code>",
                    parse_mode="HTML",
                )

        user_seen_items[user_id].add(key)
        await db_mark_seen(user_id, key)


async def hunter_loop_for_user(user_id: int, chat_id: int):
    await load_user_data(user_id)

    try:
        await autobuy_sweep_existing(user_id, chat_id)
    except Exception:
        pass

    while user_search_active[user_id]:
        try:
            items_with_sources, errors = await fetch_all_sources(user_id)
            if errors:
                user_api_errors[user_id] += len(errors)

            for item, source in items_with_sources:
                key = make_item_key(item)
                if key in user_seen_items[user_id]:
                    continue

                if not passes_filters(item, user_id):
                    user_seen_items[user_id].add(key)
                    await db_mark_seen(user_id, key)
                    continue

                if source.get("autobuy", False) and key not in user_buy_attempted[user_id]:
                    user_buy_attempted[user_id].add(key)
                    await db_mark_buy_attempted(user_id, key)

                    bought, buy_info = await try_autobuy_item(source, item)
                    if bought:
                        await send_bot_message(
                            chat_id,
                            f"🛒 <b>Автобай</b> ✅ [{html.escape(source['name'])}] item_id=<code>{html.escape(str(item.get('item_id') or item.get('id')))}</code>",
                            parse_mode="HTML",
                        )
                    else:
                        low = (buy_info or "").lower()
                        if "auth" in low or "secret" in low or "401" in low or "403" in low:
                            await send_bot_message(chat_id, f"⚠️ Автобай: {html.escape(str(buy_info))}", parse_mode="HTML")

                user_seen_items[user_id].add(key)
                await db_mark_seen(user_id, key)

                await send_bot_message(chat_id, make_card(item, source["name"]), parse_mode="HTML", disable_web_page_preview=True)
                await asyncio.sleep(0.15)

            await asyncio.sleep(await user_hunter_interval(user_id))

        except asyncio.CancelledError:
            break
        except Exception:
            user_api_errors[user_id] += 1
            await asyncio.sleep(await user_hunter_interval(user_id))


# ---------------------- HANDLERS ----------------------
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = message.from_user.id
    await load_user_data(user_id, force=True)

    await send_bot_message(message.chat.id, START_MSG_1, disable_web_page_preview=True)
    await send_bot_message(message.chat.id, START_MSG_2, reply_markup=kb_main(), disable_web_page_preview=True)

    user_last_screen_msg_id[user_id] = None
    await safe_delete(message)


@dp.message()
async def buttons_handler(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    await load_user_data(user_id)

    text = (message.text or "").strip()
    mode = user_modes[user_id]

    try:
        # -------- MODES --------
        if mode == "add_url_url":
            user_modes[user_id] = None
            url = normalize_url(text)

            ok, err = validate_market_url(url)
            if not ok:
                await send_screen(chat_id, user_id, err, reply_markup=kb_urls())
                return await safe_delete(message)

            limit = await user_url_limit(user_id)
            if len(await get_all_sources(user_id)) >= limit:
                await send_screen(chat_id, user_id, f"❌ Достигнут лимит URL: {limit}", reply_markup=kb_urls())
                return await safe_delete(message)

            _items, api_err = await fetch_with_retry(url, max_retries=2)
            if api_err:
                await send_screen(chat_id, user_id, f"❌ API ошибка: {api_err}", reply_markup=kb_urls())
                return await safe_delete(message)

            user_pending_url[user_id] = url
            user_modes[user_id] = "add_url_name"
            await send_screen(chat_id, user_id, "✏️ Введи название для этого URL:", reply_markup=kb_urls())
            return await safe_delete(message)

        if mode == "add_url_name":
            name = text.strip()
            url = user_pending_url.get(user_id)
            user_pending_url[user_id] = None
            user_modes[user_id] = None

            if not url:
                await send_screen(chat_id, user_id, "⚠️ Не нашёл ожидаемый URL. Нажми ➕ Добавить URL ещё раз.", reply_markup=kb_urls())
                return await safe_delete(message)

            if not name:
                name = f"URL {int(time.time())}"

            await db_add_url(user_id, url, name)
            user_urls[user_id] = await db_get_urls(user_id)
            await send_screen(chat_id, user_id, f"✅ URL добавлен: <b>{html.escape(name)}</b>", reply_markup=kb_urls(), parse_mode="HTML")
            return await safe_delete(message)

        if mode == "rename_url_idx":
            user_modes[user_id] = None
            try:
                idx = int(re.sub(r"[^\d]", "", text))
            except Exception:
                idx = -1

            sources = await get_all_sources(user_id)
            if idx < 1 or idx > len(sources):
                await send_screen(chat_id, user_id, "❌ Неверный номер. Смотри номер в 📄 Список URL", reply_markup=kb_urls())
                return await safe_delete(message)

            user_pending_rename_idx[user_id] = idx
            user_modes[user_id] = "rename_url_name"
            current_name = sources[idx - 1].get("name") or f"URL #{idx}"
            await send_screen(chat_id, user_id, f"✏️ Новое название для <b>{html.escape(current_name)}</b>:", reply_markup=kb_urls(), parse_mode="HTML")
            return await safe_delete(message)

        if mode == "rename_url_name":
            user_modes[user_id] = None
            name = text.strip()
            idx = user_pending_rename_idx.get(user_id)
            user_pending_rename_idx[user_id] = None

            sources = await get_all_sources(user_id)
            if not idx or idx < 1 or idx > len(sources):
                await send_screen(chat_id, user_id, "❌ Неверный номер. Смотри номер в 📄 Список URL", reply_markup=kb_urls())
                return await safe_delete(message)

            if not name:
                await send_screen(chat_id, user_id, "❌ Название не может быть пустым.", reply_markup=kb_urls())
                return await safe_delete(message)

            src = sources[idx - 1]
            await db_set_url_name(user_id, src["url"], name)
            user_urls[user_id] = await db_get_urls(user_id)

            await send_screen(chat_id, user_id, f"✅ Переименовано: <b>{html.escape(name)}</b>", reply_markup=kb_urls(), parse_mode="HTML")
            return await safe_delete(message)

        if mode in ("del_url", "tog_url", "ab_url", "test_url"):
            user_modes[user_id] = None
            try:
                idx = int(re.sub(r"[^\d]", "", text))
            except Exception:
                idx = -1

            sources = await get_all_sources(user_id)
            if idx < 1 or idx > len(sources):
                await send_screen(chat_id, user_id, "❌ Неверный номер. Смотри номер в 📄 Список URL", reply_markup=kb_urls())
                return await safe_delete(message)

            src = sources[idx - 1]
            name = src.get("name") or f"URL #{idx}"

            if mode == "del_url":
                await db_remove_url(user_id, src["url"])
                user_urls[user_id] = await db_get_urls(user_id)
                await send_screen(chat_id, user_id, f"🗑 Удалено: <b>{html.escape(name)}</b>", reply_markup=kb_urls(), parse_mode="HTML")
                return await safe_delete(message)

            if mode == "tog_url":
                new_enabled = not src.get("enabled", True)
                await db_set_url_enabled(user_id, src["url"], new_enabled)
                user_urls[user_id] = await db_get_urls(user_id)
                await send_screen(chat_id, user_id, f"🔁 {html.escape(name)}: {'ВКЛ' if new_enabled else 'ВЫКЛ'}", reply_markup=kb_urls())
                return await safe_delete(message)

            if mode == "ab_url":
                new_ab = not src.get("autobuy", False)
                await db_set_url_autobuy(user_id, src["url"], new_ab)
                user_urls[user_id] = await db_get_urls(user_id)
                await send_screen(chat_id, user_id, f"🛒 {html.escape(name)}: {'ВКЛ' if new_ab else 'ВЫКЛ'}", reply_markup=kb_urls())
                return await safe_delete(message)

            if mode == "test_url":
                await send_test_for_single_url(user_id, chat_id, src, idx)
                return await safe_delete(message)

        # -------- MAIN MENU --------
        if text == "ℹ️ Инфо":
            await send_screen(
                chat_id,
                user_id,
                "ℹ️ Управление только нижними кнопками.\n"
                "📚 Мои URL → управление источниками.\n"
                "🚀 Старт охотника → уведомления о новых лотах.\n"
                "🛒 Автобай включается по конкретному URL.",
                reply_markup=kb_main(),
            )
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
            await send_screen(chat_id, user_id, "♻️ История сброшена. Теперь лоты снова считаются новыми.", reply_markup=kb_main())
            return await safe_delete(message)

        if text == "🚀 Старт охотника":
            active_sources = await get_all_sources(user_id, enabled_only=True)
            if not active_sources:
                await send_screen(chat_id, user_id, "❌ Нет активных URL. Зайди в 📚 Мои URL и добавь источник.", reply_markup=kb_main())
                return await safe_delete(message)

            if not user_search_active[user_id]:
                user_search_active[user_id] = True
                user_seen_items[user_id] = await db_load_seen(user_id)
                user_buy_attempted[user_id] = await db_load_buy_attempted(user_id)

                task = asyncio.create_task(hunter_loop_for_user(user_id, chat_id))
                user_hunter_tasks[user_id] = task

                await send_screen(chat_id, user_id, f"🚀 Охотник запущен! Активных URL: {len(active_sources)}", reply_markup=kb_main())
                return await safe_delete(message)

            await send_screen(chat_id, user_id, "⚠️ Охотник уже запущен.", reply_markup=kb_main())
            return await safe_delete(message)

        if text == "🛑 Стоп охотника":
            user_search_active[user_id] = False
            task = user_hunter_tasks.get(user_id)
            if task:
                task.cancel()
            await send_screen(chat_id, user_id, "🛑 Охотник остановлен.", reply_markup=kb_main())
            return await safe_delete(message)

        if text == "📚 Мои URL":
            await send_screen(chat_id, user_id, "📚 Меню URL", reply_markup=kb_urls())
            return await safe_delete(message)

        # -------- URL MENU --------
        if text == "⬅️ Назад":
            await send_screen(chat_id, user_id, "🧭 Меню", reply_markup=kb_main())
            return await safe_delete(message)

        if text == "📄 Список URL":
            await show_urls_list(user_id, chat_id)
            return await safe_delete(message)

        if text == "➕ Добавить URL":
            user_modes[user_id] = "add_url_url"
            await send_screen(chat_id, user_id, "Вставь API URL (prod-api.lzt.market / api.lzt.market / api.lolz.live):", reply_markup=kb_urls())
            return await safe_delete(message)

        if text == "✏️ Переименовать URL":
            user_modes[user_id] = "rename_url_idx"
            await send_screen(chat_id, user_id, "Введи номер URL из 📄 Список URL:", reply_markup=kb_urls())
            return await safe_delete(message)

        if text == "🗑 Удалить URL":
            user_modes[user_id] = "del_url"
            await send_screen(chat_id, user_id, "Введи номер URL из 📄 Список URL:", reply_markup=kb_urls())
            return await safe_delete(message)

        if text == "🔁 Вкл/Выкл URL":
            user_modes[user_id] = "tog_url"
            await send_screen(chat_id, user_id, "Введи номер URL:", reply_markup=kb_urls())
            return await safe_delete(message)

        if text == "🛒 Автобай URL":
            user_modes[user_id] = "ab_url"
            await send_screen(chat_id, user_id, "Введи номер URL:", reply_markup=kb_urls())
            return await safe_delete(message)

        if text == "✅ Тест URL":
            user_modes[user_id] = "test_url"
            await send_screen(chat_id, user_id, "Введи номер URL:", reply_markup=kb_urls())
            return await safe_delete(message)

        # любые лишние сообщения пользователя — удаляем, чтобы чат был чистый
        if text and not text.startswith("/"):
            await asyncio.sleep(0.2)
            await safe_delete(message)

    except Exception as e:
        try:
            await send_bot_message(chat_id, f"❌ Ошибка: {html.escape(str(e))}", parse_mode="HTML")
        except Exception:
            pass
        await safe_delete(message)


# ---------------------- RUN ----------------------
async def main():
    global bot
    print("[BOT] Start: multiuser + url names + rename + bottom panels + no inline controls")

    if not has_valid_telegram_token(API_TOKEN):
        raise RuntimeError("Некорректный API_TOKEN: бот не может быть запущен")

    bot = Bot(token=API_TOKEN)

    await init_db()
    asyncio.create_task(error_reporter_loop())

    try:
        await dp.start_polling(bot)
    finally:
        await close_session()
        if bot is not None and getattr(bot, "session", None) is not None and not bot.session.closed:
            await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
