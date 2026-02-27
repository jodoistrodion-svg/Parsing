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
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from config import API_TOKEN, LZT_API_KEY

# ---------------------- НАСТРОЙКИ ----------------------
HUNTER_INTERVAL_BASE = 1.0
SHORT_CARD_MAX = 950
URL_LABEL_MAX = 64

ERROR_REPORT_INTERVAL = 3600  # seconds
MAX_URLS_PER_USER_DEFAULT = 50
MAX_URLS_PER_USER_LIMITED = 3

MAX_CONCURRENT_REQUESTS = 6
FETCH_TIMEOUT = 12
RETRY_MAX = 4
RETRY_BASE_DELAY = 1.0

ADMIN_PASSWORD = "1303"
LIMITED_EXTRA_DELAY = 3.0

DB_FILE = "bot_data.sqlite"

# секретное слово (по умолчанию "Мазда")
LZT_SECRET_WORD = (os.getenv("LZT_SECRET_WORD") or "Мазда").strip()

# ---------------------- BOT ----------------------
bot: Bot | None = None
dp = Dispatcher()

# ---------------------- ВСПОМОГАТЕЛЬНОЕ ----------------------
def has_valid_telegram_token(token: str) -> bool:
    if not token:
        return False
    return bool(re.match(r"^\d{6,12}:[A-Za-z0-9_-]{20,}$", token))


async def send_bot_message(chat_id: int, text: str, **kwargs):
    if bot is None:
        raise RuntimeError("Bot не инициализирован")
    return await bot.send_message(chat_id, text, **kwargs)


async def safe_delete(message: types.Message):
    try:
        await message.delete()
    except Exception:
        pass


def format_balance(amount: float) -> str:
    return f"{amount:,.2f} ₽".replace(",", " ")


def make_item_key(item: dict) -> str:
    iid = item.get("item_id") or item.get("id")
    if iid is not None:
        return f"id::{iid}"
    return f"noid::{item.get('title')}_{item.get('price')}"


# ---------------------- БД ----------------------
async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS urls (
            user_id INTEGER,
            url TEXT,
            added_at INTEGER,
            enabled INTEGER DEFAULT 1,
            autobuy INTEGER DEFAULT 0,
            PRIMARY KEY(user_id, url)
        )
        """)

        # seen — чтобы в чат слать только новые
        await db.execute("""
        CREATE TABLE IF NOT EXISTS seen (
            user_id INTEGER,
            item_key TEXT,
            seen_at INTEGER,
            PRIMARY KEY(user_id, item_key)
        )
        """)

        # buy_attempted — чтобы не пытаться покупать один и тот же лот бесконечно
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
            last_error_report INTEGER DEFAULT 0,
            balance REAL DEFAULT 0
        )
        """)

        await db.commit()


async def db_ensure_user(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users(user_id, role, last_error_report, balance) VALUES (?, ?, ?, ?)",
            (user_id, "unknown", 0, 0),
        )
        await db.commit()


async def db_get_role(user_id: int) -> str:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT role FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return row[0] if row else "unknown"


async def db_set_role(user_id: int, role: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users(user_id, role, last_error_report, balance) VALUES (?, ?, ?, ?)",
            (user_id, role, 0, 0),
        )
        await db.execute("UPDATE users SET role=? WHERE user_id=?", (role, user_id))
        await db.commit()


async def db_get_last_report(user_id: int) -> int:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT last_error_report FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0


async def db_set_last_report(user_id: int, ts: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE users SET last_error_report=? WHERE user_id=?", (ts, user_id))
        await db.commit()


async def db_get_balance(user_id: int) -> float:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0


async def db_change_balance(user_id: int, amount: float) -> float:
    await db_ensure_user(user_id)
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "UPDATE users SET balance = COALESCE(balance, 0) + ? WHERE user_id=?",
            (amount, user_id),
        )
        cur = await db.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        await db.commit()
        return float(row[0]) if row and row[0] is not None else 0.0


async def db_add_url(user_id: int, url: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR IGNORE INTO urls(user_id, url, added_at, enabled, autobuy) VALUES (?, ?, ?, 1, 0)",
            (user_id, url, int(time.time())),
        )
        await db.commit()


async def db_remove_url(user_id: int, url: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM urls WHERE user_id=? AND url=?", (user_id, url))
        await db.commit()


async def db_get_urls(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT url, enabled, autobuy FROM urls WHERE user_id=? ORDER BY added_at",
            (user_id,),
        )
        rows = await cur.fetchall()
        return [{"url": r[0], "enabled": bool(r[1]), "autobuy": bool(r[2])} for r in rows]


async def db_set_url_enabled(user_id: int, url: str, enabled: bool):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "UPDATE urls SET enabled=? WHERE user_id=? AND url=?",
            (1 if enabled else 0, user_id, url),
        )
        await db.commit()


async def db_set_url_autobuy(user_id: int, url: str, autobuy: bool):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "UPDATE urls SET autobuy=? WHERE user_id=? AND url=?",
            (1 if autobuy else 0, user_id, url),
        )
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


# ---------------------- URL ВАЛИДАЦИЯ/НОРМАЛИЗАЦИЯ ----------------------
def validate_market_url(url: str):
    """Разрешаем API ссылки:
    - api.lzt.market
    - api.lolz.live
    - prod-api.lzt.market
    """
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

    # НЕ трогаем prod-api если пользователь явно его указал
    if "prod-api.lzt.market" not in s.lower():
        s = re.sub(r"https?://api.*?\.market", "https://api.lzt.market", s)
        s = re.sub(r"https?://api\.lolz\.guru", "https://api.lzt.market", s)
        s = s.replace("://lzt.market", "://api.lzt.market")
        s = s.replace("://www.lzt.market", "://api.lzt.market")

    # фикс частых опечаток параметров
    s = s.replace("genshinlevelmin", "genshin_level_min")
    s = s.replace("genshinlevel_min", "genshin_level_min")
    s = s.replace("genshin_levelmin", "genshin_level_min")
    s = s.replace("brawl_cupmin", "brawl_cup_min")
    s = s.replace("clashcupmin", "clash_cup_min")
    s = s.replace("clashcupmax", "clash_cup_max")
    s = s.replace("orderby", "order_by")
    s = s.replace("order_by=pdate_to_down_upoad", "order_by=pdate_to_down_upload")
    s = s.replace("order_by=pdate_to_down_up", "order_by=pdate_to_down_upload")
    s = s.replace("order_by=pdate_to_downupload", "order_by=pdate_to_down_upload")
    return s


# ---------------------- ПЕР-ЮЗЕР ДАННЫЕ ----------------------
user_filters = defaultdict(lambda: {"title": None})
user_search_active = defaultdict(lambda: False)
user_seen_items = defaultdict(set)          # loaded from DB
user_buy_attempted = defaultdict(set)       # loaded from DB
user_hunter_tasks: dict[int, asyncio.Task] = {}
user_modes = defaultdict(lambda: None)      # None, "enter_admin_password", "title", "add_url"
user_started = set()
user_urls = defaultdict(list)               # loaded from DB
user_api_errors = defaultdict(int)


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


async def set_user_role(user_id: int, role: str):
    await db_set_role(user_id, role)
    await load_user_data(user_id, force=True)


async def user_url_limit(user_id: int) -> int:
    role = await get_user_role(user_id)
    return MAX_URLS_PER_USER_LIMITED if role == "limited" else MAX_URLS_PER_USER_DEFAULT


async def user_hunter_interval(user_id: int) -> float:
    role = await get_user_role(user_id)
    extra = LIMITED_EXTRA_DELAY if role == "limited" else 0.0
    return HUNTER_INTERVAL_BASE + extra


# ---------------------- UI / КЛАВИАТУРЫ ----------------------
def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔎 Проверка"), KeyboardButton(text="📚 URL")],
            [KeyboardButton(text="➕ Добавить"), KeyboardButton(text="🔤 Фильтр")],
            [KeyboardButton(text="🧹 Сброс фильтра"), KeyboardButton(text="♻️ Сброс истории")],
            [KeyboardButton(text="🚀 Старт"), KeyboardButton(text="🛑 Стоп")],
            [KeyboardButton(text="📊 Статус"), KeyboardButton(text="💰 Баланс")],
            [KeyboardButton(text="ℹ️ Помощь"), KeyboardButton(text="🏠 Меню")],
        ],
        resize_keyboard=True,
    )


def build_urls_list_kb_sync(sources: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    for idx, source in enumerate(sources):
        url = source["url"]
        enabled = source.get("enabled", True)
        autobuy = source.get("autobuy", False)

        label = url if len(url) <= URL_LABEL_MAX else url[:URL_LABEL_MAX - 3] + "..."
        st = "🟢" if enabled else "🔴"
        ab = "🛒" if autobuy else "—"

        rows.append([InlineKeyboardButton(text=f"{st} #{idx+1} | {ab} | {label}", callback_data="noop")])
        rows.append([
            InlineKeyboardButton(text="✅ Тест", callback_data=f"testurl:{idx}"),
            InlineKeyboardButton(text=("🔁 Выкл" if enabled else "🔁 Вкл"), callback_data=f"togurl:{idx}"),
            InlineKeyboardButton(text=("🛒 Автобай: Вкл" if not autobuy else "🛒 Автобай: Выкл"), callback_data=f"autobuyurl:{idx}"),
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delurl:{idx}"),
        ])

    rows.append([InlineKeyboardButton(text="❌ Закрыть", callback_data="noop")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def build_urls_list_kb(user_id: int) -> InlineKeyboardMarkup:
    sources = await get_all_sources(user_id)
    return build_urls_list_kb_sync(sources)


START_INFO = (
    "<b>✨ Parsing Bot</b>\n"
    "Мониторинг лотов + автобай по выбранным URL.\n\n"
    "<b>Логика:</b>\n"
    "• В чат бот отправляет <b>только новые лоты</b>\n"
    "• Автобай может пытаться купить <b>и старые</b> (при старте охотника)\n\n"
    "<b>Кнопки:</b> 📚 URL → включение/выключение/автобай/тест"
)

HELP_TEXT = (
    "<b>ℹ️ Помощь</b>\n\n"
    "1) ➕ Добавить — вставь API URL (api.lzt.market / api.lolz.live / prod-api.lzt.market)\n"
    "2) 📚 URL — включай/выключай и включай 🛒 автобай по конкретному URL\n"
    "3) 🚀 Старт — бот начнет мониторинг\n\n"
    "<b>Важно:</b>\n"
    "• В чат приходят только <u>новые</u> лоты.\n"
    "• Но 🛒 автобай при старте может пройтись по текущей выдаче и попробовать купить старые.\n"
)

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


# ---------------------- ИСТОЧНИКИ ----------------------
async def get_all_sources(user_id: int, enabled_only: bool = False):
    await load_user_data(user_id)

    # защита от дублей
    deduped: list[dict] = []
    seen = set()
    for source in user_urls[user_id]:
        url = source.get("url")
        if not url or url in seen:
            continue
        seen.add(url)
        deduped.append(source)
    user_urls[user_id] = deduped

    if enabled_only:
        return [s for s in user_urls[user_id] if s.get("enabled", True)]
    return user_urls[user_id]


async def fetch_all_sources(user_id: int):
    sources = await get_all_sources(user_id, enabled_only=True)
    results: list[tuple[dict, dict]] = []
    errors: list[tuple[str, str]] = []

    for idx, source in enumerate(sources):
        url = source["url"]
        source_info = {
            "idx": idx + 1,
            "url": url,
            "enabled": source.get("enabled", True),
            "autobuy": source.get("autobuy", False),
            "label": f"URL #{idx+1}",
        }

        items, err = await fetch_with_retry(url)
        if err:
            errors.append((url, err))
            continue

        for it in items:
            results.append((it, source_info))

    return results, errors


# ---------------------- ФИЛЬТРЫ ----------------------
def passes_filters(item: dict, user_id: int) -> bool:
    f = user_filters[user_id]
    if f["title"]:
        title = (item.get("title") or "").lower()
        if f["title"].lower() not in title:
            return False
    return True


# ---------------------- ОТОБРАЖЕНИЕ ----------------------
def format_seller(seller):
    if not seller:
        return None
    if isinstance(seller, str):
        return seller
    if isinstance(seller, dict):
        username = seller.get("username") or seller.get("user") or seller.get("name")
        sold = seller.get("sold_items_count")
        restore = seller.get("restore_percents")

        parts = []
        if username:
            parts.append(f"👤 <b>{html.escape(str(username))}</b>")
        if sold is not None:
            parts.append(f"📦 {sold} продаж")
        if restore is not None:
            parts.append(f"🛠 {restore}%")

        return " | ".join(parts) if parts else None
    return str(seller)


def make_card(item: dict, source_label: str) -> str:
    title = item.get("title", "Без названия")
    price = item.get("price", "—")
    item_id = item.get("item_id", item.get("id", "—"))

    trophies = item.get("trophies") or item.get("cups") or item.get("brawl_cup") or None
    level = item.get("level") or item.get("lvl") or item.get("user_level") or None
    townhall = item.get("townhall") or item.get("th") or None
    guarantee = item.get("guarantee") or item.get("warranty") or None
    phone_bound = item.get("phone_bound") or item.get("phone")
    seller = format_seller(item.get("seller") or item.get("user") or item.get("owner"))
    created = item.get("created_at") or item.get("date") or item.get("added_at")

    flags = []
    if item.get("discount") or item.get("sale") or item.get("discount_percent"):
        flags.append("🔥 скидка")
    if item.get("phone_bound") or item.get("phone"):
        flags.append("📱 phone")
    if item.get("guarantee") or item.get("warranty"):
        flags.append("🛡 гарантия")

    lines = [
        "━━━━━━━━━━━━━━━━━━━━",
        f"🔎 <b>{html.escape(source_label)}</b>",
        f"🎮 <b>{html.escape(str(title))}</b>",
    ]

    if level:
        lines.append(f"🔼 Уровень: <b>{html.escape(str(level))}</b>")
    if trophies:
        lines.append(f"🏆 Кубков: <b>{html.escape(str(trophies))}</b>")
    if townhall:
        lines.append(f"🏰 Ратуша: <b>{html.escape(str(townhall))}</b>")
    if created:
        lines.append(f"🗓 {html.escape(str(created))}")
    if seller:
        lines.append(seller)
    if flags:
        lines.append("🏷 " + " • ".join(flags))
    if guarantee:
        lines.append(f"🛡 {html.escape(str(guarantee))}")
    if phone_bound is not None:
        lines.append(f"📱 Телефон: <b>{'Да' if phone_bound else 'Нет'}</b>")

    lines.append(f"💰 <b>{html.escape(str(price))} ₽</b>" if price != "—" else "💰 —")
    lines.append(f"🆔 <code>{html.escape(str(item_id))}</code>")
    lines.append("━━━━━━━━━━━━━━━━━━━━")

    card = "\n".join(lines)
    if len(card) > SHORT_CARD_MAX:
        return card[: SHORT_CARD_MAX - 120] + "\n… <i>(обрезано)</i>"
    return card


def make_kb(item: dict) -> InlineKeyboardMarkup | None:
    iid = item.get("item_id") or item.get("id")
    if not iid:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Открыть лот", url=f"https://lzt.market/{iid}")]
    ])


# ---------------------- АВТОПОКУПКА ----------------------
def _autobuy_payload_variants(item: dict):
    price = item.get("price")
    payload: dict = {}

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
        {**payload, "buy_without_validation": 1},
        {**payload, "confirm": 1, "is_confirmed": True},
        {**payload, "fast_buy": 1, "instant_buy": 1},
        {k: v for k, v in payload.items() if k not in {"price", "item_price", "amount"}},
    ]

    dedup = []
    seen = set()
    for v in variants:
        frozen = tuple(sorted(v.items()))
        if frozen in seen:
            continue
        seen.add(frozen)
        dedup.append(v)
    return dedup


def _autobuy_buy_urls(source_url: str, item_id: int):
    # Основной рекомендуемый хост + фоллбеки
    base_hosts = ["https://prod-api.lzt.market"]

    su = (source_url or "").lower()
    if "api.lolz.live" in su:
        base_hosts.append("https://api.lolz.live")
    base_hosts.append("https://api.lzt.market")

    # если URL был от другого api-хоста — попробуем и его
    try:
        parts = urlsplit(source_url)
        if parts.scheme and parts.netloc:
            base_hosts.append(f"{parts.scheme}://{parts.netloc}")
    except Exception:
        pass

    # дедуп
    dedup_bases = []
    sb = set()
    for b in base_hosts:
        if b in sb:
            continue
        sb.add(b)
        dedup_bases.append(b)

    paths = [
        "{id}/fast-buy",
        "{id}/buy",
        "{id}/purchase",
        "{id}/check-account",
        "market/{id}/fast-buy",
        "market/{id}/buy",
        "item/{id}/fast-buy",
        "item/{id}/buy",
        "items/{id}/fast-buy",
        "items/{id}/buy",
    ]

    urls = []
    seen = set()
    for base in dedup_bases:
        for tpl in paths:
            u = f"{base}/{tpl.format(id=item_id)}"
            if u in seen:
                continue
            seen.add(u)
            urls.append(u)
    return urls


def _autobuy_classify_response(status: int, text: str):
    text = html.unescape(text or "")
    lower = text.lower()

    if "retry_request" in lower:
        return "retry_request", text[:400]

    success_markers = ("success", "ok", "purchased", "already bought", "already purchased", "уже куп")
    terminal_error_markers = (
        "insufficient", "not enough", "недостаточно",
        "already sold", "уже продан", "цена изменилась", "нельзя купить"
    )

    if status in (404, 405):
        return "retry", text[:400]
    if status in (200, 201, 202):
        return "success", text[:400]
    if status in (401, 403):
        return "auth", text[:400]
    if "secret" in lower or "answer" in lower or "секрет" in lower:
        return "secret", text[:400]
    if any(m in lower for m in success_markers):
        return "success", text[:400]
    if any(m in lower for m in terminal_error_markers):
        return "terminal", text[:400]
    return "retry", text[:400]


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

    session = await get_session()

    MAX_RETRY_REQUEST = 40
    RETRY_REQUEST_DELAY = 0.25

    last_err = "unknown"
    try:
        for buy_url in buy_urls:
            for payload in payload_variants:
                retry_req_count = 0
                while True:
                    async with session.post(buy_url, headers=headers, json=payload, timeout=FETCH_TIMEOUT) as resp:
                        body = await resp.text()
                        state, info = _autobuy_classify_response(resp.status, body)

                        if state == "success":
                            return True, f"{buy_url} -> {info}"
                        if state == "auth":
                            return False, f"{buy_url} -> HTTP {resp.status}: проверь API ключ/права ({info})"
                        if state == "secret":
                            last_err = f"{buy_url} -> нужен/неверный ответ на секретный вопрос ({info})"
                            break
                        if state == "terminal":
                            return False, f"{buy_url} -> {info}"

                        if state == "retry_request":
                            retry_req_count += 1
                            if retry_req_count >= MAX_RETRY_REQUEST:
                                last_err = f"{buy_url} -> слишком много retry_request ({info})"
                                break
                            await asyncio.sleep(RETRY_REQUEST_DELAY)
                            continue

                        last_err = f"{buy_url} -> HTTP {resp.status}: {info}"
                        break

        return False, last_err
    except Exception as e:
        return False, str(e)


# ---------------------- ПРОВЕРКА 10 ЛОТОВ ----------------------
async def send_compact_10_for_user(user_id: int, chat_id: int):
    items_with_sources, errors = await fetch_all_sources(user_id)

    if errors:
        for url, err in errors:
            await send_bot_message(chat_id, f"❗ <b>Ошибка</b>\n<code>{html.escape(url)}</code>\n{html.escape(str(err))}", parse_mode="HTML")

    if not items_with_sources:
        await send_bot_message(chat_id, "❗ Ничего не найдено по активным URL.")
        return

    aggregated = {}
    for item, source in items_with_sources:
        key = make_item_key(item)
        if key not in aggregated:
            aggregated[key] = (item, source)

    items_list = list(aggregated.values())
    limited = items_list[:10]

    enabled_count = len(await get_all_sources(user_id, enabled_only=True))
    await send_bot_message(
        chat_id,
        f"✅ <b>Проверка</b>\n"
        f"• Уникальных лотов: <b>{len(items_list)}</b>\n"
        f"• Показано: <b>{len(limited)}</b>\n"
        f"• Активных URL: <b>{enabled_count}</b>",
        parse_mode="HTML",
    )

    for item, source in limited:
        if not passes_filters(item, user_id):
            continue
        card = make_card(item, source["label"])
        kb = make_kb(item)
        await send_bot_message(chat_id, card, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
        await asyncio.sleep(0.2)


# ---------------------- ТЕСТ URL ----------------------
async def send_test_for_single_url(user_id: int, chat_id: int, url: str, label: str):
    items, err = await fetch_with_retry(url, max_retries=2)
    if err:
        await send_bot_message(chat_id, f"❗ <b>Ошибка теста</b>\n<b>{html.escape(label)}</b>\n<code>{html.escape(url)}</code>\n{html.escape(str(err))}", parse_mode="HTML")
        return
    if not items:
        await send_bot_message(chat_id, f"⚠️ <b>{html.escape(label)}</b>: пусто.", parse_mode="HTML")
        return

    aggregated = {}
    for item in items:
        key = make_item_key(item)
        if key not in aggregated:
            aggregated[key] = item
    items_list = list(aggregated.values())
    limited = items_list[:10]

    await send_bot_message(
        chat_id,
        f"✅ <b>Тест URL</b> — {html.escape(label)}\n"
        f"• Уникальных лотов: <b>{len(items_list)}</b>\n"
        f"• Показано: <b>{len(limited)}</b>",
        parse_mode="HTML"
    )

    for item in limited:
        if not passes_filters(item, user_id):
            continue
        card = make_card(item, label)
        kb = make_kb(item)
        await send_bot_message(chat_id, card, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
        await asyncio.sleep(0.2)


# ---------------------- ОХОТНИК ----------------------
async def autobuy_sweep_existing(user_id: int, chat_id: int):
    """
    При старте охотника:
      - чат НЕ спамим старыми лотами
      - но если на URL включён 🛒 автобай — пробуем купить текущую выдачу (старые тоже)
      - buy_attempted защитит от бесконечных повторов
    """
    items_with_sources, _ = await fetch_all_sources(user_id)

    # дедуп по лоту
    aggregated = {}
    for item, source in items_with_sources:
        key = make_item_key(item)
        if key not in aggregated:
            aggregated[key] = (item, source)

    for item, source in aggregated.values():
        key = make_item_key(item)

        # автобай: старые тоже
        if source.get("autobuy", False) and key not in user_buy_attempted[user_id]:
            user_buy_attempted[user_id].add(key)
            await db_mark_buy_attempted(user_id, key)

            bought, info = await try_autobuy_item(source, item)
            if bought:
                await send_bot_message(chat_id, f"🛒 <b>Автобай (старые)</b> ✅\n<b>{html.escape(source['label'])}</b>\n<code>{html.escape(str(item.get('item_id') or item.get('id')))}</code>", parse_mode="HTML")
            else:
                # не спамим сильно — только если реально важно
                if "auth" in (info or "").lower() or "secret" in (info or "").lower():
                    await send_bot_message(chat_id, f"⚠️ <b>Автобай</b> ({html.escape(source['label'])})\n{html.escape(str(info))}", parse_mode="HTML")

        # seen: чтобы в чат шли только новые
        user_seen_items[user_id].add(key)
        await db_mark_seen(user_id, key)


async def hunter_loop_for_user(user_id: int, chat_id: int):
    await load_user_data(user_id)

    # 1) первичная отметка + автобай старых
    try:
        await autobuy_sweep_existing(user_id, chat_id)
    except Exception:
        pass

    # 2) цикл новых
    while user_search_active[user_id]:
        try:
            items_with_sources, errors = await fetch_all_sources(user_id)

            if errors:
                user_api_errors[user_id] += len(errors)

            for item, source in items_with_sources:
                key = make_item_key(item)

                # если уже видели — пропуск
                if key in user_seen_items[user_id]:
                    continue

                # фильтр на уведомления
                if not passes_filters(item, user_id):
                    user_seen_items[user_id].add(key)
                    await db_mark_seen(user_id, key)
                    continue

                # сначала автобай, если включен на конкретном URL
                if source.get("autobuy", False) and key not in user_buy_attempted[user_id]:
                    user_buy_attempted[user_id].add(key)
                    await db_mark_buy_attempted(user_id, key)

                    bought, buy_info = await try_autobuy_item(source, item)
                    if bought:
                        await send_bot_message(
                            chat_id,
                            f"🛒 <b>Автобай</b> ✅ ({html.escape(source['label'])})\n"
                            f"🆔 <code>{html.escape(str(item.get('item_id') or item.get('id')))}</code>",
                            parse_mode="HTML",
                        )
                    else:
                        # тихо, но если критика — скажем
                        if any(x in (buy_info or "").lower() for x in ["auth", "secret", "403", "401"]):
                            await send_bot_message(
                                chat_id,
                                f"⚠️ <b>Автобай</b> ({html.escape(source['label'])})\n{html.escape(str(buy_info))}",
                                parse_mode="HTML",
                            )

                # отметить как увиденный + отправить карточку (новый лот)
                user_seen_items[user_id].add(key)
                await db_mark_seen(user_id, key)

                card = make_card(item, source["label"])
                kb = make_kb(item)
                await send_bot_message(chat_id, card, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
                await asyncio.sleep(0.2)

            await asyncio.sleep(await user_hunter_interval(user_id))

        except asyncio.CancelledError:
            break
        except Exception:
            user_api_errors[user_id] += 1
            await asyncio.sleep(await user_hunter_interval(user_id))


# ---------------------- ОТЧЁТ ОШИБОК ----------------------
async def error_reporter_loop():
    while True:
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


# ---------------------- CALLBACKS ----------------------
@dp.callback_query()
async def handle_callbacks(call: types.CallbackQuery):
    data = call.data or ""
    user_id = call.from_user.id
    await load_user_data(user_id)

    if data == "noop":
        await call.answer()
        return

    if data.startswith("delurl:"):
        idx = int(data.split(":", 1)[1])
        sources = await get_all_sources(user_id)
        if 0 <= idx < len(sources):
            removed = sources.pop(idx)
            await db_remove_url(user_id, removed["url"])
            user_urls[user_id] = sources
            await call.message.edit_text(f"🗑 Удалён URL:\n<code>{html.escape(removed['url'])}</code>", parse_mode="HTML")
            await call.answer("Удалено")
            return
        await call.answer("Некорректный индекс", show_alert=True)
        return

    if data.startswith("togurl:"):
        idx = int(data.split(":", 1)[1])
        sources = await get_all_sources(user_id)
        if 0 <= idx < len(sources):
            src = sources[idx]
            new_enabled = not src.get("enabled", True)
            src["enabled"] = new_enabled
            await db_set_url_enabled(user_id, src["url"], new_enabled)
            kb = build_urls_list_kb_sync(sources)
            await call.message.edit_reply_markup(reply_markup=kb)
            await call.answer("Включено" if new_enabled else "Выключено")
            return
        await call.answer("Некорректный индекс", show_alert=True)
        return

    if data.startswith("autobuyurl:"):
        idx = int(data.split(":", 1)[1])
        sources = await get_all_sources(user_id)
        if 0 <= idx < len(sources):
            src = sources[idx]
            new_ab = not src.get("autobuy", False)
            src["autobuy"] = new_ab
            await db_set_url_autobuy(user_id, src["url"], new_ab)
            kb = build_urls_list_kb_sync(sources)
            await call.message.edit_reply_markup(reply_markup=kb)
            await call.answer("Автобай включён" if new_ab else "Автобай выключен")
            return
        await call.answer("Некорректный индекс", show_alert=True)
        return

    if data.startswith("testurl:"):
        idx = int(data.split(":", 1)[1])
        sources = await get_all_sources(user_id)
        if 0 <= idx < len(sources):
            src = sources[idx]
            status = "ВКЛ" if src.get("enabled", True) else "ВЫКЛ"
            label = f"URL #{idx+1} ({status})"
            await call.answer("Тестирую...")
            await send_test_for_single_url(user_id, call.message.chat.id, src["url"], label)
            return
        await call.answer("Некорректный индекс", show_alert=True)
        return

    await call.answer()


# ---------------------- COMMANDS ----------------------
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = message.from_user.id
    await load_user_data(user_id, force=True)
    await message.answer(START_INFO, parse_mode="HTML")
    await message.answer("🏠 <b>Меню</b>", parse_mode="HTML", reply_markup=main_kb())
    await safe_delete(message)


@dp.message(Command("status"))
async def status_cmd(message: types.Message):
    user_id = message.from_user.id
    await load_user_data(user_id)

    role = await get_user_role(user_id) or "not set"
    active = user_search_active[user_id]
    f = user_filters[user_id]
    total = len(await get_all_sources(user_id))
    enabled = len(await get_all_sources(user_id, enabled_only=True))
    seen = len(user_seen_items[user_id])
    ab = sum(1 for s in await get_all_sources(user_id) if s.get("autobuy", False))
    balance = await db_get_balance(user_id)

    text = (
        "<b>📊 Статус</b>\n"
        f"• Роль: <b>{html.escape(role)}</b>\n"
        f"• Охотник: <b>{'ВКЛ' if active else 'ВЫКЛ'}</b>\n"
        f"• URL: <b>{enabled}/{total}</b> (автобай: <b>{ab}</b>)\n"
        f"• Увидено лотов: <b>{seen}</b>\n"
        f"• Фильтр: <b>{html.escape(f['title']) if f['title'] else 'нет'}</b>\n"
        f"• Баланс: <b>{format_balance(balance)}</b>\n"
        f"• Ошибок API: <b>{user_api_errors.get(user_id, 0)}</b>"
    )
    await message.answer(text, parse_mode="HTML")
    await safe_delete(message)


# ---------------------- BUTTONS HANDLER ----------------------
@dp.message()
async def buttons_handler(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    await load_user_data(user_id)

    text = (message.text or "").strip()
    mode = user_modes[user_id]

    try:
        # режимы ввода
        if mode == "enter_admin_password":
            user_modes[user_id] = None
            if text == ADMIN_PASSWORD:
                await set_user_role(user_id, "admin")
                await message.answer("✅ Пароль верный. Роль администратора активирована.")
            else:
                await message.answer("❌ Неверный пароль.")
            return await safe_delete(message)

        if mode == "title":
            user_filters[user_id]["title"] = text or None
            user_modes[user_id] = None
            await message.answer(f"✅ Фильтр: <b>{html.escape(text)}</b>" if text else "✅ Фильтр снят", parse_mode="HTML")
            return await safe_delete(message)

        if mode == "add_url":
            user_modes[user_id] = None

            raw = text
            url = normalize_url(raw)

            ok, err = validate_market_url(url)
            if not ok:
                await message.answer(err)
                return await safe_delete(message)

            limit = await user_url_limit(user_id)
            if len(await get_all_sources(user_id)) >= limit:
                await message.answer(f"❌ Лимит URL для вашей роли: {limit}")
                return await safe_delete(message)

            # тест на добавлении (мягкий)
            items, api_err = await fetch_with_retry(url, max_retries=2)
            if api_err:
                await message.answer(f"❌ API ошибка: {api_err}")
                return await safe_delete(message)
            _ = items

            if any(s["url"] == url for s in await get_all_sources(user_id)):
                await message.answer("⚠️ Такой URL уже добавлен.")
                return await safe_delete(message)

            user_urls[user_id].append({"url": url, "enabled": True, "autobuy": False})
            await db_add_url(user_id, url)
            await message.answer(f"✅ URL добавлен:\n<code>{html.escape(url)}</code>", parse_mode="HTML")
            return await safe_delete(message)

        # команды кнопками
        if text in ("🏠 Меню",):
            return await message.answer("🏠 <b>Меню</b>", parse_mode="HTML", reply_markup=main_kb())

        if text in ("ℹ️ Помощь",):
            return await message.answer(HELP_TEXT, parse_mode="HTML")

        if text in ("🔎 Проверка",):
            return await send_compact_10_for_user(user_id, chat_id)

        if text in ("📚 URL",):
            kb = await build_urls_list_kb(user_id)
            return await message.answer("📚 <b>Ваши URL</b>\n(🟢/🔴 — включен/выключен, 🛒 — автобай)", parse_mode="HTML", reply_markup=kb)

        if text in ("➕ Добавить",):
            user_modes[user_id] = "add_url"
            return await message.answer("Вставь API URL (api.lzt.market / api.lolz.live / prod-api.lzt.market):")

        if text in ("🔤 Фильтр",):
            user_modes[user_id] = "title"
            return await message.answer("Введи слово/фразу. Будут приходить только лоты где это есть в названии:")

        if text in ("🧹 Сброс фильтра",):
            user_filters[user_id]["title"] = None
            user_modes[user_id] = None
            return await message.answer("✅ Фильтр снят.")

        if text in ("♻️ Сброс истории",):
            # сбрасываем seen + buy_attempted чтобы можно было заново
            user_seen_items[user_id].clear()
            user_buy_attempted[user_id].clear()
            await db_clear_seen(user_id)
            await db_clear_buy_attempted(user_id)
            return await message.answer("♻️ История сброшена. Теперь текущие лоты снова будут считаться новыми (и автобай снова сможет пытаться).")

        if text in ("🚀 Старт",):
            active_sources = await get_all_sources(user_id, enabled_only=True)
            if not active_sources:
                return await message.answer("❌ Нет активных URL. Добавь или включи URL в 📚 URL.")

            if not user_search_active[user_id]:
                user_search_active[user_id] = True
                user_seen_items[user_id] = await db_load_seen(user_id)
                user_buy_attempted[user_id] = await db_load_buy_attempted(user_id)

                task = asyncio.create_task(hunter_loop_for_user(user_id, chat_id))
                user_hunter_tasks[user_id] = task

                return await message.answer(
                    f"🚀 Охотник запущен!\n"
                    f"• Активных URL: <b>{len(active_sources)}</b>\n"
                    f"• Пинг: <b>{await user_hunter_interval(user_id):.1f}s</b>",
                    parse_mode="HTML",
                )
            return await message.answer("⚠️ Охотник уже запущен.")

        if text in ("🛑 Стоп",):
            user_search_active[user_id] = False
            task = user_hunter_tasks.get(user_id)
            if task:
                task.cancel()
            return await message.answer("🛑 Охотник остановлен.")

        if text in ("💰 Баланс",):
            balance = await db_get_balance(user_id)
            # просто показываем (если хочешь — можно позже добавить пополнение как кнопки)
            return await message.answer(f"💰 Ваш баланс: <b>{format_balance(balance)}</b>", parse_mode="HTML")

        if text in ("📊 Статус",):
            # прокинем в /status
            fake = types.Message(
                message_id=message.message_id,
                date=message.date,
                chat=message.chat,
                from_user=message.from_user,
                sender_chat=message.sender_chat,
                text="/status"
            )
            return await status_cmd(fake)

        # авто-удаление мусора (как у тебя было)
        if text and not text.startswith("/"):
            await asyncio.sleep(0.35)
            await safe_delete(message)

    except Exception as e:
        await send_bot_message(chat_id, f"❌ Ошибка: {html.escape(str(e))}", parse_mode="HTML")
        await safe_delete(message)


# ---------------------- RUN ----------------------
async def main():
    global bot
    print("[BOT] Starting: multiuser, persistent seen, URL management, autobuy, retry/backoff...")

    if not has_valid_telegram_token(API_TOKEN):
        raise RuntimeError("Некорректный API_TOKEN: бот не может быть запущен")

    bot = Bot(token=API_TOKEN)

    await init_db()

    # reporter
    asyncio.create_task(error_reporter_loop())

    try:
        await dp.start_polling(bot)
    finally:
        await close_session()
        if bot is not None and getattr(bot, "session", None) is not None and not bot.session.closed:
            await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
