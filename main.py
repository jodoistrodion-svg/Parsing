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
    WebAppInfo,
)
from aiohttp import web

from config import API_TOKEN, LZT_API_KEY

bot: Bot | None = None
dp = Dispatcher()

# ---------------------- НАСТРОЙКИ ----------------------
HUNTER_INTERVAL_BASE = 1.0
SHORT_CARD_MAX = 900
URL_LABEL_MAX = 60
ERROR_REPORT_INTERVAL = 3600  # seconds (1 hour)

MAX_URLS_PER_USER_DEFAULT = 50
MAX_URLS_PER_USER_LIMITED = 3

MAX_CONCURRENT_REQUESTS = 6
FETCH_TIMEOUT = 12
RETRY_MAX = 4
RETRY_BASE_DELAY = 1.0  # seconds

ADMIN_PASSWORD = "1303"
LIMITED_EXTRA_DELAY = 3.0  # seconds added for limited users

DB_FILE = "bot_data.sqlite"

WEBAPP_HOST = "0.0.0.0"
WEBAPP_PORT = 8080

# секретное слово (по умолчанию "Мазда")
LZT_SECRET_WORD = (os.getenv("LZT_SECRET_WORD") or "Мазда").strip()

MINI_APP_TITLE = "Parsing Bot · Mini App"
WEBAPP_PUBLIC_URL = (
    os.getenv("WEBAPP_PUBLIC_URL")
    or os.getenv("RENDER_EXTERNAL_URL")
    or os.getenv("RAILWAY_PUBLIC_DOMAIN")
    or ""
).rstrip("/")


def has_valid_telegram_token(token: str) -> bool:
    if not token:
        return False
    return bool(re.match(r"^\d{6,12}:[A-Za-z0-9_-]{20,}$", token))


async def send_bot_message(chat_id: int, text: str, **kwargs):
    if bot is None:
        raise RuntimeError("Bot не инициализирован")
    return await bot.send_message(chat_id, text, **kwargs)


# ---------------------- АИО-SQLITE (асинхронная БД) ----------------------
async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
        CREATE TABLE IF NOT EXISTS urls (
            user_id INTEGER,
            url TEXT,
            added_at INTEGER,
            enabled INTEGER DEFAULT 1,
            autobuy INTEGER DEFAULT 0,
            PRIMARY KEY(user_id, url)
        )
        """
        )
        cur = await db.execute("PRAGMA table_info(urls)")
        cols = [row[1] for row in await cur.fetchall()]
        if "enabled" not in cols:
            await db.execute("ALTER TABLE urls ADD COLUMN enabled INTEGER DEFAULT 1")
        if "autobuy" not in cols:
            await db.execute("ALTER TABLE urls ADD COLUMN autobuy INTEGER DEFAULT 0")

        await db.execute(
            """
        CREATE TABLE IF NOT EXISTS seen (
            user_id INTEGER,
            item_key TEXT,
            seen_at INTEGER,
            PRIMARY KEY(user_id, item_key)
        )
        """
        )

        # чтобы не пытаться купить один и тот же лот бесконечно
        await db.execute(
            """
        CREATE TABLE IF NOT EXISTS buy_attempted (
            user_id INTEGER,
            item_key TEXT,
            attempted_at INTEGER,
            PRIMARY KEY(user_id, item_key)
        )
        """
        )

        await db.execute(
            """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            role TEXT DEFAULT 'unknown',
            last_error_report INTEGER DEFAULT 0,
            balance REAL DEFAULT 0
        )
        """
        )
        cur = await db.execute("PRAGMA table_info(users)")
        user_cols = [row[1] for row in await cur.fetchall()]
        if "balance" not in user_cols:
            await db.execute("ALTER TABLE users ADD COLUMN balance REAL DEFAULT 0")

        await db.commit()


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
        cur = await db.execute(
            "SELECT item_key FROM buy_attempted WHERE user_id=?", (user_id,)
        )
        rows = await cur.fetchall()
        return {r[0] for r in rows}


async def db_clear_buy_attempted(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM buy_attempted WHERE user_id=?", (user_id,))
        await db.commit()


async def db_ensure_user(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users(user_id, role, last_error_report, balance) VALUES (?, ?, ?, ?)",
            (user_id, "unknown", 0, 0),
        )
        await db.commit()


async def db_get_role(user_id: int):
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


async def db_get_last_report(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT last_error_report FROM users WHERE user_id=?", (user_id,)
        )
        row = await cur.fetchone()
        return row[0] if row else 0


async def db_set_last_report(user_id: int, ts: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "UPDATE users SET last_error_report=? WHERE user_id=?",
            (ts, user_id),
        )
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


# ---------------------- URL ----------------------
def validate_market_url(url: str):
    """
    Разрешаем API ссылки:
      - api.lzt.market
      - api.lolz.live
      - prod-api.lzt.market
    """
    if not url.startswith(("http://", "https://")):
        return False, "❌ Это не похоже на URL."
    lower = url.lower()
    if not (
        "api.lzt.market/" in lower
        or "api.lolz.live/" in lower
        or "prod-api.lzt.market/" in lower
    ):
        return (
            False,
            "❌ Нужна ссылка на API LZT (prod-api.lzt.market / api.lzt.market / api.lolz.live).",
        )
    return True, None


def normalize_url(url: str) -> str:
    if not url:
        return url
    s = url.strip()
    s = s.replace(" ", "").replace("\t", "").replace("\n", "")

    # НЕ трогаем prod-api, если пользователь уже его указал
    if "prod-api.lzt.market" not in s.lower():
        s = re.sub(r"https?://api.*?\.market", "https://api.lzt.market", s)
        s = re.sub(r"https?://api\.lolz\.guru", "https://api.lzt.market", s)
        s = s.replace("://lzt.market", "://api.lzt.market")
        s = s.replace("://www.lzt.market", "://api.lzt.market")

    # фикс частых опечаток в параметрах
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


# ---------------------- ПЕР-ЮЗЕР ДАННЫЕ ----------------------
user_filters = defaultdict(lambda: {"title": None})
user_search_active = defaultdict(lambda: False)
user_seen_items = defaultdict(set)          # loaded from DB
user_buy_attempted = defaultdict(set)       # loaded from DB

user_hunter_tasks: dict[int, asyncio.Task] = {}
user_modes = defaultdict(lambda: None)      # None, "enter_admin_password", "title", "add_url"
user_started = set()
user_urls = defaultdict(list)              # loaded from DB
user_api_errors = defaultdict(int)


async def load_user_data(user_id: int, force: bool = False):
    if user_id in user_started and not force:
        return
    await db_ensure_user(user_id)
    user_urls[user_id] = await db_get_urls(user_id)
    user_seen_items[user_id] = await db_load_seen(user_id)
    user_buy_attempted[user_id] = await db_load_buy_attempted(user_id)
    user_started.add(user_id)


async def get_user_role(user_id: int):
    await load_user_data(user_id)
    role = await db_get_role(user_id)
    if role == "unknown":
        return None
    return role


async def set_user_role(user_id: int, role: str):
    await db_set_role(user_id, role)
    await load_user_data(user_id, force=True)


async def user_url_limit(user_id: int):
    role = await get_user_role(user_id)
    if role == "limited":
        return MAX_URLS_PER_USER_LIMITED
    return MAX_URLS_PER_USER_DEFAULT


async def user_hunter_interval(user_id: int):
    role = await get_user_role(user_id)
    extra = LIMITED_EXTRA_DELAY if role == "limited" else 0.0
    return HUNTER_INTERVAL_BASE + extra


def format_balance(amount: float) -> str:
    return f"{amount:,.2f} ₽".replace(",", " ")


def mini_app_url(user_id: int) -> str | None:
    if not WEBAPP_PUBLIC_URL:
        return None
    base_url = WEBAPP_PUBLIC_URL
    if not base_url.startswith("http"):
        base_url = f"https://{base_url}"
    return f"{base_url}/mini-app?user_id={user_id}"


# ---------------------- КЛАВИАТУРА ----------------------
def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✨ Проверка лотов"), KeyboardButton(text="📚 Мои URL")],
            [KeyboardButton(text="➕ Добавить URL"), KeyboardButton(text="🔤 Фильтр")],
            [KeyboardButton(text="🧹 Очистить фильтры"), KeyboardButton(text="♻️ Сбросить историю")],
            [KeyboardButton(text="🚀 Старт охотника"), KeyboardButton(text="🛑 Стоп охотника")],
            [KeyboardButton(text="💎 Баланс"), KeyboardButton(text="📊 Краткий статус")],
            [KeyboardButton(text="🪄 Mini App")],
            [KeyboardButton(text="ℹ️ Помощь")],
            [KeyboardButton(text="🏠 Главное меню")],
        ],
        resize_keyboard=True,
    )


# ---------------------- ТЕКСТЫ ----------------------
START_INFO = (
    "<b>✨ Parsing Bot 2.1</b>\n"
    "Мониторинг лотов + Mini App.\n\n"
    "<b>Особенность автобая:</b>\n"
    "• уведомления — только <b>новые</b> лоты\n"
    "• автобай — пытается купить и <b>старые</b> (которые уже были в выдаче)"
)

COMMANDS_MENU = (
    "<b>🧭 Меню</b>\n\n"
    "• ✨ Проверка лотов — показать до 10 лотов\n"
    "• 📚 Мои URL — управление источниками (+ 🛒 автобай)\n"
    "• 🚀 Старт охотника — мониторинг\n"
    "• ♻️ Сбросить историю — чтобы снова считать лоты новыми\n"
)


# ---------------------- HTTP / API с retry ----------------------
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


# ---------------------- ПРОВЕРКА URL ПЕРЕД ДОБАВЛЕНИЕМ ----------------------
async def validate_url_before_add(url: str):
    ok, err = validate_market_url(url)
    if not ok:
        return False, err

    items, api_err = await fetch_with_retry(url, max_retries=2)
    if api_err:
        return False, f"❌ API ошибка: {api_err}"

    _ = items
    return True, None


# ---------------------- ИСТОЧНИКИ ----------------------
async def get_all_sources(user_id: int, enabled_only: bool = False):
    await load_user_data(user_id)

    deduped = []
    seen = set()
    for source in user_urls[user_id]:
        url = source.get("url")
        if not url or url in seen:
            continue
        seen.add(url)
        deduped.append(source)
    if deduped != user_urls[user_id]:
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


async def fetch_single_source(url: str):
    return await fetch_with_retry(url, max_retries=RETRY_MAX)


# ---------------------- ФИЛЬТРЫ ----------------------
def passes_filters(item: dict, user_id: int) -> bool:
    f = user_filters[user_id]
    if f["title"]:
        title = (item.get("title") or "").lower()
        if f["title"].lower() not in title:
            return False
    return True


# ---------------------- ВСПОМОГАТЕЛИ ----------------------
def format_seller(seller):
    if not seller:
        return None
    if isinstance(seller, str):
        return seller
    if isinstance(seller, dict):
        parts = []
        username = seller.get("username") or seller.get("user") or seller.get("name")
        if username:
            parts.append(f"👤 {username}")
        sold = seller.get("sold_items_count")
        if sold is not None:
            parts.append(f"📦 Продано: {sold}")
        active = seller.get("active_items_count")
        if active is not None:
            parts.append(f"🔸 Активных: {active}")
        restore = seller.get("restore_percents")
        if restore is not None:
            parts.append(f"🛠 Восстановление: {restore}%")
        if not parts:
            return str(seller)
        return " | ".join(parts)
    return str(seller)


def make_item_key(item: dict) -> str:
    iid = item.get("item_id") or item.get("id")
    if iid is not None:
        return f"id::{iid}"
    return f"noid::{item.get('title')}_{item.get('price')}"


def make_card(item: dict, source_label: str) -> str:
    title = item.get("title", "Без названия")
    price = item.get("price", "—")
    item_id = item.get("item_id", "—")

    trophies = item.get("trophies") or item.get("cups") or item.get("brawl_cup") or None
    level = item.get("level") or item.get("lvl") or item.get("user_level") or None
    townhall = item.get("townhall") or item.get("ratsha") or item.get("th") or None
    builder_village = item.get("builder_level") or item.get("bb_level") or None
    guarantee = item.get("guarantee") or item.get("warranty") or item.get("guarantee_text") or None
    phone_bound = item.get("phone_bound") or item.get("phone") or item.get("phone_bound_flag")
    seller_raw = item.get("seller") or item.get("user") or item.get("owner") or None
    seller = format_seller(seller_raw)
    created = item.get("created_at") or item.get("date") or item.get("added_at") or None

    extra_flags = []
    if item.get("discount") or item.get("sale") or item.get("discount_percent"):
        extra_flags.append("Скидка")
    if item.get("phone_bound") or item.get("phone"):
        extra_flags.append("Телефон привязан")
    if item.get("guarantee") or item.get("warranty"):
        extra_flags.append("Гарантия")

    lines = [
        "━━━━━━━━━━━━━━━━━━━━",
        f"🔎 <b>{source_label}</b>",
        f"🎮 <b>{html.escape(str(title))}</b>",
    ]

    if level:
        lines.append(f"🔼 Уровень: {html.escape(str(level))}")
    if trophies:
        lines.append(f"🏆 Кубков: {html.escape(str(trophies))}")
    if townhall:
        lines.append(f"🏰 Ратуша: {html.escape(str(townhall))}")
    if builder_village:
        lines.append(f"🔧 Деревня строителя: {html.escape(str(builder_village))}")
    if seller:
        lines.append(seller)
    if created:
        lines.append(f"📅 Добавлено: {html.escape(str(created))}")
    if extra_flags:
        lines.append("🔖 " + ", ".join(extra_flags))
    if guarantee:
        lines.append(f"🛡 {html.escape(str(guarantee))}")
    if phone_bound is not None:
        lines.append(f"📱 Телефон привязан: {'Да' if phone_bound else 'Нет'}")

    lines.append(f"💰 {html.escape(str(price))}₽" if price != "—" else "💰 —")
    lines.append(f"🆔 {html.escape(str(item_id))}")
    lines.append("━━━━━━━━━━━━━━━━━━━━")

    card = "\n".join(lines)
    if len(card) > SHORT_CARD_MAX:
        return card[: SHORT_CARD_MAX - 100] + "\n... (обрезано)"
    return card


def make_kb(item: dict) -> InlineKeyboardMarkup | None:
    iid = item.get("item_id")
    if not iid:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Открыть", url=f"https://lzt.market/{iid}")]]
    )


# ---------------------- ПРОВЕРКА 10 ЛОТОВ ----------------------
async def send_compact_10_for_user(user_id: int, chat_id: int):
    items_with_sources, errors = await fetch_all_sources(user_id)
    if errors:
        for url, err in errors:
            await send_bot_message(
                chat_id,
                f"❗ Ошибка {html.escape(url)}:\n{html.escape(str(err))}",
                parse_mode="HTML",
            )

    if not items_with_sources:
        await send_bot_message(chat_id, "❗ Ничего не найдено по всем источникам.")
        return

    aggregated = {}
    for item, source in items_with_sources:
        key = make_item_key(item)
        if key not in aggregated:
            aggregated[key] = (item, source)

    items_list = list(aggregated.values())
    limited = items_list[:10]

    await send_bot_message(
        chat_id,
        f"✅ Проверка\n"
        f"📦 Уникальных лотов: <b>{len(items_list)}</b>\n"
        f"📦 Показано: <b>{len(limited)}</b>\n"
        f"🔍 Активных URL: {len(await get_all_sources(user_id, enabled_only=True))}",
        parse_mode="HTML",
    )

    for item, source in limited:
        if not passes_filters(item, user_id):
            continue
        card = make_card(item, source["label"])
        kb = make_kb(item)
        try:
            await send_bot_message(chat_id, card, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
        except Exception:
            await send_bot_message(chat_id, card)
        await asyncio.sleep(0.2)


# ---------------------- ТЕСТ URL ----------------------
async def send_test_for_single_url(user_id: int, chat_id: int, url: str, label: str):
    items, err = await fetch_with_retry(url, max_retries=2)
    if err:
        await send_bot_message(
            chat_id,
            f"❗ Ошибка {html.escape(label)} ({html.escape(url)}):\n{html.escape(str(err))}",
            parse_mode="HTML",
        )
        return
    if not items:
        await send_bot_message(chat_id, f"❗ {html.escape(label)}: ничего не найдено.", parse_mode="HTML")
        return

    try:
        keys = list(items[0].keys())
        await send_bot_message(chat_id, f"🔍 Поля в первом лоте: {', '.join(keys)}")
    except Exception:
        pass

    aggregated = {}
    for item in items:
        key = make_item_key(item)
        if key not in aggregated:
            aggregated[key] = item

    items_list = list(aggregated.values())
    limited = items_list[:10]

    await send_bot_message(
        chat_id,
        f"✅ Тест URL ({html.escape(label)})\n"
        f"📦 Уникальных лотов: <b>{len(items_list)}</b>\n"
        f"📦 Показано: <b>{len(limited)}</b>",
        parse_mode="HTML",
    )

    for item in limited:
        if not passes_filters(item, user_id):
            continue
        card = make_card(item, label)
        kb = make_kb(item)
        try:
            await send_bot_message(chat_id, card, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
        except Exception:
            await send_bot_message(chat_id, card)
        await asyncio.sleep(0.2)


# ---------------------- АВТОПОКУПКА ----------------------
def _autobuy_payload_variants(item: dict):
    price = item.get("price")
    payload: dict = {}
    if price is not None:
        payload.update({"price": price, "item_price": price, "amount": price})

    if LZT_SECRET_WORD:
        payload.update(
            {
                "secret_answer": LZT_SECRET_WORD,
                "secret_word": LZT_SECRET_WORD,
                "secretWord": LZT_SECRET_WORD,
                "qa_answer": LZT_SECRET_WORD,
                "answer": LZT_SECRET_WORD,
            }
        )

    # иногда полезно покупать без валидации (оставляем как вариант)
    variants = [
        payload,
        {**payload, "buy_without_validation": 1},
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
    """
    Покупка по докам работает через prod-api:
      POST https://prod-api.lzt.market/{item_id}/fast-buy
      POST https://prod-api.lzt.market/{item_id}/check-account
    Оставляем fallback для старых окружений.
    """
    source_url = (source_url or "").strip().lower()

    base_hosts = ["https://prod-api.lzt.market"]
    if "api.lolz.live" in source_url:
        base_hosts.append("https://api.lolz.live")
    base_hosts.append("https://api.lzt.market")

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
        "{id}/check-account",
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

    success_markers = ("success", "ok", "purchased", "purchase complete", "already bought", "уже куп")
    terminal_error_markers = (
        "insufficient", "not enough", "недостаточно",
        "уже продан", "already sold",
        "already purchased", "already bought",
        "цена изменилась", "нельзя купить",
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

    # retry_request по доке — до 100, ограничим разумно
    MAX_RETRY_REQUEST = 40
    RETRY_REQUEST_DELAY = 0.25

    last_err = "unknown"

    try:
        for buy_url in buy_urls:
            for payload in payload_variants:
                retry_req_count = 0
                while True:
                    async with session.post(
                        buy_url, headers=headers, json=payload, timeout=FETCH_TIMEOUT
                    ) as resp:
                        body = await resp.text()
                        state, info = _autobuy_classify_response(resp.status, body)

                    if state == "success":
                        return True, f"{buy_url} -> {info}"
                    if state == "auth":
                        return False, f"{buy_url} -> HTTP {resp.status}: проверь API ключ/права ({info})"
                    if state == "secret":
                        last_err = f"{buy_url} -> секретный вопрос/ответ ({info})"
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


# ---------------------- ОХОТНИК ----------------------
async def autobuy_sweep_existing(user_id: int, chat_id: int, source: dict, items: list[dict], silent: bool = True):
    """
    Пытается купить ВСЕ текущие items (даже старые), но без отправки карточек.
    Чтобы не долбить один и тот же лот, использует buy_attempted.
    """
    bought_cnt = 0
    tried_cnt = 0

    for item in items:
        key = make_item_key(item)
        if key in user_buy_attempted[user_id]:
            continue

        user_buy_attempted[user_id].add(key)
        await db_mark_buy_attempted(user_id, key)
        tried_cnt += 1

        ok, info = await try_autobuy_item(source, item)
        if ok:
            bought_cnt += 1
            if not silent:
                await send_bot_message(
                    chat_id,
                    f"🛒 <b>Автопокупка успешна</b>\n{source['label']} | item_id=<b>{html.escape(str(item.get('item_id')))}</b>\n✅ {html.escape(str(info))}",
                    parse_mode="HTML",
                )

    if not silent:
        await send_bot_message(
            chat_id,
            f"🧾 Sweep {source['label']}: попыток <b>{tried_cnt}</b>, успешных <b>{bought_cnt}</b>",
            parse_mode="HTML",
        )


async def hunter_loop_for_user(user_id: int, chat_id: int):
    await load_user_data(user_id)

    # 1) первичная засветка:
    #    - уведомления не шлём
    #    - seen заполняем, чтобы дальше были только НОВЫЕ
    #    - но если autobuy ON, то пытаемся купить и старые (sweep), потом тоже помечаем seen
    try:
        sources = await get_all_sources(user_id, enabled_only=True)
        for idx, src in enumerate(sources):
            url = src["url"]
            source_info = {
                "idx": idx + 1,
                "url": url,
                "enabled": src.get("enabled", True),
                "autobuy": src.get("autobuy", False),
                "label": f"URL #{idx+1}",
            }
            items, err = await fetch_with_retry(url)
            if err or not items:
                continue

            if source_info.get("autobuy", False):
                # купить старые (без карточек)
                await autobuy_sweep_existing(user_id, chat_id, source_info, items, silent=True)

            # засветить все текущие лоты
            for it in items:
                key = make_item_key(it)
                user_seen_items[user_id].add(key)
                await db_mark_seen(user_id, key)

    except Exception:
        pass

    # 2) основной цикл — только новые лоты
    while user_search_active[user_id]:
        try:
            items_with_sources, errors = await fetch_all_sources(user_id)
            if errors:
                user_api_errors[user_id] += len(errors)
                try:
                    with open(f"api_errors_{user_id}.log", "a", encoding="utf-8") as f:
                        for url, err in errors:
                            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | {url} | {err}\n")
                except Exception:
                    pass

            for item, source in items_with_sources:
                key = make_item_key(item)
                if key in user_seen_items[user_id]:
                    continue

                # помечаем seen сразу, чтобы не повторять
                user_seen_items[user_id].add(key)
                await db_mark_seen(user_id, key)

                # если не проходит фильтр — не уведомляем, но seen остаётся
                if not passes_filters(item, user_id):
                    continue

                # автобай: пробуем купить (и для новых, и для старых — но старые уже обработаны в sweep)
                if source.get("autobuy", False) and key not in user_buy_attempted[user_id]:
                    user_buy_attempted[user_id].add(key)
                    await db_mark_buy_attempted(user_id, key)

                    bought, buy_info = await try_autobuy_item(source, item)
                    if bought:
                        await send_bot_message(
                            chat_id,
                            f"🛒 <b>Автопокупка успешна</b>\n{source['label']} | item_id=<b>{html.escape(str(item.get('item_id')))}</b>\n✅ {html.escape(str(buy_info))}",
                            parse_mode="HTML",
                        )
                    else:
                        # не спамим сильно — но один раз сообщим
                        await send_bot_message(
                            chat_id,
                            f"⚠️ <b>Автопокупка не удалась</b>\n{source['label']} | item_id=<b>{html.escape(str(item.get('item_id')))}</b>\n❌ {html.escape(str(buy_info))}",
                            parse_mode="HTML",
                        )

                # уведомление карточкой — только новые (всегда)
                card = make_card(item, source["label"])
                kb = make_kb(item)
                try:
                    await send_bot_message(
                        chat_id, card, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True
                    )
                except Exception:
                    await send_bot_message(chat_id, card)
                await asyncio.sleep(0.2)

            await asyncio.sleep(await user_hunter_interval(user_id))

        except asyncio.CancelledError:
            break
        except Exception as e:
            user_api_errors[user_id] += 1
            try:
                with open(f"hunter_errors_{user_id}.log", "a", encoding="utf-8") as f:
                    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | {str(e)}\n")
            except Exception:
                pass
            await asyncio.sleep(await user_hunter_interval(user_id))


# ---------------------- ОТЧЁТ ОШИБОК ----------------------
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
                        await send_bot_message(
                            uid,
                            f"⚠️ За последний час ошибки API: <b>{count}</b>.",
                            parse_mode="HTML",
                        )
                    except Exception:
                        pass
                    user_api_errors[uid] = 0
                    await db_set_last_report(uid, now)
        except Exception:
            try:
                with open("error_reporter.log", "a", encoding="utf-8") as f:
                    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | reporter exception\n")
            except Exception:
                pass
            await asyncio.sleep(ERROR_REPORT_INTERVAL)


# ---------------------- START / STATUS / CALLBACKS / HANDLERS ----------------------
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = message.from_user.id
    await load_user_data(user_id, force=True)

    await message.answer(START_INFO, parse_mode="HTML")
    balance = await db_get_balance(user_id)
    await message.answer(COMMANDS_MENU, parse_mode="HTML", reply_markup=main_kb())
    await message.answer(f"💎 Баланс: <b>{format_balance(balance)}</b>", parse_mode="HTML")

    access_rows = []
    app_link = mini_app_url(user_id)
    if app_link:
        access_rows.append([InlineKeyboardButton(text="🪄 Открыть Mini App", web_app=WebAppInfo(url=app_link))])
    access_rows.extend(
        [
            [InlineKeyboardButton(text="🔐 Ввести пароль (админ)", callback_data="enter_pass")],
            [InlineKeyboardButton(text="👤 У меня нет пароля", callback_data="no_pass")],
        ]
    )
    kb = InlineKeyboardMarkup(inline_keyboard=access_rows)
    access_hint = ""
    if not app_link:
        access_hint = "\n\n⚠ Mini App недоступен: задайте WEBAPP_PUBLIC_URL (https://...)."
    await message.answer(
        "<b>Доступ</b>\n"
        "Введите пароль для прав администратора\n"
        "или выберите режим ограниченного доступа."
        + access_hint,
        parse_mode="HTML",
        reply_markup=kb,
    )
    await safe_delete(message)


@dp.callback_query()
async def handle_callbacks(call: types.CallbackQuery):
    data = call.data or ""
    user_id = call.from_user.id
    await load_user_data(user_id)

    if data == "enter_pass":
        user_modes[user_id] = "enter_admin_password"
        await call.message.answer("🔐 Введите пароль администратора (только цифры):")
        await call.answer()
        return

    if data == "no_pass":
        await set_user_role(user_id, "limited")
        await call.message.answer(
            "👤 Режим без пароля.\nОграничения: задержка +3с и максимум 3 URL."
        )
        await call.answer("Ок")
        return

    if data.startswith("topup:"):
        try:
            amount = float(data.split(":", 1)[1])
        except (TypeError, ValueError):
            await call.answer("Некорректная сумма", show_alert=True)
            return
        balance = await db_change_balance(user_id, amount)
        await call.message.answer(
            f"✅ Пополнено на {format_balance(amount)}\n💎 Баланс: <b>{format_balance(balance)}</b>",
            parse_mode="HTML",
        )
        await call.answer("Ок")
        return

    if data.startswith("delurl:"):
        try:
            idx = int(data.split(":", 1)[1])
        except (TypeError, ValueError):
            await call.answer("Некорректный индекс", show_alert=True)
            return
        sources = await get_all_sources(user_id)
        if 0 <= idx < len(sources):
            removed = sources.pop(idx)
            await db_remove_url(user_id, removed["url"])
            await call.message.edit_text(f"✔ Удалён: {removed['url']}")
            await call.answer("Ок")
            return
        await call.answer("Некорректный индекс", show_alert=True)
        return

    if data.startswith("togurl:"):
        try:
            idx = int(data.split(":", 1)[1])
        except (TypeError, ValueError):
            await call.answer("Некорректный индекс", show_alert=True)
            return
        sources = await get_all_sources(user_id)
        if 0 <= idx < len(sources):
            source = sources[idx]
            new_enabled = not source.get("enabled", True)
            source["enabled"] = new_enabled
            await db_set_url_enabled(user_id, source["url"], new_enabled)
            kb = build_urls_list_kb_sync(sources)
            await call.message.edit_reply_markup(reply_markup=kb)
            await call.answer("Ок")
            return
        await call.answer("Некорректный индекс", show_alert=True)
        return

    if data.startswith("autobuyurl:"):
        # как и раньше: автобай только админ
        role = await get_user_role(user_id)
        if role != "admin":
            await call.answer("Автобай только для админа", show_alert=True)
            return

        try:
            idx = int(data.split(":", 1)[1])
        except (TypeError, ValueError):
            await call.answer("Некорректный индекс", show_alert=True)
            return

        sources = await get_all_sources(user_id)
        if 0 <= idx < len(sources):
            source = sources[idx]
            new_autobuy = not source.get("autobuy", False)
            source["autobuy"] = new_autobuy
            await db_set_url_autobuy(user_id, source["url"], new_autobuy)

            # если включили — сразу делаем sweep по текущим лотам (старым тоже)
            if new_autobuy:
                items, err = await fetch_single_source(source["url"])
                if not err and items:
                    src_info = {
                        "idx": idx + 1,
                        "url": source["url"],
                        "enabled": source.get("enabled", True),
                        "autobuy": True,
                        "label": f"URL #{idx+1}",
                    }
                    asyncio.create_task(autobuy_sweep_existing(user_id, call.message.chat.id, src_info, items, silent=True))

            kb = build_urls_list_kb_sync(sources)
            await call.message.edit_reply_markup(reply_markup=kb)
            await call.answer("Автобай ВКЛ" if new_autobuy else "Автобай ВЫКЛ")
            return

        await call.answer("Некорректный индекс", show_alert=True)
        return

    if data.startswith("testurl:"):
        try:
            idx = int(data.split(":", 1)[1])
        except (TypeError, ValueError):
            await call.answer("Некорректный индекс", show_alert=True)
            return
        sources = await get_all_sources(user_id)
        if 0 <= idx < len(sources):
            source = sources[idx]
            url = source["url"]
            status = "ВКЛ" if source.get("enabled", True) else "ВЫКЛ"
            label = f"URL #{idx+1} ({status})"
            await call.answer("Проверяю...")
            await send_test_for_single_url(user_id, call.message.chat.id, url, label)
            return
        await call.answer("Некорректный индекс", show_alert=True)
        return

    if data == "noop":
        await call.answer()
        return

    await call.answer()


@dp.message(Command("status"))
async def status_cmd(message: types.Message):
    user_id = message.from_user.id
    await load_user_data(user_id)
    f = user_filters[user_id]
    active = user_search_active[user_id]
    role = await get_user_role(user_id) or "not set"
    lines = [
        "<b>Статус</b>",
        f"🔸 Роль: {role}",
        f"🔸 Фильтр: {f['title'] if f['title'] else 'нет'}",
        f"🔸 Охотник: {'ВКЛ' if active else 'ВЫКЛ'}",
        f"🔸 URL: {len(await get_all_sources(user_id, enabled_only=True))}/{len(await get_all_sources(user_id))}",
        f"🔸 Seen: {len(user_seen_items[user_id])}",
        f"🔸 Buy-attempted: {len(user_buy_attempted[user_id])}",
        f"🔸 Ошибок API: {user_api_errors.get(user_id, 0)}",
    ]
    await message.answer("\n".join(lines), parse_mode="HTML")
    await safe_delete(message)


def build_urls_list_kb_sync(sources: list) -> InlineKeyboardMarkup:
    rows = []
    for idx, source in enumerate(sources):
        url = source["url"]
        enabled = source.get("enabled", True)
        autobuy = source.get("autobuy", False)

        label = url if len(url) <= URL_LABEL_MAX else url[: URL_LABEL_MAX - 3] + "..."
        state = "🟢 ВКЛ" if enabled else "🔴 ВЫКЛ"
        ab_state = "🛒 ON" if autobuy else "🛒 OFF"

        rows.append([InlineKeyboardButton(text=f"🔗 URL #{idx+1} ({state}): {label}", callback_data="noop")])
        rows.append(
            [
                InlineKeyboardButton(text=f"✅ Проверка #{idx+1}", callback_data=f"testurl:{idx}"),
                InlineKeyboardButton(
                    text=f"🔁 {'Выключить' if enabled else 'Включить'}",
                    callback_data=f"togurl:{idx}",
                ),
                InlineKeyboardButton(text=f"🗑 Удалить #{idx+1}", callback_data=f"delurl:{idx}"),
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{ab_state} (URL #{idx+1})",
                    callback_data=f"autobuyurl:{idx}",
                )
            ]
        )

    rows.append([InlineKeyboardButton(text="❌ Закрыть", callback_data="noop")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def build_urls_list_kb(user_id: int) -> InlineKeyboardMarkup:
    urls = await get_all_sources(user_id)
    return build_urls_list_kb_sync(urls)


@dp.message()
async def buttons_handler(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    await load_user_data(user_id)
    text = (message.text or "").strip()
    mode = user_modes[user_id]

    try:
        if mode == "enter_admin_password":
            user_modes[user_id] = None
            if text == ADMIN_PASSWORD:
                await set_user_role(user_id, "admin")
                await message.answer("✔ Пароль верный. Роль администратора активирована.")
            else:
                await message.answer("❌ Неверный пароль.")
            return await safe_delete(message)

        if mode == "title":
            user_filters[user_id]["title"] = text or None
            user_modes[user_id] = None
            await message.answer(f"✔ Фильтр: {html.escape(text)}")
            return await safe_delete(message)

        if mode == "add_url":
            user_modes[user_id] = None
            raw = text
            url = normalize_url(raw)
            if not url.startswith("http"):
                await message.answer("❌ Это не похоже на URL.")
                return await safe_delete(message)

            limit = await user_url_limit(user_id)
            if len(await get_all_sources(user_id)) >= limit:
                await message.answer(f"❌ Лимит URL: {limit}")
                return await safe_delete(message)

            ok, err = await validate_url_before_add(url)
            if not ok:
                await message.answer(err)
                return await safe_delete(message)

            if any(source["url"] == url for source in await get_all_sources(user_id)):
                await message.answer("⚠ Такой URL уже добавлен.")
                return await safe_delete(message)

            user_urls[user_id].append({"url": url, "enabled": True, "autobuy": False})
            await db_add_url(user_id, url)
            await message.answer(f"✔ URL добавлен: {url}")
            return await safe_delete(message)

        if text == "🔤 Фильтр":
            user_modes[user_id] = "title"
            return await message.answer("Введи слово/фразу для фильтра:")

        if text == "🧹 Очистить фильтры":
            user_filters[user_id]["title"] = None
            user_modes[user_id] = None
            return await message.answer("🧹 Фильтры очищены.")

        if text == "♻️ Сбросить историю":
            await db_clear_seen(user_id)
            await db_clear_buy_attempted(user_id)
            user_seen_items[user_id].clear()
            user_buy_attempted[user_id].clear()
            return await message.answer("♻️ История очищена: now все лоты снова будут считаться новыми, а автобай сможет попытаться заново.")

        if text == "➕ Добавить URL":
            user_modes[user_id] = "add_url"
            return await message.answer("Вставь API URL (prod-api/api.lzt/api.lolz):")

        if text == "📚 Мои URL":
            kb = await build_urls_list_kb(user_id)
            return await message.answer("📚 <b>Источники</b>", parse_mode="HTML", reply_markup=kb)

        if text == "✨ Проверка лотов":
            return await send_compact_10_for_user(user_id, chat_id)

        if text == "🚀 Старт охотника":
            active_sources = await get_all_sources(user_id, enabled_only=True)
            if not active_sources:
                return await message.answer("❌ Нет активных URL. Добавьте источник или включите URL.")
            if not user_search_active[user_id]:
                user_search_active[user_id] = True
                user_seen_items[user_id] = await db_load_seen(user_id)
                user_buy_attempted[user_id] = await db_load_buy_attempted(user_id)
                task = asyncio.create_task(hunter_loop_for_user(user_id, chat_id))
                user_hunter_tasks[user_id] = task
                return await message.answer(f"🧨 Охотник запущен! Активных URL: {len(active_sources)}")
            return await message.answer("⚠ Охотник уже запущен")

        if text == "🛑 Стоп охотника":
            user_search_active[user_id] = False
            task = user_hunter_tasks.get(user_id)
            if task:
                task.cancel()
            return await message.answer("🛑 Охотник остановлен")

        if text == "📊 Краткий статус":
            return await short_status_for_user(user_id, chat_id)

        if text == "ℹ️ Помощь":
            return await message.answer(
                "<b>ℹ️ Гид</b>\n"
                "1) ➕ Добавить URL (API)\n"
                "2) 📚 Мои URL → включить 🛒 ON (админ)\n"
                "3) 🚀 Старт охотника\n\n"
                "Уведомления — только новые лоты.\n"
                "Автобай — пытается купить и старые (sweep при включении/старте).",
                parse_mode="HTML",
            )

        if text == "💎 Баланс":
            balance = await db_get_balance(user_id)
            rows = [
                [InlineKeyboardButton(text="➕ +100 ₽", callback_data="topup:100")],
                [InlineKeyboardButton(text="➕ +500 ₽", callback_data="topup:500")],
            ]
            app_link = mini_app_url(user_id)
            if app_link:
                rows.append([InlineKeyboardButton(text="🪄 Mini App", web_app=WebAppInfo(url=app_link))])
            kb = InlineKeyboardMarkup(inline_keyboard=rows)
            return await message.answer(f"💎 Баланс: <b>{format_balance(balance)}</b>", parse_mode="HTML", reply_markup=kb)

        if text == "🪄 Mini App":
            app_link = mini_app_url(user_id)
            if not app_link:
                return await message.answer("⚠ Mini App недоступен без WEBAPP_PUBLIC_URL (https://...).")
            kb = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="Открыть", web_app=WebAppInfo(url=app_link))]]
            )
            return await message.answer("🪄 Откройте mini app кнопкой ниже", reply_markup=kb)

        if text == "🏠 Главное меню":
            return await message.answer("⭐ <b>Главное меню</b>", parse_mode="HTML", reply_markup=main_kb())

        if text and not text.startswith("/"):
            await asyncio.sleep(0.5)
            await safe_delete(message)

    except Exception as e:
        await send_bot_message(chat_id, f"❌ Ошибка: {html.escape(str(e))}")
        await safe_delete(message)


async def safe_delete(message: types.Message):
    try:
        await message.delete()
    except Exception:
        pass


async def short_status_for_user(user_id: int, chat_id: int):
    await load_user_data(user_id)
    active = user_search_active[user_id]
    seen = len(user_seen_items[user_id])
    attempted = len(user_buy_attempted[user_id])
    total = len(await get_all_sources(user_id))
    enabled = len(await get_all_sources(user_id, enabled_only=True))
    balance = await db_get_balance(user_id)
    await send_bot_message(
        chat_id,
        f"🔹 Охотник: {'ВКЛ' if active else 'ВЫКЛ'} | "
        f"URL: {enabled}/{total} | "
        f"Seen: {seen} | "
        f"Buy-attempted: {attempted} | "
        f"Баланс: {format_balance(balance)} | "
        f"Ошибок API: {user_api_errors.get(user_id, 0)}",
    )


# ---------------------- MINI APP ----------------------
def render_mini_app_html(user_id: int, balance: float) -> str:
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{MINI_APP_TITLE}</title>
  <style>
    body {{ margin:0; font-family: Inter, Arial, sans-serif; background: linear-gradient(135deg,#0f172a,#1e293b); color:#e2e8f0; }}
    .card {{ max-width:420px; margin:24px auto; background:rgba(30,41,59,.85); border:1px solid #334155; border-radius:18px; padding:20px; box-shadow:0 20px 40px rgba(0,0,0,.35); }}
    .balance {{ font-size:32px; font-weight:700; margin:8px 0 16px; }}
    .btns {{ display:grid; grid-template-columns:1fr 1fr; gap:10px; }}
    button {{ border:none; border-radius:12px; padding:12px; font-weight:600; color:#fff; background:linear-gradient(135deg,#22c55e,#16a34a); cursor:pointer; }}
    .muted {{ color:#94a3b8; font-size:13px; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="muted">Пользователь #{user_id}</div>
    <h2>💎 Внутренний баланс</h2>
    <div class="balance" id="balance">{format_balance(balance)}</div>
    <div class="btns">
      <button onclick="topUp(100)">+100 ₽</button>
      <button onclick="topUp(500)">+500 ₽</button>
      <button onclick="topUp(1000)">+1000 ₽</button>
      <button onclick="topUp(2500)">+2500 ₽</button>
    </div>
    <p class="muted">Демо-режим: виртуальный баланс.</p>
  </div>
<script>
async function topUp(amount) {{
  const res = await fetch('/mini-app/topup', {{
    method:'POST',
    headers:{{'Content-Type':'application/json'}},
    body: JSON.stringify({{user_id:{user_id}, amount}})
  }});
  const data = await res.json();
  if (data && data.balance) {{
    document.getElementById('balance').textContent = data.balance;
  }}
}}
</script>
</body>
</html>
"""


async def mini_app_page(request: web.Request):
    try:
        user_id = int(request.query.get("user_id", "0"))
    except ValueError:
        return web.Response(text="bad user_id", status=400)
    if user_id <= 0:
        return web.Response(text="user_id required", status=400)
    await db_ensure_user(user_id)
    balance = await db_get_balance(user_id)
    return web.Response(text=render_mini_app_html(user_id, balance), content_type="text/html")


async def mini_app_topup(request: web.Request):
    data = await request.json()
    user_id = int(data.get("user_id", 0))
    amount = float(data.get("amount", 0))
    if user_id <= 0 or amount <= 0:
        return web.json_response({"error": "invalid payload"}, status=400)
    balance = await db_change_balance(user_id, amount)
    return web.json_response({"ok": True, "balance": format_balance(balance)})


async def start_mini_app_server():
    app = web.Application()
    app.router.add_get("/mini-app", mini_app_page)
    app.router.add_post("/mini-app/topup", mini_app_topup)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEBAPP_HOST, WEBAPP_PORT)
    await site.start()
    return runner


# ---------------------- RUN ----------------------
async def main():
    global bot
    print("[BOT] Запуск: persistent urls/seen/buy_attempted, retry/backoff, prod-api fast-buy, sweep old items for autobuy...")

    if bot is None:
        if not has_valid_telegram_token(API_TOKEN):
            raise RuntimeError("Некорректный API_TOKEN")
        bot = Bot(token=API_TOKEN)

    await init_db()

    web_runner = await start_mini_app_server()

    try:
        asyncio.create_task(error_reporter_loop())
    except Exception:
        pass

    try:
        await dp.start_polling(bot)
    finally:
        await close_session()
        if bot is not None and getattr(bot, "session", None) is not None and not bot.session.closed:
            await bot.session.close()
        await web_runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
