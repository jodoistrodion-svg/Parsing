import asyncio
import json
import aiohttp
import aiosqlite
import html
import re
import time
import random
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

bot = Bot(token=API_TOKEN)
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

# ---------------------- АИО-SQLITE (асинхронная БД) ----------------------
async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS urls (
            user_id INTEGER,
            url TEXT,
            added_at INTEGER,
            PRIMARY KEY(user_id, url)
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS seen (
            user_id INTEGER,
            item_key TEXT,
            seen_at INTEGER,
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

async def db_add_url(user_id: int, url: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR IGNORE INTO urls(user_id, url, added_at) VALUES (?, ?, ?)",
            (user_id, url, int(time.time()))
        )
        await db.commit()

async def db_remove_url(user_id: int, url: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM urls WHERE user_id=? AND url=?", (user_id, url))
        await db.commit()

async def db_get_urls(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT url FROM urls WHERE user_id=? ORDER BY added_at", (user_id,))
        rows = await cur.fetchall()
        return [r[0] for r in rows]

async def db_mark_seen(user_id: int, key: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR IGNORE INTO seen(user_id, item_key, seen_at) VALUES (?, ?, ?)",
            (user_id, key, int(time.time()))
        )
        await db.commit()

async def db_load_seen(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT item_key FROM seen WHERE user_id=?", (user_id,))
        rows = await cur.fetchall()
        return {r[0] for r in rows}

async def db_ensure_user(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users(user_id, role, last_error_report) VALUES (?, ?, ?)",
            (user_id, "unknown", 0)
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
            "INSERT OR IGNORE INTO users(user_id, role, last_error_report) VALUES (?, ?, ?)",
            (user_id, role, 0)
        )
        await db.execute("UPDATE users SET role=? WHERE user_id=?", (role, user_id))
        await db.commit()

async def db_get_last_report(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT last_error_report FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return row[0] if row else 0

async def db_set_last_report(user_id: int, ts: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE users SET last_error_report=? WHERE user_id=?", (ts, user_id))
        await db.commit()

# ---------------------- АВТОПРОВЕРКА ПАРАМЕТРОВ ----------------------
VALID_PARAMS = {
    "mihoyo": {
        "pmin", "pmax", "order_by",
        "genshin_level_min", "genshin_legendary_min",
        "honkai_level_min", "honkai_legendary_min",
        "zenless_level_min"
    },
    "supercell": {
        "pmin", "pmax", "order_by",
        "brawl_cup_min", "clash_cup_min",
        "legendary_brawlers_min"
    },
    "riot": {
        "pmin", "pmax", "order_by",
        "valorant_rank_type1", "valorant_knife_min",
        "daybreak", "knife"
    },
    "hytale": {
        "pmin", "pmax", "order_by"
    }
}

def detect_section(url: str):
    for section in VALID_PARAMS.keys():
        if f"/{section}" in url:
            return section
    return None

def extract_params(url: str):
    if "?" not in url:
        return {}
    query = url.split("?", 1)[1]
    params = {}
    for part in query.split("&"):
        if "=" in part:
            k, v = part.split("=", 1)
            params[k] = v
    return params

def validate_params(url: str):
    section = detect_section(url)
    if not section:
        return False, "❌ Не удалось определить раздел (mihoyo/supercell/riot/hytale)"

    params = extract_params(url)
    valid = VALID_PARAMS[section]

    for p in params.keys():
        if p not in valid:
            return False, f"❌ Параметр '{p}' не существует в разделе '{section}'"

    return True, None

# ---------------------- НОРМАЛИЗАЦИЯ URL ----------------------
def normalize_url(url: str) -> str:
    if not url:
        return url
    s = url.strip()
    s = s.replace(" ", "").replace("\t", "").replace("\n", "").replace("+", "").replace("!", "")

    s = re.sub(r"https?://api.*?\.market", "https://api.lzt.market", s)
    s = s.replace("://lzt.market", "://api.lzt.market")
    s = s.replace("://www.lzt.market", "://api.lzt.market")

    s = s.replace("genshinlevelmin", "genshin_level_min")
    s = s.replace("genshinlevel_min", "genshin_level_min")
    s = s.replace("genshin_levelmin", "genshin_level_min")
    s = s.replace("brawl_cupmin", "brawl_cup_min")
    s = s.replace("clash_cupmin", "clash_cup_min")
    s = s.replace("orderby", "order_by")
    s = s.replace("order_by=pdate_to_down_upoad", "order_by=pdate_to_down_upload")
    s = s.replace("order_by=pdate_to_down_up", "order_by=pdate_to_down_upload")
    s = s.replace("order_by=pdate_to_downupload", "order_by=pdate_to_down_upload")

    if ".market" in s and not s.startswith("https://api.lzt.market"):
        tail = s.split(".market")[-1]
        s = "https://api.lzt.market" + tail

    return s

# ---------------------- ПЕР-ЮЗЕР ДАННЫЕ (в памяти, синхронизированы с БД) ----------------------
user_filters = defaultdict(lambda: {"title": None})
user_search_active = defaultdict(lambda: False)
user_seen_items = defaultdict(set)  # loaded from DB
user_hunter_tasks = {}
user_modes = defaultdict(lambda: None)  # modes: None, "enter_admin_password", "title", "add_url"
user_started = set()
user_urls = defaultdict(list)  # loaded from DB
user_api_errors = defaultdict(int)

# load persisted data for user on first interaction (async)
async def load_user_data(user_id: int):
    if user_id in user_started:
        return
    await db_ensure_user(user_id)
    user_urls[user_id] = await db_get_urls(user_id)
    user_seen_items[user_id] = await db_load_seen(user_id)
    user_started.add(user_id)

async def get_user_role(user_id: int):
    await load_user_data(user_id)
    role = await db_get_role(user_id)
    if role == "unknown":
        return None
    return role

async def set_user_role(user_id: int, role: str):
    await db_set_role(user_id, role)
    await load_user_data(user_id)

async def user_url_limit(user_id: int):
    role = await get_user_role(user_id)
    if role == "limited":
        return MAX_URLS_PER_USER_LIMITED
    return MAX_URLS_PER_USER_DEFAULT

async def user_hunter_interval(user_id: int):
    role = await get_user_role(user_id)
    extra = LIMITED_EXTRA_DELAY if role == "limited" else 0.0
    return HUNTER_INTERVAL_BASE + extra

# ---------------------- КЛАВИАТУРА ----------------------
def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Проверка работоспособности")],
            [KeyboardButton(text="🔤 Фильтр по названию")],
            [KeyboardButton(text="🔗 Добавить URL"), KeyboardButton(text="📚 Список URL")],
            [KeyboardButton(text="🚀 Запустить охотника"), KeyboardButton(text="🛑 Стоп охотника")],
            [KeyboardButton(text="ℹ️ Краткий статус")],
            [KeyboardButton(text="🏠 Главное меню")],
        ],
        resize_keyboard=True
    )

# ---------------------- ТЕКСТЫ ----------------------
START_INFO = (
    "<b>🤖 Добро пожаловать в Parsing Bot</b>\n"
    "Отслеживание новых лотов по вашим URL в один клик.\n\n"
    "<b>🔗 Полезные ссылки</b>\n"
    "• Канал поддержки: https://t.me/+wHlSL7Ij2rpjYmFi\n"
    "• Создатель: https://t.me/StaliNusshhAaaaaa\n"
)

COMMANDS_MENU = (
    "<b>🧭 Меню команд</b>\n\n"
    "<b>✅ Проверка работоспособности</b>\n"
    "Проверяет до 10 лотов по каждому добавленному URL.\n\n"
    "<b>🔗 Добавить URL</b>\n"
    "Добавляет новый источник для мониторинга.\n\n"
    "<b>📚 Список URL</b>\n"
    "Показывает ваши URL, позволяет проверить или удалить любой из них.\n\n"
    "<b>🚀 Запустить охотника / 🛑 Стоп охотника</b>\n"
    "Включает или останавливает фоновый мониторинг.\n\n"
    "<b>ℹ️ Краткий статус</b>\n"
    "Показывает текущую активность, число источников и ошибки API."
)

# ---------------------- HTTP / API с экспоненциальным retry ----------------------
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
                return None, f"⚠ API не вернул список items"
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
    ok, err = validate_params(url)
    if not ok:
        return False, err

    items, api_err = await fetch_with_retry(url, max_retries=2)
    if api_err:
        return False, f"❌ API ошибка: {api_err}"

    # Разрешаем добавление даже при пустом items
    return True, None

# ---------------------- ИСТОЧНИКИ ----------------------
async def get_all_sources(user_id: int):
    await load_user_data(user_id)
    # Защита от случайных дубликатов в памяти, если URL уже есть в БД.
    deduped = list(dict.fromkeys(user_urls[user_id]))
    if deduped != user_urls[user_id]:
        user_urls[user_id] = deduped
    return user_urls[user_id]

# ---------------------- ПАРСИНГ ВСЕХ ИСТОЧНИКОВ ----------------------
async def fetch_all_sources(user_id: int):
    urls = await get_all_sources(user_id)
    results = []
    errors = []
    for idx, url in enumerate(urls):
        label = f"URL #{idx+1}"
        items, err = await fetch_with_retry(url)
        if err:
            errors.append((url, err))
            continue
        for it in items:
            results.append((it, label))
    return results, errors

# ---------------------- ФИЛЬТРЫ ----------------------
def passes_filters(item: dict, user_id: int) -> bool:
    f = user_filters[user_id]
    if f["title"]:
        title = (item.get("title") or "").lower()
        if f["title"].lower() not in title:
            return False
    return True

# ---------------------- ВСПОМОГАТЕЛИ ДЛЯ ОТОБРАЖЕНИЯ ----------------------
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
        return card[:SHORT_CARD_MAX - 100] + "\n... (обрезано)"
    return card

def make_kb(item: dict) -> InlineKeyboardMarkup | None:
    iid = item.get("item_id")
    if not iid:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Открыть", url=f"https://lzt.market/{iid}")]
    ])

# ---------------------- ОТЧЁТЫ ОШИБОК ----------------------
# user_api_errors defined above

# ---------------------- ПРОВЕРКА 10 ЛОТОВ ----------------------
async def send_compact_10_for_user(user_id: int, chat_id: int):
    items_with_sources, errors = await fetch_all_sources(user_id)
    if errors:
        for url, err in errors:
            await bot.send_message(chat_id, f"❗ Ошибка {html.escape(url)}:\n{html.escape(str(err))}")
    if not items_with_sources:
        await bot.send_message(chat_id, "❗ Ничего не найдено по всем источникам.")
        return
    aggregated = {}
    for item, source in items_with_sources:
        iid = item.get("item_id")
        key = f"id::{iid}" if iid else f"noid::{item.get('title')}_{item.get('price')}"
        if key not in aggregated:
            aggregated[key] = (item, source)
    items_list = list(aggregated.values())
    limited = items_list[:10]
    await bot.send_message(
        chat_id,
        f"✅ Проверка работоспособности\n📦 Уникальных лотов всего: <b>{len(items_list)}</b>\n📦 Показано: <b>{len(limited)}</b>\n🔍 Источников: {len(await get_all_sources(user_id))} URL",
        parse_mode="HTML"
    )
    for item, source in limited:
        if not passes_filters(item, user_id):
            continue
        card = make_card(item, source)
        kb = make_kb(item)
        try:
            await bot.send_message(chat_id, card, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
        except Exception:
            await bot.send_message(chat_id, card)
        await asyncio.sleep(0.2)

# ---------------------- ТЕСТ КОНКРЕТНОГО URL ----------------------
async def send_test_for_single_url(user_id: int, chat_id: int, url: str, label: str):
    items, err = await fetch_with_retry(url, max_retries=2)
    if err:
        await bot.send_message(chat_id, f"❗ Ошибка {html.escape(label)} ({html.escape(url)}):\n{html.escape(str(err))}")
        return
    if not items:
        await bot.send_message(chat_id, f"❗ {html.escape(label)}: ничего не найдено.")
        return

    try:
        keys = list(items[0].keys())
        await bot.send_message(chat_id, f"🔍 Пример полей в первом лоте: {', '.join(keys)}")
    except Exception:
        pass

    try:
        with open("last_item_debug.json", "w", encoding="utf-8") as f:
            json.dump(items[0], f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    aggregated = {}
    for item in items:
        iid = item.get("item_id")
        key = f"id::{iid}" if iid else f"noid::{item.get('title')}_{item.get('price')}"
        if key not in aggregated:
            aggregated[key] = item
    items_list = list(aggregated.values())
    limited = items_list[:10]
    await bot.send_message(
        chat_id,
        f"✅ Тест URL ({html.escape(label)})\n📦 Уникальных лотов всего: <b>{len(items_list)}</b>\n📦 Показано: <b>{len(limited)}</b>",
        parse_mode="HTML"
    )
    for item in limited:
        if not passes_filters(item, user_id):
            continue
        card = make_card(item, label)
        kb = make_kb(item)
        try:
            await bot.send_message(chat_id, card, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
        except Exception:
            await bot.send_message(chat_id, card)
        await asyncio.sleep(0.2)

# ---------------------- ОХОТНИК ----------------------
async def hunter_loop_for_user(user_id: int, chat_id: int):
    await load_user_data(user_id)
    try:
        items_with_sources, _ = await fetch_all_sources(user_id)
        for it, _ in items_with_sources:
            iid = it.get("item_id")
            key = f"id::{iid}" if iid else f"noid::{it.get('title')}_{it.get('price')}"
            user_seen_items[user_id].add(key)
            await db_mark_seen(user_id, key)
    except Exception:
        pass

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
                iid = item.get("item_id")
                key = f"id::{iid}" if iid else f"noid::{item.get('title')}_{item.get('price')}"
                if key in user_seen_items[user_id]:
                    continue
                if not passes_filters(item, user_id):
                    user_seen_items[user_id].add(key)
                    await db_mark_seen(user_id, key)
                    continue
                user_seen_items[user_id].add(key)
                await db_mark_seen(user_id, key)
                card = make_card(item, source)
                kb = make_kb(item)
                try:
                    await bot.send_message(chat_id, card, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
                except Exception:
                    await bot.send_message(chat_id, card)
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

# ---------------------- ОТЧЁТ ОШИБОК (ФОН) ----------------------
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
                        await bot.send_message(uid, f"⚠️ За последний час API не вернул список items или произошли ошибки: <b>{count}</b> раз.", parse_mode="HTML")
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
    await load_user_data(user_id)
    await message.answer(START_INFO, parse_mode="HTML")
    await message.answer(COMMANDS_MENU, parse_mode="HTML", reply_markup=main_kb())
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔐 Ввести пароль (админ)", callback_data="enter_pass")],
        [InlineKeyboardButton(text="👤 У меня нет пароля", callback_data="no_pass")]
    ])
    await message.answer(
        "<b>Доступ</b>\n"
        "Введите пароль для прав администратора\n"
        "или выберите режим ограниченного доступа.",
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
            "👤 Выбран режим без пароля.\n"
            "Ограничения: задержка +3с и максимум 3 URL."
        )
        await call.answer("Режим ограниченного доступа активирован")
        return

    if data.startswith("delurl:"):
        try:
            idx = int(data.split(":", 1)[1])
        except (TypeError, ValueError):
            await call.answer("Некорректный индекс URL", show_alert=True)
            return
        urls = await get_all_sources(user_id)
        if 0 <= idx < len(urls):
            removed = urls.pop(idx)
            await db_remove_url(user_id, removed)
            await call.message.edit_text(f"✔ Удалён: {removed}")
            await call.answer("Удалено")
            return
        await call.answer("Некорректный индекс URL", show_alert=True)
        return

    if data.startswith("testurl:"):
        try:
            idx = int(data.split(":", 1)[1])
        except (TypeError, ValueError):
            await call.answer("Некорректный индекс URL", show_alert=True)
            return
        urls = await get_all_sources(user_id)
        if 0 <= idx < len(urls):
            url = urls[idx]
            label = f"URL #{idx+1}"
            await call.answer("Проверяю URL...")
            await send_test_for_single_url(user_id, call.message.chat.id, url, label)
            return
        await call.answer("Некорректный индекс URL", show_alert=True)
        return

    if data == "noop":
        await call.answer()
        try:
            await call.message.delete()
        except Exception:
            pass
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
        "<b>Текущие настройки</b>",
        f"🔸 Роль: {role}",
        f"🔸 Фильтр по названию: {f['title'] if f['title'] else 'не задан'}",
        f"🔸 Охотник: {'ВКЛ' if active else 'ВЫКЛ'}",
        f"🔸 Всего источников: {len(await get_all_sources(user_id))}",
        f"🔸 Увидено лотов: {len(user_seen_items[user_id])}",
        f"🔸 Ошибок API (за текущий период): {user_api_errors.get(user_id, 0)}",
    ]
    await message.answer("\n".join(lines), parse_mode="HTML")
    await safe_delete(message)

def build_urls_list_kb_sync(urls: list) -> InlineKeyboardMarkup:
    rows = []
    for idx, url in enumerate(urls):
        label = url if len(url) <= URL_LABEL_MAX else url[:URL_LABEL_MAX-3] + "..."
        rows.append([InlineKeyboardButton(text=f"🔗 URL #{idx+1}: {label}", callback_data="noop")])
        rows.append([
            InlineKeyboardButton(text=f"✅ Проверка #{idx+1}", callback_data=f"testurl:{idx}"),
            InlineKeyboardButton(text=f"🗑 Удалить #{idx+1}", callback_data=f"delurl:{idx}")
        ])
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
                await message.answer("❌ Неверный пароль. Если пароля нет — нажмите '👤 У меня нет пароля' в стартовом сообщении.")
            return await safe_delete(message)

        if mode == "title":
            user_filters[user_id]["title"] = text or None
            user_modes[user_id] = None
            await message.answer(f"✔ Фильтр по названию: {html.escape(text)}")
            return await safe_delete(message)

        if mode == "add_url":
            user_modes[user_id] = None
            raw = text
            url = normalize_url(raw)
            if not url.startswith("http"):
                await message.answer("❌ Это не похоже на URL.")
                return await safe_delete(message)

            limit = await user_url_limit(user_id)
            if len(user_urls[user_id]) >= limit:
                await message.answer(f"❌ Достигнут лимит URL для вашей роли: {limit}")
                return await safe_delete(message)

            ok, err = await validate_url_before_add(url)
            if not ok:
                await message.answer(err)
                return await safe_delete(message)

            if url in user_urls[user_id]:
                await message.answer("⚠ Такой URL уже добавлен.")
                return await safe_delete(message)

            user_urls[user_id].append(url)
            await db_add_url(user_id, url)
            await message.answer(f"✔ URL добавлен и прошёл проверку: {url}")
            return await safe_delete(message)

        if text == "🔤 Фильтр по названию":
            user_modes[user_id] = "title"
            return await message.answer("Введи слово/фразу для фильтра:")

        if text == "🔗 Добавить URL":
            user_modes[user_id] = "add_url"
            return await message.answer("Вставь URL (например https://api.lzt.market/...) :")

        if text == "📚 Список URL":
            kb = await build_urls_list_kb(user_id)
            return await message.answer("📚 <b>Ваши источники</b>", parse_mode="HTML", reply_markup=kb)

        if text == "✅ Проверка работоспособности":
            return await send_compact_10_for_user(user_id, chat_id)

        if text == "🚀 Запустить охотника":
            if not user_search_active[user_id]:
                user_search_active[user_id] = True
                user_seen_items[user_id] = await db_load_seen(user_id)
                task = asyncio.create_task(hunter_loop_for_user(user_id, chat_id))
                user_hunter_tasks[user_id] = task
                return await message.answer("🧨 Охотник запущен!")
            else:
                return await message.answer("⚠ Охотник уже запущен")

        if text == "🛑 Стоп охотника":
            user_search_active[user_id] = False
            task = user_hunter_tasks.get(user_id)
            if task:
                task.cancel()
            return await message.answer("🛑 Охотник остановлен")

        if text == "ℹ️ Краткий статус":
            return await short_status_for_user(user_id, chat_id)

        if text == "🏠 Главное меню":
            return await message.answer("⭐ <b>Главное меню</b>", parse_mode="HTML", reply_markup=main_kb())

        if text and not text.startswith("/"):
            await asyncio.sleep(0.5)
            await safe_delete(message)

    except Exception as e:
        await bot.send_message(chat_id, f"❌ Ошибка в обработке: {html.escape(str(e))}")
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
    total = len(await get_all_sources(user_id))
    await bot.send_message(chat_id, f"🔹 Охотник: {'ВКЛ' if active else 'ВЫКЛ'} | Источников: {total} | Увидено: {seen} | Ошибок API: {user_api_errors.get(user_id, 0)}")

# ---------------------- RUN ----------------------
async def main():
    print("[BOT] Запуск бота: multiuser, persistent seen (aiosqlite), exponential backoff, per-user limits, admin password flow...")
    await init_db()
    # start background reporter
    try:
        asyncio.create_task(error_reporter_loop())
    except Exception:
        pass
    try:
        await dp.start_polling(bot)
    finally:
        await close_session()

if __name__ == "__main__":
    asyncio.run(main())
