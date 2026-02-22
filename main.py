import asyncio
import json
import aiohttp
import html
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
HUNTER_INTERVAL = 1.7
SHORT_CARD_MAX = 900
URL_LABEL_MAX = 40

# ---------------------- ФУНКЦИЯ АВТО-ЧИСТКИ URL ----------------------
def normalize_url(url: str) -> str:
    url = url.strip()

    # заменяем пробелы на корректные символы
    url = url.replace(" ", "")
    url = url.replace("pdate_to_down_upload", "pdate_to_down_upload")
    url = url.replace("brawl_cup_min=", "brawl_cup_min=")
    url = url.replace("clash_cup_min=", "clash_cup_min=")

    # нормализация домена
    url = url.replace("://lzt.market", "://api.lzt.market")
    url = url.replace("://www.lzt.market", "://api.lzt.market")

    return url

# ---------------------- ЖЁСТКО ВШИТЫЕ URL ----------------------
BUILTIN_URLS = [
    normalize_url("https://api.lzt.market/mihoyo?pmax=399&genshin_level_min=30&order_by=pdate_to_down_upload"),
    normalize_url("https://api.lzt.market/supercell?pmax=399&brawl_cup_min=20000&clash_cup_min=8000"),
]

# ---------------------- ПЕР-ЮЗЕР ДАННЫЕ ----------------------
user_filters = defaultdict(lambda: {"min": None, "max": None, "title": None})
user_search_active = defaultdict(lambda: False)
user_seen_items = defaultdict(set)
user_hunter_tasks = {}
user_modes = defaultdict(lambda: None)
user_started = set()

# пользовательские URL (добавленные)
user_urls = defaultdict(list)
user_active_url_index = defaultdict(lambda: None)

# ---------------------- КЛАВИАТУРА ----------------------
def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📦 Последние 69 лотов")],
            [KeyboardButton(text="💰 Мин. цена"), KeyboardButton(text="💰 Макс. цена")],
            [KeyboardButton(text="🔤 Фильтр по названию")],
            [KeyboardButton(text="🔗 Добавить URL"), KeyboardButton(text="📚 Список URL")],
            [KeyboardButton(text="🚀 Запустить охотника"), KeyboardButton(text="🛑 Стоп охотника")],
            [KeyboardButton(text="ℹ️ Краткий статус")],
            [KeyboardButton(text="◀️ Назад")],
        ],
        resize_keyboard=True
    )

# ---------------------- ТЕКСТЫ ----------------------
START_INFO = (
    "🤖 Парсинг‑бот создан при поддержке этой прекрасной дамы — просьба подписаться неравнодушных:\n"
    "https://t.me/+wHlSL7Ij2rpjYmFi\n\n"
    "Создатель бота (вопросы, реклама, поддержка):\n"
    "https://t.me/StaliNusshhAaaaaa\n\n"
)

COMMANDS_MENU = (
    "<b>Команды и кнопки</b>\n\n"
    "📦 Последние 69 лотов — парсинг ВСЕХ URL (2 встроенных + добавленные).\n"
    "🔗 Добавить URL — добавить свой URL.\n"
    "📚 Список URL — посмотреть/удалить добавленные.\n"
    "🚀 Запустить охотника — мониторинг всех URL.\n"
    "🛑 Стоп охотника — остановить.\n"
    "ℹ️ Краткий статус — показать состояние.\n"
)

# ---------------------- API ----------------------
async def fetch_items(url: str):
    headers = {"Authorization": f"Bearer {LZT_API_KEY}"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=10) as resp:
                text = await resp.text()

                try:
                    data = json.loads(text)
                except Exception:
                    return [], f"❌ API вернул не JSON:\n{text[:200]}"

                items = data.get("items")
                if not isinstance(items, list):
                    return [], f"⚠ API не вернул список items"

                return items, None

    except Exception as e:
        return [], f"❌ Ошибка: {e}"

# ---------------------- ПОЛУЧИТЬ ВСЕ ИСТОЧНИКИ ----------------------
def get_all_sources(user_id: int):
    return BUILTIN_URLS + user_urls[user_id]

# ---------------------- ПАРСИНГ СО ВСЕХ URL ----------------------
async def fetch_all_sources(user_id: int):
    urls = get_all_sources(user_id)
    results = []
    errors = []

    for idx, url in enumerate(urls):
        label = f"URL #{idx+1}"
        items, err = await fetch_items(url)
        if err:
            errors.append((url, err))
            continue
        for it in items:
            results.append((it, label))

    return results, errors

# ---------------------- ФИЛЬТРЫ ----------------------
def passes_filters(item, user_id):
    f = user_filters[user_id]
    price = item.get("price", 0)

    if f["min"] is not None and price < f["min"]:
        return False
    if f["max"] is not None and price > f["max"]:
        return False
    if f["title"]:
        if f["title"].lower() not in (item.get("title") or "").lower():
            return False
    return True

# ---------------------- КАРТОЧКА ----------------------
def make_card(item, source):
    title = item.get("title", "Без названия")
    price = item.get("price", "—")
    item_id = item.get("item_id", "—")

    lines = [
        "━━━━━━━━━━━━━━━━━━━━",
        f"🔎 <b>{source}</b>",
        f"🎮 <b>{html.escape(title)}</b>",
        f"💰 {price}₽",
        f"🆔 {item_id}",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    return "\n".join(lines)

def make_kb(item):
    iid = item.get("item_id")
    if not iid:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Открыть", url=f"https://lzt.market/{iid}")]
        ]
    )

# ---------------------- 69 ЛОТОВ ----------------------
async def send_compact_69_for_user(user_id: int, chat_id: int):
    items_with_sources, errors = await fetch_all_sources(user_id)

    if errors:
        for url, err in errors:
            await bot.send_message(chat_id, f"❗ Ошибка {url}:\n{err}")

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

    await bot.send_message(
        chat_id,
        f"📦 Найдено уникальных лотов: <b>{len(items_list)}</b>\n"
        f"🔍 Источники: {len(get_all_sources(user_id))} URL",
        parse_mode="HTML",
    )

    for item, source in items_list:
        if not passes_filters(item, user_id):
            continue
        card = make_card(item, source)
        kb = make_kb(item)
        await bot.send_message(chat_id, card, parse_mode="HTML", reply_markup=kb)
        await asyncio.sleep(0.25)

# ---------------------- ОХОТНИК ----------------------
async def hunter_loop_for_user(user_id: int, chat_id: int):
    items_with_sources, _ = await fetch_all_sources(user_id)
    for it, _ in items_with_sources:
        iid = it.get("item_id")
        key = f"id::{iid}" if iid else f"noid::{it.get('title')}_{it.get('price')}"
        user_seen_items[user_id].add(key)

    while user_search_active[user_id]:
        try:
            items_with_sources, errors = await fetch_all_sources(user_id)
            if errors:
                for url, err in errors:
                    await bot.send_message(chat_id, f"❗ Ошибка {url}:\n{err}")

            for item, source in items_with_sources:
                iid = item.get("item_id")
                key = f"id::{iid}" if iid else f"noid::{item.get('title')}_{item.get('price')}"

                if key in user_seen_items[user_id]:
                    continue

                if not passes_filters(item, user_id):
                    user_seen_items[user_id].add(key)
                    continue

                user_seen_items[user_id].add(key)
                card = make_card(item, source)
                kb = make_kb(item)
                await bot.send_message(chat_id, card, parse_mode="HTML", reply_markup=kb)
                await asyncio.sleep(0.25)

            await asyncio.sleep(HUNTER_INTERVAL)

        except asyncio.CancelledError:
            break
        except Exception as e:
            await bot.send_message(chat_id, f"❌ Ошибка охотника:\n{e}")
            await asyncio.sleep(HUNTER_INTERVAL)

# ---------------------- START ----------------------
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = message.from_user.id

    if user_id not in user_started:
        await message.answer(START_INFO)
        await message.answer(COMMANDS_MENU, parse_mode="HTML", reply_markup=main_kb())
        user_started.add(user_id)
    else:
        await message.answer("⭐ Главное меню:", reply_markup=main_kb())

    await safe_delete(message)

# ---------------------- STATUS ----------------------
@dp.message(Command("status"))
async def status_cmd(message: types.Message):
    user_id = message.from_user.id
    f = user_filters[user_id]
    active = user_search_active[user_id]

    lines = [
        "<b>Текущие настройки</b>",
        f"🔸 Мин. цена: {f['min']}",
        f"🔸 Макс. цена: {f['max']}",
        f"🔸 Фильтр по названию: {f['title']}",
        f"🔸 Охотник: {'ВКЛ' if active else 'ВЫКЛ'}",
        f"🔸 Всего источников: {len(get_all_sources(user_id))}",
        f"🔸 Увидено лотов: {len(user_seen_items[user_id])}",
    ]

    await message.answer("\n".join(lines), parse_mode="HTML")
    await safe_delete(message)

# ---------------------- КРАТКИЙ СТАТУС ----------------------
async def short_status_for_user(user_id: int, chat_id: int):
    active = user_search_active[user_id]
    seen = len(user_seen_items[user_id])
    total = len(get_all_sources(user_id))
    await bot.send_message(chat_id, f"🔹 Охотник: {'ВКЛ' if active else 'ВЫКЛ'} | Источников: {total} | Увидено: {seen}")

# ---------------------- СПИСОК URL ----------------------
def build_urls_list_kb(user_id: int):
    urls = get_all_sources(user_id)
    rows = []

    for idx, url in enumerate(urls):
        label = url if len(url) < URL_LABEL_MAX else url[:URL_LABEL_MAX] + "..."
        if idx < len(BUILTIN_URLS):
            rows.append([InlineKeyboardButton(text=f"Встроенный #{idx+1}: {label}", callback_data="noop")])
        else:
            rows.append([InlineKeyboardButton(text=f"Пользовательский #{idx+1}: {label}", callback_data="noop")])
            rows.append([InlineKeyboardButton(text=f"Удалить #{idx+1}", callback_data=f"delurl:{idx}")])

    rows.append([InlineKeyboardButton(text="Закрыть", callback_data="noop")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ---------------------- CALLBACKS ----------------------
@dp.callback_query()
async def handle_callbacks(call: types.CallbackQuery):
    data = call.data
    user_id = call.from_user.id

    if data.startswith("delurl:"):
        idx = int(data.split(":")[1])
        builtin_count = len(BUILTIN_URLS)

        if idx >= builtin_count:
            real_idx = idx - builtin_count
            if 0 <= real_idx < len(user_urls[user_id]):
                removed = user_urls[user_id].pop(real_idx)
                await call.message.edit_text(f"✔ Удалён: {removed}")
                await call.answer("Удалено")
                return

        await call.answer("Нельзя удалить встроенный URL", show_alert=True)
        return

    await call.answer()

# ---------------------- ОБРАБОТКА КНОПОК ----------------------
@dp.message()
async def buttons(message: types.Message):
    user_id = message.from_user.id
    text = (message.text or "").strip()
    mode = user_modes[user_id]

    if mode == "min" and text.isdigit():
        user_filters[user_id]["min"] = int(text)
        user_modes[user_id] = None
        await message.answer(f"✔ Мин. цена: {text}")
        return await safe_delete(message)

    if mode == "max" and text.isdigit():
        user_filters[user_id]["max"] = int(text)
        user_modes[user_id] = None
        await message.answer(f"✔ Макс. цена: {text}")
        return await safe_delete(message)

    if mode == "title":
        user_filters[user_id]["title"] = text or None
        user_modes[user_id] = None
        await message.answer(f"✔ Фильтр по названию: {text}")
        return await safe_delete(message)

    if mode == "add_url":
        user_modes[user_id] = None
        url = normalize_url(text)
        user_urls[user_id].append(url)
        await message.answer(f"✔ URL добавлен: {url}")
        return await safe_delete(message)

    if text == "💰 Мин. цена":
        user_modes[user_id] = "min"
        return await message.answer("Введи минимальную цену:")

    if text == "💰 Макс. цена":
        user_modes[user_id] = "max"
        return await message.answer("Введи максимальную цену:")

    if text == "🔤 Фильтр по названию":
        user_modes[user_id] = "title"
        return await message.answer("Введи слово/фразу:")

    if text == "🔗 Добавить URL":
        user_modes[user_id] = "add_url"
        return await message.answer("Вставь URL:")

    if text == "📚 Список URL":
        kb = build_urls_list_kb(user_id)
        return await message.answer("📚 Источники:", reply_markup=kb)

    if text == "📦 Последние 69 лотов":
        return await send_compact_69_for_user(user_id, message.chat.id)

    if text == "🚀 Запустить охотника":
        if not user_search_active[user_id]:
            user_search_active[user_id] = True
            user_seen_items[user_id].clear()
            task = asyncio.create_task(hunter_loop_for_user(user_id, message.chat.id))
            user_hunter_tasks[user_id] = task
            return await message.answer("🧨 Охотник запущен!")
        else:
            return await message.answer("⚠ Уже запущен")

    if text == "🛑 Стоп охотника":
        user_search_active[user_id] = False
        task = user_hunter_tasks.get(user_id)
        if task:
            task.cancel()
        return await message.answer("🛑 Охотник остановлен")

    if text == "ℹ️ Краткий статус":
        return await short_status_for_user(user_id, message.chat.id)

    if text == "◀️ Назад":
        return await message.answer("⭐ Главное меню:", reply_markup=main_kb())

    if not text.startswith("/"):
        await asyncio.sleep(0.5)
        await safe_delete(message)

# ---------------------- SAFE DELETE ----------------------
async def safe_delete(message: types.Message):
    try:
        await message.delete()
    except:
        pass

# ---------------------- RUN ----------------------
async def main():
    print("[BOT] Запуск бота: встроенные URL + пользовательские URL...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
