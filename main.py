import asyncio
import json
import aiohttp
import html
import re
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
HUNTER_INTERVAL = 1.0
SHORT_CARD_MAX = 900
URL_LABEL_MAX = 60

# ---------------------- НОРМАЛИЗАЦИЯ URL ----------------------
def normalize_url(url: str) -> str:
    if not url:
        return url
    s = url.strip()
    s = s.replace(" ", "")
    s = s.replace("\t", "")
    s = s.replace("\n", "")
    s = s.replace("+", "")
    s = s.replace("!", "")

    # Приводим домен к корректному api.lzt.market если встречаются искажения
    s = re.sub(r"https?://api.*?\.market", "https://api.lzt.market", s)
    s = s.replace("://lzt.market", "://api.lzt.market")
    s = s.replace("://www.lzt.market", "://api.lzt.market")

    # Частые опечатки в параметрах
    s = s.replace("genshinlevelmin", "genshin_level_min")
    s = s.replace("genshinlevel_min", "genshin_level_min")
    s = s.replace("genshin_levelmin", "genshin_level_min")
    s = s.replace("brawl_cupmin", "brawl_cup_min")
    s = s.replace("clash_cupmin", "clash_cup_min")
    s = s.replace("orderby", "order_by")
    s = s.replace("order_by=pdate_to_down_upoad", "order_by=pdate_to_down_upload")
    s = s.replace("order_by=pdate_to_down_up", "order_by=pdate_to_down_upload")
    s = s.replace("order_by=pdate_to_downupload", "order_by=pdate_to_down_upload")

    # Если домен всё ещё не корректен — попытка восстановить хвост
    if ".market" in s and not s.startswith("https://api.lzt.market"):
        tail = s.split(".market")[-1]
        s = "https://api.lzt.market" + tail

    return s

# ---------------------- ПЕР-ЮЗЕР ДАННЫЕ ----------------------
# Убраны фильтры по цене — оставлен только фильтр по названию
user_filters = defaultdict(lambda: {"title": None})
user_search_active = defaultdict(lambda: False)
user_seen_items = defaultdict(set)
user_hunter_tasks = {}
user_modes = defaultdict(lambda: None)  # "title", "add_url"
user_started = set()

# пользовательские URL (можно добавлять несколько)
user_urls = defaultdict(list)

# ---------------------- КЛАВИАТУРА ----------------------
def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Проверка работоспособности")],
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
    "✅ Проверка работоспособности — парсинг до 10 лотов по всем добавленным URL.\n"
    "🔗 Добавить URL — добавить свой URL.\n"
    "📚 Список URL — посмотреть/удалить/проверить добавленные.\n"
    "🚀 Запустить охотника — мониторинг всех URL.\n"
    "🛑 Стоп охотника — остановить.\n"
    "ℹ️ Краткий статус — показать состояние.\n"
)

# ---------------------- HTTP / API ----------------------
async def fetch_items(url: str):
    headers = {"Authorization": f"Bearer {LZT_API_KEY}"} if LZT_API_KEY else {}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=12) as resp:
                text = await resp.text()
                try:
                    data = json.loads(text)
                except Exception:
                    return [], f"❌ API вернул не JSON:\n{text[:300]}"
                items = data.get("items")
                if not isinstance(items, list):
                    return [], f"⚠ API не вернул список items"
                return items, None
    except asyncio.TimeoutError:
        return [], "❌ Таймаут запроса"
    except aiohttp.ClientError as e:
        return [], f"❌ Ошибка сети: {e}"
    except Exception as e:
        return [], f"❌ Ошибка: {e}"

# ---------------------- ИСТОЧНИКИ ----------------------
def get_all_sources(user_id: int):
    # Только пользовательские URL — встроенные убраны по требованию
    return user_urls[user_id]

# ---------------------- ПАРСИНГ ВСЕХ ИСТОЧНИКОВ ----------------------
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
def passes_filters(item: dict, user_id: int) -> bool:
    f = user_filters[user_id]
    if f["title"]:
        title = (item.get("title") or "").lower()
        if f["title"].lower() not in title:
            return False
    return True

# ---------------------- ВСПОМОГАТЕЛИ ДЛЯ ОТОБРАЖЕНИЯ ----------------------
def make_card(item: dict, source_label: str) -> str:
    title = item.get("title", "Без названия")
    price = item.get("price", "—")
    item_id = item.get("item_id", "—")
    lines = [
        "━━━━━━━━━━━━━━━━━━━━",
        f"🔎 <b>{source_label}</b>",
        f"🎮 <b>{html.escape(str(title))}</b>",
        f"💰 {html.escape(str(price))}₽" if price != "—" else "💰 —",
        f"🆔 {html.escape(str(item_id))}",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
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

# ---------------------- ПРОВЕРКА 10 ЛОТОВ (ПРОВЕРКА РАБОТОСПОСОБНОСТИ) ----------------------
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
        f"✅ Проверка работоспособности\n📦 Уникальных лотов всего: <b>{len(items_list)}</b>\n📦 Показано: <b>{len(limited)}</b>\n🔍 Источников: {len(get_all_sources(user_id))} URL",
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

# ---------------------- ТЕСТ КОНКРЕТНОГО URL (10 ЛОТОВ) ----------------------
async def send_test_for_single_url(user_id: int, chat_id: int, url: str, label: str):
    items, err = await fetch_items(url)
    if err:
        await bot.send_message(chat_id, f"❗ Ошибка {html.escape(label)} ({html.escape(url)}):\n{html.escape(str(err))}")
        return
    if not items:
        await bot.send_message(chat_id, f"❗ {html.escape(label)}: ничего не найдено.")
        return
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
    # помечаем текущие как увиденные
    try:
        items_with_sources, _ = await fetch_all_sources(user_id)
        for it, _ in items_with_sources:
            iid = it.get("item_id")
            key = f"id::{iid}" if iid else f"noid::{it.get('title')}_{it.get('price')}"
            user_seen_items[user_id].add(key)
    except Exception:
        pass

    while user_search_active[user_id]:
        try:
            items_with_sources, errors = await fetch_all_sources(user_id)
            if errors:
                for url, err in errors:
                    await bot.send_message(chat_id, f"❗ Ошибка {html.escape(url)}:\n{html.escape(str(err))}")
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
                try:
                    await bot.send_message(chat_id, card, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
                except Exception:
                    await bot.send_message(chat_id, card)
                await asyncio.sleep(0.2)
            await asyncio.sleep(HUNTER_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            await bot.send_message(chat_id, f"❌ Ошибка охотника:\n{html.escape(str(e))}")
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
        f"🔸 Фильтр по названию: {f['title'] if f['title'] else 'не задан'}",
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

# ---------------------- СПИСОК URL (с кнопками ПРОВЕРКА и УДАЛИТЬ) ----------------------
def build_urls_list_kb(user_id: int) -> InlineKeyboardMarkup:
    urls = get_all_sources(user_id)
    rows = []
    for idx, url in enumerate(urls):
        label = url if len(url) <= URL_LABEL_MAX else url[:URL_LABEL_MAX-3] + "..."
        # Показываем строку с меткой, затем кнопки: Проверка и Удалить
        rows.append([InlineKeyboardButton(text=f"URL #{idx+1}: {label}", callback_data="noop")])
        rows.append([
            InlineKeyboardButton(text=f"Проверка #{idx+1}", callback_data=f"testurl:{idx}"),
            InlineKeyboardButton(text=f"Удалить #{idx+1}", callback_data=f"delurl:{idx}")
        ])
    rows.append([InlineKeyboardButton(text="Закрыть", callback_data="noop")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ---------------------- CALLBACKS ----------------------
@dp.callback_query()
async def handle_callbacks(call: types.CallbackQuery):
    data = call.data or ""
    user_id = call.from_user.id

    if data.startswith("delurl:"):
        idx = int(data.split(":", 1)[1])
        urls = get_all_sources(user_id)
        if 0 <= idx < len(urls):
            removed = urls.pop(idx)
            # If using user_urls[user_id] directly, ensure removal from that list
            # get_all_sources returns user_urls[user_id], so this modifies it
            await call.message.edit_text(f"✔ Удалён: {removed}")
            await call.answer("Удалено")
            return
        await call.answer("Некорректный индекс URL", show_alert=True)
        return

    if data.startswith("testurl:"):
        idx = int(data.split(":", 1)[1])
        urls = get_all_sources(user_id)
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

# ---------------------- ОБРАБОТКА ТЕКСТА / КНОПОК ----------------------
@dp.message()
async def buttons_handler(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = (message.text or "").strip()
    mode = user_modes[user_id]

    try:
        if mode == "title":
            user_filters[user_id]["title"] = text or None
            user_modes[user_id] = None
            await message.answer(f"✔ Фильтр по названию: {html.escape(text)}")
            return await safe_delete(message)

        if mode == "add_url":
            user_modes[user_id] = None
            url = normalize_url(text)
            if not url.startswith("http"):
                await message.answer("❌ Это не похоже на URL.")
                return await safe_delete(message)
            user_urls[user_id].append(url)
            await message.answer(f"✔ URL добавлен: {url}")
            return await safe_delete(message)

        # кнопки
        if text == "🔤 Фильтр по названию":
            user_modes[user_id] = "title"
            return await message.answer("Введи слово/фразу для фильтра:")

        if text == "🔗 Добавить URL":
            user_modes[user_id] = "add_url"
            return await message.answer("Вставь URL (например https://api.lzt.market/...) :")

        if text == "📚 Список URL":
            kb = build_urls_list_kb(user_id)
            return await message.answer("📚 Источники (пользовательские):", reply_markup=kb)

        if text == "✅ Проверка работоспособности":
            return await send_compact_10_for_user(user_id, chat_id)

        if text == "🚀 Запустить охотника":
            if not user_search_active[user_id]:
                user_search_active[user_id] = True
                user_seen_items[user_id].clear()
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

        if text == "◀️ Назад":
            return await message.answer("⭐ Главное меню:", reply_markup=main_kb())

        # авто-удаление пользовательских сообщений (не команд)
        if text and not text.startswith("/"):
            await asyncio.sleep(0.5)
            await safe_delete(message)

    except Exception as e:
        await bot.send_message(chat_id, f"❌ Ошибка в обработке: {html.escape(str(e))}")
        await safe_delete(message)

# ---------------------- SAFE DELETE ----------------------
async def safe_delete(message: types.Message):
    try:
        await message.delete()
    except Exception:
        pass

# ---------------------- RUN ----------------------
async def main():
    print("[BOT] Запуск бота: только пользовательские URL, проверка 10 лотов, тест URL из списка...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

