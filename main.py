import asyncio
import json
import aiohttp
import html
import time
from collections import defaultdict

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from config import API_TOKEN, LZT_API_KEY, LZT_URL, CHECK_INTERVAL

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# ---------------------- НАСТРОЙКИ ----------------------
HUNTER_INTERVAL = 1.7
SHORT_CARD_MAX = 900
URL_LABEL_MAX = 40

# ---------------------- ПЕР-ЮЗЕР СОСТОЯНИЕ ----------------------
user_filters = defaultdict(lambda: {"min": None, "max": None, "title": None})
user_search_active = defaultdict(lambda: False)
user_seen_items = defaultdict(set)
user_hunter_tasks = {}
user_modes = defaultdict(lambda: None)  # "min", "max", "title", "url"
user_started = set()

# список URL на юзера
user_urls = defaultdict(list)  # [url1, url2, ...]
user_active_url_index = defaultdict(lambda: None)  # только для статуса/отображения

# ---------------------- КЛАВА ----------------------
def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💎 Искать все"), KeyboardButton(text="📦 Последние 69 лотов")],
            [KeyboardButton(text="💰 Мин. цена"), KeyboardButton(text="💰 Макс. цена")],
            [KeyboardButton(text="🔤 Фильтр по названию"), KeyboardButton(text="🔗 URL с сайта")],
            [KeyboardButton(text="📚 Список URL"), KeyboardButton(text="🔄 Сбросить URL")],
            [KeyboardButton(text="🔧 Тест API"), KeyboardButton(text="ℹ️ Краткий статус")],
            [KeyboardButton(text="🚀 Запустить охотника"), KeyboardButton(text="🛑 Стоп охотника")],
            [KeyboardButton(text="◀️ Назад")],
        ],
        resize_keyboard=True,
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
    "💎 Искать все — сбросить фильтры бота.\n"
    "💰 Мин. цена / Макс. цена — задать фильтры бота.\n"
    "🔤 Фильтр по названию — задать текстовый фильтр.\n"
    "🔗 URL с сайта — вставить URL из браузера (lzt.market) и добавить в список.\n"
    "📚 Список URL — показать панель с твоими URL (выбрать/удалить).\n"
    "🔄 Сбросить URL — сбросить активный URL (только для статуса, логика парсит все).\n"
    "🔧 Тест API — проверить все URL (или базовый) на доступность.\n"
    "📦 Последние 69 лотов — показать текущие лоты по всем URL.\n"
    "🚀 Запустить охотника — включить/выключить режим охотника по всем URL.\n"
    "ℹ️ Краткий статус — однострочный статус.\n"
    "/status — полный статус и настройки.\n"
)

# ---------------------- ПАРСЕР ПЕРСОНАЖЕЙ ----------------------
def extract_characters(title: str):
    result = []
    if not title:
        return result

    def grab(block_name: str):
        nonlocal result, title
        key = block_name + "("
        if key in title:
            start = title.find(key) + len(key)
            end = title.find(")", start)
            if end != -1:
                inner = title[start:end].strip()
                if inner:
                    result.append(f"{block_name}: {inner}")

    grab("Genshin")
    grab("Genshin Impact")
    grab("ZZZ")
    grab("Zenless Zone Zero")
    return result

# ---------------------- URL HELPERS ----------------------
def get_all_user_urls(user_id: int) -> list:
    """
    Если у юзера есть свои URL — парсим ИХ ВСЕ.
    Если нет — парсим только LZT_URL.
    Никаких активных для логики, только список.
    """
    urls = user_urls[user_id]
    if urls:
        return urls.copy()
    return [LZT_URL]

def get_active_url_label(user_id: int) -> str:
    idx = user_active_url_index[user_id]
    urls = user_urls[user_id]
    if idx is not None and 0 <= idx < len(urls):
        return f"URL #{idx+1}: {urls[idx]}"
    if urls:
        return f"всего URL: {len(urls)} (парсим все)"
    return "базовый API (LZT_URL)"

# ---------------------- API ----------------------
async def fetch_items(url: str):
    headers = {"Authorization": f"Bearer {LZT_API_KEY}"}
    start_ts = time.time()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=10) as resp:
                elapsed = time.time() - start_ts
                text = await resp.text()

                print("\n===== RAW API RESPONSE =====")
                print("URL:", url)
                print("STATUS:", resp.status)
                print("TEXT:", text[:500])
                print("============================\n")

                try:
                    data = json.loads(text)
                except Exception as e:
                    return [], f"❌ API вернул не JSON: {e}\nОтвет: {text[:300]}", elapsed

                items = data.get("items")
                if items is None:
                    return [], f"⚠ API не вернул поле 'items'. Ответ: {data}", elapsed
                if not isinstance(items, list):
                    return [], f"⚠ Поле 'items' не список. Тип: {type(items)}", elapsed
                return items, None, elapsed

    except asyncio.TimeoutError:
        return [], "❌ Таймаут запроса к API (10 секунд).", time.time() - start_ts
    except aiohttp.ClientError as e:
        return [], f"❌ Ошибка сети: {e}", time.time() - start_ts
    except Exception as e:
        return [], f"❌ Неизвестная ошибка: {e}", time.time() - start_ts

# ---------------------- ФИЛЬТРЫ ----------------------
def passes_filters_local(item: dict, user_id: int) -> bool:
    f = user_filters[user_id]
    price = item.get("price", 0)
    if f["min"] is not None and price < f["min"]:
        return False
    if f["max"] is not None and price > f["max"]:
        return False
    if f["title"]:
        title = item.get("title", "") or ""
        if f["title"].lower() not in title.lower():
            return False
    return True

# ---------------------- INLINE КНОПКА ----------------------
def make_item_inline_kb(item: dict) -> InlineKeyboardMarkup:
    item_id = item.get("item_id")
    rows = []
    if item_id:
        url = f"https://lzt.market/{item_id}"
        rows.append([InlineKeyboardButton(text="Открыть в браузере", url=url)])
    rows.append([InlineKeyboardButton(text="Сбросить активный URL", callback_data="reset_active_url")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ---------------------- КАРТОЧКА ----------------------
def format_item_card_short(item: dict, source_label: str) -> str:
    title = item.get("title", "Без названия")
    price = item.get("price", "—")
    item_id = item.get("item_id", "—")
    uid = item.get("uid") or item.get("seller_uid") or item.get("user_id") or "—"
    region = item.get("region") or item.get("server") or "—"
    created = item.get("created_at") or item.get("date") or "—"

    lines = []
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"🔎 <b>Источник: {html.escape(source_label)}</b>")
    lines.append(f"🎮 <b>{html.escape(str(title))}</b>")
    if price != "—":
        lines.append(f"💰 <b>{html.escape(str(price))}₽</b>")
    else:
        lines.append("💰 —")
    lines.append(f"🆔 <b>{html.escape(str(item_id))}</b>")
    lines.append(f"👤 UID: {html.escape(str(uid))}")
    lines.append(f"🌍 {html.escape(str(region))}")
    lines.append(f"🕒 {html.escape(str(created))}")

    chars = extract_characters(title)
    if chars:
        for c in chars:
            lines.append(f"✨ {html.escape(c)}")

    lines.append("━━━━━━━━━━━━━━━━━━━━")

    card = "\n".join(lines)
    if len(card) > SHORT_CARD_MAX:
        return card[:SHORT_CARD_MAX - 100] + "\n... (обрезано)"
    return card

# ---------------------- FETCH СО ВСЕХ URL ----------------------
async def fetch_items_from_all_user_urls(user_id: int):
    """
    Парсим ВСЕ URL юзера (или один LZT_URL).
    Возвращаем [(item, 'URL #1'), (item, 'URL #2'), ...].
    """
    urls = get_all_user_urls(user_id)
    results = []
    errors = []

    has_custom = bool(user_urls[user_id])

    for idx, url in enumerate(urls):
        if has_custom:
            label = f"URL #{idx+1}"
        else:
            label = "базовый API"
        items, error, _ = await fetch_items(url)
        if error:
            errors.append((url, error))
            continue
        for it in items:
            results.append((it, label))

    return results, errors

# ---------------------- 69 ЛОТОВ ----------------------
async def send_compact_69_for_user(user_id: int, chat_id: int):
    try:
        items_with_sources, errors = await fetch_items_from_all_user_urls(user_id)
        if errors:
            for url, err in errors:
                await bot.send_message(
                    chat_id,
                    f"❗ Ошибка при запросе {html.escape(url)}:\n{html.escape(str(err))}",
                )

        if not items_with_sources:
            await bot.send_message(chat_id, "❗ Ничего не найдено по всем источникам.")
            return

        aggregated = {}
        for item, source in items_with_sources:
            iid = item.get("item_id")
            if iid:
                key = f"id::{iid}"
            else:
                key = f"noid::{item.get('title','')}_{item.get('price','')}"
            if key not in aggregated:
                aggregated[key] = (item, source)

        items_list = list(aggregated.values())

        await bot.send_message(
            chat_id,
            f"ℹ Всего найдено уникальных лотов: <b>{len(items_list)}</b>\n"
            f"🔍 Источники: {get_active_url_label(user_id)}",
            parse_mode="HTML",
        )

        sent_any = False
        for item, source in items_list:
            if not passes_filters_local(item, user_id):
                continue
            card = format_item_card_short(item, source)
            kb = make_item_inline_kb(item)
            try:
                await bot.send_message(
                    chat_id,
                    card,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                    reply_markup=kb,
                )
            except Exception:
                await bot.send_message(chat_id, card)
            sent_any = True
            await asyncio.sleep(0.25)

        if not sent_any:
            await bot.send_message(chat_id, "❗ Лоты есть, но они не проходят фильтры бота.")
    except Exception as e:
        await bot.send_message(chat_id, f"❌ Ошибка в send_compact_69:\n{html.escape(str(e))}")

# ---------------------- ОХОТНИК ----------------------
async def hunter_loop_for_user(user_id: int, chat_id: int):
    try:
        items_with_sources, _ = await fetch_items_from_all_user_urls(user_id)
        for it, _ in items_with_sources:
            iid = it.get("item_id")
            if iid:
                user_seen_items[user_id].add(f"id::{iid}")
            else:
                user_seen_items[user_id].add(f"noid::{it.get('title','')}_{it.get('price','')}")
    except Exception:
        pass

    while user_search_active[user_id]:
        try:
            items_with_sources, errors = await fetch_items_from_all_user_urls(user_id)
            if errors:
                for url, err in errors:
                    await bot.send_message(
                        chat_id,
                        f"❗ Ошибка при запросе {html.escape(url)}:\n{html.escape(str(err))}",
                    )
            if not items_with_sources:
                await asyncio.sleep(HUNTER_INTERVAL)
                continue

            for item, source in items_with_sources:
                iid = item.get("item_id")
                if iid:
                    key = f"id::{iid}"
                else:
                    key = f"noid::{item.get('title','')}_{item.get('price','')}"
                if key in user_seen_items[user_id]:
                    continue
                if not passes_filters_local(item, user_id):
                    user_seen_items[user_id].add(key)
                    continue
                user_seen_items[user_id].add(key)
                card = format_item_card_short(item, source)
                kb = make_item_inline_kb(item)
                try:
                    await bot.send_message(
                        chat_id,
                        card,
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                        reply_markup=kb,
                    )
                except Exception:
                    await bot.send_message(chat_id, card)
                await asyncio.sleep(0.25)

            await asyncio.sleep(HUNTER_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            await bot.send_message(
                chat_id,
                f"❌ Ошибка в режиме охотника:\n{html.escape(str(e))}",
            )
            await asyncio.sleep(HUNTER_INTERVAL)

# ---------------------- /start ----------------------
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id

    if user_id not in user_started:
        try:
            await message.answer(START_INFO)
            await message.answer(COMMANDS_MENU, parse_mode="HTML", reply_markup=main_kb())
        except Exception:
            try:
                await message.answer(
                    START_INFO + "\n" + COMMANDS_MENU,
                    parse_mode="HTML",
                    reply_markup=main_kb(),
                )
            except Exception:
                pass
        user_started.add(user_id)
    else:
        await message.answer("⭐ Главное меню:", reply_markup=main_kb())

    await safe_delete(message)

# ---------------------- /status ----------------------
@dp.message(Command("status"))
async def status_cmd(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    f = user_filters[user_id]
    active = user_search_active[user_id]
    urls = user_urls[user_id]
    lines = [
        "<b>Текущие настройки</b>",
        f"🔸 Мин. цена: {f['min'] if f['min'] is not None else 'не задана'}",
        f"🔸 Макс. цена: {f['max'] if f['max'] is not None else 'не задана'}",
        f"🔸 Фильтр по названию: {html.escape(f['title']) if f['title'] else 'не задан'}",
        f"🔸 Режим охотника: {'ВКЛЮЧЁН' if active else 'ВЫКЛЮЧЕН'}",
        f"🔸 Источники: {get_active_url_label(user_id)}",
        f"🔸 Всего URL в списке: {len(urls)}",
        f"🔸 Отправлено лотов (анти-дубликаты): {len(user_seen_items[user_id])}",
    ]
    await message.answer("\n".join(lines), parse_mode="HTML")
    await safe_delete(message)

# ---------------------- КРАТКИЙ СТАТУС ----------------------
async def short_status_for_user(user_id: int, chat_id: int):
    active = user_search_active[user_id]
    urls = user_urls[user_id]
    seen = len(user_seen_items[user_id])
    text = (
        f"🔹 Охотник: {'ВКЛ' if active else 'ВЫКЛ'} | "
        f"URL в списке: {len(urls)} | Увидено: {seen}"
    )
    await bot.send_message(chat_id, text)

# ---------------------- /stop_hunter ----------------------
@dp.message(Command("stop_hunter"))
async def stop_hunter_cmd(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    if user_search_active[user_id]:
        user_search_active[user_id] = False
        task = user_hunter_tasks.get(user_id)
        if task:
            task.cancel()
            user_hunter_tasks.pop(user_id, None)
        await message.answer("🛑 Охотник остановлен у вас.")
    else:
        await message.answer("⚠ Охотник и так не запущен у вас.")
    await safe_delete(message)

# ---------------------- ПАНЕЛЬ URL ----------------------
def build_urls_list_kb(user_id: int) -> InlineKeyboardMarkup:
    urls = user_urls[user_id]
    rows = []
    if not urls:
        rows.append([InlineKeyboardButton(text="Список пуст", callback_data="noop")])
    else:
        for idx, u in enumerate(urls):
            label = u
            if len(label) > URL_LABEL_MAX:
                label = label[:URL_LABEL_MAX - 3] + "..."
            rows.append(
                [InlineKeyboardButton(text=f"Выбрать #{idx+1}: {label}", callback_data=f"useurl:{idx}")]
            )
            rows.append(
                [InlineKeyboardButton(text=f"Удалить #{idx+1}", callback_data=f"delurl:{idx}")]
            )
    rows.append([InlineKeyboardButton(text="Отмена", callback_data="noop")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ---------------------- ОБРАБОТКА ТЕКСТА ----------------------
@dp.message()
async def buttons(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = (message.text or "").strip()
    mode = user_modes[user_id]

    try:
        # режимы ввода
        if mode == "min" and text.isdigit():
            user_filters[user_id]["min"] = int(text)
            user_modes[user_id] = None
            await bot.send_message(chat_id, f"✔ Мин. цена: {user_filters[user_id]['min']}₽")
            await safe_delete(message)
            return

        if mode == "max" and text.isdigit():
            user_filters[user_id]["max"] = int(text)
            user_modes[user_id] = None
            await bot.send_message(chat_id, f"✔ Макс. цена: {user_filters[user_id]['max']}₽")
            await safe_delete(message)
            return

        if mode == "title":
            user_filters[user_id]["title"] = text or None
            user_modes[user_id] = None
            if user_filters[user_id]["title"]:
                await bot.send_message(
                    chat_id,
                    f"✔ Фильтр по названию: <b>{html.escape(user_filters[user_id]['title'])}</b>",
                    parse_mode="HTML",
                )
            else:
                await bot.send_message(chat_id, "✔ Фильтр по названию сброшен.")
            await safe_delete(message)
            return

        if mode == "url":
            user_modes[user_id] = None
            url_text = text.strip()

            if url_text.lower() == "сброс" or url_text == "":
                user_active_url_index[user_id] = None
                await bot.send_message(
                    chat_id,
                    "✔ Активный URL (для статуса) сброшен. Логика всё равно парсит все URL.",
                )
                await safe_delete(message)
                return

            if not (url_text.startswith("http://") or url_text.startswith("https://")):
                await bot.send_message(
                    chat_id,
                    "❌ Это не похоже на URL. Вставь ссылку вида:\nhttps://lzt.market/...",
                )
                await safe_delete(message)
                return

            url_text = url_text.replace("://lzt.market", "://api.lzt.market")
            url_text = url_text.replace("://www.lzt.market", "://api.lzt.market")

            user_urls[user_id].append(url_text)
            user_active_url_index[user_id] = len(user_urls[user_id]) - 1
            user_seen_items[user_id].clear()

            await bot.send_message(
                chat_id,
                f"✔ Кастомный URL добавлен (#{user_active_url_index[user_id]+1}).\n"
                f"Парсинг идёт по всем URL сразу.\n"
                f"Управление списком — кнопка <b>📚 Список URL</b>.",
                parse_mode="HTML",
            )
            await safe_delete(message)
            return

        # кнопки
        if text == "💎 Искать все":
            user_filters[user_id]["min"] = None
            user_filters[user_id]["max"] = None
            user_filters[user_id]["title"] = None
            user_seen_items[user_id].clear()
            await bot.send_message(
                chat_id,
                "🧹 Фильтры бота сброшены. Охотник начнёт с чистого списка.",
            )

        elif text == "💰 Мин. цена":
            user_modes[user_id] = "min"
            await bot.send_message(chat_id, "Введи минимальную цену (число):")

        elif text == "💰 Макс. цена":
            user_modes[user_id] = "max"
            await bot.send_message(chat_id, "Введи максимальную цену (число):")

        elif text == "🔤 Фильтр по названию":
            user_modes[user_id] = "title"
            await bot.send_message(
                chat_id,
                "Введи слово/фразу, которая должна быть в названии:",
            )

        elif text == "🔗 URL с сайта":
            user_modes[user_id] = "url"
            await bot.send_message(
                chat_id,
                "Вставь ссылку из браузера с lzt.market, например:\n"
                "https://lzt.market/mihoyo?pmin=1&pmax=399&ea=no&genshin_legendary_min=3\n\n"
                "Это добавит URL в твой список. Парсинг будет идти по всем URL сразу.",
                parse_mode="HTML",
            )

        elif text == "📚 Список URL":
            kb = build_urls_list_kb(user_id)
            await bot.send_message(
                chat_id,
                "📚 Твои URL (выбери для статуса или удали):",
                reply_markup=kb,
            )

        elif text == "🔄 Сбросить URL":
            user_active_url_index[user_id] = None
            await bot.send_message(
                chat_id,
                "✔ Активный URL (для статуса) сброшен. Логика всё равно парсит все URL.",
            )

        elif text == "🔧 Тест API":
            urls = get_all_user_urls(user_id)
            await bot.send_message(chat_id, "🔎 Тестирую все источники...")
            for idx, url in enumerate(urls):
                label = f"URL #{idx+1}" if user_urls[user_id] else "базовый API"
                items, error, elapsed = await fetch_items(url)
                if error:
                    await bot.send_message(
                        chat_id,
                        f"❗ {label} ({html.escape(url)}): {html.escape(str(error))} — {elapsed:.2f}s",
                    )
                else:
                    await bot.send_message(
                        chat_id,
                        f"✅ {label} ({html.escape(url)}): OK — {len(items)} лотов, {elapsed:.2f}s",
                    )

        elif text == "📦 Последние 69 лотов":
            await send_compact_69_for_user(user_id, chat_id)

        elif text == "🚀 Запустить охотника":
            if not user_search_active[user_id]:
                user_seen_items[user_id].clear()
                try:
                    items_with_sources, _ = await fetch_items_from_all_user_urls(user_id)
                    for it, _ in items_with_sources:
                        iid = it.get("item_id")
                        if iid:
                            user_seen_items[user_id].add(f"id::{iid}")
                        else:
                            user_seen_items[user_id].add(
                                f"noid::{it.get('title','')}_{it.get('price','')}"
                            )
                except Exception:
                    pass

                user_search_active[user_id] = True
                task = asyncio.create_task(hunter_loop_for_user(user_id, chat_id))
                user_hunter_tasks[user_id] = task
                await bot.send_message(
                    chat_id,
                    f"🧨 Режим охотника запущен (интервал {HUNTER_INTERVAL}s).\n"
                    f"Источники: {get_active_url_label(user_id)}",
                )
            else:
                user_search_active[user_id] = False
                task = user_hunter_tasks.get(user_id)
                if task:
                    task.cancel()
                    user_hunter_tasks.pop(user_id, None)
                await bot.send_message(
                    chat_id,
                    "🛑 Охотник остановлен у вас (по повторному нажатию).",
                )

        elif text == "🛑 Стоп охотника":
            if user_search_active[user_id]:
                user_search_active[user_id] = False
                task = user_hunter_tasks.get(user_id)
                if task:
                    task.cancel()
                    user_hunter_tasks.pop(user_id, None)
                await bot.send_message(chat_id, "🛑 Охотник остановлен у вас.")
            else:
                await bot.send_message(chat_id, "⚠ Охотник и так не запущен у вас.")

        elif text == "ℹ️ Краткий статус":
            await short_status_for_user(user_id, chat_id)

        elif text == "◀️ Назад":
            await bot.send_message(chat_id, "⭐ Главное меню:", reply_markup=main_kb())

        if text and not text.startswith("/"):
            await asyncio.sleep(0.5)
            await safe_delete(message)

    except Exception as e:
        await bot.send_message(
            chat_id,
            f"❌ Ошибка в обработке кнопок:\n{html.escape(str(e))}",
        )
        await safe_delete(message)

# ---------------------- CALLBACKS ----------------------
@dp.callback_query()
async def handle_callbacks(call: types.CallbackQuery):
    data = call.data or ""
    user_id = call.from_user.id

    try:
        if data.startswith("useurl:"):
            idx = int(data.split(":", 1)[1])
            urls = user_urls[user_id]
            if 0 <= idx < len(urls):
                user_active_url_index[user_id] = idx
                try:
                    await call.message.edit_text(
                        f"✔ Для статуса выбран URL #{idx+1}:\n{urls[idx]}"
                    )
                except Exception:
                    pass
                await call.answer("URL выбран для статуса. Парсинг идёт по всем URL.")
            else:
                await call.answer("URL не найден.", show_alert=True)
            return

        if data.startswith("delurl:"):
            idx = int(data.split(":", 1)[1])
            urls = user_urls[user_id]
            if 0 <= idx < len(urls):
                removed = urls.pop(idx)
                if user_active_url_index[user_id] is not None:
                    if user_active_url_index[user_id] == idx:
                        user_active_url_index[user_id] = None
                    elif user_active_url_index[user_id] > idx:
                        user_active_url_index[user_id] -= 1
                user_seen_items[user_id].clear()
                try:
                    await call.message.edit_text(f"✔ URL #{idx+1} удалён:\n{removed}")
                except Exception:
                    pass
                await call.answer("URL удалён.")
            else:
                await call.answer("URL не найден.", show_alert=True)
            return

        if data == "reset_active_url":
            user_active_url_index[user_id] = None
            await call.answer("Активный URL (для статуса) сброшен.")
            try:
                await call.message.edit_text(
                    "✔ Активный URL (для статуса) сброшен. Парсинг идёт по всем URL."
                )
            except Exception:
                pass
            return

        if data == "noop":
            await call.answer()
            try:
                await call.message.delete()
            except Exception:
                pass
            return

        await call.answer()
    except Exception:
        try:
            await call.answer("Ошибка обработки.", show_alert=True)
        except Exception:
            pass

# ---------------------- SAFE DELETE ----------------------
async def safe_delete(message: types.Message):
    try:
        await message.delete()
    except Exception:
        pass

# ---------------------- RUN ----------------------
async def main():
    print("[BOT] Запуск бота: парсинг по всем URL пользователя одновременно...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
