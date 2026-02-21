import asyncio
import json
import aiohttp
import html
from collections import defaultdict

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

from config import API_TOKEN, LZT_API_KEY, LZT_URL, CHECK_INTERVAL

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# ---------------------- НАСТРОЙКИ ----------------------
HUNTER_INTERVAL = 1.7  # интервал охотника (секунды)
SHORT_CARD_MAX = 900  # максимально допустимая длина компактной карточки

# ---------------------- ПЕРСОНАЛЬНЫЕ СТАТЫ (PER-USER) ----------------------
user_filters = defaultdict(lambda: {"min": None, "max": None, "title": None})
user_search_active = defaultdict(lambda: False)
user_seen_items = defaultdict(set)        # анти-дубликаты per-user
user_hunter_tasks = {}
user_modes = defaultdict(lambda: None)    # "min", "max", "title", "url"
user_started = set()                      # пользователям, которым уже отправили стартовое сообщение
user_urls = defaultdict(lambda: None)     # кастомный URL с сайта per-user (API-версия)

# ---------------------- КЛАВИАТУРА ----------------------
def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💎 Искать все")],
            [KeyboardButton(text="💰 Мин. цена"), KeyboardButton(text="💰 Макс. цена")],
            [KeyboardButton(text="🔤 Фильтр по названию")],
            [KeyboardButton(text="🔗 URL с сайта")],
            [KeyboardButton(text="📦 Последние 69 лотов")],
            [KeyboardButton(text="🚀 Запустить охотника")],
            [KeyboardButton(text="🛑 Стоп охотника")],
            [KeyboardButton(text="◀️ Назад")]
        ],
        resize_keyboard=True
    )

# ---------------------- СТАРТОВОЕ СООБЩЕНИЕ / МЕНЮ ----------------------
START_INFO = (
    "🤖 Бот создан при поддержке этой прекрасной девушки, подпишитесь, не пожалеете:\n"
    "https://t.me/+wHlSL7Ij2rpjYmFi\n\n"
    "💡 Бот — первый проект, сделан с душой, автор проекта:\n"
    "https://t.me/StaliNusshhAaaaaa\n\n"
)

COMMANDS_MENU = (
    "<b>Основные команды и описание</b>\n\n"
    "💎 <b>Искать все</b> — сбросить все фильтры бота.\n"
    "💰 <b>Мин. цена</b> — ввести минимальную цену (число).\n"
    "💰 <b>Макс. цена</b> — ввести максимальную цену (число).\n"
    "🔤 <b>Фильтр по названию</b> — ввести слово/фразу для поиска в названии.\n"
    "🔗 <b>URL с сайта</b> — вставить ссылку из браузера (lzt.market), бот будет парсить именно её через API.\n"
    "📦 <b>Последние 69 лотов</b> — показать текущие лоты по фильтрам/URL.\n"
    "🚀 <b>Запустить охотника</b> — включить/выключить режим охотника только для вас.\n"
    "🛑 <b>Стоп охотника</b> или <b>/stop_hunter</b> — остановить охотника только для вас.\n"
    "/status — показать текущие фильтры, URL и состояние охотника.\n\n"
    "<i>Режим охотника</i> делает запросы каждые 1.7 секунды и отправляет только новые лоты.\n"
    "Фильтры и URL применяются отдельно для каждого пользователя.\n"
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

# ---------------------- ВЫБОР URL ДЛЯ ПОЛЬЗОВАТЕЛЯ ----------------------
def get_user_url(user_id: int) -> str:
    """
    Если у пользователя задан свой URL — используем его.
    Иначе используем базовый LZT_URL из конфига.
    """
    custom = user_urls[user_id]
    if custom:
        return custom
    return LZT_URL

# ---------------------- API LZT ----------------------
async def fetch_items(url: str):
    headers = {"Authorization": f"Bearer {LZT_API_KEY}"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=10) as resp:
                status = resp.status
                text = await resp.text()

                print("\n===== RAW API RESPONSE =====")
                print("URL:", url)
                print("STATUS:", status)
                print("TEXT:", text[:500])
                print("============================\n")

                try:
                    data = json.loads(text)
                except Exception as e:
                    return [], f"❌ API вернул не JSON: {e}\nОтвет: {text[:300]}"

                items = data.get("items")
                if items is None:
                    return [], f"⚠ API не вернул поле 'items'. Ответ: {data}"
                if not isinstance(items, list):
                    return [], f"⚠ Поле 'items' не список. Тип: {type(items)}"
                return items, None

    except asyncio.TimeoutError:
        return [], "❌ Таймаут запроса к API (10 секунд)."
    except aiohttp.ClientError as e:
        return [], f"❌ Ошибка сети: {e}"
    except Exception as e:
        return [], f"❌ Неизвестная ошибка: {e}"

# ---------------------- ЛОКАЛЬНЫЕ ФИЛЬТРЫ (PER-USER) ----------------------
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

# ---------------------- КОМПАКТНАЯ КАРТОЧКА ----------------------
def format_item_card_short(item: dict) -> str:
    title = item.get("title", "Без названия")
    price = item.get("price", "—")
    item_id = item.get("item_id", "—")
    uid = item.get("uid") or item.get("seller_uid") or item.get("user_id") or "—"
    region = item.get("region") or item.get("server") or "—"
    created = item.get("created_at") or item.get("date") or "—"

    lines = []
    lines.append("━━━━━━━━━━━━━━━━━━━━")
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

    link = f"https://lzt.market/{item_id}" if item_id != "—" else "—"
    lines.append(f"🔗 <a href=\"{html.escape(link)}\">{html.escape(link)}</a>")
    lines.append("━━━━━━━━━━━━━━━━━━━━")

    card = "\n".join(lines)
    if len(card) > SHORT_CARD_MAX:
        truncated = card[:SHORT_CARD_MAX - 100] + "\n... (обрезано)"
        return truncated
    return card

# ---------------------- ПОСЛЕДНИЕ 69 ЛОТОВ (PER-USER) ----------------------
async def send_compact_69_for_user(user_id: int, chat_id: int):
    try:
        url = get_user_url(user_id)
        items, error = await fetch_items(url)
        if error:
            await bot.send_message(chat_id, f"❗ Ошибка API:\n{error}")
            return

        await bot.send_message(
            chat_id,
            f"ℹ API вернул лотов: <b>{len(items)}</b>\n"
            f"🔗 Источник: {'кастомный URL' if user_urls[user_id] else 'базовый LZT_URL'}",
            parse_mode="HTML"
        )

        if not items:
            await bot.send_message(chat_id, "❗ API вернул пустой список.")
            return

        filtered = [i for i in items if passes_filters_local(i, user_id)]
        if not filtered:
            await bot.send_message(chat_id, "❗ Лоты есть, но они не проходят фильтры бота.")
            return

        # отправляем компактные карточки с паузой
        for item in filtered:
            card = format_item_card_short(item)
            await bot.send_message(chat_id, card, parse_mode="HTML", disable_web_page_preview=True)
            await asyncio.sleep(0.25)
    except Exception as e:
        await bot.send_message(chat_id, f"❌ Ошибка в send_compact_69:\n{e}")

# ---------------------- ОХОТНИК PER-USER (без сбора всех данных) ----------------------
async def hunter_loop_for_user(user_id: int, chat_id: int):
    """
    Персональный охотник:
    - при старте помечаем текущие лоты как увиденные (чтобы не спамить)
    - отправляем только новые item_id, применяя фильтры per-user
    - источник: либо базовый LZT_URL, либо кастомный URL с сайта
    """
    url = get_user_url(user_id)

    # при старте помечаем текущие лоты как увиденные
    try:
        items, error = await fetch_items(url)
        if not error and isinstance(items, list):
            for it in items:
                iid = it.get("item_id")
                if iid:
                    user_seen_items[user_id].add(iid)
    except Exception:
        pass  # не критично

    while user_search_active[user_id]:
        try:
            items, error = await fetch_items(url)
            if error:
                await bot.send_message(chat_id, f"❗ Ошибка API (охотник):\n{error}")
                await asyncio.sleep(HUNTER_INTERVAL)
                continue

            for item in items:
                item_id = item.get("item_id")
                if not item_id:
                    continue
                if item_id in user_seen_items[user_id]:
                    continue
                if not passes_filters_local(item, user_id):
                    # помечаем как увиденное, чтобы не проверять снова
                    user_seen_items[user_id].add(item_id)
                    continue
                # новый лот, отправляем компактную карточку
                user_seen_items[user_id].add(item_id)
                card = format_item_card_short(item)
                await bot.send_message(chat_id, card, parse_mode="HTML", disable_web_page_preview=True)
                await asyncio.sleep(0.25)  # пауза между отправками
            await asyncio.sleep(HUNTER_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            await bot.send_message(chat_id, f"❌ Ошибка в режиме охотника:\n{e}")
            await asyncio.sleep(HUNTER_INTERVAL)

# ---------------------- /start ----------------------
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user = message.from_user
    user_id = user.id
    chat_id = message.chat.id

    if user_id not in user_started:
        try:
            await message.answer(START_INFO)
            await message.answer(COMMANDS_MENU, parse_mode="HTML", reply_markup=main_kb())
        except Exception:
            try:
                await message.answer(START_INFO + "\n" + COMMANDS_MENU, parse_mode="HTML", reply_markup=main_kb())
            except Exception:
                pass
        user_started.add(user_id)
    else:
        # при повторном нажатии /start просто показываем меню (не дублируем стартовые тексты)
        await message.answer("⭐ Главное меню:", reply_markup=main_kb())

    await safe_delete(message)

# ---------------------- /status ----------------------
@dp.message(Command("status"))
async def status_cmd(message: types.Message):
    user = message.from_user
    user_id = user.id
    chat_id = message.chat.id
    f = user_filters[user_id]
    active = user_search_active[user_id]
    url = user_urls[user_id]
    lines = [
        "<b>Текущие настройки</b>",
        f"🔸 Мин. цена: {f['min'] if f['min'] is not None else 'не задана'}",
        f"🔸 Макс. цена: {f['max'] if f['max'] is not None else 'не задана'}",
        f"🔸 Фильтр по названию: {html.escape(f['title']) if f['title'] else 'не задан'}",
        f"🔸 Режим охотника: {'ВКЛЮЧЁН' if active else 'ВЫКЛЮЧЕН'}",
        f"🔸 Кастомный URL: {html.escape(url) if url else 'не задан (используется базовый LZT_URL)'}",
        f"🔸 Отправлено лотов (анти-дубликаты): {len(user_seen_items[user_id])}"
    ]
    await message.answer("\n".join(lines), parse_mode="HTML")
    await safe_delete(message)

# ---------------------- /stop_hunter ----------------------
@dp.message(Command("stop_hunter"))
async def stop_hunter_cmd(message: types.Message):
    user = message.from_user
    user_id = user.id
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

# ---------------------- ОБРАБОТКА КНОПОК И ВВОДА (PER-USER) ----------------------
@dp.message()
async def buttons(message: types.Message):
    user = message.from_user
    user_id = user.id
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
                    parse_mode="HTML"
                )
            else:
                await bot.send_message(chat_id, "✔ Фильтр по названию сброшен.")
            await safe_delete(message)
            return

        if mode == "url":
            # пользователь прислал URL с сайта
            user_modes[user_id] = None
            url_text = text.strip()

            if not (url_text.startswith("http://") or url_text.startswith("https://")):
                await bot.send_message(chat_id, "❌ Это не похоже на URL. Вставь ссылку вида:\nhttps://lzt.market/...")
                await safe_delete(message)
                return

            # нормализуем: lzt.market -> api.lzt.market
            # пример: https://lzt.market/mihoyo?pmin=1...
            # станет:  https://api.lzt.market/mihoyo?pmin=1...
            url_text = url_text.replace("://lzt.market", "://api.lzt.market")
            url_text = url_text.replace("://www.lzt.market", "://api.lzt.market")

            user_urls[user_id] = url_text
            user_seen_items[user_id].clear()

            await bot.send_message(
                chat_id,
                "✔ Кастомный URL сохранён.\n"
                "Теперь бот будет использовать его для поиска и охотника.\n"
                "Если хочешь вернуться к базовому API — просто очисти URL командой:\n"
                "<code>🔗 URL с сайта</code> и отправь пустую строку или слово <b>сброс</b>.",
                parse_mode="HTML"
            )
            await safe_delete(message)
            return

        # кнопки
        if text == "💎 Искать все":
            user_filters[user_id]["min"] = None
            user_filters[user_id]["max"] = None
            user_filters[user_id]["title"] = None
            user_seen_items[user_id].clear()
            await bot.send_message(chat_id, "🧹 Фильтры бота сброшены. Охотник начнёт с чистого списка.")

        elif text == "💰 Мин. цена":
            user_modes[user_id] = "min"
            await bot.send_message(chat_id, "Введи минимальную цену (число):")

        elif text == "💰 Макс. цена":
            user_modes[user_id] = "max"
            await bot.send_message(chat_id, "Введи максимальную цену (число):")

        elif text == "🔤 Фильтр по названию":
            user_modes[user_id] = "title"
            await bot.send_message(chat_id, "Введи слово/фразу, которая должна быть в названии:")

        elif text == "🔗 URL с сайта":
            user_modes[user_id] = "url"
            await bot.send_message(
                chat_id,
                "Вставь ссылку из браузера с lzt.market, например:\n"
                "https://lzt.market/mihoyo?pmin=1&pmax=399&ea=no&genshin_legendary_min=3\n\n"
                "Чтобы сбросить кастомный URL и вернуться к базовому API — отправь слово <b>сброс</b>.",
                parse_mode="HTML"
            )

        elif text.lower() == "сброс" and mode == "url":
            # сброс URL, если пользователь в режиме url и решил сбросить
            user_modes[user_id] = None
            user_urls[user_id] = None
            user_seen_items[user_id].clear()
            await bot.send_message(chat_id, "✔ Кастомный URL сброшен. Используется базовый LZT_URL.")
            await safe_delete(message)
            return

        elif text == "📦 Последние 69 лотов":
            await send_compact_69_for_user(user_id, chat_id)

        elif text == "🚀 Запустить охотника":
            # toggle: если не запущен — запускаем, если запущен — останавливаем
            if not user_search_active[user_id]:
                # запускаем: помечаем текущие лоты как увиденные, чтобы не спамить
                user_seen_items[user_id].clear()
                try:
                    url = get_user_url(user_id)
                    items, error = await fetch_items(url)
                    if not error and isinstance(items, list):
                        for it in items:
                            iid = it.get("item_id")
                            if iid:
                                user_seen_items[user_id].add(iid)
                except Exception:
                    pass

                user_search_active[user_id] = True
                task = asyncio.create_task(hunter_loop_for_user(user_id, chat_id))
                user_hunter_tasks[user_id] = task
                await bot.send_message(
                    chat_id,
                    f"🧨 Режим охотника запущен для вас (интервал {HUNTER_INTERVAL} сек).\n"
                    f"Источник: {'кастомный URL' if user_urls[user_id] else 'базовый LZT_URL'}."
                )
            else:
                user_search_active[user_id] = False
                task = user_hunter_tasks.get(user_id)
                if task:
                    task.cancel()
                    user_hunter_tasks.pop(user_id, None)
                await bot.send_message(chat_id, "🛑 Охотник остановлен у вас (по повторному нажатию).")

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

        elif text == "◀️ Назад":
            await bot.send_message(chat_id, "⭐ Главное меню:", reply_markup=main_kb())

        # авто-удаление любых текстов пользователя (кроме /команд)
        if text and not text.startswith("/"):
            await asyncio.sleep(0.5)
            await safe_delete(message)

    except Exception as e:
        await bot.send_message(chat_id, f"❌ Ошибка в обработке кнопок:\n{html.escape(str(e))}")
        await safe_delete(message)

# ---------------------- УДАЛЕНИЕ СООБЩЕНИЯ ----------------------
async def safe_delete(message: types.Message):
    try:
        await message.delete()
    except Exception:
        pass

# ---------------------- RUN ----------------------
async def main():
    print("[BOT] Запуск персонального бота (персональные фильтры + кастомный URL с сайта)...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
