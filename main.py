import asyncio
import json
import aiohttp
import html
from collections import defaultdict
from typing import Any

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

from config import API_TOKEN, LZT_API_KEY, LZT_URL, CHECK_INTERVAL

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# ---------------------- НАСТРОЙКИ ----------------------
HUNTER_INTERVAL = 1.7  # интервал охотника (секунды)
MAX_MESSAGE_PART = 4000  # безопасный лимит для Telegram HTML сообщений
SHORT_CARD_MAX = 900  # максимально допустимая длина компактной карточки

# ---------------------- ПЕРСОНАЛЬНЫЕ СТАТЫ (PER-USER) ----------------------
user_filters = defaultdict(lambda: {"min": None, "max": None, "title": None})
user_search_active = defaultdict(lambda: False)
user_seen_items = defaultdict(set)
user_hunter_tasks = {}
user_modes = defaultdict(lambda: None)  # "min", "max", "title"
user_started = set()  # пользователи, которым уже отправили стартовое сообщение

# Хранилище последних полученных лотов per-user: user_id -> {item_id: item_dict}
user_last_items = defaultdict(dict)

# ---------------------- КЛАВИАТУРА ----------------------
def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💎 Искать все")],
            [KeyboardButton(text="💰 Мин. цена"), KeyboardButton(text="💰 Макс. цена")],
            [KeyboardButton(text="🔤 Фильтр по названию")],
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
    "💎 <b>Искать все</b> — сбросить все фильтры.\n"
    "💰 <b>Мин. цена</b> — ввести минимальную цену (число).\n"
    "💰 <b>Макс. цена</b> — ввести максимальную цену (число).\n"
    "🔤 <b>Фильтр по названию</b> — ввести слово/фразу для поиска в названии.\n"
    "📦 <b>Последние 69 лотов</b> — показать текущие лоты по фильтрам.\n"
    "🚀 <b>Запустить охотника</b> — включить режим охотника только для вас.\n"
    "🛑 <b>Стоп охотника</b> или <b>/stop_hunter</b> — остановить охотника только для вас.\n"
    "/status — показать текущие фильтры и состояние охотника.\n"
    "/full <item_id> — получить полное описание конкретного лота.\n\n"
    "<i>Режим охотника</i> делает запросы каждые 1.7 секунды и отправляет только новые лоты.\n"
    "Фильтры применяются отдельно для каждого пользователя — если кто-то включит охотника, "
    "это не запустит его у других.\n"
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

# ---------------------- API LZT ----------------------
async def fetch_items():
    headers = {"Authorization": f"Bearer {LZT_API_KEY}"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(LZT_URL, headers=headers, timeout=10) as resp:
                status = resp.status
                text = await resp.text()

                print("\n===== RAW API RESPONSE =====")
                print("URL:", LZT_URL)
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

# ---------------------- УТИЛИТА: ПРЕОБРАЗОВАНИЕ ПОЛЕЙ В HTML ----------------------
def format_field_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        try:
            s = json.dumps(value, ensure_ascii=False)
        except Exception:
            s = str(value)
    else:
        s = str(value)
    return html.escape(s)

# ---------------------- БЕЗОПАСНАЯ ОТПРАВКА ДЛИННЫХ СООБЩЕНИЙ ----------------------
async def send_long_message(chat_id: int, text: str, parse_mode: str = "HTML", disable_web_page_preview: bool = True):
    MAX_LEN = MAX_MESSAGE_PART
    if len(text) <= MAX_LEN:
        try:
            msg = await bot.send_message(chat_id, text, parse_mode=parse_mode, disable_web_page_preview=disable_web_page_preview)
            return [msg.message_id]
        except Exception as e:
            print("send_long_message error:", e)
            try:
                msg = await bot.send_message(chat_id, text[:MAX_LEN], parse_mode=None, disable_web_page_preview=disable_web_page_preview)
                return [msg.message_id]
            except Exception:
                return []

    parts = []
    lines = text.split("\n")
    cur = ""
    for line in lines:
        if len(cur) + len(line) + 1 <= MAX_LEN:
            cur += (line + "\n")
        else:
            if cur:
                parts.append(cur)
            if len(line) > MAX_LEN:
                start = 0
                while start < len(line):
                    chunk = line[start:start + MAX_LEN - 10]
                    parts.append(chunk)
                    start += len(chunk)
                cur = ""
            else:
                cur = line + "\n"
    if cur:
        parts.append(cur)

    sent_ids = []
    for p in parts:
        try:
            msg = await bot.send_message(chat_id, p, parse_mode=parse_mode, disable_web_page_preview=disable_web_page_preview)
            sent_ids.append(msg.message_id)
        except Exception as e:
            print("send_long_message part error:", e)
            try:
                msg = await bot.send_message(chat_id, p[:MAX_LEN], parse_mode=None, disable_web_page_preview=disable_web_page_preview)
                sent_ids.append(msg.message_id)
            except Exception:
                pass
        await asyncio.sleep(0.15)
    return sent_ids

# ---------------------- КОМПАКТНАЯ КАРТОЧКА (короткая, безопасная) ----------------------
def format_item_card_short(item: dict) -> str:
    """
    Формируем компактную карточку: ключевые поля, коротко.
    Если итог > SHORT_CARD_MAX — обрезаем и добавляем подсказку /full <item_id>.
    """
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

    # небольшая выборка дополнительных полей, если есть
    extra_keys = ["stock", "count", "condition", "platform", "tag", "title_extra"]
    for k in extra_keys:
        if k in item:
            lines.append(f"🔸 {html.escape(str(k))}: {html.escape(str(item.get(k)))}")

    # characters
    chars = extract_characters(title)
    if chars:
        for c in chars:
            lines.append(f"✨ {html.escape(c)}")

    link = f"https://lzt.market/{item_id}" if item_id != "—" else "—"
    lines.append(f"🔗 <a href=\"{html.escape(link)}\">{html.escape(link)}</a>")
    lines.append("━━━━━━━━━━━━━━━━━━━━")

    card = "\n".join(lines)
    if len(card) > SHORT_CARD_MAX:
        # обрезаем аккуратно
        truncated = card[:SHORT_CARD_MAX - 100] + "\n... (обрезано)\n"
        truncated += f"Для полного описания: /full {html.escape(str(item_id))}"
        return truncated
    else:
        return card

# ---------------------- ПОЛНАЯ КАРТОЧКА (весь JSON, но безопасно обрезаем очень длинные поля) ----------------------
def format_item_card_full(item: dict) -> str:
    lines = []
    title = item.get("title", "Без названия")
    price = item.get("price", "—")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"🎮 <b>{html.escape(str(title))}</b>")
    if price != "—":
        lines.append(f"💰 <b>{html.escape(str(price))}₽</b>")
    else:
        lines.append("💰 —")

    # characters
    chars = extract_characters(title)
    if chars:
        for c in chars:
            lines.append(f"✨ {html.escape(c)}")

    # все поля
    for key in sorted(item.keys()):
        value = item.get(key)
        try:
            if isinstance(value, (dict, list)):
                formatted = json.dumps(value, ensure_ascii=False)
            else:
                formatted = str(value)
        except Exception:
            formatted = str(value)
        # обрезаем очень длинные поля
        if len(formatted) > 3000:
            formatted = formatted[:3000] + "... (обрезано)"
        lines.append(f"🔹 <b>{html.escape(str(key))}</b>: {html.escape(formatted)}")

    item_id = item.get("item_id")
    link = f"https://lzt.market/{item_id}" if item_id else "—"
    lines.append(f"🔗 <a href=\"{html.escape(link)}\">{html.escape(link)}</a>")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)

# ---------------------- ПОСЛЕДНИЕ 69 ЛОТОВ (PER-USER) ----------------------
async def send_compact_69_for_user(user_id: int, chat_id: int):
    try:
        items, error = await fetch_items()
        if error:
            await send_long_message(chat_id, f"❗ Ошибка API:\n{html.escape(str(error))}")
            return

        await send_long_message(chat_id, f"ℹ API вернул лотов: <b>{len(items)}</b>")

        if not items:
            await send_long_message(chat_id, "❗ API вернул пустой список.")
            return

        filtered = [i for i in items if passes_filters_local(i, user_id)]
        if not filtered:
            await send_long_message(chat_id, "❗ Лоты есть, но они не проходят фильтры.")
            return

        # сохраняем последние лоты для пользователя (по item_id)
        user_last_items[user_id].clear()
        for item in filtered:
            item_id = item.get("item_id")
            if item_id:
                user_last_items[user_id][str(item_id)] = item

        # отправляем компактные карточки
        for item in filtered:
            card = format_item_card_short(item)
            # гарантируем, что каждая карточка не превышает лимит
            if len(card) > MAX_MESSAGE_PART:
                card = card[:MAX_MESSAGE_PART - 100] + "\n... (обрезано)"
            await send_long_message(chat_id, card)
            await asyncio.sleep(0.25)
    except Exception as e:
        await send_long_message(chat_id, f"❌ Ошибка в send_compact_69:\n{html.escape(str(e))}")

# ---------------------- ОХОТНИК PER-USER ----------------------
async def hunter_loop_for_user(user_id: int, chat_id: int):
    while user_search_active[user_id]:
        try:
            items, error = await fetch_items()
            if error:
                await send_long_message(chat_id, f"❗ Ошибка API (охотник):\n{html.escape(str(error))}")
                await asyncio.sleep(HUNTER_INTERVAL)
                continue

            # обновляем локальное хранилище последних лотов (не перезаписываем старые)
            for item in items:
                item_id = item.get("item_id")
                if item_id:
                    user_last_items[user_id][str(item_id)] = item

            for item in items:
                item_id = item.get("item_id")
                if not item_id:
                    continue
                if item_id in user_seen_items[user_id]:
                    continue
                if not passes_filters_local(item, user_id):
                    continue
                user_seen_items[user_id].add(item_id)
                card = format_item_card_short(item)
                await send_long_message(chat_id, card)
                await asyncio.sleep(0.25)
            await asyncio.sleep(HUNTER_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            await send_long_message(chat_id, f"❌ Ошибка в режиме охотника:\n{html.escape(str(e))}")
            await asyncio.sleep(HUNTER_INTERVAL)

# ---------------------- /start (отправляем стартовое сообщение один раз пользователю) ----------------------
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
        await message.answer("⭐ Главное меню:", reply_markup=main_kb())
    await safe_delete(message)

# ---------------------- /status (показать текущие фильтры и состояние) ----------------------
@dp.message(Command("status"))
async def status_cmd(message: types.Message):
    user = message.from_user
    user_id = user.id
    chat_id = message.chat.id
    f = user_filters[user_id]
    active = user_search_active[user_id]
    lines = [
        "<b>Текущие настройки</b>",
        f"🔸 Мин. цена: {f['min'] if f['min'] is not None else 'не задана'}",
        f"🔸 Макс. цена: {f['max'] if f['max'] is not None else 'не задана'}",
        f"🔸 Фильтр по названию: {html.escape(f['title']) if f['title'] else 'не задан'}",
        f"🔸 Режим охотника: {'ВКЛЮЧЁН' if active else 'ВЫКЛЮЧЕН'}",
        f"🔸 Отправлено лотов (анти-дубликаты): {len(user_seen_items[user_id])}",
        f"🔸 Сохранено последних лотов для /full: {len(user_last_items[user_id])}"
    ]
    await message.answer("\n".join(lines), parse_mode="HTML")
    await safe_delete(message)

# ---------------------- /stop_hunter (команда для остановки охотника) ----------------------
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

# ---------------------- /full <item_id> — вернуть полное описание лота ----------------------
@dp.message()
async def full_handler(message: types.Message):
    text = (message.text or "").strip()
    if not text.startswith("/full"):
        return  # не наша команда — пропускаем (будет обработано в buttons)
    parts = text.split()
    if len(parts) < 2:
        await message.answer("Использование: /full <item_id>")
        await safe_delete(message)
        return
    item_id = parts[1]
    user = message.from_user
    user_id = user.id
    chat_id = message.chat.id

    item = user_last_items[user_id].get(item_id)
    if not item:
        # попробуем найти по int key
        item = user_last_items[user_id].get(str(item_id))
    if not item:
        await message.answer("❗ Лот с таким item_id не найден в ваших последних лотах. Сначала вызовите 'Последние 69 лотов' или дождитесь охотника.")
        await safe_delete(message)
        return

    # формируем полную карточку и отправляем безопасно
    full_text = format_item_card_full(item)
    await send_long_message(chat_id, full_text)
    await safe_delete(message)

# ---------------------- ОБРАБОТКА КНОПОК И ВВОДА (PER-USER) ----------------------
@dp.message()
async def buttons(message: types.Message):
    # если это /full — уже обработано в full_handler (он сработает раньше)
    text = (message.text or "").strip()
    if text.startswith("/full"):
        return

    user = message.from_user
    user_id = user.id
    chat_id = message.chat.id
    mode = user_modes[user_id]

    try:
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
                await bot.send_message(chat_id, f"✔ Фильтр по названию: <b>{html.escape(user_filters[user_id]['title'])}</b>", parse_mode="HTML")
            else:
                await bot.send_message(chat_id, "✔ Фильтр по названию сброшен.")
            await safe_delete(message)
            return

        if text == "💎 Искать все":
            user_filters[user_id]["min"] = None
            user_filters[user_id]["max"] = None
            user_filters[user_id]["title"] = None
            user_seen_items[user_id].clear()
            user_last_items[user_id].clear()
            await bot.send_message(chat_id, "🧹 Фильтры сброшены. Охотник начнёт с чистого списка.")

        elif text == "💰 Мин. цена":
            user_modes[user_id] = "min"
            await bot.send_message(chat_id, "Введи минимальную цену (число):")

        elif text == "💰 Макс. цена":
            user_modes[user_id] = "max"
            await bot.send_message(chat_id, "Введи максимальную цену (число):")

        elif text == "🔤 Фильтр по названию":
            user_modes[user_id] = "title"
            await bot.send_message(chat_id, "Введи слово/фразу, которая должна быть в названии:")

        elif text == "📦 Последние 69 лотов":
            await send_compact_69_for_user(user_id, chat_id)

        elif text == "🚀 Запустить охотника":
            if not user_search_active[user_id]:
                user_search_active[user_id] = True
                user_seen_items[user_id].clear()
                user_last_items[user_id].clear()
                task = asyncio.create_task(hunter_loop_for_user(user_id, chat_id))
                user_hunter_tasks[user_id] = task
                await bot.send_message(chat_id, f"🧨 Режим охотника запущен для вас (интервал {HUNTER_INTERVAL} сек).")
            else:
                await bot.send_message(chat_id, "⚠ Охотник уже работает у вас.")

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
    print("[BOT] Запуск персонального бота (охотник per-user, короткие карточки + /full)...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
