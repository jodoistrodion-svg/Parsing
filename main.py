import asyncio
import json
import aiohttp
from collections import defaultdict

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

from config import API_TOKEN, LZT_API_KEY, LZT_URL, CHECK_INTERVAL

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# ---------------------- ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ----------------------
# Фильтры храним per-chat
filters = defaultdict(lambda: {"min": None, "max": None, "title": None})

# Режим охотника per-chat
search_active = defaultdict(lambda: False)
# Интервал охотника (по умолчанию 1.7 сек, можно менять)
HUNTER_INTERVAL = 1.7

# seen items per-chat (анти-дубликаты между сессиями для каждого чата)
seen_items = defaultdict(set)

# задачи охотника per-chat (чтобы можно было отменять)
hunter_tasks = {}

# режимы ввода per-chat (min/max/title)
modes = defaultdict(lambda: None)


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


# ---------------------- СТАРТОВОЕ СООБЩЕНИЕ / МЕНЮ КОМАНД ----------------------
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
    "🚀 <b>Запустить охотника</b> — включить режим охотника только для вашего чата.\n"
    "🛑 <b>Стоп охотника</b> — остановить охотника в вашем чате.\n\n"
    "<i>Режим охотника</i> делает запросы каждые 1.7 секунды и отправляет только новые лоты.\n"
    "Фильтры применяются отдельно для каждого чата — если кто-то включит охотника, "
    "это не запустит его у других.\n"
)


# ---------------------- ПАРСЕР ПЕРСОНАЖЕЙ ----------------------
def extract_characters(title: str):
    result = []

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

    if title:
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


# ---------------------- ЛОКАЛЬНЫЕ ФИЛЬТРЫ ----------------------
def passes_filters_local(item, chat_id):
    f = filters[chat_id]
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


# ---------------------- ПРЕМИУМ-КАРТОЧКА ----------------------
def format_item_card(item):
    item_id = item.get("item_id")
    title = item.get("title", "Без названия")
    price = item.get("price", 0)

    chars = extract_characters(title)
    chars_block = ""
    if chars:
        chars_block = "\n".join(f"✨ {c}" for c in chars)

    link = f"https://lzt.market/{item_id}" if item_id else "—"

    card = (
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🎮 <b>{title}</b>\n"
        f"💰 <b>{price}₽</b>\n"
    )

    if chars_block:
        card += chars_block + "\n"

    card += f"🔗 <a href=\"{link}\">{link}</a>\n"
    card += "━━━━━━━━━━━━━━━━━━━━"

    return card


# ---------------------- ПОСЛЕДНИЕ 69 ЛОТОВ ----------------------
async def send_compact_69(chat_id):
    try:
        items, error = await fetch_items()

        if error:
            await bot.send_message(chat_id, f"❗ Ошибка API:\n{error}")
            return

        await bot.send_message(chat_id, f"ℹ API вернул лотов: <b>{len(items)}</b>", parse_mode="HTML")

        if not items:
            await bot.send_message(chat_id, "❗ API вернул пустой список.")
            return

        filtered = [i for i in items if passes_filters_local(i, chat_id)]

        if not filtered:
            await bot.send_message(chat_id, "❗ Лоты есть, но они не проходят фильтры.")
            return

        for item in filtered:
            card = format_item_card(item)
            await bot.send_message(chat_id, card, parse_mode="HTML", disable_web_page_preview=True)

    except Exception as e:
        await bot.send_message(chat_id, f"❌ Ошибка в send_compact_69:\n{e}")


# ---------------------- РЕЖИМ ОХОТНИКА PER-CHAT ----------------------
async def hunter_loop(chat_id):
    """
    Режим охотника для конкретного чата:
    - запрос каждые HUNTER_INTERVAL сек
    - отправка только новых лотов (per-chat seen_items)
    - авто-рестарт при ошибках (цикл не падает)
    """
    while search_active[chat_id]:
        try:
            items, error = await fetch_items()

            if error:
                await bot.send_message(chat_id, f"❗ Ошибка API (охотник):\n{error}")
                await asyncio.sleep(HUNTER_INTERVAL)
                continue

            for item in items:
                item_id = item.get("item_id")
                if not item_id:
                    continue

                # анти-дубликаты per-chat
                if item_id in seen_items[chat_id]:
                    continue

                if not passes_filters_local(item, chat_id):
                    continue

                seen_items[chat_id].add(item_id)

                card = format_item_card(item)
                await bot.send_message(chat_id, card, parse_mode="HTML", disable_web_page_preview=True)

            await asyncio.sleep(HUNTER_INTERVAL)

        except Exception as e:
            await bot.send_message(chat_id, f"❌ Ошибка в режиме охотника:\n{e}")
            await asyncio.sleep(HUNTER_INTERVAL)


# ---------------------- START ----------------------
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    chat_id = message.chat.id

    # Отправляем стартовую информацию один раз при команде /start
    try:
        await message.answer(START_INFO)
        await message.answer(COMMANDS_MENU, parse_mode="HTML", reply_markup=main_kb())
    except Exception:
        # если не получилось отправить как отдельные сообщения, пробуем одно
        try:
            await message.answer(START_INFO + "\n" + COMMANDS_MENU, parse_mode="HTML", reply_markup=main_kb())
        except Exception:
            pass

    # удаляем команду пользователя
    await safe_delete(message)


# ---------------------- КНОПКИ + АВТО-УДАЛЕНИЕ (PER-CHAT) ----------------------
@dp.message()
async def buttons(message: types.Message):
    chat_id = message.chat.id
    user_msg = message
    text = (message.text or "").strip()
    mode = modes[chat_id]

    try:
        # режимы ввода
        if mode == "min" and text.isdigit():
            filters[chat_id]["min"] = int(text)
            modes[chat_id] = None
            await bot.send_message(chat_id, f"✔ Мин. цена: {filters[chat_id]['min']}₽")
            await safe_delete(user_msg)
            return

        if mode == "max" and text.isdigit():
            filters[chat_id]["max"] = int(text)
            modes[chat_id] = None
            await bot.send_message(chat_id, f"✔ Макс. цена: {filters[chat_id]['max']}₽")
            await safe_delete(user_msg)
            return

        if mode == "title":
            filters[chat_id]["title"] = text or None
            modes[chat_id] = None
            if filters[chat_id]["title"]:
                await bot.send_message(chat_id, f"✔ Фильтр по названию: <b>{filters[chat_id]['title']}</b>", parse_mode="HTML")
            else:
                await bot.send_message(chat_id, "✔ Фильтр по названию сброшен.")
            await safe_delete(user_msg)
            return

        # кнопки
        if text == "💎 Искать все":
            filters[chat_id]["min"] = None
            filters[chat_id]["max"] = None
            filters[chat_id]["title"] = None
            seen_items[chat_id].clear()
            await bot.send_message(chat_id, "🧹 Фильтры сброшены. Охотник начнёт с чистого списка.")

        elif text == "💰 Мин. цена":
            modes[chat_id] = "min"
            await bot.send_message(chat_id, "Введи минимальную цену (число):")

        elif text == "💰 Макс. цена":
            modes[chat_id] = "max"
            await bot.send_message(chat_id, "Введи максимальную цену (число):")

        elif text == "🔤 Фильтр по названию":
            modes[chat_id] = "title"
            await bot.send_message(chat_id, "Введи слово/фразу, которая должна быть в названии:")

        elif text == "📦 Последние 69 лотов":
            await send_compact_69(chat_id)

        elif text == "🚀 Запустить охотника":
            if not search_active[chat_id]:
                search_active[chat_id] = True
                seen_items[chat_id].clear()
                # создаём задачу и сохраняем её
                task = asyncio.create_task(hunter_loop(chat_id))
                hunter_tasks[chat_id] = task
                await bot.send_message(chat_id, f"🧨 Режим охотника запущен (интервал {HUNTER_INTERVAL} сек).")
            else:
                await bot.send_message(chat_id, "⚠ Охотник уже работает в этом чате.")

        elif text == "🛑 Стоп охотника":
            if search_active[chat_id]:
                search_active[chat_id] = False
                # отменяем задачу, если есть
                task = hunter_tasks.get(chat_id)
                if task:
                    task.cancel()
                    hunter_tasks.pop(chat_id, None)
                await bot.send_message(chat_id, "🛑 Охотник остановлен в этом чате.")
            else:
                await bot.send_message(chat_id, "⚠ Охотник и так не запущен в этом чате.")

        elif text == "◀️ Назад":
            await bot.send_message(chat_id, "⭐ Главное меню:", reply_markup=main_kb())

        # авто-удаление любых текстов пользователя (кроме /команд)
        if text and not text.startswith("/"):
            await asyncio.sleep(0.5)
            await safe_delete(user_msg)

    except Exception as e:
        await bot.send_message(chat_id, f"❌ Ошибка в обработке кнопок:\n{e}")
        await safe_delete(user_msg)


async def safe_delete(message: types.Message):
    try:
        await message.delete()
    except Exception:
        pass


# ---------------------- RUN ----------------------
async def main():
    print("[BOT] Запуск бота (многопользовательный режим, охотник per-chat)...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
