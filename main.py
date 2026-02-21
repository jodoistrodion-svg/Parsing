import asyncio
import aiohttp
import sys
import time
from collections import defaultdict

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

from config import API_TOKEN, LZT_API_KEY, LZT_URL, CHECK_INTERVAL

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

sent_ids = set()

current_min_price = None
current_max_price = None
search_active = False

status_message_id = None
status_chat_id = None

attempt = 0
found_count = 0

input_mode = None
temp_messages = []

# ---------------------- КЛАВИАТУРА ----------------------
def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💎 Искать все")],
            [KeyboardButton(text="💰 Мин. цена"), KeyboardButton(text="💰 Макс. цена")],
            [KeyboardButton(text="📦 Последние 69 лотов")],
            [KeyboardButton(text="🚀 Запустить поиск")],
            [KeyboardButton(text="🔄 Перезапустить"), KeyboardButton(text="🛑 Стоп")],
            [KeyboardButton(text="◀️ Назад")]
        ],
        resize_keyboard=True
    )

# ---------------------- ПАРСИНГ ----------------------
async def fetch_items():
    headers = {"Authorization": f"Bearer {LZT_API_KEY}"}
    async with aiohttp.ClientSession() as session:
        async with session.get(LZT_URL, headers=headers) as resp:
            data = await resp.json()
            return data.get("data", [])

def passes_filters(item):
    price = item.get("price", 0)

    if current_min_price is not None and price < current_min_price:
        return False
    if current_max_price is not None and price > current_max_price:
        return False

    return True

# ---------------------- ПОСЛЕДНИЕ 69 ЛОТОВ ----------------------
async def fetch_last_69():
    items = await fetch_items()
    return items[:69]  # просто берём последние 69

async def send_compact_69(message: types.Message):
    items = await fetch_last_69()

    # фильтруем по цене
    filtered = [i for i in items if passes_filters(i)]

    # фильтр по miHoYo
    def is_mihoyo(item):
        game = item.get("game", "").lower()
        return any(x in game for x in ["genshin", "star", "honkai", "mihoyo"])

    filtered = [i for i in filtered if is_mihoyo(i)]

    if not filtered:
        await message.answer("❗ Лоты не найдены.")
        return

    # группировка по цене
    groups = defaultdict(list)
    for item in filtered:
        price = item.get("price", 0)
        item_id = item.get("item_id")
        groups[price].append(item_id)

    # отправка
    for price, ids in groups.items():
        if len(ids) == 1:
            # одиночный лот
            link = f"https://lzt.market/{ids[0]}"
            await message.answer(
                f"💰 Цена: <b>{price}₽</b>\n🔗 {link}",
                parse_mode="HTML"
            )
        else:
            # несколько ссылок одной ценой
            links = "\n".join(f"🔗 https://lzt.market/{i}" for i in ids)
            await message.answer(
                f"💰 Цена: <b>{price}₽</b>\n{links}",
                parse_mode="HTML"
            )

# ---------------------- МОНИТОРИНГ ----------------------
async def monitor_new_items(message: types.Message):
    global search_active, attempt, found_count

    attempt = 0
    found_count = 0

    while search_active:
        attempt += 1
        items = await fetch_items()

        for item in items:
            item_id = item.get("item_id")

            if item_id not in sent_ids and passes_filters(item):
                sent_ids.add(item_id)
                found_count += 1

                title = item.get("title", "Без названия")
                game = item.get("game", "miHoYo")
                price = item.get("price", 0)
                link = f"https://lzt.market/{item_id}"

                text = (
                    f"> <b>{title}</b>\n"
                    f"> Игра: {game}\n"
                    f"> Цена: {price}₽\n"
                    f"> <a href=\"{link}\">Открыть лот</a>"
                )

                await message.answer(text, parse_mode="HTML")

        await asyncio.sleep(CHECK_INTERVAL)

# ---------------------- START ----------------------
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await message.answer("⭐️ Главное меню:", reply_markup=main_kb())

# ---------------------- КНОПКИ ----------------------
@dp.message()
async def buttons(message: types.Message):
    global current_min_price, current_max_price, search_active
    global input_mode, temp_messages

    text = message.text

    if input_mode == "min":
        try:
            current_min_price = int(text)
            await message.answer(f"✔ Мин. цена: {current_min_price}₽")
        except:
            await message.answer("⚠ Введи число.")
        input_mode = None
        return

    if input_mode == "max":
        try:
            current_max_price = int(text)
            await message.answer(f"✔ Макс. цена: {current_max_price}₽")
        except:
            await message.answer("⚠ Введи число.")
        input_mode = None
        return

    # кнопки
    if text == "💎 Искать все":
        current_min_price = None
        current_max_price = None
        await message.answer("✔ Фильтры сброшены.")

    elif text == "💰 Мин. цена":
        input_mode = "min"
        await message.answer("Введи минимальную цену:")

    elif text == "💰 Макс. цена":
        input_mode = "max"
        await message.answer("Введи максимальную цену:")

    elif text == "📦 Последние 69 лотов":
        await send_compact_69(message)

    elif text == "🚀 Запустить поиск":
        if not search_active:
            search_active = True
            asyncio.create_task(monitor_new_items(message))
            await message.answer("🔎 Поиск запущен.")
        else:
            await message.answer("⚠ Поиск уже работает.")

    elif text == "🔄 Перезапустить":
        sent_ids.clear()
        await message.answer("✔ Перезапущено.")

    elif text == "🛑 Стоп":
        search_active = False
        await message.answer("🛑 Поиск остановлен.")

    elif text == "◀️ Назад":
        await message.answer("⭐️ Главное меню:", reply_markup=main_kb())

# ---------------------- RUN ----------------------
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
