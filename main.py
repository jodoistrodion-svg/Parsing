import asyncio
import aiohttp
from collections import defaultdict

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

from config import API_TOKEN, LZT_API_KEY, LZT_URL, CHECK_INTERVAL

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

current_min_price = None
current_max_price = None
search_active = False


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

            # структура ответа:
            # { "data": { "items": [ ... ] } }
            return data.get("data", {}).get("items", [])


def passes_filters(item):
    price = item.get("price", 0)

    if current_min_price is not None and price < current_min_price:
        return False
    if current_max_price is not None and price > current_max_price:
        return False

    return True


# ---------------------- ПОСЛЕДНИЕ 69 ЛОТОВ ----------------------
async def send_compact_69(message: types.Message):
    items = await fetch_items()

    global current_min_price, current_max_price

    # если фильтров нет — берём всё
    if current_min_price is None and current_max_price is None:
        filtered = items
    else:
        filtered = [i for i in items if passes_filters(i)]

    if not filtered:
        await message.answer("❗ Лоты не найдены.")
        return

    # группировка по цене
    groups = defaultdict(list)

    for item in filtered:
        item_id = item.get("item_id") or item.get("id")
        if not item_id:
            continue

        price = item.get("price", 0)
        groups[price].append(item_id)

    # отправка
    for price, ids in groups.items():
        if len(ids) == 1:
            await message.answer(
                f"💰 Цена: <b>{price}₽</b>\n🔗 https://lzt.market/{ids[0]}",
                parse_mode="HTML"
            )
        else:
            links = "\n".join(f"🔗 https://lzt.market/{i}" for i in ids)
            await message.answer(
                f"💰 Цена: <b>{price}₽</b>\n{links}",
                parse_mode="HTML"
            )


# ---------------------- МОНИТОРИНГ ----------------------
async def monitor_new_items(message: types.Message):
    global search_active

    sent_ids = set()

    while search_active:
        items = await fetch_items()

        for item in items:
            item_id = item.get("item_id") or item.get("id")
            if not item_id:
                continue

            if item_id not in sent_ids and passes_filters(item):
                sent_ids.add(item_id)

                title = item.get("title", "Без названия")
                price = item.get("price", 0)
                link = f"https://lzt.market/{item_id}"

                await message.answer(
                    f"<b>{title}</b>\n💰 {price}₽\n🔗 {link}",
                    parse_mode="HTML"
                )

        await asyncio.sleep(CHECK_INTERVAL)


# ---------------------- START ----------------------
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await message.answer("⭐️ Главное меню:", reply_markup=main_kb())


# ---------------------- КНОПКИ ----------------------
@dp.message()
async def buttons(message: types.Message):
    global current_min_price, current_max_price, search_active

    text = message.text

    # ввод чисел
    if dp.get("mode") == "min" and text.isdigit():
        current_min_price = int(text)
        dp["mode"] = None
        await message.answer(f"✔ Мин. цена: {current_min_price}₽")
        return

    if dp.get("mode") == "max" and text.isdigit():
        current_max_price = int(text)
        dp["mode"] = None
        await message.answer(f"✔ Макс. цена: {current_max_price}₽")
        return

    # кнопки
    if text == "💎 Искать все":
        current_min_price = None
        current_max_price = None
        await message.answer("✔ Фильтры сброшены.")

    elif text == "💰 Мин. цена":
        dp["mode"] = "min"
        await message.answer("Введи минимальную цену:")

    elif text == "💰 Макс. цена":
        dp["mode"] = "max"
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
