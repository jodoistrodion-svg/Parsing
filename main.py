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


# ---------------------- API LZT ----------------------
async def fetch_items():
    """
    Делаем запрос к API и ВСЕГДА возвращаем:
    - список items
    - текст ошибки (или None)
    """
    headers = {"Authorization": f"Bearer {LZT_API_KEY}"}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(LZT_URL, headers=headers, timeout=10) as resp:
                status = resp.status
                text = await resp.text()

                # ЛОГИ В КОНСОЛЬ
                print("\n===== RAW API RESPONSE =====")
                print("STATUS:", status)
                print("TEXT:", text[:500])
                print("============================\n")

                # Парсим JSON
                try:
                    data = json.loads(text)
                except Exception as e:
                    return [], f"❌ API вернул не JSON: {e}\nОтвет: {text[:300]}"

                # ТВОЙ API отдаёт items
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


def passes_filters(item):
    price = item.get("price", 0)

    if current_min_price is not None and price < current_min_price:
        return False
    if current_max_price is not None and price > current_max_price:
        return False

    return True


# ---------------------- ПОСЛЕДНИЕ 69 ЛОТОВ ----------------------
async def send_compact_69(message: types.Message):
    try:
        items, error = await fetch_items()

        if error:
            await message.answer(f"❗ Ошибка API:\n{error}")
            return

        await message.answer(f"ℹ API вернул лотов: <b>{len(items)}</b>", parse_mode="HTML")

        if not items:
            await message.answer("❗ API вернул пустой список.")
            return

        filtered = [i for i in items if passes_filters(i)]

        if not filtered:
            await message.answer("❗ Лоты есть, но они не проходят фильтры.")
            return

        groups = defaultdict(list)

        for item in filtered:
            item_id = item.get("item_id")
            price = item.get("price", 0)

            if not item_id:
                print("[WARN] Лот без item_id:", item)
                continue

            groups[price].append(item_id)

        if not groups:
            await message.answer("❗ Лоты есть, но у них нет item_id.")
            return

        for price, ids in groups.items():
            if len(ids) == 1:
                await message.answer(
                    f"💰 {price}₽\n🔗 https://lzt.market/{ids[0]}",
                    parse_mode="HTML"
                )
            else:
                links = "\n".join(f"🔗 https://lzt.market/{i}" for i in ids)
                await message.answer(
                    f"💰 {price}₽\n{links}",
                    parse_mode="HTML"
                )

    except Exception as e:
        await message.answer(f"❌ Ошибка в send_compact_69:\n{e}")


# ---------------------- МОНИТОРИНГ ----------------------
async def monitor_new_items(message: types.Message):
    global search_active
    sent = set()

    while search_active:
        try:
            items, error = await fetch_items()

            if error:
                await message.answer(f"❗ Ошибка API:\n{error}")
                await asyncio.sleep(CHECK_INTERVAL)
                continue

            for item in items:
                item_id = item.get("item_id")
                if not item_id:
                    continue

                if item_id not in sent and passes_filters(item):
                    sent.add(item_id)
                    await message.answer(
                        f"<b>{item.get('title','Без названия')}</b>\n"
                        f"💰 {item.get('price',0)}₽\n"
                        f"🔗 https://lzt.market/{item_id}",
                        parse_mode="HTML"
                    )

            await asyncio.sleep(CHECK_INTERVAL)

        except Exception as e:
            await message.answer(f"❌ Ошибка в мониторинге:\n{e}")
            await asyncio.sleep(CHECK_INTERVAL)


# ---------------------- START ----------------------
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await message.answer("⭐ Главное меню:", reply_markup=main_kb())


# ---------------------- КНОПКИ ----------------------
@dp.message()
async def buttons(message: types.Message):
    global current_min_price, current_max_price, search_active

    try:
        text = message.text

        if getattr(dp, "mode", None) == "min" and text.isdigit():
            current_min_price = int(text)
            dp.mode = None
            await message.answer(f"✔ Мин. цена: {current_min_price}")
            return

        if getattr(dp, "mode", None) == "max" and text.isdigit():
            current_max_price = int(text)
            dp.mode = None
            await message.answer(f"✔ Макс. цена: {current_max_price}")
            return

        if text == "💎 Искать все":
            current_min_price = None
            current_max_price = None
            await message.answer("🧹 Фильтры сброшены.")

        elif text == "💰 Мин. цена":
            dp.mode = "min"
            await message.answer("Введи минимальную цену:")

        elif text == "💰 Макс. цена":
            dp.mode = "max"
            await message.answer("Введи максимальную цену:")

        elif text == "📦 Последние 69 лотов":
            await send_compact_69(message)

        elif text == "🚀 Запустить поиск":
            if not search_active:
                search_active = True
                asyncio.create_task(monitor_new_items(message))
                await message.answer("🔎 Поиск запущен.")
            else:
                await message.answer("⚠ Уже работает.")

        elif text == "🛑 Стоп":
            search_active = False
            await message.answer("🛑 Остановлено.")

        elif text == "◀️ Назад":
            await message.answer("⭐ Главное меню:", reply_markup=main_kb())

    except Exception as e:
        await message.answer(f"❌ Ошибка в обработке кнопок:\n{e}")


# ---------------------- RUN ----------------------
async def main():
    print("[BOT] Запуск бота...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
