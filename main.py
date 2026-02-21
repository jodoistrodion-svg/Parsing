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

# Фильтры
current_min_price = None
current_max_price = None
current_title_filter = None

# Режим охотника
search_active = False
HUNTER_INTERVAL = 1.7  # твои 1.7 секунды

# Глобальный набор уже увиденных лотов (анти-дубликаты в рамках запуска)
seen_items = set()


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

    grab("Genshin")
    grab("Genshin Impact")
    grab("ZZZ")
    grab("Zenless Zone Zero")

    return result


# ---------------------- API LZT ----------------------
async def fetch_items():
    """
    Работает строго с твоим URL:
    https://api.lzt.market/mihoyo?per_page=69&order_by=date_to_down
    """
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
def passes_filters_local(item):
    price = item.get("price", 0)

    if current_min_price is not None and price < current_min_price:
        return False
    if current_max_price is not None and price > current_max_price:
        return False

    if current_title_filter:
        title = item.get("title", "") or ""
        if current_title_filter.lower() not in title.lower():
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
async def send_compact_69(message: types.Message):
    try:
        items, error = await fetch_items()

        if error:
            await message.answer(f"❗ Ошибка API:\n{error}")
            return

        await message.answer(
            f"ℹ API вернул лотов: <b>{len(items)}</b>",
            parse_mode="HTML"
        )

        if not items:
            await message.answer("❗ API вернул пустой список.")
            return

        filtered = [i for i in items if passes_filters_local(i)]

        if not filtered:
            await message.answer("❗ Лоты есть, но они не проходят фильтры.")
            return

        for item in filtered:
            card = format_item_card(item)
            await message.answer(card, parse_mode="HTML", disable_web_page_preview=True)

    except Exception as e:
        await message.answer(f"❌ Ошибка в send_compact_69:\n{e}")


# ---------------------- РЕЖИМ ОХОТНИКА ----------------------
async def hunter_loop(message: types.Message):
    """
    Режим охотника:
    - запрос каждые 1.7 сек
    - отправка только новых лотов
    - авто-рестарт при ошибках
    """
    global search_active, seen_items

    while search_active:
        try:
            items, error = await fetch_items()

            if error:
                await message.answer(f"❗ Ошибка API (охотник):\n{error}")
                await asyncio.sleep(HUNTER_INTERVAL)
                continue

            for item in items:
                item_id = item.get("item_id")
                if not item_id:
                    continue

                # анти-дубликаты
                if item_id in seen_items:
                    continue

                if not passes_filters_local(item):
                    continue

                seen_items.add(item_id)

                card = format_item_card(item)
                await message.answer(card, parse_mode="HTML", disable_web_page_preview=True)

            await asyncio.sleep(HUNTER_INTERVAL)

        except Exception as e:
            await message.answer(f"❌ Ошибка в режиме охотника:\n{e}")
            await asyncio.sleep(HUNTER_INTERVAL)


# ---------------------- START ----------------------
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await message.answer("⭐ Главное меню:", reply_markup=main_kb())


# ---------------------- КНОПКИ + АВТО-УДАЛЕНИЕ ----------------------
@dp.message()
async def buttons(message: types.Message):
    global current_min_price, current_max_price, current_title_filter, search_active, seen_items

    user_msg = message

    try:
        text = message.text or ""
        mode = getattr(dp, "mode", None)

        # режимы ввода
        if mode == "min" and text.isdigit():
            current_min_price = int(text)
            dp.mode = None
            await message.answer(f"✔ Мин. цена: {current_min_price}₽")
            await safe_delete(user_msg)
            return

        if mode == "max" and text.isdigit():
            current_max_price = int(text)
            dp.mode = None
            await message.answer(f"✔ Макс. цена: {current_max_price}₽")
            await safe_delete(user_msg)
            return

        if mode == "title":
            current_title_filter = text.strip() or None
            dp.mode = None
            if current_title_filter:
                await message.answer(f"✔ Фильтр по названию: <b>{current_title_filter}</b>", parse_mode="HTML")
            else:
                await message.answer("✔ Фильтр по названию сброшен.")
            await safe_delete(user_msg)
            return

        # кнопки
        if text == "💎 Искать все":
            current_min_price = None
            current_max_price = None
            current_title_filter = None
            seen_items.clear()
            await message.answer("🧹 Фильтры сброшены. Охотник начнёт с чистого списка.")

        elif text == "💰 Мин. цена":
            dp.mode = "min"
            await message.answer("Введи минимальную цену (число):")

        elif text == "💰 Макс. цена":
            dp.mode = "max"
            await message.answer("Введи максимальную цену (число):")

        elif text == "🔤 Фильтр по названию":
            dp.mode = "title"
            await message.answer("Введи слово/фразу, которая должна быть в названии:")

        elif text == "📦 Последние 69 лотов":
            await send_compact_69(message)

        elif text == "🚀 Запустить охотника":
            if not search_active:
                search_active = True
                seen_items.clear()
                asyncio.create_task(hunter_loop(message))
                await message.answer("🧨 Режим охотника запущен (интервал 1.7 сек).")
            else:
                await message.answer("⚠ Охотник уже работает.")

        elif text == "🛑 Стоп охотника":
            if search_active:
                search_active = False
                await message.answer("🛑 Охотник остановлен.")
            else:
                await message.answer("⚠ Охотник и так не запущен.")

        elif text == "◀️ Назад":
            await message.answer("⭐ Главное меню:", reply_markup=main_kb())

        # авто-удаление любых текстов пользователя (кроме /команд)
        if text and not text.startswith("/"):
            await asyncio.sleep(0.5)
            await safe_delete(user_msg)

    except Exception as e:
        await message.answer(f"❌ Ошибка в обработке кнопок:\n{e}")
        await safe_delete(user_msg)


async def safe_delete(message: types.Message):
    try:
        await message.delete()
    except:
        pass


# ---------------------- RUN ----------------------
async def main():
    print("[BOT] Запуск бота с режимом охотника 1.7 сек...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
