import asyncio
import aiohttp
import sys
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

# режим ввода: None / "min" / "max"
input_mode = None

# временные сообщения
temp_messages = []


# ---------------------- ANSI COLORS ----------------------
GREEN = "\033[92m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
MAGENTA = "\033[95m"
RESET = "\033[0m"

spinner_frames = ["|", "/", "-", "\\"]


# ---------------------- КОНСОЛЬНЫЙ ВЫВОД ----------------------
def console_header():
    print(MAGENTA + "======================================" + RESET)
    print(GREEN + "        ПРОГРАММА ЗАПУЩЕНА" + RESET)
    print(MAGENTA + "======================================" + RESET)
    print()


def console_status(progress, attempt, found, frame_id):
    bar_len = 30
    filled = int(bar_len * progress / 100)
    bar = GREEN + "█" * filled + RESET + "·" * (bar_len - filled)

    spinner = CYAN + spinner_frames[frame_id % len(spinner_frames)] + RESET

    line = (
        f"\r{spinner} {YELLOW}ПОИСК{RESET} [{bar}] "
        f"{progress:3d}% | Попытка: {attempt} | Найдено: {found}"
    )

    sys.stdout.write(line)
    sys.stdout.flush()


# ---------------------- КЛАВИАТУРА ----------------------
def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💎 Искать все")],
            [KeyboardButton(text="💰 Мин. цена"), KeyboardButton(text="💰 Макс. цена")],
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


async def update_status():
    global status_message_id, status_chat_id, attempt, found_count

    if status_message_id is None:
        return

    text = (
        f"🔎 <b>Поиск активен</b>\n"
        f"⚙️ Попытка: {attempt}\n"
        f"💎 Найдено новых лотов: {found_count}\n"
        f"🌀 Статус: выполняется…"
    )

    try:
        await bot.edit_message_text(
            chat_id=status_chat_id,
            message_id=status_message_id,
            text=text,
            parse_mode="HTML"
        )
    except:
        pass


async def monitor_new_items(message: types.Message):
    global search_active, attempt, found_count

    attempt = 0
    found_count = 0
    frame = 0

    while search_active:
        attempt += 1
        frame += 1

        progress = (attempt % 20) * 5
        console_status(progress, attempt, found_count, frame)

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

        await update_status()
        await asyncio.sleep(CHECK_INTERVAL)

    console_status(100, attempt, found_count, frame)
    print("\n" + GREEN + "ПОИСК ЗАВЕРШЁН" + RESET)

    await bot.edit_message_text(
        chat_id=status_chat_id,
        message_id=status_message_id,
        text=(
            f"✨ <b>Поиск завершён</b>\n"
            f"💎 Всего найдено новых лотов: {found_count}\n"
            f"🌙 Статус: завершён"
        ),
        parse_mode="HTML"
    )


# ---------------------- СТАРТ ----------------------
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    console_header()

    logo = r"""
   ____              _             
  |  _ \ __ _ _ __ (_)_ __  _   _ 
  | |_) / _` | '_ \| | '_ \| | | |
  |  __/ (_| | | | | | | | | |_| |
  |_|   \__,_|_| |_|_|_| |_|\__, |
                            |___/ 
"""

    # Анимация загрузки
    logo_msg = await message.answer(
        f"<pre>{logo}</pre>\n<b>🚀 Запуск бота…</b>",
        parse_mode="HTML"
    )

    for i in range(3):
        await asyncio.sleep(0.4)
        dots = "." * ((i + 1) % 4)
        await logo_msg.edit_text(
            f"<pre>{logo}</pre>\n<b>🚀 Запуск бота{dots}</b>",
            parse_mode="HTML"
        )

    await logo_msg.delete()

    # Основное стартовое сообщение
    text = (
        "💠✨ <b>Добро пожаловать!</b> ✨💠\n\n"
        "💎 <b>Возможности бота:</b>\n"
        "• 🚀 Мониторинг новых лотов на LZT Market\n"
        "• 💰 Фильтры по цене (мин/макс)\n"
        "• 🔔 Уведомления только о новых аккаунтах\n"
        "• ⚡️ Статус поиска в реальном времени\n"
        "• 🌙 Минимум спама — максимум пользы\n\n"
        "💜 <b>Бот создан при поддержке канала прекрасной дамы</b>\n"
        "👉 https://t.me/+wHlSL7Ij2rpjYmFi\n\n"
        "👑 <b>Автор:</b> @StaliNusshhAaaaaa\n"
        "✨ Первый проект, сделанный с душой ✨\n\n"
        "⭐️ <b>Меню ниже:</b>"
    )

    await message.answer(text, parse_mode="HTML", reply_markup=main_kb())


# ---------------------- ХЕНДЛЕР ВСЕХ КНОПОК ----------------------
@dp.message()
async def buttons(message: types.Message):
    global current_min_price, current_max_price, search_active
    global status_message_id, status_chat_id, attempt, found_count, input_mode, temp_messages

    text = message.text

    # кнопки
    buttons_texts = {
        "💎 Искать все",
        "💰 Мин. цена",
        "💰 Макс. цена",
        "🚀 Запустить поиск",
        "🔄 Перезапустить",
        "🛑 Стоп",
        "◀️ Назад",
    }

    # ---- режим ввода чисел ----
    if input_mode == "min" and text not in buttons_texts:
        try:
            await message.delete()
            current_min_price = int(text)

            for msg in temp_messages:
                try: await msg.delete()
                except: pass
            temp_messages.clear()

            confirm = await message.answer(
                f"💎 Мин. цена установлена: <b>{current_min_price}₽</b>",
                parse_mode="HTML"
            )
            await asyncio.sleep(2)
            await confirm.delete()

        except ValueError:
            err = await message.answer("⚠️ Ошибка. Введи число.")
            await asyncio.sleep(2)
            await err.delete()

        input_mode = None
        return

    if input_mode == "max" and text not in buttons_texts:
        try:
            await message.delete()
            current_max_price = int(text)

            for msg in temp_messages:
                try: await msg.delete()
                except: pass
            temp_messages.clear()

            confirm = await message.answer(
                f"💎 Макс. цена установлена: <b>{current_max_price}₽</b>",
                parse_mode="HTML"
            )
            await asyncio.sleep(2)
            await confirm.delete()

        except ValueError:
            err = await message.answer("⚠️ Ошибка. Введи число.")
            await asyncio.sleep(2)
            await err.delete()

        input_mode = None
        return

    # ---- обычные кнопки ----
    if text == "💎 Искать все":
        current_min_price = None
        current_max_price = None
        msg = await message.answer("🔄 Фильтр сброшен. Ищем все лоты.")
        await asyncio.sleep(2)
        await msg.delete()

    elif text == "💰 Мин. цена":
        input_mode = "min"
        msg = await message.answer("💰 Введи минимальную цену:")
        temp_messages.append(msg)
        await message.delete()

    elif text == "💰 Макс. цена":
        input_mode = "max"
        msg = await message.answer("💰 Введи максимальную цену:")
        temp_messages.append(msg)
        await message.delete()

    elif text == "🚀 Запустить поиск":
        if not search_active:
            search_active = True

            msg = await message.answer("🔎 Поиск запускается…")
            status_message_id = msg.message_id
            status_chat_id = msg.chat.id

            attempt = 0
            found_count = 0

            asyncio.create_task(monitor_new_items(message))
        else:
            warn = await message.answer("⚠️ Поиск уже работает.")
            await asyncio.sleep(2)
            await warn.delete()

    elif text == "🔄 Перезапустить":
        sent_ids.clear()
        attempt = 0
        found_count = 0
        msg = await message.answer("🔄 Поиск перезапущен.")
        await asyncio.sleep(2)
        await msg.delete()

    elif text == "🛑 Стоп":
        search_active = False
        msg = await message.answer("🛑 Поиск остановлен.")
        await asyncio.sleep(2)
        await msg.delete()

    elif text == "◀️ Назад":
        await message.answer("⭐️ Главное меню:", reply_markup=main_kb())


# ---------------------- ЗАПУСК ----------------------
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    console_header()
    asyncio.run(main())
