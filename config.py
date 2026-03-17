import os

from dotenv import load_dotenv


load_dotenv()


def _read_env(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value:
            return value.strip()
    return default.strip()


# Telegram bot token. Supported env names for compatibility:
# - API_TOKEN (project default)
# - TELEGRAM_BOT_TOKEN / BOT_TOKEN (common aliases)
API_TOKEN = _read_env("API_TOKEN", "TELEGRAM_BOT_TOKEN", "BOT_TOKEN", default="")

# LZT market API token. Supported env names for compatibility:
# - LZT_API_KEY (project default)
# - LZT_TOKEN / LOLZ_API_KEY (common aliases)
LZT_API_KEY = _read_env("LZT_API_KEY", "LZT_TOKEN", "LOLZ_API_KEY", default="")

# URL категории miHoYo
LZT_URL = _read_env("LZT_URL", default="https://api.lzt.market/category/mihoyo?sort_by=date&order=desc")

# Интервал проверки новых лотов (в секундах)
CHECK_INTERVAL = int(_read_env("CHECK_INTERVAL", default="5"))
