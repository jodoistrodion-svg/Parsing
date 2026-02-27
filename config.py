import os


# Можно хранить токены в переменных окружения,
# либо временно прописать напрямую для быстрого запуска.
API_TOKEN = (os.getenv("API_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
LZT_API_KEY = (os.getenv("LZT_API_KEY") or "").strip()

# Legacy constants kept for backward compatibility with older deployments.
LZT_URL = (os.getenv("LZT_URL") or "https://api.lzt.market/mihoyo?per_page=69&order_by=date_to_down").strip()
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "5"))