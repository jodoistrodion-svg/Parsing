import os

API_TOKEN = (os.getenv("API_TOKEN") or "").strip()
LZT_API_KEY = (os.getenv("LZT_API_KEY") or "").strip()

# URL категории miHoYo
LZT_URL = (os.getenv("LZT_URL") or "https://api.lzt.market/category/mihoyo?sort_by=date&order=desc").strip()

# Интервал проверки новых лотов (в секундах)
CHECK_INTERVAL = int((os.getenv("CHECK_INTERVAL") or "5").strip())
