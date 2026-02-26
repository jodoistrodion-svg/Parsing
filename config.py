import os


API_TOKEN = os.getenv("API_TOKEN", "")
LZT_API_KEY = os.getenv("LZT_API_KEY", "")

# URL категории miHoYo
LZT_URL = "https://api.lzt.market/mihoyo?per_page=69&order_by=date_to_down"

# Интервал проверки новых лотов (в секундах)
CHECK_INTERVAL = 5
