
import asyncio
import json
import aiohttp
import aiosqlite
import html
import re
import time
import random
import os
import logging
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from collections import defaultdict

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramConflictError,
    TelegramForbiddenError,
    TelegramRetryAfter,
    TelegramUnauthorizedError,
)

from bot.autobuy_strategy import build_buy_urls, prioritize_buy_urls
from bot.ui import render_status_card
from buyer.queue import UserAutobuyQueueManager
from market.discovery import iter_sources_split
from services.logging_setup import setup_logging

from config import API_TOKEN as _API_TOKEN, LZT_API_KEY as _LZT_API_KEY

# ====================== ENV ======================
def _normalize_telegram_token(raw: str | None) -> str:
    token = (raw or "").strip().strip('"').strip("'")
    if token.lower().startswith("bot") and re.match(r"^bot\d{6,12}:", token, flags=re.IGNORECASE):
        token = token[3:]
    return token


def _cfg(name: str, fallback: str = "") -> str:
    env_val = os.getenv(name)
    if env_val is not None and env_val.strip() != "":
        return env_val.strip()
    return fallback


API_TOKEN = _normalize_telegram_token(_cfg("API_TOKEN", _API_TOKEN))
LZT_API_KEY = _cfg("LZT_API_KEY", _LZT_API_KEY)
LZT_BALANCE_ID = int((_cfg("LZT_BALANCE_ID", "20212") or "20212").strip())

bot: Bot | None = None
dp = Dispatcher()

# balance cache
user_balance_cache = defaultdict(lambda: {"text": "—", "ts": 0})
BALANCE_CACHE_TTL = 60

# ====================== OWNER / ACCESS ======================
def _parse_int_list(raw: str) -> set[int]:
    result: set[int] = set()
    for chunk in (raw or "").split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            result.add(int(chunk))
        except ValueError:
            continue
    return result


OWNER_ID = int((_cfg("OWNER_ID") or "1377985336").strip())
OWNER_IDS = _parse_int_list(_cfg("OWNER_IDS") or "") or {OWNER_ID}
ACCESS_MODE = (_cfg("ACCESS_MODE") or "open").strip().lower()
ACCESS_OPEN = ACCESS_MODE in {"open", "all", "public", "0"}

# ====================== НАСТРОЙКИ ======================
HUNTER_INTERVAL_BASE = float((_cfg("HUNTER_INTERVAL_BASE") or "0.02").strip())
FETCH_TIMEOUT = float((_cfg("FETCH_TIMEOUT") or "1.20").strip())
BUY_TIMEOUT = float((_cfg("BUY_TIMEOUT") or "0.32").strip())
RETRY_MAX = int((_cfg("RETRY_MAX") or "1").strip())
RETRY_BASE_DELAY = float((_cfg("RETRY_BASE_DELAY") or "0.01").strip())

SHORT_CARD_MAX = 3200
ERROR_REPORT_INTERVAL = 3600

MAX_URLS_PER_USER_DEFAULT = 50
MAX_URLS_PER_USER_LIMITED = 3

MAX_CONCURRENT_REQUESTS = int((_cfg("MAX_CONCURRENT_REQUESTS") or "512").strip())
LIMITED_EXTRA_DELAY = 0.0
MAX_NEW_ITEMS_PER_CYCLE = int((_cfg("MAX_NEW_ITEMS_PER_CYCLE") or "1000").strip())
SEARCH_MIN_REQUEST_INTERVAL = float((_cfg("SEARCH_MIN_REQUEST_INTERVAL") or "0.0").strip())
OTHER_MIN_REQUEST_INTERVAL = float((_cfg("OTHER_MIN_REQUEST_INTERVAL") or "0.0").strip())
BUY_MIN_REQUEST_INTERVAL = float((_cfg("BUY_MIN_REQUEST_INTERVAL") or "0.0").strip())
NON_AUTOBUY_CYCLE_EVERY = int((_cfg("NON_AUTOBUY_CYCLE_EVERY") or "5").strip())

DB_FILE = (_cfg("DB_FILE") or ("/data/bot_data.sqlite" if os.path.isdir("/data") else "bot_data.sqlite")).strip()

LZT_SECRET_WORD = (_cfg("LZT_SECRET_WORD") or "Мазда").strip()
SEED_URLS_JSON = (_cfg("SEED_URLS_JSON") or "").strip()

URL_PAGE_SIZE = 12
USER_PAGE_SIZE = 14
MAX_URL_NAME_LEN = 64

TG_SEND_DELAY = float((_cfg("TG_SEND_DELAY") or "0.01").strip())
AUTOBUY_RETRY_ATTEMPTS = int((_cfg("AUTOBUY_RETRY_ATTEMPTS") or "0").strip())
AUTOBUY_RETRY_MIN_DELAY = float((_cfg("AUTOBUY_RETRY_MIN_DELAY") or "0.03").strip())
AUTOBUY_RETRY_MAX_DELAY = float((_cfg("AUTOBUY_RETRY_MAX_DELAY") or "0.12").strip())
AUTOBUY_QUEUE_RETRY_MIN_DELAY = float((_cfg("AUTOBUY_QUEUE_RETRY_MIN_DELAY") or "0.06").strip())
AUTOBUY_QUEUE_RETRY_MAX_DELAY = float((_cfg("AUTOBUY_QUEUE_RETRY_MAX_DELAY") or "0.18").strip())
FAST_AUTOBUY_TIMEOUT = float((_cfg("FAST_AUTOBUY_TIMEOUT") or "0.45").strip())
AUTOBUY_URL_LIMIT = int((_cfg("AUTOBUY_URL_LIMIT") or "0").strip())
AUTOBUY_MAX_HTTP_ATTEMPTS = int((_cfg("AUTOBUY_MAX_HTTP_ATTEMPTS") or "0").strip())
AUTOBUY_PARALLEL_HTTP = int((_cfg("AUTOBUY_PARALLEL_HTTP") or "24").strip())
AUTOBUY_MAX_DURATION_SEC = float((_cfg("AUTOBUY_MAX_DURATION_SEC") or "2.8").strip())
AUTOBUY_TOTAL_RETRY_WINDOW_SEC = float((_cfg("AUTOBUY_TOTAL_RETRY_WINDOW_SEC") or "6.0").strip())
MAX_ITEMS_PER_SOURCE_SCAN = int((_cfg("MAX_ITEMS_PER_SOURCE_SCAN") or "200").strip())
AUTOBUY_BURST_FIRST_WAVE = int((_cfg("AUTOBUY_BURST_FIRST_WAVE") or "24").strip())
USER_ACTION_FETCH_TIMEOUT = float((_cfg("USER_ACTION_FETCH_TIMEOUT") or "2.4").strip())

# ====================== LOGGING ======================
AUTOBUY_LOG_FILE = _cfg("AUTOBUY_LOG_FILE") or "autobuy.log"
LOG_MAX_BYTES = 15 * 1024 * 1024
LOG_ROTATE_KEEP = 2
logger = setup_logging(AUTOBUY_LOG_FILE, LOG_MAX_BYTES, LOG_ROTATE_KEEP)


def _safe_compact(s: str, n: int = 400) -> str:
    s = (s or "").replace("\n", "\\n").replace("\r", "\\r")
    if len(s) <= n:
        return s
    return s[: n - 20] + f"...(len={len(s)})"


def log_autobuy(line: str):
    logger.info(line)
