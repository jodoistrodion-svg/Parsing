from __future__ import annotations

import html


def render_status_card(
    *,
    total_sources: int,
    active_sources: int,
    autobuy_sources: int,
    hunter_state: str,
    api_errors: int,
    balance_text: str,
    notify_mode: str = "Покупка",
    cache_mode: str = "TTL",
) -> str:
    safe_hunter_state = html.escape(str(hunter_state or "—"))
    safe_balance_text = html.escape(str(balance_text or "—"))
    safe_notify_mode = html.escape(str(notify_mode or "—"))
    safe_cache_mode = html.escape(str(cache_mode or "—"))
    return (
        "📊 <b>Статус бота</b>\n"
        "╭────────────────────╮\n"
        f"│ ⚙️ Охотник: {safe_hunter_state}\n"
        f"│ 🔗 URL всего: <b>{total_sources}</b>\n"
        f"│ 🟢 Активных URL: <b>{active_sources}</b>\n"
        f"│ 🛒 Автобай URL: <b>{autobuy_sources}</b>\n"
        f"│ 🚨 Ошибки API: <b>{api_errors}</b>\n"
        f"│ 💳 Баланс (accounts): <b>{safe_balance_text}</b>\n"
        f"│ 🔔 Уведомления: <b>{safe_notify_mode}</b>\n"
        f"│ 🧠 Кэш: <b>{safe_cache_mode}</b>\n"
        "╰────────────────────╯"
    )
