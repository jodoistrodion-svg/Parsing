from __future__ import annotations


def render_status_card(
    *,
    total_sources: int,
    active_sources: int,
    autobuy_sources: int,
    hunter_state: str,
    api_errors: int,
    balance_text: str,
) -> str:
    return (
        "📊 <b>Статус бота</b>\n"
        "╭────────────────────╮\n"
        f"│ ⚙️ Охотник: {hunter_state}\n"
        f"│ 🔗 URL всего: <b>{total_sources}</b>\n"
        f"│ 🟢 Активных URL: <b>{active_sources}</b>\n"
        f"│ 🛒 Автобай URL: <b>{autobuy_sources}</b>\n"
        f"│ 🚨 Ошибки API: <b>{api_errors}</b>\n"
        f"│ 💳 Баланс (accounts): <b>{balance_text}</b>\n"
        "╰────────────────────╯"
    )
