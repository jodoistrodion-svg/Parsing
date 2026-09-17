from __future__ import annotations

SCHEMA_VERSION = 1

MIGRATIONS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS schema_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_seen_user_seen_at ON seen(user_id, seen_at);",
    "CREATE INDEX IF NOT EXISTS idx_buy_attempted_user_at ON buy_attempted(user_id, attempted_at);",
    "CREATE INDEX IF NOT EXISTS idx_urls_user_enabled ON urls(user_id, enabled);",
)


async def apply_migrations(db) -> None:
    """Apply additive, idempotent schema improvements to an existing SQLite DB."""
    for sql in MIGRATIONS:
        try:
            await db.executescript(sql)
        except Exception:
            # Base tables are still created by the legacy bootstrap; an index
            # simply waits until those tables exist on the next startup.
            continue
    await db.commit()
