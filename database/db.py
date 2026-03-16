from __future__ import annotations

import aiosqlite


async def apply_sqlite_pragmas(conn: aiosqlite.Connection) -> None:
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.execute("PRAGMA foreign_keys=ON")
    await conn.execute("PRAGMA temp_store=MEMORY")
    await conn.execute("PRAGMA cache_size=-20000")
    await conn.execute("PRAGMA busy_timeout=5000")
