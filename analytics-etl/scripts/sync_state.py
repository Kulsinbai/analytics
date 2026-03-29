"""
Состояние инкрементальных выгрузок ETL в PostgreSQL (таблица etl_sync_state).

Ожидаемая схема (если таблица создана иначе — приведите столбцы к этим именам):

  CREATE TABLE etl_sync_state (
      client_id INTEGER NOT NULL,
      entity TEXT NOT NULL,
      watermark_updated_at TIMESTAMPTZ,
      last_success_at TIMESTAMPTZ,
      last_error TEXT,
      PRIMARY KEY (client_id, entity)
  );

watermark_updated_at — верхняя граница updated_at (amoCRM) по реально загруженным сущностям, UTC.

Поведение при ошибках: save_watermark вызывается только после успешной загрузки в ClickHouse;
save_last_error / сбой пайплайна не изменяют watermark_updated_at (колонка не трогается
в save_last_error — обновляется только last_error).
"""

from __future__ import annotations

from datetime import datetime, timezone

from scripts.db import get_connection


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_watermark(client_id: int, entity: str) -> datetime | None:
    """
    Возвращает сохранённый watermark (UTC, aware) или None, если записи нет.
    """
    cid = int(client_id)
    ent = (entity or "").strip()
    if cid <= 0 or not ent:
        raise ValueError("client_id и entity должны быть заданы")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT watermark_updated_at
                FROM etl_sync_state
                WHERE client_id = %s AND entity = %s
                """,
                (cid, ent),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row or row[0] is None:
        return None
    ts = row[0]
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)
    raise TypeError(f"Неожиданный тип watermark_updated_at: {type(ts)!r}")


def save_watermark(
    client_id: int,
    entity: str,
    watermark_updated_at: datetime,
) -> None:
    """
    Сохраняет watermark и отмечает успешный прогон (last_success_at, last_error = NULL).

    Вызывать только после успешной обработки батча (например загрузки в ClickHouse);
    при исключении в пайплайне до этого шага watermark не продвигают.
    watermark_updated_at интерпретируется как UTC, если naive.
    """
    cid = int(client_id)
    ent = (entity or "").strip()
    if cid <= 0 or not ent:
        raise ValueError("client_id и entity должны быть заданы")

    wm = watermark_updated_at
    if wm.tzinfo is None:
        wm = wm.replace(tzinfo=timezone.utc)
    else:
        wm = wm.astimezone(timezone.utc)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            now = _utcnow()
            cur.execute(
                """
                INSERT INTO etl_sync_state (
                    client_id,
                    entity,
                    watermark_updated_at,
                    last_success_at,
                    last_error
                )
                VALUES (%s, %s, %s, %s, NULL)
                ON CONFLICT (client_id, entity) DO UPDATE SET
                    watermark_updated_at = EXCLUDED.watermark_updated_at,
                    last_success_at = EXCLUDED.last_success_at,
                    last_error = NULL
                """,
                (cid, ent, wm, now),
            )
        conn.commit()
    finally:
        conn.close()


def save_last_error(client_id: int, entity: str, error: str) -> None:
    """Сохраняет текст последней ошибки по сущности (без изменения watermark)."""
    cid = int(client_id)
    ent = (entity or "").strip()
    if cid <= 0 or not ent:
        raise ValueError("client_id и entity должны быть заданы")
    msg = (error or "").strip()
    if len(msg) > 10000:
        msg = msg[:9997] + "..."

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO etl_sync_state (client_id, entity, last_error)
                VALUES (%s, %s, %s)
                ON CONFLICT (client_id, entity) DO UPDATE SET
                    last_error = EXCLUDED.last_error
                """,
                (cid, ent, msg),
            )
        conn.commit()
    finally:
        conn.close()


def touch_last_success(client_id: int, entity: str) -> None:
    """Только обновляет last_success_at и сбрасывает last_error (watermark не трогаем)."""
    cid = int(client_id)
    ent = (entity or "").strip()
    if cid <= 0 or not ent:
        raise ValueError("client_id и entity должны быть заданы")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            now = _utcnow()
            cur.execute(
                """
                INSERT INTO etl_sync_state (client_id, entity, last_success_at, last_error)
                VALUES (%s, %s, %s, NULL)
                ON CONFLICT (client_id, entity) DO UPDATE SET
                    last_success_at = EXCLUDED.last_success_at,
                    last_error = NULL
                """,
                (cid, ent, now),
            )
        conn.commit()
    finally:
        conn.close()
