"""
Загрузка и сохранение OAuth-токенов amoCRM в PostgreSQL (таблица amocrm_oauth_tokens).

TODO(production): сейчас токены читаются/пишутся как открытый UTF-8 в BYTEA (dev-friendly).
  В проде нужно шифрование at-rest (Fernet/KMS), хранить ciphertext и не логировать значения.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Tuple

from scripts.db import get_connection


class TokenStoreError(Exception):
    pass


def _bytes_to_str(b: bytes | memoryview | None) -> str:
    if b is None:
        return ""
    if isinstance(b, memoryview):
        b = b.tobytes()
    if not isinstance(b, bytes):
        return str(b)
    return b.decode("utf-8")


def _str_to_bytes(s: str) -> bytes:
    return (s or "").encode("utf-8")


def load_tokens(integration_id: int) -> Tuple[str, str, datetime]:
    """
    Возвращает (access_token, refresh_token, expires_at_utc).
    expires_at — aware UTC.
    """
    if integration_id <= 0:
        raise TokenStoreError("integration_id должен быть > 0")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT access_token_encrypted, refresh_token_encrypted, expires_at
                FROM amocrm_oauth_tokens
                WHERE integration_id = %s
                """,
                (integration_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        raise TokenStoreError(f"Нет строки токенов для integration_id={integration_id}")

    access_raw, refresh_raw, exp = row
    access = _bytes_to_str(access_raw).strip()
    refresh = _bytes_to_str(refresh_raw).strip()

    if not isinstance(exp, datetime):
        raise TokenStoreError("expires_at в БД должен быть TIMESTAMPTZ/DATETIME")

    if exp.tzinfo is None:
        exp = exp.replace(tzinfo=timezone.utc)
    else:
        exp = exp.astimezone(timezone.utc)

    return access, refresh, exp


def save_tokens_after_refresh(
    integration_id: int,
    access_token: str,
    refresh_token: str,
    expires_at_utc: datetime,
) -> None:
    """
    Обновляет токены после успешного refresh.
    """
    if integration_id <= 0:
        raise TokenStoreError("integration_id должен быть > 0")
    if not access_token or not refresh_token:
        raise TokenStoreError("access_token и refresh_token не должны быть пустыми")

    if expires_at_utc.tzinfo is None:
        expires_at_utc = expires_at_utc.replace(tzinfo=timezone.utc)
    else:
        expires_at_utc = expires_at_utc.astimezone(timezone.utc)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE amocrm_oauth_tokens
                SET
                    access_token_encrypted = %s,
                    refresh_token_encrypted = %s,
                    expires_at = %s,
                    token_type = COALESCE(token_type, 'Bearer'),
                    updated_at = now()
                WHERE integration_id = %s
                """,
                (
                    _str_to_bytes(access_token),
                    _str_to_bytes(refresh_token),
                    expires_at_utc,
                    integration_id,
                ),
            )
            if cur.rowcount != 1:
                raise TokenStoreError(
                    f"UPDATE токенов не затронул ровно 1 строку (rowcount={cur.rowcount})"
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
