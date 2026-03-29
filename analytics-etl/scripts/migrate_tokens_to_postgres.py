#!/usr/bin/env python3
"""
Одноразовая миграция OAuth-токенов amoCRM из legacy JSON в PostgreSQL (amocrm_oauth_tokens).

TODO(production): сейчас значения пишутся в BYTEA как открытый UTF-8 (как в token_store.py).
  Для production нужно настоящее шифрование at-rest (Fernet/KMS), не хранить plaintext.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

# python scripts/migrate_tokens_to_postgres.py — импорт scripts.*
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.amocrm_client import AmoClientError, _resolve_secrets_paths, load_json
from scripts.client_registry import ClientRegistryError, resolve_client_context
from scripts.db import get_connection


def _str_to_bytes(s: str) -> bytes:
    return (s or "").encode("utf-8")


def _parse_legacy_tokens(raw: dict, path: Path) -> tuple[str, str, datetime, str]:
    access = (raw.get("access_token") or "").strip()
    refresh = (raw.get("refresh_token") or "").strip()
    exp_raw = raw.get("expires_at")
    if exp_raw is None:
        raise SystemExit(f"В {path} нет поля expires_at")
    try:
        exp_unix = int(exp_raw)
    except (TypeError, ValueError) as e:
        raise SystemExit(f"expires_at в {path} должен быть unix timestamp (int), получено: {exp_raw!r}") from e
    if not exp_unix:
        raise SystemExit(f"expires_at в {path} не может быть нулём")

    exp_utc = datetime.fromtimestamp(exp_unix, tz=timezone.utc)
    token_type = (raw.get("token_type") or "Bearer").strip() or "Bearer"

    if not access or not refresh:
        raise SystemExit(f"В {path} пустые access_token или refresh_token")

    return access, refresh, exp_utc, token_type


def main() -> None:
    p = argparse.ArgumentParser(
        description="Миграция токенов amoCRM из JSON (legacy) в amocrm_oauth_tokens."
    )
    p.add_argument(
        "--client-slug",
        required=True,
        help="slug клиента в таблице clients (например artroyal_detailing)",
    )
    args = p.parse_args()
    slug = args.client_slug.strip()
    if not slug:
        p.error("--client-slug не может быть пустым")

    try:
        ctx = resolve_client_context(slug)
    except ClientRegistryError as e:
        raise SystemExit(str(e)) from e

    try:
        _, tokens_path = _resolve_secrets_paths(slug)
    except AmoClientError as e:
        raise SystemExit(str(e)) from e

    raw = load_json(tokens_path)
    access, refresh, exp_utc, token_type = _parse_legacy_tokens(raw, tokens_path)

    iid = ctx.integration_id
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM amocrm_oauth_tokens WHERE integration_id = %s",
                (iid,),
            )
            exists = cur.fetchone() is not None

            if exists:
                cur.execute(
                    """
                    UPDATE amocrm_oauth_tokens
                    SET
                        access_token_encrypted = %s,
                        refresh_token_encrypted = %s,
                        expires_at = %s,
                        token_type = %s,
                        updated_at = now()
                    WHERE integration_id = %s
                    """,
                    (
                        _str_to_bytes(access),
                        _str_to_bytes(refresh),
                        exp_utc,
                        token_type,
                        iid,
                    ),
                )
                if cur.rowcount != 1:
                    raise SystemExit(
                        f"UPDATE: ожидалась 1 строка, rowcount={cur.rowcount}"
                    )
                action = "UPDATE"
            else:
                cur.execute(
                    """
                    INSERT INTO amocrm_oauth_tokens (
                        integration_id,
                        access_token_encrypted,
                        refresh_token_encrypted,
                        expires_at,
                        token_type,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, now())
                    """,
                    (
                        iid,
                        _str_to_bytes(access),
                        _str_to_bytes(refresh),
                        exp_utc,
                        token_type,
                    ),
                )
                action = "INSERT"

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(
        f"OK: {action} amocrm_oauth_tokens для integration_id={iid} "
        f"(client_slug={slug!r}, expires_at_utc={exp_utc.isoformat()})"
    )


if __name__ == "__main__":
    try:
        main()
    except AmoClientError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
