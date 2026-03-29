"""
Реестр клиентов и интеграций amoCRM в PostgreSQL.

Схема clients (текущая): id, client_id, client_slug, client_name, is_enabled, created_at.
Поле timezone в таблице отсутствует — подставляется константа по умолчанию.
"""

from __future__ import annotations

from dataclasses import dataclass

from scripts.db import get_connection

# Пока в БД нет timezone; при появлении колонки можно читать её из SELECT.
DEFAULT_CLIENT_TIMEZONE = "Asia/Yekaterinburg"


class ClientRegistryError(Exception):
    pass


def _normalize_account_domain(raw: str) -> str:
    """
    amoCRM API и urllib ожидают URL со схемой. В БД иногда хранят только хост
    (например artroyaldetailing.amocrm.ru) — тогда добавляем https://.
    Если уже есть http:// или https:// — не меняем.
    """
    s = (raw or "").strip().rstrip("/")
    if not s:
        return s
    low = s.lower()
    if low.startswith("https://") or low.startswith("http://"):
        return s
    return f"https://{s}"


@dataclass(frozen=True)
class ClientContext:
    """client_id — PK строки в clients (c.id), как в JOIN с amocrm_integrations."""

    client_id: int
    client_slug: str
    account_domain: str
    integration_id: int
    amo_oauth_client_id: str
    amo_oauth_client_secret: str
    amo_oauth_redirect_uri: str
    timezone: str
    is_enabled: bool


def resolve_client_context(slug: str) -> ClientContext:
    """
    Возвращает контекст клиента по client_slug (clients + amocrm_integrations).
    """
    s = (slug or "").strip()
    if not s:
        raise ClientRegistryError("client slug пустой")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    c.id,
                    c.client_slug,
                    c.is_enabled,
                    i.id AS integration_id,
                    i.account_domain,
                    i.amo_oauth_client_id,
                    i.amo_oauth_client_secret,
                    i.amo_oauth_redirect_uri
                FROM clients c
                INNER JOIN amocrm_integrations i ON i.client_id = c.id
                WHERE c.client_slug = %s
                """,
                (s,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        raise ClientRegistryError(
            f"Клиент не найден или нет интеграции amoCRM: slug={s!r}"
        )

    (
        row_id,
        client_slug_val,
        is_enabled,
        integration_id,
        account_domain,
        amo_oauth_client_id,
        amo_oauth_client_secret,
        amo_oauth_redirect_uri,
    ) = row
    domain = _normalize_account_domain(str(account_domain or ""))
    if not domain:
        raise ClientRegistryError(f"Пустой account_domain для slug={s!r}")

    oauth_id = (amo_oauth_client_id or "").strip() if amo_oauth_client_id is not None else ""
    oauth_secret = (
        (amo_oauth_client_secret or "").strip() if amo_oauth_client_secret is not None else ""
    )
    oauth_redirect = (
        (amo_oauth_redirect_uri or "").strip() if amo_oauth_redirect_uri is not None else ""
    )

    return ClientContext(
        client_id=int(row_id),
        client_slug=str(client_slug_val),
        account_domain=domain,
        integration_id=int(integration_id),
        amo_oauth_client_id=oauth_id,
        amo_oauth_client_secret=oauth_secret,
        amo_oauth_redirect_uri=oauth_redirect,
        timezone=DEFAULT_CLIENT_TIMEZONE,
        is_enabled=bool(is_enabled),
    )
