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


@dataclass(frozen=True)
class ClientContext:
    """client_id — PK строки в clients (c.id), как в JOIN с amocrm_integrations."""

    client_id: int
    client_slug: str
    account_domain: str
    integration_id: int
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
                    i.account_domain
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

    row_id, client_slug_val, is_enabled, integration_id, account_domain = row
    domain = (account_domain or "").strip().rstrip("/")
    if not domain:
        raise ClientRegistryError(f"Пустой account_domain для slug={s!r}")

    return ClientContext(
        client_id=int(row_id),
        client_slug=str(client_slug_val),
        account_domain=domain,
        integration_id=int(integration_id),
        timezone=DEFAULT_CLIENT_TIMEZONE,
        is_enabled=bool(is_enabled),
    )
