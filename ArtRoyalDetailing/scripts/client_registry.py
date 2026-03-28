"""
Реестр клиентов и интеграций amoCRM в PostgreSQL.
"""

from __future__ import annotations

from dataclasses import dataclass

from scripts.db import get_connection


class ClientRegistryError(Exception):
    pass


@dataclass(frozen=True)
class ClientContext:
    client_id: int
    client_slug: str
    account_domain: str
    integration_id: int
    timezone: str
    is_active: bool


def resolve_client_context(slug: str) -> ClientContext:
    """
    Возвращает контекст клиента по slug (clients + amocrm_integrations).
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
                    c.slug,
                    c.timezone,
                    c.is_active,
                    i.id AS integration_id,
                    i.account_domain
                FROM clients c
                INNER JOIN amocrm_integrations i ON i.client_id = c.id
                WHERE c.slug = %s
                """,
                (s,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        raise ClientRegistryError(f"Клиент не найден или нет интеграции amoCRM: slug={s!r}")

    client_id, client_slug, tz, is_active, integration_id, account_domain = row
    domain = (account_domain or "").strip().rstrip("/")
    if not domain:
        raise ClientRegistryError(f"Пустой account_domain для slug={s!r}")

    return ClientContext(
        client_id=int(client_id),
        client_slug=str(client_slug),
        account_domain=domain,
        integration_id=int(integration_id),
        timezone=str(tz or "UTC"),
        is_active=bool(is_active),
    )
