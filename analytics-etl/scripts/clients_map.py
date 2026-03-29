"""
Резолв client_slug → client_id.

Источник истины: PostgreSQL (clients + amocrm_integrations) через
client_registry.resolve_client_context.

CLIENTS_MAP — временный fallback, если клиент не найден в БД (ClientRegistryError)
или при ошибке подключения к PostgreSQL (только для slug, присутствующих в карте).
"""

from __future__ import annotations

import logging

from scripts.client_registry import ClientRegistryError, resolve_client_context

logger = logging.getLogger(__name__)

CLIENTS_MAP = {
    "artroyal_detailing": 1,
    # "eurooptik_ufa": 2,
    # "tabib_ufa": 3,
}


def get_client_id(client_slug: str) -> int:
    s = (client_slug or "").strip()
    if not s:
        raise ValueError("client_slug пустой")

    try:
        return resolve_client_context(s).client_id
    except ClientRegistryError:
        if s in CLIENTS_MAP:
            logger.warning(
                "client_id для %r взят из CLIENTS_MAP (клиент не найден в БД или ошибка реестра)",
                s,
            )
            return CLIENTS_MAP[s]
        raise ValueError(
            f"Неизвестный client_slug={s!r}. Добавь клиента в PostgreSQL "
            f"(clients + amocrm_integrations) или временно в CLIENTS_MAP."
        ) from None
    except Exception as e:
        if s not in CLIENTS_MAP:
            raise
        logger.warning(
            "PostgreSQL недоступен или ошибка запроса, client_id для %r из CLIENTS_MAP: %s",
            s,
            e,
        )
        return CLIENTS_MAP[s]
