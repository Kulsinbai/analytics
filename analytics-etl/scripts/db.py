"""
Минимальный доступ к PostgreSQL для ETL (одно соединение на вызов get_connection).

Env:
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

from __future__ import annotations

import os

import psycopg2
from psycopg2.extensions import connection as PgConnection


def get_connection() -> PgConnection:
    host = os.getenv("POSTGRES_HOST", "").strip()
    db = os.getenv("POSTGRES_DB", "").strip()
    user = os.getenv("POSTGRES_USER", "").strip()
    password = os.getenv("POSTGRES_PASSWORD", "").strip()
    port = os.getenv("POSTGRES_PORT", "5432").strip() or "5432"

    missing = [
        k
        for k, v in [
            ("POSTGRES_HOST", host),
            ("POSTGRES_DB", db),
            ("POSTGRES_USER", user),
            ("POSTGRES_PASSWORD", password),
        ]
        if not v
    ]
    if missing:
        raise RuntimeError(
            "Не заданы переменные окружения PostgreSQL: " + ", ".join(missing)
        )

    try:
        port_i = int(port)
    except ValueError as e:
        raise RuntimeError(f"POSTGRES_PORT должен быть числом, получено: {port!r}") from e

    return psycopg2.connect(
        host=host,
        port=port_i,
        dbname=db,
        user=user,
        password=password,
    )
