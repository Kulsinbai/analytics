"""
Подгрузка локальных env-файлов для разработки.

Читает только существующие файлы в корне проекта analytics-etl:
  .env.local, затем .env

override=False: не перезаписывает переменные, уже заданные в окружении процесса
(типичный production: значения из systemd / k8s / CI остаются приоритетными).

Точки входа ETL вызывают load_local_env_files() в начале (run_pipeline, loaders и т.д.);
импорт только scripts.db больше не обязателен для подгрузки .env.
"""

from __future__ import annotations

from pathlib import Path

_DONE = False


def load_local_env_files() -> None:
    global _DONE
    if _DONE:
        return
    _DONE = True

    root = Path(__file__).resolve().parent.parent
    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    for name in (".env.local", ".env"):
        path = root / name
        if path.is_file():
            load_dotenv(path, override=False)
