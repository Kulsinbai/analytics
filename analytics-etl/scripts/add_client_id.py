import json
import argparse
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.clients_map import get_client_id

DEFAULT_INPUT_FILE = BASE_DIR / "data" / "add_leads_crm.json"
DEFAULT_OUTPUT_FILE = BASE_DIR / "data" / "add_leads_crm_with_client.json"


def _iter_records(data):
    """
    Поддерживаем 2 формата:
    - list[dict] (как у выгрузки лидов)
    - dict с любым list внутри (fallback)
    """
    if isinstance(data, list):
        for it in data:
            if isinstance(it, dict):
                yield it
        return

    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                for it in v:
                    if isinstance(it, dict):
                        yield it
        return

    return


def _ensure_not_mixed_client(data, client_id: int, client_slug: str) -> None:
    """
    Защита от смешения клиентов:
    - если запись уже содержит client_id/client_slug и они НЕ совпадают с целевыми — abort
    """
    bad = 0
    for rec in _iter_records(data):
        existing_id = rec.get("client_id", None)
        existing_slug = rec.get("client_slug", None)

        if existing_id is not None and existing_id != client_id:
            bad += 1
            break
        if existing_slug is not None and str(existing_slug) != client_slug:
            bad += 1
            break

    if bad:
        raise ValueError(
            "Обнаружено смешение клиента: в JSON уже есть client_id/client_slug, "
            "которые не совпадают с целевыми. Остановлено, чтобы не испортить данные."
        )


def _apply_client_fields(data, client_id: int, client_slug: str) -> int:
    n = 0
    for rec in _iter_records(data):
        rec["client_id"] = client_id
        rec["client_slug"] = client_slug
        n += 1
    return n


def main() -> None:
    p = argparse.ArgumentParser(description="Добавляет client_id/client_slug в JSON с защитой от смешения клиента.")
    p.add_argument(
        "--client-slug",
        required=True,
        help="client_slug клиента (PostgreSQL / client_registry; например artroyal_detailing)",
    )
    p.add_argument("--in", dest="in_path", default=str(DEFAULT_INPUT_FILE), help="Путь к входному JSON")
    p.add_argument("--out", dest="out_path", default=str(DEFAULT_OUTPUT_FILE), help="Путь к выходному JSON")
    args = p.parse_args()

    client_slug = args.client_slug
    client_id = get_client_id(client_slug)
    if not isinstance(client_id, int) or client_id <= 0:
        raise ValueError("client_id должен быть положительным целым числом.")

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)

    with open(in_path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)

    _ensure_not_mixed_client(data, client_id=client_id, client_slug=client_slug)
    n = _apply_client_fields(data, client_id=client_id, client_slug=client_slug)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("OK. Файл сохранён:", out_path)
    print("client_id =", client_id, "| client_slug =", client_slug)
    print("Записей обработано:", n)


if __name__ == "__main__":
    main()