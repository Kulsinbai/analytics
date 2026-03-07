import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import json
import csv
import re
import sys
from pathlib import Path
from datetime import datetime, timezone
from scripts.clients_map import get_client_id
from scripts.transform_utils import (
    clean_text,
    fix_mojibake,
    normalize_phone,
    parse_name_fields,
    extract_utm as extract_custom_fields,
)

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "data" / "add_leads_crm_with_client.json"
OUTPUT_FILE = BASE_DIR / "data" / "add_leads_crm_flat_datalens.csv"

# client_slug берём из параметра запуска при запуске как скрипт; при импорте — дефолт
CLIENT_SLUG = "artroyal_detailing"
CLIENT_ID = get_client_id(CLIENT_SLUG)

BASE_FIELDS = [
    "client_id",
    "client_slug",
    "id",
    "name",
    "account_id",
    "pipeline_id",
    "status_id",
    "price",
    "created_at",
    "updated_at",
    "closed_at",
    "responsible_user_id",
    "created_by",
    "updated_by",
    "is_deleted",
    "loss_reason_id",
    "score",
]

# Даты “красиво” для DataLens (ISO)
DATE_FIELDS = [
    "created_dt",
    "updated_dt",
    "closed_dt",
]

EXTRA_FIELDS = [
    "phone",
    "email",
    "source",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_content",
    "utm_term",
    "channel",
    "phone_from_name",
    "name_clean",
    "tags",
]

FIELDS = BASE_FIELDS + DATE_FIELDS + EXTRA_FIELDS

TAG_RE = re.compile(r"<[^>]+>")
PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{8,}\d")
DEAL_ONLY_RE = re.compile(r"^\s*сделка\s*#\s*\d+\s*$", re.IGNORECASE)
SITE_MARKER = "artroyal-detailing.ru"
VA_SIP_RE = re.compile(r"\b(?:va|sip)\b", re.IGNORECASE)


def ts_to_iso(ts):
    """
    Unix timestamp (секунды) -> 'YYYY-MM-DD HH:MM:SS' в UTC.
    DataLens такое понимает стабильно.
    """
    if ts is None or ts == "":
        return ""
    try:
        ts_int = int(ts)
        return datetime.fromtimestamp(ts_int, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""

def extract_tags(lead: dict) -> str:
    """
    amoCRM хранит теги здесь: lead["_embedded"]["tags"].
    Возвращаем строку вида: "instagram; avito"
    """
    emb = lead.get("_embedded") or {}
    tags = emb.get("tags") or []
    names = []
    for t in tags:
        n = t.get("name")
        if n:
            names.append(clean_text(str(n)).lower())
    # убираем дубли, сортируем чтобы было стабильно
    return "; ".join(sorted(set(names)))

def apply_rules(row: dict) -> dict:
    """
    Правила:
    - если name == 'Сделка #123' -> source = 'оффлайн'
    - если встречается artroyal-detailing.ru -> source/channel = 'заявка с сайта'
    - source: va/sip -> 'звонок'
    - source lower()
    - если в name есть 'Сделка по звонку' / 'по звонку' / 'звонок' -> source/channel = 'звонок'
    """
    name = clean_text(str(row.get("name", "")))
    low_name = name.lower()

    # оффлайн по чистому имени вида "Сделка #123"
    if DEAL_ONLY_RE.match(name):
        row["source"] = "оффлайн"

    # если по name явно звонок
    if ("сделка по звонку" in low_name) or ("по звонку" in low_name) or ("звонок" in low_name):
        row["source"] = "звонок"
        row["channel"] = "звонок"

    # если где-то есть домен сайта -> заявка с сайта (приоритет выше, чем va/sip)
    combined = " ".join(
        clean_text(str(row.get(k, ""))) for k in [
            "name", "name_clean", "source", "channel",
            "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term", "tags"
        ]
    ).lower()

    if SITE_MARKER in combined:
        row["source"] = "заявка с сайта"
        row["channel"] = "заявка с сайта"

    # source: va/sip -> звонок (после сайта)
    src = clean_text(str(row.get("source", "")))
    if src:
        src = VA_SIP_RE.sub("звонок", src)
        # добиваем варианты типа sip: / SIP-...
        src = re.sub(r"(?i)sip", "звонок", src)
        src = re.sub(r"(?i)\bva\b", "звонок", src)
        src = src.lower()

    row["source"] = src

    ## ФИНАЛЬНОЕ ПРАВИЛО: если среди тегов есть instagram — source всегда instagram
    tags = [t.strip() for t in clean_text(str(row.get("tags", ""))).lower().split(";") if t.strip()]
    if "instagram" in tags:
        row["source"] = "instagram"

    if isinstance(row.get("channel"), str):
        row["channel"] = clean_text(row["channel"])

    return row


# --- main ---
if __name__ == "__main__":
    if len(sys.argv) > 1:
        CLIENT_SLUG = sys.argv[1]
        CLIENT_ID = get_client_id(CLIENT_SLUG)
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        leads = json.load(f)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=FIELDS,
            delimiter=";",
            quoting=csv.QUOTE_MINIMAL  # без лишних кавычек
        )
        writer.writeheader()

        for lead in leads:
            row = {k: lead.get(k, "") for k in BASE_FIELDS}

            # принудительно проставляем корректного клиента
            row["client_id"] = CLIENT_ID
            row["client_slug"] = CLIENT_SLUG

            # ===== даты: timestamp -> ISO (UTC) =====
            created_iso = ts_to_iso(row.get("created_at"))
            updated_iso = ts_to_iso(row.get("updated_at"))
            closed_iso = ts_to_iso(row.get("closed_at"))

            # Основные поля (для ClickHouse и отчётов) — делаем нормальными DateTime-строками
            row["created_at"] = created_iso
            row["updated_at"] = updated_iso
            row["closed_at"] = closed_iso

            # Дубли для DataLens (можно оставить)
            row["created_dt"] = created_iso
            row["updated_dt"] = updated_iso
            row["closed_dt"] = closed_iso

            # чистим name и распаковываем из него поля
            row["name"] = clean_text(str(row.get("name", "")))
            channel, phone_from_name, name_clean = parse_name_fields(row["name"])
            row["channel"] = channel
            row["phone_from_name"] = phone_from_name
            row["name_clean"] = name_clean

            # кастомные поля
            row.update(extract_custom_fields(lead.get("custom_fields_values")))

            # теги
            row["tags"] = extract_tags(lead)

            # правила по source/channel
            row = apply_rules(row)

            writer.writerow(row)

    print("Готово. CSV сохранён:", OUTPUT_FILE)
    print("Лидов выгружено:", len(leads))