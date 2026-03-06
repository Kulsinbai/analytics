# DEPRECATED: используйте scripts/leads_json_to_datalens_csv.py вместо этого скрипта.

import json
import csv
import re
from pathlib import Path

from scripts.transform_utils import (
    clean_text,
    normalize_phone,
    parse_name_fields,
    extract_utm as extract_custom_fields,
)

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "data" / "add_leads_crm_with_client.json"
OUTPUT_FILE = BASE_DIR / "data" / "add_leads_crm_flat_final.csv"

BASE_FIELDS = [
    "client_id",
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
]

FIELDS = BASE_FIELDS + EXTRA_FIELDS

DEAL_NUM_RE = re.compile(r"^\s*Сделка\s*#\d+\s*$", re.IGNORECASE)
DOMAIN = "artroyal-detailing.ru"


def apply_rules(row: dict) -> dict:
    """
    Твои правила:
    1) Всё, что содержит 'artroyal-detailing.ru' -> 'заявка с сайта' в source и channel
    2) В source: все va -> звонок
    3) source должен начинаться с маленькой буквы
    4) Если name похож на 'Сделка #4495279' -> source = 'оффлайн'
    """
    name = clean_text(str(row.get("name", "")))
    source = clean_text(str(row.get("source", "")))
    channel = clean_text(str(row.get("channel", "")))

    # 4) "Сделка #123" => оффлайн
    if DEAL_NUM_RE.match(name):
        source = "оффлайн"

    # 1) домен => заявка с сайта (приоритетно)
    if (DOMAIN in name.lower()) or (DOMAIN in source.lower()) or (DOMAIN in channel.lower()):
        source = "заявка с сайта"
        channel = "заявка с сайта"

    # 2) va => звонок (делаю как значение целиком, чтобы не ломать слова)
    if source.strip().lower() == "va":
        source = "звонок"

    # 3) source с маленькой буквы
    source = starts_with_lower(source)

    row["source"] = source
    row["channel"] = channel
    return row


# --- main ---
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    leads = json.load(f)

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=FIELDS,
        delimiter=";",
        quoting=csv.QUOTE_MINIMAL  # не вставляем кавычки везде подряд
    )
    writer.writeheader()

    for lead in leads:
        row = {k: lead.get(k, "") for k in BASE_FIELDS}

        # чистим name + парсим name в отдельные поля
        row["name"] = clean_text(str(row.get("name", "")))
        channel, phone_from_name, name_clean = parse_name_fields(row["name"])
        row["channel"] = channel
        row["phone_from_name"] = phone_from_name
        row["name_clean"] = name_clean

        # кастомные поля
        extra = extract_custom_fields(lead.get("custom_fields_values"))
        row.update(extra)

        # применяем твои правила
        row = apply_rules(row)

        writer.writerow(row)

print("Готово. CSV сохранён:", OUTPUT_FILE)
print("Лидов выгружено:", len(leads))

