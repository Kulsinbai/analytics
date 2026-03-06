# DEPRECATED: используйте scripts/leads_json_to_datalens_csv.py вместо этого скрипта.

import json
import csv
from pathlib import Path

from scripts.transform_utils import (
    clean_text,
    normalize_phone,
    parse_name_fields,
    extract_utm as extract_custom_fields,
)

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "data" / "add_leads_crm_with_client.json"
OUTPUT_FILE = BASE_DIR / "data" / "add_leads_crm_flat_v4.csv"

SITE_TOKEN = "artroyal-detailing.ru"
SITE_TEXT = "заявка с сайта"

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


def contains_site_token(val) -> bool:
    return isinstance(val, str) and SITE_TOKEN in val


def replace_site_token(val):
    if contains_site_token(val):
        return SITE_TEXT
    return val


# --- main ---
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    leads = json.load(f)

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=FIELDS,
        delimiter=";",
        quoting=csv.QUOTE_MINIMAL  # НЕ добавляем кавычки везде
    )
    writer.writeheader()

    for lead in leads:
        row = {k: lead.get(k, "") for k in BASE_FIELDS}

        # 1) чистим name и вытаскиваем поля из name
        row["name"] = clean_text(str(row.get("name", "")))
        channel, phone_from_name, name_clean = parse_name_fields(row["name"])
        row["channel"] = channel
        row["phone_from_name"] = phone_from_name
        row["name_clean"] = name_clean

        # 2) кастомные поля
        extra = extract_custom_fields(lead.get("custom_fields_values"))
        row.update(extra)

        # 3) Замена "artroyal-detailing.ru" -> "заявка с сайта"
        # Сначала проверяем нужные колонки, потом принудительно ставим source
        site_hit = False

        for col in ("name", "name_clean", "source", "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term"):
            if contains_site_token(row.get(col, "")):
                row[col] = SITE_TEXT
                site_hit = True

        if site_hit:
            row["source"] = SITE_TEXT

        writer.writerow(row)

print("Готово. CSV сохранён:", OUTPUT_FILE)
print("Лидов выгружено:", len(leads))

