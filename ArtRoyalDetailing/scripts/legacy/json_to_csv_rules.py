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
OUTPUT_FILE = BASE_DIR / "data" / "add_leads_crm_flat_rules.csv"

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

TAG_RE = re.compile(r"<[^>]+>")
PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{8,}\d")
DEAL_ONLY_RE = re.compile(r"^\s*сделка\s*#\s*\d+\s*$", re.IGNORECASE)
SITE_MARKER = "artroyal-detailing.ru"

# для замены va/sip -> звонок (ловим как отдельные токены и внутри "sip:", "va.", "va-" и т.п.)
VA_SIP_RE = re.compile(r"\b(?:va|sip)\b", re.IGNORECASE)


def apply_rules(row: dict) -> dict:
    """
    Твои правила:
    1) если где-то встречается artroyal-detailing.ru -> source/channel = "заявка с сайта"
    2) source: va/sip -> "звонок"
    3) source в нижнем регистре
    4) если name == "Сделка #123" -> source = "оффлайн"
    """
    # 4) оффлайн по name (до остальных замен)
    name = clean_text(str(row.get("name", "")))
    if DEAL_ONLY_RE.match(name):
        row["source"] = "оффлайн"

    # 1) если где-то есть домен сайта -> заявка с сайта
    # собираем "весь текст строки", чтобы ловить домен хоть в UTM, хоть в source/name
    combined = " ".join(
        clean_text(str(row.get(k, ""))) for k in [
            "name", "name_clean", "source", "channel",
            "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term"
        ]
    ).lower()

    if SITE_MARKER in combined:
        row["source"] = "заявка с сайта"
        row["channel"] = "заявка с сайта"

    # 2) source: va/sip -> звонок (после "заявка с сайта", чтобы не ломать её)
    src = clean_text(str(row.get("source", "")))
    if src:
        # заменяем любые va/sip как отдельные слова
        src = VA_SIP_RE.sub("звонок", src)

        # иногда va/sip попадаются в виде "sip:" или "va-" — добиваем простым contains
        low = src.lower()
        if "sip" in low or "va" in low:
            # заменим явные куски, но аккуратно (без фанатизма)
            src = re.sub(r"(?i)sip", "звонок", src)
            src = re.sub(r"(?i)\bva\b", "звонок", src)

        # 3) весь source начинается с маленькой буквы (а ты просил "весь текст" — делаю всё lower)
        src = src.lower()

    row["source"] = src

    # channel тоже можно нормализовать (если есть)
    if isinstance(row.get("channel"), str):
        row["channel"] = clean_text(row["channel"])

    return row


# --- main ---
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    leads = json.load(f)

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=FIELDS,
        delimiter=";",
        quoting=csv.QUOTE_MINIMAL  # кавычки ставит только когда необходимо
    )
    writer.writeheader()

    for lead in leads:
        row = {k: lead.get(k, "") for k in BASE_FIELDS}

        # чистим name и распаковываем поля из name
        row["name"] = clean_text(str(row.get("name", "")))
        channel, phone_from_name, name_clean = parse_name_fields(row["name"])
        row["channel"] = channel
        row["phone_from_name"] = phone_from_name
        row["name_clean"] = name_clean

        # кастомные поля
        row.update(extract_custom_fields(lead.get("custom_fields_values")))

        # применяем твои правила
        row = apply_rules(row)

        writer.writerow(row)

print("Готово. CSV сохранён:", OUTPUT_FILE)
print("Лидов выгружено:", len(leads))

