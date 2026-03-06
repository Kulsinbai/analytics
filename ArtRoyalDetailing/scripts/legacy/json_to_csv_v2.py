# DEPRECATED: используйте scripts/leads_json_to_datalens_csv.py вместо этого скрипта.

import json
import csv
from pathlib import Path

from scripts.transform_utils import clean_text, fix_mojibake

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "data" / "add_leads_crm_with_client.json"
OUTPUT_FILE = BASE_DIR / "data" / "add_leads_crm_flat_v2.csv"

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

# Колонки, которые хотим получить в CSV
EXTRA_FIELDS = [
    "phone",
    "email",
    "source",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_content",
    "utm_term",
]


def extract_by_code(custom_fields_values):
    """
    Достаём PHONE/EMAIL по field_code, а UTM/Источник — по field_name (если попадутся).
    Возвращаем dict с нужными нам полями.
    """
    out = {
        "phone": "",
        "email": "",
        "source": "",
        "utm_source": "",
        "utm_medium": "",
        "utm_campaign": "",
        "utm_content": "",
        "utm_term": "",
    }

    if not custom_fields_values:
        return out

    # Вспомогательная функция: собрать все value в одну строку
    def join_values(values):
        arr = []
        for v in values or []:
            val = v.get("value")
            if val is None:
                continue
            arr.append(clean_text(str(val)))
        return "; ".join([x for x in arr if x])

    for field in custom_fields_values:
        code = (field.get("field_code") or "").upper()
        name = clean_text(field.get("field_name") or "")
        value_str = join_values(field.get("values"))

        if not value_str:
            continue

        # PHONE / EMAIL (самое надежное)
        if code == "PHONE" and not out["phone"]:
            out["phone"] = value_str
            continue
        if code == "EMAIL" and not out["email"]:
            out["email"] = value_str
            continue

        # UTM (по имени поля, если кодов нет)
        lname = name.lower()
        if "utm_source" in lname and not out["utm_source"]:
            out["utm_source"] = value_str
        elif "utm_medium" in lname and not out["utm_medium"]:
            out["utm_medium"] = value_str
        elif "utm_campaign" in lname and not out["utm_campaign"]:
            out["utm_campaign"] = value_str
        elif "utm_content" in lname and not out["utm_content"]:
            out["utm_content"] = value_str
        elif "utm_term" in lname and not out["utm_term"]:
            out["utm_term"] = value_str

        # Источник (тоже по имени)
        if ("источник" in lname or "source" == lname) and not out["source"]:
            out["source"] = value_str

    return out


with open(INPUT_FILE, "r", encoding="utf-8") as f:
    leads = json.load(f)

FIELDS = BASE_FIELDS + EXTRA_FIELDS

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=FIELDS,
        delimiter=";",
        quoting=csv.QUOTE_ALL
    )
    writer.writeheader()

    for lead in leads:
        row = {k: lead.get(k, "") for k in BASE_FIELDS}
        # чистим name (там как раз кракозябры)
        row["name"] = clean_text(str(row.get("name", "")))

        extra = extract_by_code(lead.get("custom_fields_values"))
        row.update(extra)

        writer.writerow(row)

print("Готово. CSV сохранён:", OUTPUT_FILE)
print("Лидов выгружено:", len(leads))

