import json
import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "data" / "add_leads_crm_with_client.json"
OUTPUT_FILE = BASE_DIR / "data" / "add_leads_crm_flat.csv"

# Базовые поля (они у тебя точно есть)
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

# Какие кастомные поля хотим достать (потом подстроим по выводу list_custom_fields.py)
CUSTOM_FIELDS_WANTED = [
    "Телефон",
    "Email",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_content",
    "utm_term",
    "Источник",
]

def extract_custom_fields(custom_fields_values):
    """
    Возвращает словарь: {field_name: "значение1; значение2"}
    """
    result = {}
    if not custom_fields_values:
        return result

    for field in custom_fields_values:
        field_name = field.get("field_name")
        values = field.get("values") or []
        extracted = []
        for v in values:
            val = v.get("value")
            if val is None:
                continue
            extracted.append(str(val))

        if field_name and extracted:
            # если несколько значений — склеим через ;
            result[field_name] = "; ".join(extracted)

    return result

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    leads = json.load(f)

# Соберём заголовки CSV: базовые + выбранные кастомные
FIELDS = BASE_FIELDS + CUSTOM_FIELDS_WANTED

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDS, delimiter=";")
    writer.writeheader()

    for lead in leads:
        row = {k: lead.get(k, "") for k in BASE_FIELDS}

        cf_map = extract_custom_fields(lead.get("custom_fields_values"))
        for name in CUSTOM_FIELDS_WANTED:
            row[name] = cf_map.get(name, "")

        writer.writerow(row)

print("Готово. CSV сохранён:", OUTPUT_FILE)
print("Лидов выгружено:", len(leads))