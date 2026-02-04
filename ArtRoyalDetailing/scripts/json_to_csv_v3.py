import json
import csv
import re
import html
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "data" / "add_leads_crm_with_client.json"
OUTPUT_FILE = BASE_DIR / "data" / "add_leads_crm_flat_v3.csv"

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


def fix_mojibake(s: str) -> str:
    """
    Чинит многоходовые кракозябры ('Ð...', 'Ñ...') в несколько проходов.
    """
    if not isinstance(s, str) or not s:
        return s

    for _ in range(3):
        if "Ð" not in s and "Ñ" not in s:
            break
        try:
            candidate = s.encode("latin1").decode("utf-8")
        except Exception:
            break
        if candidate == s:
            break
        s = candidate

    return s


def clean_text(s: str) -> str:
    """
    Убираем HTML-теги/сущности, нормализуем пробелы, лечим кракозябры.
    """
    if not isinstance(s, str):
        return s

    s = html.unescape(s)
    s = TAG_RE.sub(" ", s)
    s = s.replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = fix_mojibake(s)
    return s


def normalize_phone(raw: str) -> str:
    """
    Приводит телефон к виду +7XXXXXXXXXX (если похоже на РФ).
    Иначе возвращает просто цифры.
    """
    raw = clean_text(raw)
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return ""

    # 8XXXXXXXXXX -> +7XXXXXXXXXX
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]

    if len(digits) == 11 and digits.startswith("7"):
        return "+" + digits

    return digits


def extract_custom_fields(custom_fields_values):
    """
    Достаём phone/email по field_code (самое надежное).
    UTM и source пытаемся достать по названию поля.
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

        # PHONE / EMAIL
        if code == "PHONE" and not out["phone"]:
            out["phone"] = normalize_phone(value_str)
            continue
        if code == "EMAIL" and not out["email"]:
            out["email"] = value_str
            continue

        lname = name.lower()

        # UTM по имени (если есть)
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

        # Источник по имени
        if (("источник" in lname) or (lname == "source")) and not out["source"]:
            out["source"] = value_str

    return out


def parse_name_fields(name: str):
    """
    Из name вытаскиваем:
    - channel (WhatsApp/Telegram/Call)
    - phone_from_name (если номер зашит в тексте)
    - name_clean (без префиксов и телефона)
    """
    name = clean_text(name)
    low = name.lower()

    channel = ""
    if "whatsapp" in low:
        channel = "WhatsApp"
    elif "telegram" in low:
        channel = "Telegram"
    elif "звон" in low:
        channel = "Call"

    m = PHONE_RE.search(name)
    phone_from_name = normalize_phone(m.group(0)) if m else ""

    name_clean = name

    # Частые префиксы CRM
    prefixes = [
        "Новый лид ",
        "Новый лид: ",
        "Новый лид - ",
        "Сделка по звонку ",
        "Новый лид звонок с ",
    ]
    for p in prefixes:
        if name_clean.startswith(p):
            name_clean = name_clean[len(p):].strip()
            break

    # Убираем телефон из clean-имени
    if phone_from_name:
        name_clean = PHONE_RE.sub("", name_clean).strip()
        name_clean = re.sub(r"\s+", " ", name_clean)

    return channel, phone_from_name, name_clean


# --- main ---
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    leads = json.load(f)

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

        # чистим name и извлекаем доп. поля из name
        row["name"] = clean_text(str(row.get("name", "")))
        channel, phone_from_name, name_clean = parse_name_fields(row["name"])
        row["channel"] = channel
        row["phone_from_name"] = phone_from_name
        row["name_clean"] = name_clean

        # кастомные поля (phone/email/utm/source)
        extra = extract_custom_fields(lead.get("custom_fields_values"))
        row.update(extra)

        writer.writerow(row)

print("Готово. CSV сохранён:", OUTPUT_FILE)
print("Лидов выгружено:", len(leads))