import json
import csv
import re
import html
from pathlib import Path

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


def fix_mojibake(s: str) -> str:
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
    if not isinstance(s, str):
        return s
    s = html.unescape(s)
    s = TAG_RE.sub(" ", s)
    s = s.replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = fix_mojibake(s)
    return s


def normalize_phone(raw: str) -> str:
    raw = clean_text(raw)
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return ""
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 11 and digits.startswith("7"):
        return "+" + digits
    return digits


def extract_custom_fields(custom_fields_values):
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

        if code == "PHONE" and not out["phone"]:
            out["phone"] = normalize_phone(value_str)
            continue

        if code == "EMAIL" and not out["email"]:
            out["email"] = value_str
            continue

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

        if (("источник" in lname) or (lname == "source")) and not out["source"]:
            out["source"] = value_str

    return out


def parse_name_fields(name: str):
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

    if phone_from_name:
        name_clean = PHONE_RE.sub("", name_clean).strip()
        name_clean = re.sub(r"\s+", " ", name_clean)

    return channel, phone_from_name, name_clean


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