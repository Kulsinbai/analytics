import re
import html
from typing import Any, Dict, List, Tuple


TAG_RE = re.compile(r"<[^>]+>")
PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{8,}\d")


def fix_mojibake(s: str) -> str:
    """
    Чинит многоходовые кракозябры ('Ð...', 'Ñ...') в несколько проходов.
    Логика совпадает с реализацией в leads_json_to_datalens_csv.py.
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
    Совместимо с реализацией в leads_json_to_datalens_csv.py.
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
    Приводит телефон к виду +7XXXXXXXXXX (если похоже на РФ),
    иначе возвращает просто цифры (как в текущих скриптах).
    """
    raw = clean_text(raw)
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return ""

    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]

    if len(digits) == 11 and digits.startswith("7"):
        return "+" + digits

    return digits


def parse_name_fields(name: str) -> Tuple[str, str, str]:
    """
    Разбор поля name:
    - channel (WhatsApp/Telegram/Call)
    - phone_from_name (номер из текста)
    - name_clean (без префиксов CRM и телефона)
    Поведение совпадает с реализацией в leads_json_to_datalens_csv.py.
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


def extract_utm(custom_fields_values: Any) -> Dict[str, str]:
    """
    Достаёт phone/email по field_code и UTM/source по названию поля.
    Поведение идентично extract_custom_fields из leads_json_to_datalens_csv.py.
    """
    out: Dict[str, str] = {
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

    def join_values(values: List[Dict[str, Any]] | None) -> str:
        arr: List[str] = []
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


# Алиас для старого имени, чтобы не ломать код, который ожидает extract_custom_fields.
extract_custom_fields = extract_utm

