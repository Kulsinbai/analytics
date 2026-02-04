import json
from pathlib import Path

# ====== настройки ======
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "data" / "add_leads_crm.json"
OUTPUT_FILE = BASE_DIR / "data" / "add_leads_crm_with_client.json"

CLIENT_ID = 1  # ВАЖНО: число, как в ClickHouse (UInt32)
CLIENT_SLUG = "artroyal_detailing"  # удобное имя для отладки, справочников и фильтров

# ====== валидация ======
if not isinstance(CLIENT_ID, int) or CLIENT_ID <= 0:
    raise ValueError("CLIENT_ID должен быть положительным целым числом (например 1, 2, 3).")

# ====== загрузка JSON (utf-8-sig на случай BOM) ======
with open(INPUT_FILE, "r", encoding="utf-8-sig") as f:
    data = json.load(f)

def add_client_fields(obj: dict) -> None:
    """Добавляет поля клиента в один объект (словарь)."""
    obj["client_id"] = CLIENT_ID
    obj["client_slug"] = CLIENT_SLUG

# ====== обработка JSON разных форм ======
if isinstance(data, list):
    for item in data:
        if isinstance(item, dict):
            add_client_fields(item)

elif isinstance(data, dict):
    # если словарь, пробегаемся по значениям и ищем списки объектов
    for value in data.values():
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    add_client_fields(item)

else:
    raise TypeError("Неподдерживаемый формат JSON. Ожидался list или dict.")

# ====== сохранение ======
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("Готово. Файл сохранён:", OUTPUT_FILE)
print("client_id =", CLIENT_ID, "| client_slug =", CLIENT_SLUG)