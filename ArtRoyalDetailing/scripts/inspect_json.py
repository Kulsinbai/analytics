import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "data" / "add_leads_crm_with_client.json"

def find_records(data):
    # 1) если это список — скорее всего это записи
    if isinstance(data, list):
        return data

    # 2) если это словарь — ищем первую "похожую" коллекцию записей
    if isinstance(data, dict):
        # прямое поле records/items/leads и т.п.
        for key in ("records", "items", "leads", "data", "result"):
            value = data.get(key)
            if isinstance(value, list):
                return value

        # иначе ищем вообще любую list внутри
        for value in data.values():
            if isinstance(value, list):
                return value

    return None

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

records = find_records(data)

print("Тип корня JSON:", type(data).__name__)
print("Найдено записей:", len(records) if records else 0)

if records:
    first = records[0]
    print("Тип одной записи:", type(first).__name__)
    if isinstance(first, dict):
        keys = sorted(first.keys())
        print("Ключи первой записи (первые 40):")
        print(keys[:40])
