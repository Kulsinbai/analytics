import csv
from pathlib import Path
from datetime import datetime

from scripts.amocrm_client import get_valid_access_token, get_json, AmoClientError
from scripts.clients_map import get_client_id

CLIENT_SLUG = "artroyal_detailing"
CLIENT_ID = get_client_id(CLIENT_SLUG)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
OUT_PATH = DATA_DIR / "loss_reasons.csv"

def unix_to_dt_str(value):
    """
    amoCRM отдаёт created_at/updated_at как Unix timestamp (секунды).
    Превращаем в строку 'YYYY-MM-DD HH:MM:SS' (UTC).
    """
    if value is None or value == "":
        return ""
    ts = int(float(str(value).replace(",", ".")))
    return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def main():
    try:
        account_domain, access_token = get_valid_access_token()

        url = f"{account_domain}/api/v4/leads/loss_reasons"
        data = get_json(url, access_token)

        items = data.get("_embedded", {}).get("loss_reasons", [])
        if not items:
            print("⚠️ Справочник loss_reasons пуст или не найден в ответе API.")
            print("Ответ:", data)
            return

        DATA_DIR.mkdir(parents=True, exist_ok=True)

        fieldnames = [
            "client_id", "client_slug",
            "loss_reason_id", "loss_reason_name",
            "created_at", "updated_at",
            "sort"
        ]

        with open(OUT_PATH, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=fieldnames,
                delimiter=";"
            )
            writer.writeheader()

            for it in items:
                writer.writerow({
                    "client_id": CLIENT_ID,
                    "client_slug": CLIENT_SLUG,

                    "loss_reason_id": it.get("id"),
                    "loss_reason_name": it.get("name"),
                    "created_at": unix_to_dt_str(it.get("created_at")),
                    "updated_at": unix_to_dt_str(it.get("updated_at")),
                    "sort": it.get("sort")
                })

        print("✅ CSV выгружен:", OUT_PATH)
        print(f"✅ Записей: {len(items)}")

    except AmoClientError as e:
        print("❌ Ошибка клиента amoCRM:", e)


if __name__ == "__main__":
    main()
