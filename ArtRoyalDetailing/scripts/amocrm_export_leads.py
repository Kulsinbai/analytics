import json
from pathlib import Path
from scripts.amocrm_client import get_valid_access_token, get_json

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_FILE = BASE_DIR / "data" / "add_leads_crm.json"


def main():
    account_domain, access_token = get_valid_access_token()

    url = f"{account_domain}/api/v4/leads?limit=250"
    all_leads = []

    while url:
        data = get_json(url, access_token)
        leads = data.get("_embedded", {}).get("leads", [])
        all_leads.extend(leads)

        url = data.get("_links", {}).get("next", {}).get("href")

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_leads, f, ensure_ascii=False, indent=2)

    print(f"Готово. Лидов выгружено: {len(all_leads)}")


if __name__ == "__main__":
    main()
