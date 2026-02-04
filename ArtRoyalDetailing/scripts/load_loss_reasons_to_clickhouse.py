import pandas as pd
import clickhouse_connect
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent.parent
LOSS_CSV = BASE_DIR / "data" / "loss_reasons.csv"

CLIENT_ID = 1
CH_DB = "default_db"
CH_TABLE = "loss_reasons_dim"

CH_HOST = "217.18.63.106"
CH_PORT = 8123
CH_USER = "gen_user"
CH_PASSWORD = "tucxERGS+7SLVu"

def main():
    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
    )

    client.command(f"ALTER TABLE {CH_DB}.{CH_TABLE} DELETE WHERE client_id = {CLIENT_ID}")

    df = pd.read_csv(LOSS_CSV, sep=";", encoding="utf-8-sig")
    etl_loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)

    out = pd.DataFrame()
    out["client_id"] = CLIENT_ID
    out["id"] = pd.to_numeric(df.get("id"), errors="coerce").fillna(0).astype("int64")
    out["name"] = df.get("name").astype("string")
    out["etl_loaded_at"] = etl_loaded_at

    out = out[out["id"] > 0].copy()
    out = out.where(pd.notnull(out), None)

    # фиксируем NOT NULL поля
    out["client_id"] = CLIENT_ID

    client.insert_df(f"{CH_DB}.{CH_TABLE}", out)
    print("OK. Inserted loss reasons:", len(out))

if __name__ == "__main__":
    main()
