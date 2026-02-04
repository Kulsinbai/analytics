import pandas as pd
import clickhouse_connect
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent.parent

LEADS_CSV = BASE_DIR / "data" / "add_leads_crm_flat_datalens.csv"

CLIENT_ID = 1
CH_DB = "default_db"
CH_TABLE = "leads_fact"

# ====== заполни своими данными ======
CH_HOST = "217.18.63.106"      # например: xxx.timeweb.cloud или IP
CH_PORT = 8123             # чаще всего 8123 (HTTP)
CH_USER = "gen_user"
CH_PASSWORD = "tucxERGS+7SLVu"
# ================================


def parse_dt(series: pd.Series) -> pd.Series:
    """
    Парсим 'YYYY-MM-DD HH:MM:SS' в datetime (без таймзоны),
    чтобы ClickHouse (DateTime) принял без проблем.
    """
    dt = pd.to_datetime(series, errors="coerce")
    # ClickHouse DateTime обычно без tz, убираем timezone если вдруг появилась
    try:
        dt = dt.dt.tz_localize(None)
    except Exception:
        pass
    return dt


def main():
    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
    )

    # 0) читаем CSV
    df = pd.read_csv(LEADS_CSV, sep=";", encoding="utf-8-sig")

    # 1) перезаливка данных клиента (тестовый режим)
    client.command(f"ALTER TABLE {CH_DB}.{CH_TABLE} DELETE WHERE client_id = 1")

    etl_loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)

    # 2) строим датафрейм ровно под схему ClickHouse
    out = pd.DataFrame()
    out["client_id"] = 1  # жёстко для теста

    # CSV.id -> CH.lead_id
    out["lead_id"] = pd.to_numeric(df.get("id"), errors="coerce").fillna(0).astype("int64")

    # created_at/updated_at/closed_at в CH будем заполнять из *_dt (они уже красивые)
    out["created_at"] = parse_dt(df.get("created_dt"))
    out["updated_at"] = parse_dt(df.get("updated_dt"))
    out["closed_at"] = parse_dt(df.get("closed_dt"))

    # оставим и сами *_dt как отдельные поля (если хочешь хранить их тоже)
    out["created_dt"] = parse_dt(df.get("created_dt"))
    out["updated_dt"] = parse_dt(df.get("updated_dt"))
    out["closed_dt"] = parse_dt(df.get("closed_dt"))

    # числовые поля
    out["status_id"] = pd.to_numeric(df.get("status_id"), errors="coerce")
    out["pipeline_id"] = pd.to_numeric(df.get("pipeline_id"), errors="coerce")
    out["loss_reason_id"] = pd.to_numeric(df.get("loss_reason_id"), errors="coerce")
    out["price"] = pd.to_numeric(df.get("price"), errors="coerce")
    out["account_id"] = pd.to_numeric(df.get("account_id"), errors="coerce")
    out["created_by"] = pd.to_numeric(df.get("created_by"), errors="coerce")
    out["updated_by"] = pd.to_numeric(df.get("updated_by"), errors="coerce")
    out["score"] = pd.to_numeric(df.get("score"), errors="coerce")

    # responsible_user_id -> manager_id
    out["manager_id"] = pd.to_numeric(df.get("responsible_user_id"), errors="coerce")

    # is_deleted -> UInt8 (0/1)
    if "is_deleted" in df.columns:
        out["is_deleted"] = pd.to_numeric(df["is_deleted"], errors="coerce").fillna(0).astype("int64")
    else:
        out["is_deleted"] = 0

    # строки
    str_cols = [
        "client_slug", "name", "utm_source", "utm_medium", "utm_campaign",
        "utm_content", "utm_term", "source", "phone", "email",
        "channel", "phone_from_name", "name_clean"
    ]
    for col in str_cols:
        if col in df.columns:
            out[col] = df[col].astype("string")
        else:
            out[col] = pd.Series([None] * len(out), dtype="string")

    out["etl_loaded_at"] = etl_loaded_at

    # 3) базовые фильтры качества
    out = out[out["lead_id"] > 0].copy()

    # NaN -> None для Nullable
    out = out.where(pd.notnull(out), None)

    # client_id — NOT NULL, фиксируем ЖЁСТКО
    out["client_id"] = 1

    # 4) вставка
    client.insert_df(f"{CH_DB}.{CH_TABLE}", out)

    print("OK. Inserted rows:", len(out))
    print("Client:", CLIENT_ID)


if __name__ == "__main__":
    main()
