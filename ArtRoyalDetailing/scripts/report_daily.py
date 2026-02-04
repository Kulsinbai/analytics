from clickhouse_driver import Client
from pathlib import Path
from datetime import datetime

# ===== НАСТРОЙКИ =====
CLICKHOUSE_HOST = "217.18.63.106"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = "gen_user"
CLICKHOUSE_PASSWORD = "ТУТ_ПАРОЛЬ"
CLICKHOUSE_DB = "default_db"

CLIENT_ID = 1  # artroyal_detailing
SQL_DIR = Path(__file__).resolve().parent.parent / "sql" / "daily_report"

COMMUNICATIONS_MAP = {
    "звонок": ("📞", "Звонки"),
    "звонки": ("📞", "Звонки"),
    "call": ("📞", "Звонки"),
    "telephone": ("📞", "Звонки"),
    "telegram": ("💬", "telegram"),
    "tg": ("💬", "telegram"),
    "whatsapp": ("💬", "whatsapp"),
    "wa": ("💬", "whatsapp"),
    "avito": ("💬", "avito"),
    "instagram": ("💬", "instagram"),
    "insta": ("💬", "instagram"),
    "вконтакте": ("💬", "вконтакте"),
    "vk": ("💬", "вконтакте"),
    "заявка с сайта": ("🌐", "заявка с сайта"),
}


# ===== ВСПОМОГАТЕЛЬНОЕ =====
def read_sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text(encoding="utf-8")


def nvl(value, default=0):
    return default if value is None else value


def money(value: int) -> str:
    return f"{int(value):,}".replace(",", " ")

def normalize_comm_label(raw: str) -> tuple[str, str]:
    raw = (raw or "").strip().lower()
    if not raw:
        return "💬", "прочее"
    if "заявка" in raw and "сайт" in raw:
        return COMMUNICATIONS_MAP["заявка с сайта"]
    for key, label in COMMUNICATIONS_MAP.items():
        if raw == key:
            return label
    return "💬", raw

def format_comm_line(label: str, cnt: int) -> str:
    emoji, title = normalize_comm_label(label)
    suffix = " заявок" if title == "заявка с сайта" else ""
    return f"{emoji} {title} — {cnt}{suffix}"


# ===== ОСНОВНАЯ ЛОГИКА =====
def build_daily_report(client_id: int) -> str:
    ch = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )

    # --- 01 Коммуникации ---
    q1 = read_sql("01_communications.sql")
    comm_rows = ch.execute(q1, params={"client_id": client_id})
    # формат: [(source, cnt), ...]

    # --- 02 Продажи ---
    q2 = read_sql("02_sales.sql")
    won_cnt, won_sum, lost_cnt = ch.execute(q2, params={"client_id": client_id})[0]
    won_cnt = nvl(won_cnt)
    won_sum = nvl(won_sum)
    lost_cnt = nvl(lost_cnt)

    # --- 03 Потенциально недополучено ---
    q3 = read_sql("03_lost_sum.sql")
    lost_sum, unknown_budget_cnt = ch.execute(q3, params={"client_id": client_id})[0]
    lost_sum = nvl(lost_sum)
    unknown_budget_cnt = nvl(unknown_budget_cnt)

    # --- 04 Причины отказов ---
    q4 = read_sql("04_loss_reasons.sql")
    reasons_rows = ch.execute(q4, params={"client_id": client_id})
    # формат: [(reason, cnt, sum_price)]

    # ===== СБОРКА ОТЧЁТА =====
    date_str = datetime.now().strftime("%d.%m.%Y")

    # Коммуникации
    comm_lines = []
    for source, cnt in comm_rows:
        comm_lines.append(format_comm_line(source, cnt))
    if not comm_lines:
        comm_lines.append("— данных нет")

    # Причины отказов
    reasons_lines = []
    empty_reason_cnt = 0

    for reason, cnt, sum_price in reasons_rows:
        cnt = nvl(cnt)
        sum_price = nvl(sum_price)

        if reason == "Причины отказов не заполнены":
            empty_reason_cnt = cnt
        else:
            if sum_price > 0:
                reasons_lines.append(f"— {reason} — {cnt} (≈{money(sum_price)} ₽)")
            else:
                reasons_lines.append(f"— {reason} — {cnt}")

    if empty_reason_cnt > 0:
        reasons_lines.append(f"— Причины отказов не заполнены — {empty_reason_cnt}")

    if not reasons_lines:
        reasons_lines.append("— нет данных")