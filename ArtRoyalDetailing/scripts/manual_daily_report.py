from pathlib import Path
from datetime import datetime, timedelta
import clickhouse_connect

CLIENT_ID = 1

CH_HOST = "217.18.63.106"
CH_PORT = 8123
CH_USER = "gen_user"
CH_PASSWORD = "tucxERGS+7SLVu"
CH_DB = "default_db"

PIPELINES = {
    9524230: "Детейлинг",
    10636486: "Мойка",
}

BASE_DIR = Path(__file__).resolve().parent.parent
SQL_DIR = BASE_DIR / "sql" / "daily_report"

SQL_01 = "01_communications.sql"
SQL_02 = "02_sales.sql"
SQL_03 = "03_lost_sum.sql"
SQL_04 = "04_loss_reasons.sql"


def read_sql(filename: str) -> str:
    path = SQL_DIR / filename
    return path.read_text(encoding="utf-8")


def normalize_sql(sql_text: str) -> str:
    s = sql_text.strip().lstrip("\ufeff").strip()
    parts = [p.strip() for p in s.split(";") if p.strip()]
    return parts[0]


def money(x) -> str:
    if x is None:
        x = 0
    try:
        x = float(x)
    except Exception:
        x = 0
    return f"{x:,.0f}".replace(",", " ") + " ₽"


def run_query(ch, filename: str, params: dict):
    sql = normalize_sql(read_sql(filename))
    return ch.query(sql, parameters=params).result_rows


def run():
    ch = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
    )

    report_date = (datetime.now() - timedelta(days=1)).strftime("%d.%m")
    lines = [f"📊 Отчёт за {report_date}", ""]

    total_inbound_all = 0
    won_cnt_all = 0
    won_sum_all = 0.0
    lost_cnt_all = 0
    lost_sum_all = 0.0
    lost_unknown_all = 0

    for pipeline_id, pipeline_name in PIPELINES.items():
        params = {"client_id": CLIENT_ID, "pipeline_id": pipeline_id}

        comm_rows = run_query(ch, SQL_01, params)
        sales_row = run_query(ch, SQL_02, params)[0]
        lost_row = run_query(ch, SQL_03, params)[0]
        reasons_rows = run_query(ch, SQL_04, params)

        inbound_total = sum(int(r[1] or 0) for r in comm_rows)

        won_cnt = int(sales_row[0] or 0)
        won_sum = float(sales_row[1] or 0)
        lost_cnt = int(sales_row[2] or 0)

        lost_sum = float(lost_row[0] or 0)
        lost_unknown = int(lost_row[1] or 0)

        total_inbound_all += inbound_total
        won_cnt_all += won_cnt
        won_sum_all += won_sum
        lost_cnt_all += lost_cnt
        lost_sum_all += lost_sum
        lost_unknown_all += lost_unknown

        lines.append(f"📌 {pipeline_name}")
        lines.append("")

        lines.append("Обращения:")
        if comm_rows:
            for source, cnt in comm_rows:
                lines.append(f"• {source} — {int(cnt or 0)}")
        else:
            lines.append("• нет данных")
        lines.append(f"Итого: {inbound_total} обращений")
        lines.append("")

        lines.append("Продажи (amoCRM):")
        lines.append(f"✅ Успешно — {won_cnt} сделок")
        lines.append(f"💰 Выручка — {money(won_sum)}")
        lines.append("")

        lines.append("❌ Нереализовано:")
        lines.append(f"• {lost_cnt} сделок")
        if lost_sum > 0:
            lines.append(f"💸 Потенциально недополучено — ~{money(lost_sum)}")
        else:
            lines.append("💸 Потенциально недополучено — неизвестно (бюджет не указан)")
            if lost_unknown > 0:
                lines.append(f"• Сделок без бюджета: {lost_unknown}")
        lines.append("")

        lines.append("Причины отказов:")
        if reasons_rows:
            for reason, cnt, sum_price in reasons_rows:
                cnt = int(cnt or 0)
                sum_price = float(sum_price or 0)
                if sum_price > 0:
                    lines.append(f"— {reason} — {cnt} (≈ {money(sum_price)})")
                else:
                    lines.append(f"— {reason} — {cnt}")
        else:
            lines.append("— нет данных")

        lines.append("")
        lines.append("")

    lines.append("Итого по всем воронкам:")
    lines.append(f"Обращения: {total_inbound_all}")
    lines.append(f"✅ Успешно: {won_cnt_all} сделок, выручка {money(won_sum_all)}")
    lines.append(f"❌ Отказы: {lost_cnt_all} сделок")
    if lost_sum_all > 0:
        lines.append(f"💸 Потенциально недополучено: ~{money(lost_sum_all)}")
    else:
        lines.append("💸 Потенциально недополучено: неизвестно (бюджет не указан)")
        if lost_unknown_all > 0:
            lines.append(f"• Сделок без бюджета: {lost_unknown_all}")

    print("\n".join(lines))


if __name__ == "__main__":
    run()