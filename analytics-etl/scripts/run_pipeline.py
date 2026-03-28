import argparse
import csv
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# чтобы работал импорт "from scripts...." при запуске файла напрямую:
# python3 scripts/run_pipeline.py ...
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.clients_map import get_client_id

DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
VAR_DIR = BASE_DIR / "var"


def make_logger(client_slug: str):
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOGS_DIR / f"pipeline_{client_slug}_{ts}.log"

    def log(msg: str) -> None:
        line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
        print(line)
        with log_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    return log, log_path


def run_step(cmd: list[str], description: str, log) -> None:
    log(f"=== START: {description} ===")
    log("Команда: " + " ".join(cmd))
    start = time.monotonic()
    try:
        result = subprocess.run(
            [sys.executable, *cmd],
            cwd=BASE_DIR,
            text=True,
            capture_output=True,
        )
    except Exception as e:
        log(f"✖ Не удалось запустить шаг '{description}': {e}")
        raise

    duration = time.monotonic() - start

    if result.stdout:
        for line in result.stdout.splitlines():
            log(f"[stdout] {line}")
    if result.stderr:
        for line in result.stderr.splitlines():
            log(f"[stderr] {line}")

    if result.returncode != 0:
        log(f"✖ Шаг '{description}' завершился с ошибкой, код={result.returncode}, время={duration:.1f} c")
        raise RuntimeError(f"Step failed: {description}")

    log(f"✔ Шаг '{description}' успешно выполнен за {duration:.1f} c")


def count_csv_rows(path: Path, log, required_cols: list[str] | None = None) -> int:
    if not path.exists():
        log(f"ERROR: CSV не найден: {path}")
        return -1
    if path.stat().st_size == 0:
        log(f"ERROR: CSV пустой (size=0): {path}")
        return -1

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        fieldnames = reader.fieldnames or []
        if required_cols:
            missing = [c for c in required_cols if c not in fieldnames]
            if missing:
                log(
                    "ERROR: в CSV отсутствуют обязательные колонки "
                    f"(файл={path}): {', '.join(missing)}"
                )
                return -1

        row_count = sum(1 for _ in reader)

    log(f"CSV '{path.name}': строк данных (без заголовка) = {row_count}")
    if row_count == 0:
        log(f"ERROR: CSV '{path.name}' не содержит ни одной строки данных.")
        return -1

    return row_count


def count_json_leads(path: Path, log) -> int:
    if not path.exists():
        log(f"ERROR: JSON не найден: {path}")
        return -1

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        log(f"ERROR: не удалось прочитать JSON {path}: {e}")
        return -1

    if isinstance(data, list):
        n = len(data)
    else:
        # fallback: попробуем найти первую коллекцию записей
        records = None
        if isinstance(data, dict):
            for key in ("records", "items", "leads", "data", "result"):
                v = data.get(key)
                if isinstance(v, list):
                    records = v
                    break
        n = len(records) if records else 0

    log(f"JSON '{path.name}': найдено лидов = {n}")
    if n == 0:
        log(f"WARNING: JSON '{path.name}' не содержит лидов.")
    return n


def run_leads_pipeline(client_slug: str, log) -> None:
    client_id = get_client_id(client_slug)
    log(f"##### Запуск пайплайна лидов для клиента: {client_slug} (id={client_id}) #####")

    client_data_dir = VAR_DIR / "data" / client_slug
    client_data_dir.mkdir(parents=True, exist_ok=True)

    leads_json = client_data_dir / "add_leads_crm.json"
    leads_with_client_json = client_data_dir / "add_leads_crm_with_client.json"
    leads_csv = client_data_dir / "add_leads_crm_flat_datalens.csv"

    log("Контекст клиента (leads):")
    log(f"- client_slug={client_slug}")
    log(f"- client_id={client_id}")
    log(f"- leads_json={leads_json}")
    log(f"- leads_with_client_json={leads_with_client_json}")
    log(f"- leads_csv={leads_csv}")

    # 1) Выгрузка лидов из amoCRM
    run_step(
        ["scripts/amocrm_export_leads.py", "--client-slug", client_slug, "--out", str(leads_json)],
        "Лиды: шаг 1/4 — выгрузка лидов из amoCRM",
        log,
    )
    count_json_leads(leads_json, log)

    # 2) Добавление client_id / client_slug в JSON
    run_step(
        [
            "scripts/add_client_id.py",
            "--client-slug",
            client_slug,
            "--in",
            str(leads_json),
            "--out",
            str(leads_with_client_json),
        ],
        "Лиды: шаг 2/4 — добавление client_id/client_slug в JSON",
        log,
    )
    count_json_leads(leads_with_client_json, log)

    # 3) Построение плоского CSV для DataLens/ClickHouse
    run_step(
        [
            "scripts/leads_json_to_datalens_csv.py",
            "--client-slug",
            client_slug,
            "--in",
            str(leads_with_client_json),
            "--out",
            str(leads_csv),
        ],
        "Лиды: шаг 3/4 — преобразование JSON в плоский CSV для отчётности",
        log,
    )

    leads_required_cols = [
        "id",
        "created_dt",
        "updated_dt",
        "closed_dt",
        "status_id",
        "pipeline_id",
        "account_id",
        "responsible_user_id",
        "client_slug",
        "name",
    ]
    rows_csv = count_csv_rows(leads_csv, log, leads_required_cols)
    if rows_csv < 0:
        raise RuntimeError("Валидация CSV лидов не пройдена, загрузка в ClickHouse отменена.")

    # 4) Загрузка факта лидов в ClickHouse
    run_step(
        [
            "scripts/load_leads_csv_to_clickhouse.py",
            "--client-slug",
            client_slug,
            "--csv-path",
            str(leads_csv),
        ],
        "Лиды: шаг 4/4 — загрузка CSV в ClickHouse",
        log,
    )

    log(f"##### Пайплайн лидов для {client_slug} (id={client_id}) завершён успешно #####")


def run_dims_pipeline(client_slug: str, log) -> None:
    client_id = get_client_id(client_slug)
    log(f"##### Запуск пайплайна справочников для клиента: {client_slug} (id={client_id}) #####")

    client_data_dir = VAR_DIR / "data" / client_slug
    client_data_dir.mkdir(parents=True, exist_ok=True)

    loss_csv = client_data_dir / "loss_reasons.csv"
    statuses_csv = client_data_dir / "pipelines_statuses_dim.csv"

    log("Контекст клиента (dims):")
    log(f"- client_slug={client_slug}")
    log(f"- client_id={client_id}")
    log(f"- loss_csv={loss_csv}")
    log(f"- statuses_csv={statuses_csv}")

    # 1) Выгрузка причин потерь
    run_step(
        ["scripts/export_loss_reasons.py", "--client-slug", client_slug, "--out", str(loss_csv)],
        "Справочники: шаг 1/4 — выгрузка причин потерь из amoCRM",
        log,
    )
    loss_required_cols = [
        "client_id",
        "client_slug",
        "loss_reason_id",
        "loss_reason_name",
        "created_at",
        "updated_at",
        "sort",
    ]
    loss_rows = count_csv_rows(loss_csv, log, loss_required_cols)
    if loss_rows < 0:
        raise RuntimeError("Валидация CSV loss_reasons не пройдена, загрузка в ClickHouse отменена.")

    # 2) Загрузка причин потерь в ClickHouse
    run_step(
        [
            "scripts/load_loss_reasons_dim_to_clickhouse.py",
            "--client-slug",
            client_slug,
            "--csv-path",
            str(loss_csv),
        ],
        "Справочники: шаг 2/4 — загрузка loss_reasons в ClickHouse",
        log,
    )

    # 3) Выгрузка статусов по пайплайнам
    run_step(
        ["scripts/amocrm_get_statuses_dim.py", "--client-slug", client_slug, "--out", str(statuses_csv)],
        "Справочники: шаг 3/4 — выгрузка статусов по пайплайнам из amoCRM",
        log,
    )
    statuses_required_cols = [
        "client_id",
        "client_slug",
        "pipeline_id",
        "pipeline_name",
        "status_id",
        "status_name",
        "sort",
        "is_final",
        "is_won",
        "is_lost",
        "updated_at",
    ]
    statuses_rows = count_csv_rows(statuses_csv, log, statuses_required_cols)
    if statuses_rows < 0:
        raise RuntimeError("Валидация CSV статусов не пройдена, загрузка в ClickHouse отменена.")

    # 4) Загрузка статусов в ClickHouse
    run_step(
        [
            "scripts/load_statuses_dim_to_clickhouse.py",
            "--client-slug",
            client_slug,
            "--csv-path",
            str(statuses_csv),
        ],
        "Справочники: шаг 4/4 — загрузка статусов в ClickHouse",
        log,
    )

    log(f"##### Пайплайн справочников для {client_slug} (id={client_id}) завершён успешно #####")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Боевой one-click ETL из amoCRM в ClickHouse (лиды и справочники)."
    )
    parser.add_argument(
        "--client-slug",
        dest="client_slug",
        required=True,
        help="client_slug из scripts/clients_map.py (например, artroyal_detailing)",
    )
    parser.add_argument(
        "--leads",
        action="store_true",
        help="Запустить только пайплайн лидов",
    )
    parser.add_argument(
        "--dims",
        action="store_true",
        help="Запустить только пайплайн справочников (loss_reasons + statuses)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Полный refresh: справочники + лиды (dims → leads)",
    )

    args = parser.parse_args()

    if not (args.leads or args.dims or args.all):
        print("Нужно указать хотя бы один флаг: --leads, --dims или --all.")
        parser.print_help()
        return

    client_slug = args.client_slug
    log, log_path = make_logger(client_slug)
    client_id = get_client_id(client_slug)
    log(f"Старт пайплайна для клиента '{client_slug}' (id={client_id}). Лог-файл: {log_path}")
    log(f"Артефакты будут писаться в: {VAR_DIR / 'data' / client_slug}")
    started_at = time.monotonic()

    try:
        if args.all:
            # Полный refresh в заданном порядке: dims → leads
            run_dims_pipeline(client_slug, log)
            run_leads_pipeline(client_slug, log)
        else:
            if args.dims:
                run_dims_pipeline(client_slug, log)
            if args.leads:
                run_leads_pipeline(client_slug, log)
    except Exception as e:
        log(f"✖ Пайплайн завершился с ошибкой: {e}")
        total = time.monotonic() - started_at
        log(f"Итоговое время выполнения (c ошибкой): {total:.1f} c")
        raise
    else:
        total = time.monotonic() - started_at
        log(f"✔ Пайплайн успешно завершён. Общее время выполнения: {total:.1f} c")


if __name__ == "__main__":
    main()
