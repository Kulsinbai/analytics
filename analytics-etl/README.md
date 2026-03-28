## ArtRoyal Detailing — ETL по amoCRM

Этот проект содержит набор скриптов для полного контура ETL по данным amoCRM для клиента **ArtRoyal Detailing**:

- **OAuth и клиент amoCRM**
- **Выгрузка лидов и справочников**
- **Обогащение/нормализация JSON**
- **Формирование плоских CSV для ClickHouse/DataLens**
- **Загрузка фактов и справочников в ClickHouse**

---

## Структура проекта

```
ArtRoyalDetailing
│
├── scripts
│   ├── run_pipeline.py                # основной оркестратор ETL
│   ├── amocrm_export_leads.py         # выгрузка лидов из amoCRM
│   ├── leads_json_to_datalens_csv.py  # преобразование JSON → CSV
│   ├── load_leads_csv_to_clickhouse.py
│   ├── export_loss_reasons.py
│   ├── amocrm_get_statuses_dim.py
│   ├── manual_daily_report.py         # генерация текста отчёта клиенту
│
├── data                               # промежуточные JSON и CSV
├── logs                               # логи пайплайна
├── secrets                            # OAuth токены amoCRM
├── requirements.txt
└── README.md
```

---

## Установка зависимостей

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

---

## Ежедневная работа

**Обновить лиды:**

```bash
python scripts/run_pipeline.py --client-slug artroyal_detailing --leads
```

**Полный refresh (справочники + лиды):**

```bash
python scripts/run_pipeline.py --client-slug artroyal_detailing --all
```

**Сформировать отчёт клиенту:**

```bash
python scripts/manual_daily_report.py
```

---

## Боевой запуск

### Боевой one‑click запуск пайплайна

Для полного обновления данных из amoCRM в ClickHouse (full refresh) есть единый оркестратор:

- **Скрипт**: `scripts/run_pipeline.py`
- **Параметры (флаги запуска)**:
  - `--client-slug <slug>` — **обязательный** параметр, `client_slug` из `scripts/clients_map.py` (например, `artroyal_detailing`).
  - `--all` — запустить полный пайплайн (full refresh) в порядке:  
    **справочники (dims) → лиды (leads)**:
    - loss_reasons dim
    - statuses dim
    - leads fact
  - `--leads` — запустить только пайплайн **лидов** (amoCRM → JSON → CSV → ClickHouse).
  - `--dims` — запустить только пайплайн **справочников** (причины потерь + статусы).

Если не указано ни одного из флагов `--leads/--dims/--all`, скрипт покажет `help` и ничего не выполнит.

Примеры:

- **Полный refresh** (dims → leads) для ArtRoyal Detailing:

  ```bash
  python scripts/run_pipeline.py --client-slug artroyal_detailing --all
  ```

- Только лиды:

  ```bash
  python scripts/run_pipeline.py --client-slug artroyal_detailing --leads
  ```

- Только справочники (loss_reasons + statuses):

  ```bash
  python scripts/run_pipeline.py --client-slug artroyal_detailing --dims
  ```

Во время выполнения пайплайна скрипт:

- логирует старт/финиш каждого шага;
- считает и логирует:
  - сколько лидов выгружено из amoCRM (JSON);
  - сколько строк в промежуточных CSV;
  - сколько строк загружено в ClickHouse (по логам шагов загрузки);
- пишет логи в консоль и в файл вида `logs/pipeline_<client_slug>_<дата>.log`.

### 1. Подготовка OAuth (разово при первом подключении или при перевыпуске токенов)

- **Шаг 1.1. Получить authorization code**
  - **Скрипт**: `scripts/oauth_get_code.py`
  - **Вход**:
    - `secrets/amocrm_app.json` — содержит как минимум `auth_domain`, `client_id`, `redirect_uri`
  - **Выход**:
    - `secrets/auth_code.txt` — сохранённый `authorization_code`

- **Шаг 1.2. Однократно обменять code на access/refresh токены**
  - **Скрипт**: `scripts/oauth_exchange_tokens.py`
  - **Вход**:
    - `secrets/amocrm_app.json` — `account_domain`, `client_id`, `client_secret`, `redirect_uri`
    - `secrets/auth_code.txt` — код из предыдущего шага
  - **Выход**:
    - `secrets/amocrm_tokens.json` — `access_token`, `refresh_token`, `expires_at`, `token_type`

После этого все скрипты, использующие `scripts/amocrm_client.py`, автоматически обновляют `access_token` по `refresh_token` через `get_valid_access_token()`. Запускать `scripts/oauth_exchange_tokens.py` в ежедневных/регулярных джобах **не нужно** — он нужен только при первом подключении или полном перевыпуске токенов/интеграции.

---

## ETL по лидам

### 2. Выгрузка лидов из amoCRM

- **Шаг 2.1. Выгрузить лиды**
  - **Скрипт**: `scripts/amocrm_export_leads.py`
  - **Вход**:
    - `secrets/amocrm_app.json`
    - `secrets/amocrm_tokens.json`
  - **Выход**:
    - `data/add_leads_crm.json` — «сырые» лиды из amoCRM (без клиентских полей)

- **Шаг 2.2. Добавить client_id / client_slug**
  - **Скрипт**: `scripts/add_client_id.py`
  - **Вход**:
    - `data/add_leads_crm.json`
  - **Выход**:
    - `data/add_leads_crm_with_client.json` — те же лиды, дополненные `client_id` и `client_slug`

> При необходимости можно запустить `scripts/inspect_json.py`, чтобы диагностировать структуру `add_leads_crm_with_client.json`.

### 3. Преобразование лидов в плоский CSV для отчётности (вариант для DataLens/ClickHouse)

Рекомендуемый «боевой» путь — использовать конвейер, который уже готовит поля под отчётность и ClickHouse.

- **Шаг 3.1. Построить плоский CSV с обогащением**
  - **Скрипт**: `scripts/leads_json_to_datalens_csv.py`
  - **Вход**:
    - `data/add_leads_crm_with_client.json`
    - `scripts/clients_map.py` — для маппинга `client_slug → client_id`
  - **Ключевые преобразования**:
    - нормализация текстов (удаление HTML, фиксация кракозябр)
    - нормализация телефонов
    - разбор UTM‑параметров и источников
    - извлечение канала связи, тегов, очищенного имени и т.д.
  - **Выход**:
    - `data/add_leads_crm_flat_datalens.csv` — плоский CSV с расширенным набором полей для отчётов

### 4. Загрузка факта лидов в ClickHouse

- **Шаг 4.1. Загрузить лиды в факт‑таблицу**
  - **Скрипт**: `scripts/load_leads_csv_to_clickhouse.py`
  - **Вход**:
    - `data/add_leads_crm_flat_datalens.csv`
    - параметры подключения к ClickHouse (захардкожены в скрипте:
      `CH_HOST`, `CH_PORT`, `CH_USER`, `CH_PASSWORD`, `CH_DB`, `CH_TABLE`)
  - **Поведение**:
    - выполняет `ALTER TABLE {CH_DB}.{CH_TABLE} DELETE WHERE client_id = 1` (полная перезаливка по клиенту)
    - маппит поля из CSV в схему таблицы (`lead_id`, статусы, даты, UTM, source, channel и т.д.)
  - **Выход**:
    - данные загружены в таблицу ClickHouse `leads_fact` (или другую, указанную в `CH_TABLE`)

---

## ETL по справочникам

### 5. Справочник причин потерь (loss_reasons)

#### 5.1. Выгрузка из amoCRM

- **Шаг 5.1.1. Выгрузить справочник причин потерь**
  - **Скрипт**: `scripts/export_loss_reasons.py`
  - **Вход**:
    - `secrets/amocrm_app.json`
    - `secrets/amocrm_tokens.json`
    - `scripts/clients_map.py` (для `client_id`/`client_slug`)
  - **Выход**:
    - `data/loss_reasons.csv` — CSV со списком причин потерь:
      `client_id`, `client_slug`, `loss_reason_id`, `loss_reason_name`, `created_at`, `updated_at`, `sort`

#### 5.2. Загрузка в ClickHouse

- **Боевой загрузчик**: `scripts/load_loss_reasons_dim_to_clickhouse.py`
  - **Вход**: `data/loss_reasons.csv`
  - **Поведение**: удаляет данные клиента в таблице `default_db.loss_reasons_dim_v2`, затем вставляет строки с полями `client_id`, `client_slug`, `loss_reason_id`, `loss_reason_name`, `created_at`, `updated_at`, `sort`.

Остальные варианты загрузки loss_reasons перенесены в `scripts/_archive`.

---

### 6. Справочник статусов (pipelines/statuses)

#### 6.1. Выгрузка статусов из amoCRM

- **Шаг 6.1.1. Выгрузить статусы по пайплайнам**
  - **Скрипт**: `scripts/amocrm_get_statuses_dim.py`
  - **Вход**:
    - `secrets/amocrm_app.json`
    - `secrets/amocrm_tokens.json`
    - `scripts/clients_map.py`
  - **Выход**:
    - `data/pipelines_statuses_dim.csv` — CSV со статусами:
      `client_id`, `client_slug`, `pipeline_id`, `pipeline_name`,
      `status_id`, `status_name`, `sort`, `is_final`, `is_won`, `is_lost`, `updated_at`

#### 6.2. Загрузка статусов в ClickHouse

- **Боевой загрузчик**: `scripts/load_statuses_dim_to_clickhouse.py`
  - **Вход**: `data/pipelines_statuses_dim.csv`
  - **Поведение**: удаляет данные клиента в таблице `default_db.statuses_dim_v2`, затем вставляет статусы с приведением типов (`is_final`, `is_won`, `is_lost` и т.д.).

---

## Генерация ежедневного отчёта

Скрипт **`scripts/manual_daily_report.py`** формирует готовый текст ежедневного отчёта для клиента на основе данных в ClickHouse (лиды, коммуникации, продажи, потерянные сделки, причины потерь).

**Пример запуска:**

```bash
python scripts/manual_daily_report.py
```

Скрипт не входит в пайплайн `run_pipeline.py` и запускается отдельно после обновления данных.
