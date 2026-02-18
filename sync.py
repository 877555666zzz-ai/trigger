import os
import re
import sys
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()

SOURCE_SPREADSHEET_ID = os.environ["SOURCE_SPREADSHEET_ID"]
TARGET_SPREADSHEET_ID = os.environ["TARGET_SPREADSHEET_ID"]
SETTINGS_SHEET_NAME = os.getenv("SETTINGS_SHEET_NAME", "Settings")

WORK_START_HOUR = int(os.getenv("WORK_START_HOUR", "10"))
WORK_END_HOUR = int(os.getenv("WORK_END_HOUR", "22"))
TZ = os.getenv("TZ", "Asia/Almaty")

LOCK_FILE = os.getenv("LOCK_FILE", "/tmp/gs_sync.lock")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def now_local() -> datetime:
    return datetime.now(ZoneInfo(TZ))


def in_work_window(dt: datetime) -> bool:
    return WORK_START_HOUR <= dt.hour < WORK_END_HOUR


def current_month_name(dt: datetime) -> str:
    months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    return months[dt.month - 1]


def col_to_a1(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def is_valid_date_cell(v) -> bool:
    if isinstance(v, str):
        return bool(re.match(r"^\d{2}\.\d{2}\.\d{4}$", v.strip()))
    return False


def is_technical_word(val) -> bool:
    v = str(val).lower().strip()
    forbidden = {"итого", "сотрудники", "s", "m", "дата", "менеджер", "ф.и.о", "оферты"}
    return v in forbidden or v == ""


def safe_num(x):
    if x in ("", None):
        return 0
    if isinstance(x, (int, float)):
        return x
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return 0


def get_service():
    """
    Railway: храним JSON сервис-аккаунта в переменной окружения GCP_SA_JSON (полный текст json).
    Local: можно использовать файл через GOOGLE_APPLICATION_CREDENTIALS.
    """
    sa_json = os.getenv("GCP_SA_JSON")
    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)

    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def read_values(service, spreadsheet_id: str, a1_range: str):
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=a1_range,
        valueRenderOption="UNFORMATTED_VALUE",
        dateTimeRenderOption="FORMATTED_STRING",
    ).execute()
    return resp.get("values", [])


def clear_sheet(service, spreadsheet_id: str, sheet_name: str):
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=sheet_name,
        body={},
    ).execute()


def write_values(service, spreadsheet_id: str, a1_range: str, values):
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=a1_range,
        valueInputOption="RAW",
        body={"values": values},
    ).execute()


def get_spreadsheet_metadata(service, spreadsheet_id: str):
    return service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()


def ensure_sheet_exists(service, spreadsheet_id: str, sheet_name: str):
    meta = get_spreadsheet_metadata(service, spreadsheet_id)
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == sheet_name:
            return s["properties"]["sheetId"]

    req = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
    resp = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=req
    ).execute()
    return resp["replies"][0]["addSheet"]["properties"]["sheetId"]


def sheet_last_row_estimate(service, spreadsheet_id: str, sheet_name: str) -> int:
    values = read_values(service, spreadsheet_id, f"{sheet_name}!A:A")
    return len(values)


def get_settings_sheet_names(service):
    values = read_values(service, TARGET_SPREADSHEET_ID, f"{SETTINGS_SHEET_NAME}!A2:A")
    names = []
    for r in values:
        if not r:
            continue
        name = str(r[0]).strip()
        if name:
            names.append(name)
    return names


def find_header_row_and_cols(data):
    info = {
        "found": False,
        "rowIdx": -1,
        "managerCol": -1,
        "timeCols": [],
        "totalCol": -1,
        "warmCol": -1,
        "warmOffCol": -1,
        "convCol": -1,
    }

    for r in range(min(10, len(data))):
        row = data[r]
        has_time = False
        first_time_col = -1

        for c in range(len(row)):
            val = str(row[c]).lower()

            if (":" in val) and ("-" in val) and re.search(r"\d", val):
                info["timeCols"].append(c)
                if not has_time:
                    has_time = True
                    first_time_col = c

            if "итого оферт" in val:
                info["totalCol"] = c
            if ("zvonobot" in val) or ("тотал теплых" in val) or ("теплых лидов" in val):
                info["warmCol"] = c
            if ("офферт из теплых" in val) or ("из теплых" in val) or ("от quanta" in val):
                info["warmOffCol"] = c
            if "конверси" in val:
                info["convCol"] = c

        if has_time:
            info["found"] = True
            info["rowIdx"] = r
            info["managerCol"] = max(0, first_time_col - 1)
            break

    return info


def has_data(row, info) -> bool:
    total_val = safe_num(row[info["totalCol"]]) if info["totalCol"] > -1 and info["totalCol"] < len(row) else 0
    warm_val = safe_num(row[info["warmCol"]]) if info["warmCol"] > -1 and info["warmCol"] < len(row) else 0
    return (isinstance(total_val, (int, float)) and total_val > 0) or (isinstance(warm_val, (int, float)) and warm_val > 0)


def get_val(row, idx):
    if idx <= -1 or idx >= len(row):
        return 0
    return 0 if row[idx] in ("", None) else row[idx]


def build_db_result(source_values):
    data = source_values
    header = find_header_row_and_cols(data)
    if not header["found"]:
        return None, "Не удалось найти заголовки времени в листе"

    result = [[
        "Date",
        "Manager",
        "Time_Interval",
        "Offers",
        "Total_Day_Offers",
        "Warm_Leads_Given",
        "Offers_From_Warm",
        "Conversion_Rate",
    ]]

    current_date = None
    manager_col = header["managerCol"]

    for i in range(header["rowIdx"] + 1, len(data)):
        row = data[i]
        cell_val = row[manager_col] if manager_col < len(row) else ""
        is_date = is_valid_date_cell(cell_val)

        if is_date:
            current_date = cell_val

        row_owner = None

        if is_date and has_data(row, header):
            row_owner = "DEPARTMENT_TOTAL"
        elif (not str(cell_val).strip()) and current_date and has_data(row, header):
            row_owner = "DEPARTMENT_TOTAL"
        elif str(cell_val).strip() and (not is_date) and (not is_technical_word(cell_val)):
            row_owner = str(cell_val).strip()

        if row_owner and current_date:
            for time_col_idx in header["timeCols"]:
                time_label = data[header["rowIdx"]][time_col_idx] if time_col_idx < len(data[header["rowIdx"]]) else ""
                offers = row[time_col_idx] if time_col_idx < len(row) and row[time_col_idx] not in ("", None) else 0

                total = get_val(row, header["totalCol"])
                warm_given = get_val(row, header["warmCol"])
                warm_off = get_val(row, header["warmOffCol"])
                conv = get_val(row, header["convCol"])

                if row_owner == "DEPARTMENT_TOTAL":
                    if time_col_idx == header["timeCols"][0]:
                        result.append([current_date, row_owner, "ALL_DAY", offers, total, warm_given, warm_off, conv])
                else:
                    result.append([current_date, row_owner, str(time_label), offers, total, warm_given, warm_off, conv])

    return result, None


def should_sync_sheet(service, sheet_name: str, current_month: str) -> bool:
    db_name = f"DB_{sheet_name}"
    meta = get_spreadsheet_metadata(service, TARGET_SPREADSHEET_ID)
    exists = any(s["properties"]["title"] == db_name for s in meta.get("sheets", []))
    if not exists:
        return True
    if current_month in sheet_name:
        return True
    lr = sheet_last_row_estimate(service, TARGET_SPREADSHEET_ID, db_name)
    return lr <= 1


def sync_one(service, sheet_name: str):
    db_name = f"DB_{sheet_name}"

    source_values = read_values(service, SOURCE_SPREADSHEET_ID, sheet_name)
    if not source_values:
        print(f"[SKIP] SOURCE лист не найден/пустой: {sheet_name}")
        return

    result, err = build_db_result(source_values)
    if err:
        print(f"[SKIP] {sheet_name}: {err}")
        return

    ensure_sheet_exists(service, TARGET_SPREADSHEET_ID, db_name)

    rows = len(result)
    cols = len(result[0])
    end_a1 = f"{col_to_a1(cols)}{rows}"
    write_range = f"{db_name}!A1:{end_a1}"

    clear_sheet(service, TARGET_SPREADSHEET_ID, db_name)
    write_values(service, TARGET_SPREADSHEET_ID, write_range, result)

    print(f"[OK] SYNC: {sheet_name} -> {db_name} ({rows}x{cols})")


def acquire_lock(path: str) -> bool:
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        return True
    except FileExistsError:
        return False


def release_lock(path: str):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def main():
    dt = now_local()
    if not in_work_window(dt):
        print(f"[INFO] Вне рабочего времени ({WORK_START_HOUR}:00-{WORK_END_HOUR}:00). Пропуск.")
        return 0

    if not acquire_lock(LOCK_FILE):
        print("[INFO] Уже запущено (lock существует). Пропуск.")
        return 0

    try:
        service = get_service()
        sheets_to_sync = get_settings_sheet_names(service)

        if not sheets_to_sync:
            print("[WARN] Settings пустой: добавь названия листов в Settings!A2:A")
            return 0

        cm = current_month_name(dt)
        for name in sheets_to_sync:
            if should_sync_sheet(service, name, cm):
                print(f"[INFO] SYNC: {name} -> DB_{name}")
                sync_one(service, name)
            else:
                print(f"[INFO] SKIP(no need): {name}")

        return 0

    except HttpError as e:
        print("[ERROR] Google API:", e)
        return 2
    except Exception as e:
        print("[ERROR]", e)
        return 1
    finally:
        release_lock(LOCK_FILE)


if __name__ == "__main__":
    sys.exit(main())