import pandas as pd
import polars as pl
import requests
import io
import gspread
import os
import json
import time
from datetime import datetime, timedelta
from google.oauth2 import service_account
from gspread_dataframe import get_as_dataframe
import pytz
from collections import defaultdict

SHEET_CONFIG_NAME = "luu_cau_hinh" 
SHEET_AUTO_LOG_NAME = "log_chay_auto_github"
SHEET_LOCK_NAME = "sys_lock"
SHEET_SYS_CONFIG = "sys_config"

COL_LINK_SRC = "Link file nguồn"
COL_LABEL_SRC = "Sheet nguồn"
COL_MONTH_SRC = "Tháng chốt"
COL_BLOCK_NAME = "Block_Name"
DEFAULT_BLOCK_NAME = "Block_Mac_Dinh"

def get_creds():
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT")
    if not creds_json: return None
    try:
        creds_info = json.loads(creds_json)
        if isinstance(creds_info, str): creds_info = json.loads(creds_info)
        return service_account.Credentials.from_service_account_info(
            creds_info, 
            scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        )
    except: return None

def extract_id(url):
    if url and "docs.google.com" in str(url):
        try: return url.split("/d/")[1].split("/")[0]
        except: return None
    return None

def write_auto_log(creds, history_sheet_id, block_name, status, message):
    try:
        gc = gspread.authorize(creds); sh = gc.open_by_key(history_sheet_id)
        try: wks = sh.worksheet(SHEET_AUTO_LOG_NAME)
        except: wks = sh.add_worksheet(SHEET_AUTO_LOG_NAME, rows=1000, cols=5); wks.append_row(["Thời gian (VN)", "Block", "Trạng thái", "Chi tiết", "Ghi chú"])
        tz = pytz.timezone('Asia/Ho_Chi_Minh'); wks.append_row([datetime.now(tz).strftime("%d/%m/%Y %H:%M:%S"), block_name, status, message, "GitHub Action"])
    except: pass

def get_system_lock(creds, history_id):
    try:
        gc = gspread.authorize(creds); sh = gc.open_by_key(history_id)
        try: wks = sh.worksheet(SHEET_LOCK_NAME)
        except: return False, "", ""
        val = wks.cell(2, 1).value; user = wks.cell(2, 2).value; time_str = wks.cell(2, 3).value
        if val == "TRUE":
            try:
                if (datetime.now() - datetime.strptime(time_str, "%d/%m/%Y %H:%M:%S")).total_seconds() > 1800: return False, "", ""
            except: pass
            return True, user, time_str
        return False, "", ""
    except: return False, "", ""

def set_system_lock(creds, history_id, user_id, lock=True):
    try:
        gc = gspread.authorize(creds); sh = gc.open_by_key(history_id)
        try: wks = sh.worksheet(SHEET_LOCK_NAME)
        except: wks = sh.add_worksheet(SHEET_LOCK_NAME, rows=10, cols=5)
        now_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        wks.update("A2:C2", [["TRUE", user_id, now_str]] if lock else [["FALSE", "", ""]])
    except: pass

def get_blocks_to_run(creds, history_sheet_id):
    try:
        gc = gspread.authorize(creds); sh = gc.open_by_key(history_sheet_id)
        try: wks = sh.worksheet(SHEET_SYS_CONFIG)
        except: return [] 
        df_sys = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if 'Block_Name' not in df_sys.columns: return []
        
        # LOGIC TỰ ĐỘNG THÊM KHỐI MẶC ĐỊNH VÀO LỊCH CHẠY NẾU CHƯA CÓ
        if DEFAULT_BLOCK_NAME not in df_sys['Block_Name'].values:
             # Nếu chưa có trong config hệ thống, ta giả lập nó cần chạy lúc 8h sáng hàng ngày (default cũ)
             new_row = pd.DataFrame([{'Block_Name': DEFAULT_BLOCK_NAME, 'Run_Hour': '8', 'Run_Freq': 'Hàng ngày'}])
             df_sys = pd.concat([df_sys, new_row], ignore_index=True)

        blocks_run = []; tz_vn = pytz.timezone('Asia/Ho_Chi_Minh'); now_vn = datetime.now(tz_vn)
        for _, row in df_sys.iterrows():
            b_name = row.get('Block_Name')
            b_hour = int(row.get('Run_Hour', -1)); b_freq = row.get('Run_Freq', 'Hàng ngày')
            if now_vn.hour == b_hour:
                is_run = False
                if b_freq == "Hàng ngày": is_run = True
                elif b_freq == "Hàng tuần" and now_vn.weekday() == 0: is_run = True 
                elif b_freq == "Hàng tháng" and now_vn.day == 1: is_run = True 
                if is_run: blocks_run.append(b_name)
        return blocks_run
    except: return []

def fetch_single_csv_safe(row_config, token):
    link_src = str(row_config.get('Link dữ liệu lấy dữ liệu', '')); source_label = str(row_config.get('Tên sheet nguồn dữ liệu gốc', '')).strip(); month_val = str(row_config.get('Tháng', '')); sheet_id = extract_id(link_src)
    if not sheet_id: return None, "Link lỗi"
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"; headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            df = pl.read_csv(io.BytesIO(response.content), infer_schema_length=0)
            df = df.with_columns([pl.lit(link_src).cast(pl.Utf8).alias(COL_LINK_SRC), pl.lit(source_label).cast(pl.Utf8).alias(COL_LABEL_SRC), pl.lit(month_val).cast(pl.Utf8).alias(COL_MONTH_SRC)])
            return df, "Thành công"
        return None, "Lỗi HTTP"
    except: return None, "Lỗi Exception"

def smart_update_safe(df_new_updates, target_link, target_sheet_name, creds, links_to_remove):
    try:
        gc = gspread.authorize(creds); target_id = extract_id(target_link)
        if not target_id: return False, "Link đích lỗi"
        sh = gc.open_by_key(target_id); real_sheet_name = str(target_sheet_name).strip() if str(target_sheet_name).strip() else "Tong_Hop_Data"
        try: wks = sh.worksheet(real_sheet_name)
        except: wks = sh.add_worksheet(title=real_sheet_name, rows=1000, cols=20)
        
        token = creds.token 
        if not token: import google.auth.transport.requests; auth_req = google.auth.transport.requests.Request(); creds.refresh(auth_req); token = creds.token
        existing_headers = []
        try: existing_headers = wks.row_values(1)
        except: pass
        if existing_headers:
            try: link_col_idx = existing_headers.index(COL_LINK_SRC) + 1
            except ValueError: link_col_idx = None
            if link_col_idx:
                col_values = wks.col_values(link_col_idx); rows_to_delete = []
                for i, val in enumerate(col_values):
                    if val in links_to_remove: rows_to_delete.append(i + 1)
                if rows_to_delete:
                    rows_to_delete.sort(); ranges = []; start = rows_to_delete[0]; end = start
                    for r in rows_to_delete[1:]:
                        if r == end + 1: end = r
                        else: ranges.append((start, end)); start = r; end = r
                    ranges.append((start, end)); delete_reqs = []
                    for start, end in reversed(ranges): delete_reqs.append({"deleteDimension": {"range": {"sheetId": wks.id, "dimension": "ROWS", "startIndex": start - 1, "endIndex": end}}})
                    if delete_reqs: sh.batch_update({'requests': delete_reqs}); time.sleep(1)

        if not df_new_updates.is_empty():
            pdf = df_new_updates.to_pandas().fillna(''); new_cols = pdf.columns.tolist()
            if not existing_headers: wks.append_row(new_cols); final_headers = new_cols
            else:
                missing = [c for c in new_cols if c not in existing_headers]
                if missing: wks.resize(cols=len(existing_headers) + len(missing)); final_headers = existing_headers + missing; wks.update(range_name="A1", values=[final_headers])
                else: final_headers = existing_headers
            pdf_aligned = pdf.reindex(columns=final_headers, fill_value=""); data_values = pdf_aligned.values.tolist(); BATCH_SIZE = 5000
            for i in range(0, len(data_values), BATCH_SIZE): chunk = data_values[i : i + BATCH_SIZE]; wks.append_rows(chunk); time.sleep(1)
            return True, f"OK +{len(data_values)} dòng"
        return True, "OK (Cleaned)"
    except Exception as e: return False, str(e)

def run_auto_job():
    print("🚀 Auto Job Multi-Block...")
    creds = get_creds(); HISTORY_SHEET_ID = os.environ.get("HISTORY_SHEET_ID")
    if not creds: return
    blocks_to_run = get_blocks_to_run(creds, HISTORY_SHEET_ID)
    if not blocks_to_run: print("Không có khối nào hẹn giờ."); return
    is_locked, user, _ = get_system_lock(creds, HISTORY_SHEET_ID)
    if is_locked: write_auto_log(creds, HISTORY_SHEET_ID, "ALL", "BỎ QUA", f"Lock bởi {user}"); return
    set_system_lock(creds, HISTORY_SHEET_ID, "AUTO_BOT", lock=True)
    try: 
        gc = gspread.authorize(creds); sh = gc.open_by_key(HISTORY_SHEET_ID)
        wks_config = sh.worksheet(SHEET_CONFIG_NAME); df_config = get_as_dataframe(wks_config, evaluate_formulas=True, dtype=str)
        
        # --- QUAN TRỌNG: Gán Default Block cho dữ liệu cũ ---
        if 'Block_Name' not in df_config.columns: df_config['Block_Name'] = DEFAULT_BLOCK_NAME
        else: df_config['Block_Name'] = df_config['Block_Name'].fillna(DEFAULT_BLOCK_NAME).replace('', DEFAULT_BLOCK_NAME)
        
        if 'Trạng thái' in df_config.columns: df_config['Trạng thái'] = df_config['Trạng thái'].apply(lambda x: "Đã chốt" if str(x).strip() in ["Đã chốt", "Đã cập nhật", "TRUE"] else "Chưa chốt & đang cập nhật")
        
        import google.auth.transport.requests; auth_req = google.auth.transport.requests.Request(); creds.refresh(auth_req); token = creds.token
        for b_name in blocks_to_run:
            print(f"--- Chạy khối: {b_name} ---")
            write_auto_log(creds, HISTORY_SHEET_ID, b_name, "ĐANG CHẠY", "Bắt đầu...")
            rows_to_run = df_config[(df_config['Block_Name'] == b_name) & (df_config['Trạng thái'] == "Chưa chốt & đang cập nhật")].to_dict('records')
            if not rows_to_run: write_auto_log(creds, HISTORY_SHEET_ID, b_name, "XONG", "Không có dòng chờ xử lý."); continue

            grouped_tasks = defaultdict(list)
            for row in rows_to_run:
                t_link = row.get('Link dữ liệu đích', ''); t_sheet = str(row.get('Tên sheet dữ liệu đích', '')).strip()
                if not t_sheet: t_sheet = "Tong_Hop_Data"
                grouped_tasks[(t_link, t_sheet)].append(row)

            final_msgs = []; all_block_success = True
            for (target_link, target_sheet), group_rows in grouped_tasks.items():
                results = []; links_remove = []
                for row in group_rows:
                    df, msg = fetch_single_csv_safe(row, token)
                    if df is not None: results.append(df); links_remove.append(row.get('Link dữ liệu lấy dữ liệu'))
                if results or links_remove:
                    if results: df_new = pl.concat(results, how="vertical", rechunk=True)
                    else: df_new = pl.DataFrame()
                    success, msg = smart_update_safe(df_new, target_link, target_sheet, creds, links_remove); final_msgs.append(msg)
                    if not success: all_block_success = False
                else: final_msgs.append(f"Sheet '{target_sheet}': Thất bại."); all_block_success = False

            msg_sum = " | ".join(final_msgs)
            if all_block_success:
                mask = (df_config['Block_Name'] == b_name) & (df_config['Trạng thái'] == "Chưa chốt & đang cập nhật")
                df_config.loc[mask, 'Kết quả'] = msg_sum; write_auto_log(creds, HISTORY_SHEET_ID, b_name, "THÀNH CÔNG", msg_sum)
            else: write_auto_log(creds, HISTORY_SHEET_ID, b_name, "CÓ LỖI", msg_sum)
        wks_config.clear(); wks_config.update([df_config.columns.tolist()] + df_config.fillna('').values.tolist())
    finally: set_system_lock(creds, HISTORY_SHEET_ID, "AUTO_BOT", lock=False)

if __name__ == "__main__":
    run_auto_job()
