import pandas as pd
import polars as pl
import requests
import io
import gspread
import os
import json
import time
from datetime import datetime
from google.oauth2 import service_account
from gspread_dataframe import get_as_dataframe
import pytz
from collections import defaultdict

SHEET_CONFIG_NAME = "luu_cau_hinh" 
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_AUTO_LOG_NAME = "log_chay_auto_github"

COL_LINK_SRC = "Link file nguồn"
COL_LABEL_SRC = "Sheet nguồn"
COL_MONTH_SRC = "Tháng chốt"

def get_creds():
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT")
    if not creds_json: return None
    return service_account.Credentials.from_service_account_info(
        json.loads(creds_json), 
        scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    )

def extract_id(url):
    if url and "docs.google.com" in str(url):
        try: return url.split("/d/")[1].split("/")[0]
        except: return None
    return None

def write_auto_log(creds, history_sheet_id, status, message):
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(history_sheet_id)
        try: wks = sh.worksheet(SHEET_AUTO_LOG_NAME)
        except: 
            wks = sh.add_worksheet(SHEET_AUTO_LOG_NAME, rows=1000, cols=4)
            wks.append_row(["Thời gian (VN)", "Trạng thái", "Chi tiết", "Ghi chú"])
        tz = pytz.timezone('Asia/Ho_Chi_Minh')
        wks.append_row([datetime.now(tz).strftime("%d/%m/%Y %H:%M:%S"), status, message, "GitHub Action"])
    except: pass

def check_is_run_time(creds, history_sheet_id):
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(history_sheet_id)
        try: wks = sh.worksheet("sys_config")
        except: return datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).hour == 8

        records = wks.get_all_values()
        conf = {r[0]: r[1] for r in records if len(r) > 1}
        
        scheduled_hour = int(conf.get("run_hour", "8"))
        run_freq = conf.get("run_freq", "1 ngày/1 lần")
        
        tz_vn = pytz.timezone('Asia/Ho_Chi_Minh')
        now_vn = datetime.now(tz_vn)
        
        if now_vn.hour != scheduled_hour: return False

        if run_freq == "1 ngày/1 lần": return True
        elif run_freq == "1 tuần/1 lần" and now_vn.weekday() == 0: return True
        elif run_freq == "1 tháng/1 lần" and now_vn.day == 1: return True
        
        return False
    except: return False

def fetch_single_csv_safe(row_config, token):
    link_src = str(row_config.get('Link dữ liệu lấy dữ liệu', ''))
    source_label = str(row_config.get('Tên sheet nguồn dữ liệu gốc', '')).strip()
    month_val = str(row_config.get('Tháng', ''))
    sheet_id = extract_id(link_src)
    
    if not sheet_id: return None, "Link lỗi"

    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            df = pl.read_csv(io.BytesIO(response.content), infer_schema_length=0)
            
            cols_drop = [c for c in df.columns if c in [COL_LINK_SRC, COL_LABEL_SRC, COL_MONTH_SRC]]
            if cols_drop: df = df.drop(cols_drop)

            df = df.with_columns([
                pl.lit(link_src).cast(pl.Utf8).alias(COL_LINK_SRC),
                pl.lit(source_label).cast(pl.Utf8).alias(COL_LABEL_SRC),
                pl.lit(month_val).cast(pl.Utf8).alias(COL_MONTH_SRC)
            ])
            return df, "Thành công"
        return None, "Lỗi HTTP"
    except: return None, "Lỗi Exception"

def smart_update_safe(df_new_updates, target_link, target_sheet_name, creds, links_to_remove):
    try:
        gc = gspread.authorize(creds)
        target_id = extract_id(target_link)
        sh = gc.open_by_key(target_id)
        
        real_sheet_name = str(target_sheet_name).strip()
        if not real_sheet_name: real_sheet_name = "Tong_Hop_Data"
        
        try: wks = sh.worksheet(real_sheet_name)
        except: wks = sh.add_worksheet(title=real_sheet_name, rows=1000, cols=20)
        
        token = creds.token 
        if not token:
            import google.auth.transport.requests
            auth_req = google.auth.transport.requests.Request()
            creds.refresh(auth_req)
            token = creds.token

        export_url = f"https://docs.google.com/spreadsheets/d/{target_id}/export?format=csv&gid={wks.id}"
        headers = {'Authorization': f'Bearer {token}'}
        
        df_current = pl.DataFrame()
        try:
            r = requests.get(export_url, headers=headers)
            if r.status_code == 200:
                df_current = pl.read_csv(io.BytesIO(r.content), infer_schema_length=0)
                rename_map = {}
                for col in df_current.columns:
                    if col.strip() in ["Link Nguồn", "Link URL nguồn"]: rename_map[col] = COL_LINK_SRC
                if rename_map: df_current = df_current.rename(rename_map)
        except: pass

        if not df_current.is_empty():
            if COL_LINK_SRC in df_current.columns:
                df_keep = df_current.filter(~pl.col(COL_LINK_SRC).is_in(links_to_remove))
            else: df_keep = df_current 
        else: df_keep = pl.DataFrame()

        if not df_new_updates.is_empty():
            df_final = pl.concat([df_keep, df_new_updates], how="diagonal")
        else: df_final = df_keep

        all_cols = df_final.columns
        data_cols = [c for c in all_cols if c not in [COL_LINK_SRC, COL_LABEL_SRC, COL_MONTH_SRC]]
        final_order = data_cols + [COL_LINK_SRC, COL_LABEL_SRC, COL_MONTH_SRC]
        final_cols = [c for c in final_order if c in df_final.columns]
        df_final = df_final.select(final_cols)

        pdf = df_final.to_pandas().fillna('')
        wks.clear()
        wks.update([pdf.columns.tolist()] + pdf.values.tolist())
        return True, f"Sheet '{real_sheet_name}': OK {len(pdf)} dòng"
    except Exception as e: return False, str(e)

def run_auto_job():
    print("🚀 Auto Job...")
    creds = get_creds()
    if not creds: return

    HISTORY_SHEET_ID = os.environ.get("HISTORY_SHEET_ID")
    if not check_is_run_time(creds, HISTORY_SHEET_ID): return

    write_auto_log(creds, HISTORY_SHEET_ID, "ĐANG CHẠY", "Bắt đầu...")
    
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(HISTORY_SHEET_ID)
    wks_config = sh.worksheet(SHEET_CONFIG_NAME)
    df_config = get_as_dataframe(wks_config, evaluate_formulas=True, dtype=str)
    
    # 1. Tìm dòng có Trạng thái = "Chưa cập nhật"
    rows_to_run = []
    if 'Trạng thái' in df_config.columns:
        # Chuẩn hóa giá trị
        df_config['Trạng thái'] = df_config['Trạng thái'].apply(lambda x: "Đã cập nhật" if str(x).strip() in ["Đã cập nhật", "Đã chốt", "TRUE"] else "Chưa cập nhật")
        
        rows_to_run = df_config[df_config['Trạng thái'] == "Chưa cập nhật"].to_dict('records')

    if not rows_to_run:
        write_auto_log(creds, HISTORY_SHEET_ID, "BỎ QUA", "Không có dòng nào 'Chưa cập nhật'.")
        return

    import google.auth.transport.requests
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)
    token = creds.token

    grouped_tasks = defaultdict(list)
    for row in rows_to_run:
        t_link = row.get('Link dữ liệu đích', '')
        t_sheet = str(row.get('Tên sheet dữ liệu đích', '')).strip()
        if not t_sheet: t_sheet = "Tong_Hop_Data"
        grouped_tasks[(t_link, t_sheet)].append(row)

    final_msgs = []
    all_success = True

    for (target_link, target_sheet), group_rows in grouped_tasks.items():
        results = []
        links_remove = []
        for row in group_rows:
            df, msg = fetch_single_csv_safe(row, token)
            if df is not None:
                results.append(df)
                links_remove.append(row.get('Link dữ liệu lấy dữ liệu'))
        
        if results:
            df_new = pl.concat(results, how="vertical", rechunk=True)
            success, msg = smart_update_safe(df_new, target_link, target_sheet, creds, links_remove)
            final_msgs.append(msg)
            if not success: all_success = False
        else:
            final_msgs.append(f"Sheet '{target_sheet}': Thất bại.")
            all_success = False

    msg_sum = " | ".join(final_msgs)
    if all_success:
        # 2. Chạy xong chuyển thành "Đã cập nhật"
        if 'Trạng thái' in df_config.columns:
            df_config.loc[df_config['Trạng thái'] == "Chưa cập nhật", 'Hành động'] = "Đã xong (Auto)"
            df_config.loc[df_config['Trạng thái'] == "Chưa cập nhật", 'Trạng thái'] = "Đã cập nhật"
        
        wks_config.clear()
        wks_config.update([df_config.columns.tolist()] + df_config.fillna('').values.tolist())
        write_auto_log(creds, HISTORY_SHEET_ID, "THÀNH CÔNG", msg_sum)
    else:
        write_auto_log(creds, HISTORY_SHEET_ID, "LỖI", msg_sum)

if __name__ == "__main__":
    run_auto_job()
