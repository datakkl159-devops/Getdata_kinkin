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

# --- CẤU HÌNH ---
SHEET_CONFIG_NAME = "luu_cau_hinh" 
SHEET_AUTO_LOG_NAME = "log_chay_auto_github" # <--- Tên sheet log mới
# 3 Cột quản lý
COL_LINK_SRC = "Link file nguồn"
COL_LABEL_SRC = "Tên nguồn (Nhãn)"
COL_MONTH_SRC = "Tháng"

def get_creds():
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT")
    if not creds_json:
        print("❌ Thiếu biến môi trường GCP_SERVICE_ACCOUNT")
        return None
    creds_dict = json.loads(creds_json)
    return service_account.Credentials.from_service_account_info(
        creds_dict, 
        scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    )

def extract_id(url):
    if url and "docs.google.com" in str(url):
        try: return url.split("/d/")[1].split("/")[0]
        except: return None
    return None

# --- HÀM GHI LOG RIÊNG CHO AUTO ---
def write_auto_log(creds, history_sheet_id, status, message):
    """
    Ghi log vào sheet 'log_chay_auto_github'.
    Tự tạo sheet nếu chưa có.
    """
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(history_sheet_id)
        
        # 1. Thử mở sheet log, nếu chưa có thì tạo mới
        try:
            wks = sh.worksheet(SHEET_AUTO_LOG_NAME)
        except:
            wks = sh.add_worksheet(SHEET_AUTO_LOG_NAME, rows=1000, cols=4)
            # Ghi tiêu đề nếu mới tạo
            wks.append_row(["Thời gian (VN)", "Trạng thái", "Chi tiết", "Ghi chú"])
            
        # 2. Lấy giờ VN
        tz_vn = pytz.timezone('Asia/Ho_Chi_Minh')
        now_str = datetime.now(tz_vn).strftime("%d/%m/%Y %H:%M:%S")
        
        # 3. Ghi log
        wks.append_row([now_str, status, message, "GitHub Action Run"])
        print(f"📝 Đã ghi log: [{status}] {message}")
        
    except Exception as e:
        print(f"❌ Lỗi ghi log sheet: {e}")

# --- KIỂM TRA ĐIỀU KIỆN CHẠY ---
def check_is_run_time(creds, history_sheet_id):
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(history_sheet_id)
        try: wks = sh.worksheet("sys_config")
        except: 
            # Mặc định chạy 8h nếu chưa cấu hình
            return datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).hour == 8

        records = wks.get_all_values()
        conf = {r[0]: r[1] for r in records if len(r) > 1}
        
        scheduled_hour = int(conf.get("run_hour", "8"))
        run_freq = conf.get("run_freq", "1 ngày/1 lần")
        
        tz_vn = pytz.timezone('Asia/Ho_Chi_Minh')
        now_vn = datetime.now(tz_vn)
        current_hour = now_vn.hour
        weekday = now_vn.weekday()
        day_of_month = now_vn.day

        print(f"🕒 Check Time: {now_vn.strftime('%H:%M')} | Config: {run_freq} @ {scheduled_hour}h")

        if current_hour != scheduled_hour:
            return False

        if run_freq == "1 ngày/1 lần":
            return True
        elif run_freq == "1 tuần/1 lần":
            return weekday == 0 # Thứ 2
        elif run_freq == "1 tháng/1 lần":
            return day_of_month == 1 # Mùng 1
        
        return False
            
    except Exception as e:
        print(f"❌ Lỗi check giờ: {e}")
        return False

# --- LOGIC XỬ LÝ DỮ LIỆU ---
def fetch_single_csv_safe(row_config, token):
    link_src = str(row_config.get('Link dữ liệu lấy dữ liệu', ''))
    display_label = str(row_config.get('Tên nguồn (Nhãn)', ''))
    month_val = str(row_config.get('Tháng', ''))
    sheet_id = extract_id(link_src)
    
    if not sheet_id: return None, "Link lỗi"

    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            df = pl.read_csv(io.BytesIO(response.content), infer_schema_length=0)
            
            # Xóa cột quản lý cũ nếu file nguồn vô tình có
            cols_drop = [c for c in df.columns if c in [COL_LINK_SRC, COL_LABEL_SRC, COL_MONTH_SRC]]
            if cols_drop: df = df.drop(cols_drop)

            # Thêm 3 cột quản lý
            df = df.with_columns([
                pl.lit(link_src).cast(pl.Utf8).alias(COL_LINK_SRC),
                pl.lit(display_label).cast(pl.Utf8).alias(COL_LABEL_SRC),
                pl.lit(month_val).cast(pl.Utf8).alias(COL_MONTH_SRC)
            ])
            return df, "Thành công"
        return None, f"Lỗi HTTP {response.status_code}"
    except Exception as e: return None, str(e)

def smart_update_by_link(df_new_updates, target_link, creds, links_to_remove):
    try:
        gc = gspread.authorize(creds)
        target_id = extract_id(target_link)
        sh = gc.open_by_key(target_id)
        try: wks = sh.worksheet("Tong_Hop_Data")
        except: wks = sh.get_worksheet(0)
        
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
                # Chuẩn hóa tên cột Link
                rename_map = {}
                for col in df_current.columns:
                    if col.strip() in ["Link Nguồn", "Link URL nguồn"]: rename_map[col] = COL_LINK_SRC
                if rename_map: df_current = df_current.rename(rename_map)
        except: pass

        # Xóa cũ
        if not df_current.is_empty():
            if COL_LINK_SRC in df_current.columns:
                df_keep = df_current.filter(~pl.col(COL_LINK_SRC).is_in(links_to_remove))
            else:
                df_keep = df_current 
        else:
            df_keep = pl.DataFrame()

        # Gộp mới
        if not df_new_updates.is_empty():
            df_final = pl.concat([df_keep, df_new_updates], how="diagonal")
        else:
            df_final = df_keep

        # Cố định cột quản lý xuống cuối
        all_cols = df_final.columns
        data_cols = [c for c in all_cols if c not in [COL_LINK_SRC, COL_LABEL_SRC, COL_MONTH_SRC]]
        final_order = data_cols + [COL_LINK_SRC, COL_LABEL_SRC, COL_MONTH_SRC]
        final_cols = [c for c in final_order if c in df_final.columns]
        df_final = df_final.select(final_cols)

        pdf = df_final.to_pandas().fillna('')
        data_values = pdf.values.tolist()
        
        wks.clear()
        wks.update([pdf.columns.tolist()] + data_values)
        return True, f"OK. Tổng: {len(pdf)}"
    except Exception as e: return False, str(e)

# --- RUN ---
def run_auto_job():
    print("🚀 Auto Job Wake Up...")
    creds = get_creds()
    if not creds: return

    HISTORY_SHEET_ID = os.environ.get("HISTORY_SHEET_ID")
    
    # Check Time
    if not check_is_run_time(creds, HISTORY_SHEET_ID):
        print("💤 Chưa đến giờ chạy.")
        return

    # --- BẮT ĐẦU CHẠY THÌ GHI LOG NGAY ---
    write_auto_log(creds, HISTORY_SHEET_ID, "ĐANG CHẠY", "Hệ thống bắt đầu quét dữ liệu...")

    print("⚡ Bắt đầu xử lý...")
    
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(HISTORY_SHEET_ID)
    wks_config = sh.worksheet(SHEET_CONFIG_NAME)
    df_config = get_as_dataframe(wks_config, evaluate_formulas=True, dtype=str)
    
    # Logic tìm dòng chạy (hỗ trợ cả cột 'Chọn' và 'Trạng thái')
    rows_to_run = []
    if 'Trạng thái' in df_config.columns:
        # Chuẩn hóa
        df_config['Trạng thái'] = df_config['Trạng thái'].apply(lambda x: "Chưa chốt" if pd.isna(x) or str(x).strip() == "" else str(x))
        rows_to_run = df_config[df_config['Trạng thái'] == "Chưa chốt"].to_dict('records')
    elif 'Chọn' in df_config.columns:
        # Hỗ trợ logic mới (Checkbox)
        rows_to_run = df_config[df_config['Chọn'].str.upper() == "TRUE"].to_dict('records')

    if not rows_to_run:
        msg = "Không có dòng nào được chọn/chưa chốt."
        print(f"✅ {msg}")
        write_auto_log(creds, HISTORY_SHEET_ID, "BỎ QUA", msg)
        return

    print(f"🔄 Phát hiện {len(rows_to_run)} nguồn...")
    target_link = rows_to_run[0]['Link dữ liệu đích']
    
    import google.auth.transport.requests
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)
    token = creds.token
    
    results_dfs = []
    links_to_remove = []
    
    for row in rows_to_run:
        print(f"   - Đang tải: {row.get('Tên nguồn (Nhãn)')}...")
        df, msg = fetch_single_csv_safe(row, token)
        if df is not None:
            results_dfs.append(df)
            links_to_remove.append(row.get('Link dữ liệu lấy dữ liệu'))
    
    if results_dfs:
        df_new = pl.concat(results_dfs, how="vertical", rechunk=True)
        success, msg = smart_update_by_link(df_new, target_link, creds, links_to_remove)
        print(f"📝 Kết quả: {msg}")
        
        if success:
            # Update config -> Đã xong
            if 'Trạng thái' in df_config.columns:
                df_config.loc[df_config['Trạng thái'] == "Chưa chốt", 'Hành động'] = "Đã cập nhật (Auto)"
                df_config.loc[df_config['Trạng thái'] == "Chưa chốt", 'Trạng thái'] = "Đã chốt"
            elif 'Chọn' in df_config.columns:
                df_config.loc[df_config['Chọn'].str.upper() == "TRUE", 'Hành động'] = "Đã cập nhật (Auto)"
                df_config.loc[df_config['Chọn'].str.upper() == "TRUE", 'Chọn'] = "FALSE"
            
            wks_config.clear()
            wks_config.update([df_config.columns.tolist()] + df_config.fillna('').values.tolist())
            
            # GHI LOG THÀNH CÔNG
            write_auto_log(creds, HISTORY_SHEET_ID, "THÀNH CÔNG", f"Cập nhật {len(links_to_remove)} nguồn. {msg}")
        else:
            # GHI LOG LỖI KHI GHI
            write_auto_log(creds, HISTORY_SHEET_ID, "LỖI GHI", msg)
    else:
        print("❌ Không tải được dữ liệu nào.")
        write_auto_log(creds, HISTORY_SHEET_ID, "THẤT BẠI", "Không tải được bất kỳ dữ liệu nào từ nguồn.")

if __name__ == "__main__":
    run_auto_job()
