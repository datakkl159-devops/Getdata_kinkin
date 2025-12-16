import gspread
import pandas as pd
import polars as pl
import requests
import io
import time
import json
import os
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe

# --- CẤU HÌNH TÊN SHEET & CỘT (Theo yêu cầu của bạn) ---
DB_NAME = "DB_Tool_Dong_Bo" # Tên file Google Sheet Database tổng
SHEET_USERS = "users"
SHEET_CONFIG = "luu_cau_hinh"
SHEET_LOCK = "sys_lock"
SHEET_LOG = "log_lanthucthi"
SHEET_LOG_GITHUB = "log_chay_auto_github"
SHEET_SYS_CONFIG = "sys_config"

# 3 cột hệ thống bắt buộc
COL_SYS_LINK = "Link file nguồn"
COL_SYS_SHEET = "Sheet nguồn"
COL_SYS_MONTH = "Tháng chốt"

# --- 1. KẾT NỐI & AUTH ---
def get_creds():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    # Lấy từ Environment (Github Actions)
    if os.environ.get("GCP_SERVICE_ACCOUNT"):
        creds_dict = json.loads(os.environ.get("GCP_SERVICE_ACCOUNT"))
        return ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    
    # Lấy từ Streamlit Secrets (App)
    try:
        import streamlit as st
        if hasattr(st, "secrets") and "gcp_service_account" in st.secrets:
            creds_dict = dict(st.secrets["gcp_service_account"])
            if "private_key" in creds_dict:
                creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
            return ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    except: pass
    
    # Lấy từ file local
    return ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)

def get_client():
    creds = get_creds()
    return gspread.authorize(creds), creds

def extract_id(url):
    if "docs.google.com" in str(url):
        try: return url.split("/d/")[1].split("/")[0]
        except: return None
    return None

# --- 2. TỰ ĐỘNG KHỞI TẠO DATABASE (Logic cũ bạn cần) ---
def init_database_if_needed():
    client, _ = get_client()
    try:
        sh = client.open(DB_NAME)
    except gspread.SpreadsheetNotFound:
        return f"Lỗi: Không tìm thấy file '{DB_NAME}'. Hãy tạo file này trước và share quyền cho bot."

    # Danh sách sheet và header cần có
    required_sheets = {
        SHEET_USERS: ["Username", "Password", "Role"],
        SHEET_CONFIG: ["Username", "Block_Name", "Status", "Link_Nguon", "Sheet_Nguon", "Link_Dich", "Sheet_Dich", "Tan_Suat_Hen_Gio", "Last_Run", "Next_Run", "So_Dong_Du_Lieu", "Thang", "Realtime_Range"],
        SHEET_LOCK: ["STATUS", "USER_dang_dung", "TIME"],
        SHEET_LOG: ["Ngày & giờ get dữ liệu", "Ngày chốt", "Tháng", "Nhân sự get", "Link nguồn", "Link đích", "Sheet Đích", "Sheet nguồn lấy dữ liệu", "Trạng Thái", "Số Dòng Đã Lấy"],
        SHEET_LOG_GITHUB: ["Ngày & giờ get dữ liệu", "Ngày chốt", "Tháng", "Nhân sự get", "Link nguồn", "Link đích", "Sheet Đích", "Sheet nguồn lấy dữ liệu", "Trạng Thái", "Số Dòng Đã Lấy"],
        SHEET_SYS_CONFIG: ["Key", "Value"]
    }

    for s_name, headers in required_sheets.items():
        try:
            sh.worksheet(s_name)
        except:
            # Nếu chưa có thì tạo mới và thêm header
            ws = sh.add_worksheet(s_name, rows=100, cols=20)
            ws.append_row(headers)
            if s_name == SHEET_LOCK:
                ws.append_row(["UNLOCKED", "", ""])
            
    return "OK"

def get_db_worksheet(sheet_name):
    client, _ = get_client()
    sh = client.open(DB_NAME)
    return sh.worksheet(sheet_name)

# --- 3. CHECK QUYỀN (Tính năng cũ) ---
def verify_access(url, mode="view"):
    client, _ = get_client()
    sheet_id = extract_id(url)
    if not sheet_id: return False, "Link lỗi"
    try:
        if mode == "view":
            client.open_by_key(sheet_id)
            return True, "OK"
        elif mode == "edit":
            sh = client.open_by_key(sheet_id)
            return True, "OK"
    except Exception as e:
        return False, str(e)

# --- 4. HÀM QUÉT DÒNG REALTIME (Tính năng cũ quan trọng) ---
def scan_realtime_range(target_link, target_sheet_name, source_link_signature):
    """
    Quét file đích xem link nguồn này đang nằm ở dòng bao nhiêu đến bao nhiêu.
    """
    client, _ = get_client()
    try:
        sh = client.open_by_url(target_link)
        wks = sh.worksheet(target_sheet_name)
        
        # Lấy toàn bộ cột "Link file nguồn"
        headers = wks.row_values(1)
        try:
            col_idx = headers.index(COL_SYS_LINK) + 1
        except: return "Chưa có cột hệ thống"

        col_values = wks.col_values(col_idx) # List các link
        
        # Tìm min/max index
        rows = [i+1 for i, x in enumerate(col_values) if x == source_link_signature]
        
        if rows:
            return f"{min(rows)} - {max(rows)}"
        return "0 - 0"
    except:
        return "Lỗi quét"

# --- 5. LOGIC XỬ LÝ DỮ LIỆU (Giữ nguyên logic phức tạp) ---
def process_single_block(block_row, user_running, is_auto=False):
    client, creds = get_client()
    
    # 1. Parse Config
    link_src = block_row['Link_Nguon']
    sheet_src_name = block_row['Sheet_Nguon']
    link_dest = block_row['Link_Dich']
    sheet_dest_name = block_row['Sheet_Dich']
    thang_val = block_row['Thang']

    # 2. Lấy Data Nguồn (Dùng Polars/Pandas cho nhanh)
    try:
        src_id = extract_id(link_src)
        if creds.access_token_expired: creds.refresh(requests.Request())
        token = creds.access_token
        
        # Lấy GID
        gid = "0"
        if sheet_src_name:
            try:
                sh_s = client.open_by_key(src_id)
                ws_s = sh_s.worksheet(sheet_src_name)
                gid = ws_s.id
            except: return False, f"Không tìm thấy sheet nguồn: {sheet_src_name}", 0, ""

        url = f"https://docs.google.com/spreadsheets/d/{src_id}/export?format=csv&gid={gid}"
        df_new = pl.read_csv(io.BytesIO(requests.get(url, headers={'Authorization': f'Bearer {token}'}).content), infer_schema_length=0)
        
        # Thêm 3 cột hệ thống
        df_new = df_new.with_columns([
            pl.lit(link_src).cast(pl.Utf8).alias(COL_SYS_LINK),
            pl.lit(sheet_src_name).cast(pl.Utf8).alias(COL_SYS_SHEET),
            pl.lit(thang_val).cast(pl.Utf8).alias(COL_SYS_MONTH)
        ])
    except Exception as e: return False, f"Lỗi nguồn: {e}", 0, ""

    if df_new.height == 0: return True, "Nguồn rỗng", 0, ""

    # 3. Xử lý Đích (Xóa cũ - Thêm mới)
    try:
        dest_id = extract_id(link_dest)
        sh_dest = client.open_by_key(dest_id)
        
        # Tạo sheet đích nếu chưa có
        try: wks_dest = sh_dest.worksheet(sheet_dest_name)
        except: wks_dest = sh_dest.add_worksheet(sheet_dest_name, 1000, 20)

        data_dest = wks_dest.get_all_values()
        df_final = pd.DataFrame()

        if data_dest:
            headers = data_dest[0]
            df_old = pd.DataFrame(data_dest[1:], columns=headers)
            
            # Logic quan trọng: Chỉ xóa dòng của Link Nguồn này, giữ nguyên dòng của khối khác
            if COL_SYS_LINK in df_old.columns:
                df_dest_kept = df_old[df_old[COL_SYS_LINK] != link_src]
            else:
                df_dest_kept = df_old
            
            df_new_pd = df_new.to_pandas()
            df_final = pd.concat([df_dest_kept, df_new_pd], ignore_index=True)
        else:
            df_final = df_new.to_pandas()

        # Ghi lại
        wks_dest.clear()
        set_with_dataframe(wks_dest, df_final)
        
    except Exception as e: return False, f"Lỗi đích: {e}", 0, ""

    # 4. Quét lại Realtime Range
    real_range = scan_realtime_range(link_dest, sheet_dest_name, link_src)

    # 5. Ghi Log
    try:
        log_sheet = SHEET_LOG_GITHUB if is_auto else SHEET_LOG
        ws_log = get_db_worksheet(log_sheet)
        ws_log.append_row([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            datetime.now().strftime("%Y-%m-%d"),
            thang_val, user_running, link_src, link_dest, sheet_dest_name, sheet_src_name,
            "Thành công", df_new.height
        ])
    except: pass

    return True, "Thành công", df_new.height, real_range

# --- 6. LOCK SYSTEM ---
def check_lock():
    try:
        ws = get_db_worksheet(SHEET_LOCK)
        status = ws.acell('A2').value
        user = ws.acell('B2').value
        return status == 'LOCKED', user
    except: return False, ""

def set_lock(status, user):
    try:
        ws = get_db_worksheet(SHEET_LOCK)
        ws.update('A2', status)
        ws.update('B2', user)
        ws.update('C2', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    except: pass
