import streamlit as st
import pandas as pd
import time
import gspread
import json
import re
import pytz
import uuid
import numpy as np
import gc
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from gspread.exceptions import APIError
from datetime import datetime
from google.oauth2 import service_account
from collections import defaultdict
from st_copy_to_clipboard import st_copy_to_clipboard

# ==========================================
# 1. CẤU HÌNH HỆ THỐNG
# ==========================================
st.set_page_config(page_title="Kinkin Tool 2.0 (V92 - Tiếng Việt)", layout="wide", page_icon="🇻🇳")

AUTHORIZED_USERS = {
    "admin2025": "Admin_Master",
    "team_hn": "Team_HaNoi",
    "team_hcm": "Team_HCM"
}

BOT_EMAIL_DISPLAY = "getdulieu@kin-kin-477902.iam.gserviceaccount.com"

# Tên Sheet
SHEET_CONFIG_NAME = "luu_cau_hinh" 
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_ACTIVITY_NAME = "log_hanh_vi"
SHEET_LOCK_NAME = "sys_lock"
SHEET_SYS_CONFIG = "sys_config"
SHEET_NOTE_NAME = "database_ghi_chu"

# --- [V92] ĐỊNH NGHĨA CỘT TIẾNG VIỆT ---
COL_BLOCK_NAME = "Block_Name"
COL_STATUS = "Trạng thái"
COL_WRITE_MODE = "Cách ghi"          # Đã sửa thành tiếng Việt
COL_DATA_RANGE = "Vùng lấy"          # Đã sửa
COL_MONTH = "Tháng"
COL_SRC_LINK = "Link nguồn"          # Đã sửa
COL_TGT_LINK = "Link đích"           # Đã sửa
COL_SRC_SHEET = "Sheet nguồn"        # Đã sửa
COL_TGT_SHEET = "Sheet đích"         # Đã sửa
COL_RESULT = "Kết quả"
COL_LOG_ROW = "Log Row"
COL_FILTER = "Điều kiện lọc"         # Đã sửa
COL_HEADER = "Lấy Header?"           # Đã sửa
COL_COPY_FLAG = "Copy"               # Đã sửa

REQUIRED_COLS_CONFIG = [
    COL_BLOCK_NAME, COL_STATUS, COL_WRITE_MODE, COL_DATA_RANGE, COL_MONTH, 
    COL_SRC_LINK, COL_TGT_LINK, COL_TGT_SHEET, COL_SRC_SHEET, 
    COL_RESULT, COL_LOG_ROW, COL_FILTER, COL_HEADER
]

SCHED_COL_BLOCK = "Block_Name"
SCHED_COL_TYPE = "Loai_Lich"
SCHED_COL_VAL1 = "Thong_So_Chinh"
SCHED_COL_VAL2 = "Thong_So_Phu"
REQUIRED_COLS_SCHED = [SCHED_COL_BLOCK, SCHED_COL_TYPE, SCHED_COL_VAL1, SCHED_COL_VAL2]

NOTE_COL_ID = "ID"; NOTE_COL_BLOCK = "Tên Khối"; NOTE_COL_CONTENT = "Nội dung Note"
REQUIRED_COLS_NOTE = [NOTE_COL_ID, NOTE_COL_BLOCK, NOTE_COL_CONTENT]

# Cột hệ thống trong Sheet Đích (Không nên đổi tiếng Việt có dấu để tránh lỗi code bên dưới)
SYS_COL_LINK = "Src_Link"
SYS_COL_SHEET = "Src_Sheet"
SYS_COL_MONTH = "Month"

DEFAULT_BLOCK_NAME = "Block_Mac_Dinh"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# Cấu hình Log Buffer
LOG_BUFFER_SIZE = 5 
LOG_FLUSH_INTERVAL = 10 

# ==========================================
# 2. AUTHENTICATION & UTILS (SAFE API)
# ==========================================
def get_creds():
    raw_creds = st.secrets["gcp_service_account"]
    if isinstance(raw_creds, str):
        try: creds_info = json.loads(raw_creds)
        except: return None
    else: creds_info = dict(raw_creds)
    if "private_key" in creds_info: creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
    return service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)

def safe_api_call(func, *args, **kwargs):
    max_retries = 5
    for i in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_str = str(e).lower()
            if "429" in error_str or "quota" in error_str or "api" in error_str or "500" in error_str:
                wait_time = (2 ** i) + 3 
                print(f"⚠️ API Busy. Waiting {wait_time}s...")
                time.sleep(wait_time)
            elif i == max_retries - 1: raise e
            else: time.sleep(2)
    return None

def safe_get_as_dataframe(wks, **kwargs):
    return safe_api_call(get_as_dataframe, wks, **kwargs)

def safe_set_with_dataframe(wks, df, **kwargs):
    return safe_api_call(set_with_dataframe, wks, df, **kwargs)

def get_sh_with_retry(creds, sheet_id_or_key):
    gc = gspread.authorize(creds)
    return safe_api_call(gc.open_by_key, sheet_id_or_key)

def col_name_to_index(col_name):
    col_name = col_name.upper()
    index = 0
    for char in col_name: index = index * 26 + (ord(char) - ord('A')) + 1
    return index - 1

def extract_id(url):
    if not isinstance(url, str): return None
    if "docs.google.com" in url:
        try: return url.split("/d/")[1].split("/")[0]
        except: return None
    return None

def ensure_sheet_headers(wks, required_columns):
    try:
        current_headers = wks.row_values(1)
        if not current_headers: wks.append_row(required_columns)
    except: pass

@st.dialog("📚 Hướng Dẫn Sử Dụng", width="large")
def show_guide_popup():
    st.markdown("""
    ### 1. Cấu Trúc Bộ Lọc (Filter)
    Sử dụng dấu chấm phẩy **`;`** để ngăn cách.
    * `Tên SP contains Cam`
    * `Ngày >= 2025-01-01` (Dùng định dạng YYYY-MM-DD)
    
    ### 2. Chế Độ Ghi
    * **Ghi Đè:** Xóa dữ liệu cũ rồi ghi mới.
    * **Ghi Nối Tiếp:** Giữ nguyên cũ, ghi tiếp xuống dưới đáy.
    """)

# --- SMART FILTER ENGINE ---
def apply_smart_filter_v92(df, filter_str, debug_container=None):
    if not filter_str or str(filter_str).strip().lower() in ['nan', 'none', 'null', '']:
        return df, None
    conditions = str(filter_str).split(';')
    current_df = df.copy()
    if debug_container: debug_container.markdown(f"**🔍 Lọc: {len(current_df)} dòng gốc**")
    for i, cond in enumerate(conditions):
        fs = cond.strip()
        if not fs: continue 
        operators = [" contains ", "==", "!=", ">=", "<=", ">", "<", "="]
        selected_op = None
        for op in operators:
            if op in fs: selected_op = op; break
        if not selected_op: return None, f"Lỗi cú pháp: '{fs}'"
        parts = fs.split(selected_op, 1)
        user_col = parts[0].strip().replace("`", "").replace("'", "").replace('"', "")
        user_val = parts[1].strip()
        clean_val = user_val
        if (user_val.startswith("'") and user_val.endswith("'")) or (user_val.startswith('"') and user_val.endswith('"')):
            clean_val = user_val[1:-1]
        clean_val = clean_val.strip()
        
        real_col_name = None
        if user_col in current_df.columns: real_col_name = user_col
        else:
            for col in current_df.columns:
                if str(col).strip().lower() == user_col.lower(): real_col_name = col; break
        if not real_col_name: return None, f"Không tìm thấy cột '{user_col}'"
        
        try:
            col_series = current_df[real_col_name]
            if selected_op == " contains ":
                current_df = current_df[col_series.astype(str).str.contains(clean_val, case=False, na=False)]
            else:
                is_date = False
                try:
                    s_dt = pd.to_datetime(col_series, dayfirst=True, errors='coerce')
                    v_dt = pd.to_datetime(clean_val, dayfirst=True)
                    if s_dt.notna().any(): is_date = True
                except: is_date = False
                
                is_num = False
                if not is_date:
                    try:
                        s_num = pd.to_numeric(col_series, errors='coerce'); v_num = float(clean_val)
                        if s_num.notna().any(): is_num = True
                    except: is_num = False
                
                if is_date:
                    if selected_op == ">": current_df = current_df[s_dt > v_dt]
                    elif selected_op == "<": current_df = current_df[s_dt < v_dt]
                    elif selected_op == ">=": current_df = current_df[s_dt >= v_dt]
                    elif selected_op == "<=": current_df = current_df[s_dt <= v_dt]
                    elif selected_op in ["=", "=="]: current_df = current_df[s_dt == v_dt]
                    elif selected_op == "!=": current_df = current_df[s_dt != v_dt]
                    type_msg = "Ngày"
                elif is_num:
                    if selected_op == ">": current_df = current_df[s_num > v_num]
                    elif selected_op == "<": current_df = current_df[s_num < v_num]
                    elif selected_op == ">=": current_df = current_df[s_num >= v_num]
                    elif selected_op == "<=": current_df = current_df[s_num <= v_num]
                    elif selected_op in ["=", "=="]: current_df = current_df[s_num == v_num]
                    elif selected_op == "!=": current_df = current_df[s_num != v_num]
                    type_msg = "Số"
                else:
                    s_str = col_series.astype(str).str.strip()
                    if selected_op == ">": current_df = current_df[s_str > str(clean_val)]
                    elif selected_op == "<": current_df = current_df[s_str < str(clean_val)]
                    elif selected_op == ">=": current_df = current_df[s_str >= str(clean_val)]
                    elif selected_op == "<=": current_df = current_df[s_str <= str(clean_val)]
                    elif selected_op in ["=", "=="]: current_df = current_df[s_str == str(clean_val)]
                    elif selected_op == "!=": current_df = current_df[s_str != str(clean_val)]
                    type_msg = "Chuỗi"
            if debug_container: debug_container.caption(f"👉 Lọc '{clean_val}' ({type_msg}) -> Còn {len(current_df)}")
        except Exception as e: return None, f"Lỗi xử lý '{fs}': {str(e)}"
    return current_df, None

# --- LOGGING SYSTEM ---
def init_log_buffer():
    if 'log_buffer' not in st.session_state: st.session_state['log_buffer'] = []
    if 'last_log_flush' not in st.session_state: st.session_state['last_log_flush'] = time.time()

def flush_logs(creds, force=False):
    buffer = st.session_state.get('log_buffer', [])
    last_flush = st.session_state.get('last_log_flush', 0)
    if (force or len(buffer) >= LOG_BUFFER_SIZE or (time.time() - last_flush > LOG_FLUSH_INTERVAL)) and buffer:
        try:
            sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
            try: wks = sh.worksheet(SHEET_ACTIVITY_NAME)
            except: 
                wks = sh.add_worksheet(SHEET_ACTIVITY_NAME, rows=1000, cols=4)
                wks.append_row(["Thời gian", "Người dùng", "Hành vi", "Trạng thái"])
            safe_api_call(wks.append_rows, buffer)
            st.session_state['log_buffer'] = []
            st.session_state['last_log_flush'] = time.time()
        except: pass

def log_user_action_buffered(creds, user_id, action, status="", force_flush=False):
    init_log_buffer()
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    log_entry = [datetime.now(tz).strftime("%d/%m/%Y %H:%M:%S"), user_id, action, status]
    st.session_state['log_buffer'].append(log_entry)
    flush_logs(creds, force=force_flush)

def detect_df_changes(df_old, df_new):
    if len(df_old) != len(df_new): return f"Thay đổi dòng: {len(df_old)} -> {len(df_new)}"
    changes = []
    ignore_cols = [COL_BLOCK_NAME, COL_LOG_ROW, COL_RESULT, "STT", COL_COPY_FLAG, "_index"]
    compare_cols = [c for c in df_new.columns if c not in ignore_cols and c in df_old.columns]
    dfo = df_old.reset_index(drop=True); dfn = df_new.reset_index(drop=True)
    for i in range(len(dfo)):
        for col in compare_cols:
            val_old = str(dfo.at[i, col]).strip(); val_new = str(dfn.at[i, col]).strip()
            if val_old != val_new:
                vo = (val_old[:15] + '..') if len(val_old) > 15 else val_old
                vn = (val_new[:15] + '..') if len(val_new) > 15 else val_new
                changes.append(f"Dòng {i+1} [{col}]: {vo} -> {vn}")
                if len(changes) >= 3: 
                    changes.append("..."); return " | ".join(changes)
    return " | ".join(changes) if changes else "Không có thay đổi nội dung"

# --- LOGIN ---
def check_login():
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
    if 'current_user_id' not in st.session_state: st.session_state['current_user_id'] = "Unknown"
    if "auto_key" in st.query_params:
        key = st.query_params["auto_key"]
        if key in AUTHORIZED_USERS:
            st.session_state['logged_in'] = True; st.session_state['current_user_id'] = AUTHORIZED_USERS[key]; return True
    if st.session_state['logged_in']: return True
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.header("🛡️ Đăng nhập")
        pwd = st.text_input("Mật khẩu:", type="password")
        if st.button("Đăng Nhập", use_container_width=True):
            if pwd in AUTHORIZED_USERS:
                st.session_state['logged_in'] = True; st.session_state['current_user_id'] = AUTHORIZED_USERS[pwd]
                log_user_action_buffered(get_creds(), AUTHORIZED_USERS[pwd], "Đăng nhập", "Thành công", force_flush=True)
                st.rerun()
            else: st.error("Sai mật khẩu")
    return False

# ==========================================
# 3. SYSTEM MANAGERS
# ==========================================
def get_system_lock_status(creds):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_LOCK_NAME)
        except: wks = sh.add_worksheet(SHEET_LOCK_NAME, rows=10, cols=5); wks.update([["is_locked", "user", "time_start"], ["FALSE", "", ""]]); return False, "", ""
        val = wks.cell(2, 1).value; user = wks.cell(2, 2).value; time_str = wks.cell(2, 3).value
        if val == "TRUE":
            try:
                if (datetime.now() - datetime.strptime(time_str, "%d/%m/%Y %H:%M:%S")).total_seconds() > 300: return False, "", ""
            except: pass
            return True, user, time_str
        return False, "", ""
    except: return False, "", ""

def acquire_lock(creds, user_id):
    is_locked, locking_user, t = get_system_lock_status(creds)
    if is_locked and locking_user != user_id: return False
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_LOCK_NAME)
        now_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        wks.update("A2:C2", [["TRUE", user_id, now_str]])
        return True
    except: return False

def release_lock(creds, user_id):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_LOCK_NAME)
        val = wks.cell(2, 2).value
        if val == user_id: wks.update("A2:C2", [["FALSE", "", ""]])
    except: pass

def load_notes_data(creds):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_NOTE_NAME)
        except: wks = sh.add_worksheet(SHEET_NOTE_NAME, rows=100, cols=5); ensure_sheet_headers(wks, REQUIRED_COLS_NOTE)
        ensure_sheet_headers(wks, REQUIRED_COLS_NOTE)
        df = safe_get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if df.empty: return pd.DataFrame(columns=REQUIRED_COLS_NOTE)
        return df.dropna(how='all')
    except: return pd.DataFrame(columns=REQUIRED_COLS_NOTE)

def save_notes_data(df_notes, creds, user_id, block_name):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_NOTE_NAME)
        if not df_notes.empty:
            for idx, row in df_notes.iterrows():
                if not row[NOTE_COL_ID]: df_notes.at[idx, NOTE_COL_ID] = str(uuid.uuid4())[:8]
        safe_set_with_dataframe(wks, df_notes, row=1, col=1)
        log_user_action_buffered(creds, user_id, "Lưu Ghi Chú", f"Cập nhật note cho {block_name}", force_flush=True)
        return True
    except: return False

@st.dialog("📝 Note", width="large")
def show_note_popup(creds, all_blocks, user_id):
    if 'df_notes_temp' not in st.session_state: st.session_state['df_notes_temp'] = load_notes_data(creds)
    df_notes = st.session_state['df_notes_temp']
    edited_notes = st.data_editor(
        df_notes, num_rows="dynamic", use_container_width=True,
        column_config={
            NOTE_COL_ID: st.column_config.TextColumn("ID", disabled=True, width="small"),
            NOTE_COL_BLOCK: st.column_config.SelectboxColumn("Khối", options=all_blocks, required=True),
            NOTE_COL_CONTENT: st.column_config.TextColumn("Nội dung", width="large")
        }, key="note_popup"
    )
    if st.button("💾 Lưu Note", type="primary"):
        blk_ref = edited_notes[NOTE_COL_BLOCK].iloc[0] if not edited_notes.empty else "All"
        if save_notes_data(edited_notes, creds, user_id, blk_ref):
            st.success("Đã lưu!"); time.sleep(1); st.rerun()

def load_scheduler_config(creds):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_SYS_CONFIG)
        except: 
            wks = sh.add_worksheet(SHEET_SYS_CONFIG, rows=50, cols=5)
            wks.append_row(REQUIRED_COLS_SCHED)
        ensure_sheet_headers(wks, REQUIRED_COLS_SCHED)
        df = safe_get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if SCHED_COL_BLOCK not in df.columns: return pd.DataFrame(columns=REQUIRED_COLS_SCHED)
        return df.dropna(how='all')
    except: return pd.DataFrame(columns=REQUIRED_COLS_SCHED)

def save_scheduler_config(df_sched, creds, user_id, type_run, v1, v2):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_SYS_CONFIG)
        cols = REQUIRED_COLS_SCHED
        for c in cols:
            if c not in df_sched.columns: df_sched[c] = ""
        wks.clear(); 
        safe_set_with_dataframe(wks, df_sched[cols].fillna(""), row=1, col=1)
        msg = f"Cài đặt: {type_run} | {v1} {v2}".strip()
        log_user_action_buffered(creds, user_id, "Cài Lịch Chạy", msg, force_flush=True)
        return True
    except: return False

def fetch_activity_logs(creds, limit=50):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_ACTIVITY_NAME)
        df = safe_get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if df.empty: return pd.DataFrame()
        return df.tail(limit).iloc[::-1]
    except: return pd.DataFrame()

def write_detailed_log(creds, log_data_list):
    if not log_data_list: return
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_LOG_NAME)
        except: 
            wks = sh.add_worksheet(SHEET_LOG_NAME, rows=1000, cols=15)
            wks.append_row(["Thời gian", "Vùng lấy", "Tháng", "User", "Link Nguồn", "Link Đích", "Sheet Đích", "Sheet Nguồn", "Kết Quả", "Số Dòng", "Range", "Block"])
        
        cleaned_list = []
        for row in log_data_list:
            cleaned_list.append([str(x) for x in row])
            
        safe_api_call(wks.append_rows, cleaned_list)
    except Exception as e:
        st.warning(f"Lỗi ghi log (V92): {str(e)}")

# ==========================================
# 4. CORE ETL
# ==========================================
def fetch_data_v4(row_config, creds, target_headers=None, status_container=None):
    # [V92] Mapping Column Names from Vietnamese to Code Logic
    link_src = str(row_config.get(COL_SRC_LINK, '')).strip()
    source_label = str(row_config.get(COL_SRC_SHEET, '')).strip()
    month_val = str(row_config.get(COL_MONTH, ''))
    
    raw_range = str(row_config.get(COL_DATA_RANGE, '')).strip()
    data_range_str = "Lấy hết" if raw_range.lower() in ['nan', 'none', 'null', '', 'lấy hết'] else raw_range

    raw_filter = str(row_config.get(COL_FILTER, '')).strip()
    if raw_filter.lower() in ['nan', 'none', 'null']: raw_filter = ""
    
    include_header = str(row_config.get(COL_HEADER, 'FALSE')).strip().upper() == 'TRUE'
    sheet_id = extract_id(link_src)
    if not sheet_id: return None, sheet_id, "Link lỗi"
    
    try:
        sh_source = get_sh_with_retry(creds, sheet_id)
        if source_label:
            try: wks_source = sh_source.worksheet(source_label)
            except: return None, sheet_id, f"❌ 404 Sheet: {source_label}"
        else: wks_source = sh_source.sheet1
            
        data = safe_api_call(wks_source.get_all_values)
        if not data: return pd.DataFrame(), sheet_id, "Sheet trắng/Lỗi tải"

        header_row = data[0]
        body_rows = data[1:]
        
        unique_headers = []
        seen = {}
        for col in header_row:
            if col in seen:
                seen[col] += 1
                unique_headers.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                unique_headers.append(col)
        
        df_working = pd.DataFrame(body_rows, columns=unique_headers)

        if target_headers:
            num_src = len(df_working.columns); num_tgt = len(target_headers)
            min_cols = min(num_src, num_tgt)
            old_cols = df_working.columns.tolist()
            rename_map = {old_cols[i]: target_headers[i] for i in range(min_cols)}
            df_working = df_working.rename(columns=rename_map)
            if num_src > num_tgt: df_working = df_working.iloc[:, :num_tgt]

        if data_range_str != "Lấy hết" and ":" in data_range_str:
            try:
                s_str, e_str = data_range_str.split(":")
                s_idx = col_name_to_index(s_str.strip()); e_idx = col_name_to_index(e_str.strip())
                if s_idx >= 0: df_working = df_working.iloc[:, s_idx : e_idx + 1]
            except: pass

        if raw_filter:
            df_filtered, err = apply_smart_filter_v92(df_working, raw_filter, debug_container=status_container)
            if err: return None, sheet_id, f"⚠️ {err}"
            df_working = df_filtered

        if include_header:
            df_header_row = pd.DataFrame([df_working.columns.tolist()], columns=df_working.columns)
            df_final = pd.concat([df_header_row, df_working], ignore_index=True)
        else:
            df_final = df_working

        df_final = df_final.astype(str).replace(['nan', 'None', '<NA>', 'null'], '')
        
        # Cột hệ thống giữ nguyên tên tiếng Anh cho an toàn logic
        df_final[SYS_COL_LINK] = link_src.strip()
        df_final[SYS_COL_SHEET] = source_label.strip()
        df_final[SYS_COL_MONTH] = month_val.strip()
        
        return df_final, sheet_id, "Thành công"

    except Exception as e: return None, sheet_id, f"Lỗi tải: {str(e)}"

def get_rows_to_delete_dynamic(wks, keys_to_delete, log_container):
    all_values = safe_api_call(wks.get_all_values)
    if not all_values: return []
    headers = all_values[0]
    try:
        idx_link = headers.index(SYS_COL_LINK); idx_sheet = headers.index(SYS_COL_SHEET); idx_month = headers.index(SYS_COL_MONTH)
    except ValueError: return [] 
    rows_to_delete = []
    for i, row in enumerate(all_values[1:], start=2): 
        l = row[idx_link].strip() if len(row) > idx_link else ""
        s = row[idx_sheet].strip() if len(row) > idx_sheet else ""
        m = row[idx_month].strip() if len(row) > idx_month else ""
        if (l, s, m) in keys_to_delete: rows_to_delete.append(i)
    return rows_to_delete

def batch_delete_rows(sh, sheet_id, row_indices, log_container=None):
    if not row_indices: return
    row_indices.sort(reverse=True) 
    ranges = []
    if len(row_indices) > 0:
        start = row_indices[0]; end = start
        for r in row_indices[1:]:
            if r == start - 1: start = r
            else: ranges.append((start, end)); start = r; end = r
        ranges.append((start, end))
    requests = []
    for start, end in ranges:
        requests.append({"deleteDimension": {"range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": start - 1, "endIndex": end}}})
    batch_size = 100
    for i in range(0, len(requests), batch_size):
        if log_container: log_container.write(f"✂️ Xóa batch {i//batch_size + 1}...")
        safe_api_call(sh.batch_update, {'requests': requests[i:i+batch_size]})
        time.sleep(1)

def write_strict_sync_v2(tasks_list, target_link, target_sheet_name, creds, log_container):
    result_map = {} 
    debug_data = [] 
    try:
        target_id = extract_id(target_link)
        if not target_id: return False, "Link lỗi", {}, []
        sh = get_sh_with_retry(creds, target_id)
        real_sheet_name = str(target_sheet_name).strip() or "Tong_Hop_Data"
        log_container.write(f"📂 Đích: ...{target_link[-10:]} | Sheet: {real_sheet_name}")
        
        all_titles = [s.title for s in safe_api_call(sh.worksheets)]
        if real_sheet_name in all_titles:
            wks = sh.worksheet(real_sheet_name)
        else:
            wks = sh.add_worksheet(title=real_sheet_name, rows=1000, cols=20)
            log_container.write(f"✨ Tạo mới sheet: {real_sheet_name}")
        
        df_new_all = pd.DataFrame()
        for df, src_link, r_idx, w_mode in tasks_list:
            df_new_all = pd.concat([df_new_all, df], ignore_index=True)
        
        if df_new_all.empty: return True, "No Data", {}, []

        existing_headers = safe_api_call(wks.row_values, 1)
        if not existing_headers:
            final_headers = df_new_all.columns.tolist()
            wks.update(range_name="A1", values=[final_headers])
            existing_headers = final_headers
            log_container.write("🆕 Tạo Header mới.")
        else:
            updated = existing_headers.copy(); added = False
            for col in [SYS_COL_LINK, SYS_COL_SHEET, SYS_COL_MONTH]:
                if col not in updated: updated.append(col); added = True
            if added: wks.update(range_name="A1", values=[updated]); existing_headers = updated; log_container.write("➕ Cập nhật cột hệ thống.")

        df_aligned = pd.DataFrame()
        for col in existing_headers:
            if col in df_new_all.columns: df_aligned[col] = df_new_all[col]
            else: df_aligned[col] = ""
        
        keys_to_delete = set()
        for df, _, _, w_mode in tasks_list:
            if w_mode == "Ghi Đè" and not df.empty:
                l = str(df[SYS_COL_LINK].iloc[0]).strip()
                s = str(df[SYS_COL_SHEET].iloc[0]).strip()
                m = str(df[SYS_COL_MONTH].iloc[0]).strip()
                keys_to_delete.add((l, s, m))
        
        if keys_to_delete:
            log_container.write(f"🔍 Quét dữ liệu cũ (Ghi Đè)...")
            rows_to_del = get_rows_to_delete_dynamic(wks, keys_to_delete, log_container)
            if rows_to_del:
                log_container.write(f"✂️ Xóa {len(rows_to_del)} dòng cũ...")
                batch_delete_rows(sh, wks.id, rows_to_del, log_container)
                log_container.write("✅ Đã xóa xong. Đang cập nhật index...")
                time.sleep(3) 
        
        current_data = safe_api_call(wks.get_all_values)
        if not current_data: start_row = 1 
        else: start_row = len(current_data) + 1
        
        log_container.write(f"🚀 Ghi {len(df_aligned)} dòng mới (từ dòng {start_row})...")
        
        chunk_size = 5000
        new_vals = df_aligned.fillna('').values.tolist()
        for i in range(0, len(new_vals), chunk_size):
            safe_api_call(wks.append_rows, new_vals[i:i+chunk_size], value_input_option='USER_ENTERED')
            time.sleep(1)

        current_cursor = int(start_row)
        for df, src_link, r_idx, w_mode in tasks_list:
            count = len(df)
            if count > 0:
                end = current_cursor + count - 1
                rng_str = f"{current_cursor} - {end}"
                current_cursor += count
            else:
                rng_str = "0 dòng"
            
            result_map[r_idx] = ("Thành công", rng_str, count)
            debug_data.append({"File": src_link[-10:], "Mode": w_mode, "Start": current_cursor - count, "End": end if count >0 else 0, "Range Log": rng_str})
            
        return True, f"Cập nhật {len(df_aligned)} dòng", result_map, debug_data

    except Exception as e: return False, f"Lỗi Ghi: {str(e)}", {}, []

# --- PIPELINE MAIN FUNCTION ---
def verify_access_fast(url, creds):
    sheet_id = extract_id(url)
    if not sheet_id: return False, "Link lỗi"
    try: get_sh_with_retry(creds, sheet_id); return True, "OK"
    except: return False, "Chặn quyền"

def check_permissions_ui(rows, creds, container, user_id):
    log_user_action_buffered(creds, user_id, "Quét Quyền", "Bắt đầu...", force_flush=False)
    src_links = set(); tgt_links = set()
    for r in rows:
        s_link = str(r.get(COL_SRC_LINK, '')).strip(); t_link = str(r.get(COL_TGT_LINK, '')).strip()
        if "docs.google.com" in s_link: src_links.add(s_link)
        if "docs.google.com" in t_link: tgt_links.add(t_link)
    all_unique_links = list(src_links.union(tgt_links)); total = len(all_unique_links)
    if total == 0: container.info("Không tìm thấy link Google Sheet nào."); return
    prog = container.progress(0); err_count = 0
    for i, link in enumerate(all_unique_links):
        prog.progress((i + 1) / total); time.sleep(0.2)
        ok, msg = verify_access_fast(link, creds)
        if not ok:
            err_count += 1; error_msgs = []
            if link in src_links: error_msgs.append("Link Nguồn: cần quyền XEM")
            if link in tgt_links: error_msgs.append("Link Đích: cần quyền SỬA")
            container.error(f"❌ {link}\n👉 {' & '.join(error_msgs)}")
    if err_count == 0: container.success("✅ OK: Bot có đủ quyền.")
    else: container.warning(f"⚠️ {err_count} link thiếu quyền.")
    log_user_action_buffered(creds, user_id, "Quét Quyền", f"Lỗi: {err_count}", force_flush=True)

def process_pipeline_mixed(rows_to_run, user_id, block_name_run, status_container):
    creds = get_creds()
    if not acquire_lock(creds, user_id): 
        st.error("⚠️ Hệ thống đang bận. Vui lòng thử lại sau."); return False, {}, 0
    
    log_user_action_buffered(creds, user_id, f"Chạy: {block_name_run}", "Đang xử lý...", force_flush=True)
    try:
        grouped = defaultdict(list)
        for r in rows_to_run:
            if str(r.get(COL_STATUS, '')).strip() == "Chưa chốt & đang cập nhật":
                grouped[(str(r.get(COL_TGT_LINK, '')).strip(), str(r.get(COL_TGT_SHEET, '')).strip())].append(r)
        
        final_res_map = {}; all_ok = True; total_rows = 0; log_ents = []
        all_debug_data = [] 
        tz = pytz.timezone('Asia/Ho_Chi_Minh'); now = datetime.now(tz).strftime("%d/%m/%Y %H:%M:%S")

        for idx, ((t_link, t_sheet), group_rows) in enumerate(grouped.items()):
            with status_container.expander(f"Processing File {idx+1}: ...{t_link[-10:]}", expanded=True):
                target_headers = []
                try:
                    tid = extract_id(t_link)
                    if tid:
                        sh_t = get_sh_with_retry(creds, tid)
                        all_titles = [s.title for s in safe_api_call(sh_t.worksheets)]
                        if t_sheet in all_titles:
                            wks_t = sh_t.worksheet(t_sheet)
                            target_headers = safe_api_call(wks_t.row_values, 1)
                except: pass

                tasks = []
                for i, r in enumerate(group_rows):
                    lnk = r.get(COL_SRC_LINK, ''); lbl = r.get(COL_SRC_SHEET, '')
                    row_idx = r.get('_index', -1)
                    w_mode = str(r.get(COL_WRITE_MODE, 'Ghi Đè')).strip()
                    if w_mode not in ["Ghi Đè", "Ghi Nối Tiếp"]: w_mode = "Ghi Đè"

                    msg_placeholder = st.empty()
                    msg_placeholder.write(f"⏳ Đang tải: {lnk[-10:]} ({lbl})...")
                    
                    df, sid, msg = fetch_data_v4(r, creds, target_headers, status_container=msg_placeholder)
                    time.sleep(1.0)
                    
                    if df is not None: 
                        count = len(df)
                        msg_placeholder.success(f"✅ Tải xong: {lnk[-10:]} ({lbl}) | SL: {count} dòng")
                        tasks.append((df, lnk, row_idx, w_mode))
                        total_rows += len(df)
                    else: 
                        msg_placeholder.error(f"❌ Lỗi tải: {lnk[-10:]} | {msg}")
                        final_res_map[row_idx] = ("Lỗi tải", "", 0)
                    
                    del df; gc.collect()

                if tasks:
                    ok, msg, batch_res_map, batch_debug = write_strict_sync_v2(tasks, t_link, t_sheet, creds, st)
                    if not ok: st.error(msg); all_ok = False
                    else: st.success(msg)
                    final_res_map.update(batch_res_map)
                    all_debug_data.extend(batch_debug)
                    del tasks; gc.collect()
                
                for r in group_rows:
                    row_idx = r.get('_index', -1)
                    res_status, res_range, res_count = final_res_map.get(row_idx, ("Lỗi", "", 0))
                    
                    log_ents.append([
                        now, r.get(COL_DATA_RANGE), r.get(COL_MONTH), user_id, 
                        r.get(COL_SRC_LINK), t_link, t_sheet, r.get(COL_SRC_SHEET), 
                        res_status, res_count, res_range, block_name_run
                    ])
        
        write_detailed_log(creds, log_ents)
        status_msg = f"Hoàn tất: Xử lý {total_rows} dòng. Lỗi: {not all_ok}"
        log_user_action_buffered(creds, user_id, f"Kết quả chạy {block_name_run}", status_msg, force_flush=True)
        
        if all_debug_data:
            st.markdown("### 📊 Kết quả chi tiết (Debug)")
            st.dataframe(pd.DataFrame(all_debug_data))

        return all_ok, final_res_map, total_rows
    finally: release_lock(creds, user_id)

# ==========================================
# 5. UI & MAIN
# ==========================================
@st.cache_data
def load_full_config(_creds):
    sh = get_sh_with_retry(_creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    ensure_sheet_headers(wks, REQUIRED_COLS_CONFIG)
    # [V92] Safe
    df = safe_get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
    
    if df.empty: return pd.DataFrame(columns=REQUIRED_COLS_CONFIG)
    else: df = df.dropna(how='all')
    
    df[COL_BLOCK_NAME] = df[COL_BLOCK_NAME].replace('', DEFAULT_BLOCK_NAME).fillna(DEFAULT_BLOCK_NAME)
    if COL_WRITE_MODE not in df.columns: df[COL_WRITE_MODE] = "Ghi Đè"
    df[COL_WRITE_MODE] = df[COL_WRITE_MODE].fillna("Ghi Đè").replace("", "Ghi Đè")
    
    df[COL_HEADER] = df[COL_HEADER].replace('', 'FALSE').fillna('FALSE')
    if 'STT' in df.columns: df = df.drop(columns=['STT'])
    if 'Che_Do_Ghi' in df.columns: df = df.drop(columns=['Che_Do_Ghi'])
    return df

def save_block_config_to_sheet(df_ui, blk_name, creds, uid):
    if not acquire_lock(creds, uid): st.error("Busy!"); return
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        
        df_svr = safe_get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if df_svr.empty: df_svr = pd.DataFrame(columns=REQUIRED_COLS_CONFIG)
        else: df_svr = df_svr.dropna(how='all')

        if COL_BLOCK_NAME not in df_svr.columns: df_svr[COL_BLOCK_NAME] = DEFAULT_BLOCK_NAME
        
        df_old_blk = df_svr[df_svr[COL_BLOCK_NAME] == blk_name].copy().reset_index(drop=True)
        df_new_blk = df_ui.copy().reset_index(drop=True)
        
        action_type = "Cập nhật Khối"
        if df_old_blk.empty and not df_new_blk.empty:
            action_type = "Tạo mới Khối"
            change_msg = f"Khởi tạo {len(df_new_blk)} dòng cấu hình."
        else:
            change_msg = detect_df_changes(df_old_blk, df_new_blk)
        
        if "Không có thay đổi" in change_msg:
            change_msg = "Đã lưu (Không có thay đổi nội dung)"
        
        log_user_action_buffered(creds, uid, f"{action_type} {blk_name}", change_msg, force_flush=True)
        
        df_oth = df_svr[df_svr[COL_BLOCK_NAME] != blk_name]
        if 'STT' in df_new_blk.columns: df_new_blk = df_new_blk.drop(columns=['STT'])
        if COL_COPY_FLAG in df_new_blk.columns: df_new_blk = df_new_blk.drop(columns=[COL_COPY_FLAG])
        if '_index' in df_new_blk.columns: df_new_blk = df_new_blk.drop(columns=['_index'])
        if 'Che_Do_Ghi' in df_new_blk.columns: df_new_blk = df_new_blk.drop(columns=['Che_Do_Ghi'])
        
        df_fin = pd.concat([df_oth, df_new_blk], ignore_index=True).astype(str).replace(['nan', 'None'], '')
        wks.clear(); 
        safe_set_with_dataframe(wks, df_fin, row=1, col=1)
        st.toast("Saved!", icon="💾")
    finally: release_lock(creds, uid)

def rename_block_action(old, new, creds, uid):
    if not acquire_lock(creds, uid): return False
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"]); wks = sh.worksheet(SHEET_CONFIG_NAME)
        # [V92] Safe
        df = safe_get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        df.loc[df[COL_BLOCK_NAME] == old, COL_BLOCK_NAME] = new
        wks.clear(); 
        safe_set_with_dataframe(wks, df, row=1, col=1)
        log_user_action_buffered(creds, uid, "Đổi tên Khối", f"{old} -> {new}", force_flush=True)
        return True
    finally: release_lock(creds, uid)

def delete_block_direct(blk, creds, uid):
    if not acquire_lock(creds, uid): return
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"]); wks = sh.worksheet(SHEET_CONFIG_NAME)
        # [V92] Safe
        df = safe_get_as_dataframe(wks, evaluate_formulas=True, dtype=str).dropna(how='all')
        df_new = df[df[COL_BLOCK_NAME] != blk]
        wks.clear(); 
        safe_set_with_dataframe(wks, df_new, row=1, col=1)
        log_user_action_buffered(creds, uid, "Xóa Khối", f"Đã xóa: {blk}", force_flush=True)
    finally: release_lock(creds, uid)

def save_full_direct(df, creds, uid):
    if not acquire_lock(creds, uid): return
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"]); wks = sh.worksheet(SHEET_CONFIG_NAME)
        df = df.astype(str).replace(['nan', 'None'], '')
        wks.clear(); 
        safe_set_with_dataframe(wks, df, row=1, col=1)
    finally: release_lock(creds, uid)

def main_ui():
    init_log_buffer()
    if not check_login(): return
    uid = st.session_state['current_user_id']; creds = get_creds()
    c1, c2 = st.columns([3, 1])
    with c1: st.title("💎 Kinkin Tool 2.0 (V92 - Tiếng Việt)", help="V92: Vietnamese Headers"); st.caption(f"User: {uid}")
    with c2: st.code(BOT_EMAIL_DISPLAY)

    with st.sidebar:
        if 'df_full_config' not in st.session_state: st.session_state['df_full_config'] = load_full_config(creds)
        if st.button("🔄 Reload"): st.cache_data.clear(); st.session_state['df_full_config'] = load_full_config(creds); st.rerun()
        df_cfg = st.session_state['df_full_config']
        blks = df_cfg[COL_BLOCK_NAME].unique().tolist() if not df_cfg.empty else [DEFAULT_BLOCK_NAME]
        if 'target_block_display' not in st.session_state: st.session_state['target_block_display'] = blks[0]
        sel_blk = st.selectbox("Chọn Khối:", blks, index=blks.index(st.session_state['target_block_display']) if st.session_state['target_block_display'] in blks else 0)
        st.session_state['target_block_display'] = sel_blk 

        if st.button("©️ Copy Block"):
             new_b = f"{sel_blk}_copy"
             bd = df_cfg[df_cfg[COL_BLOCK_NAME] == sel_blk].copy(); bd[COL_BLOCK_NAME] = new_b
             st.session_state['df_full_config'] = pd.concat([df_cfg, bd], ignore_index=True)
             save_block_config_to_sheet(bd, new_b, creds, uid); st.session_state['target_block_display'] = new_b; st.rerun()

        with st.expander("⏰ Lịch chạy tự động", expanded=True):
            df_sched = load_scheduler_config(creds)
            if SCHED_COL_BLOCK in df_sched.columns: curr_row = df_sched[df_sched[SCHED_COL_BLOCK] == sel_blk]
            else: curr_row = pd.DataFrame()
            d_type = str(curr_row.iloc[0].get(SCHED_COL_TYPE, "Không chạy")) if not curr_row.empty else "Không chạy"
            d_val1 = str(curr_row.iloc[0].get(SCHED_COL_VAL1, "")) if not curr_row.empty else ""
            d_val2 = str(curr_row.iloc[0].get(SCHED_COL_VAL2, "")) if not curr_row.empty else ""
            
            if d_type != "Không chạy": st.info(f"✅ Đang cài: {d_type} | {d_val1} {d_val2}")
            else: st.info("⚪ Chưa cài đặt lịch")

            opts = ["Không chạy", "Chạy theo phút", "Hàng ngày", "Hàng tuần", "Hàng tháng"]
            new_type = st.selectbox("Kiểu:", opts, index=opts.index(d_type) if d_type in opts else 0)
            n_val1 = d_val1; n_val2 = d_val2
            
            if new_type == "Chạy theo phút":
                v = int(d_val1) if (d_type == "Chạy theo phút" and d_val1.isdigit()) else 50
                n_val1 = str(st.slider("Tần suất (Phút):", 30, 180, max(30, v), 10))
                hrs = [f"{i:02d}:00" for i in range(24)]
                idx_h = hrs.index(d_val2) if (d_type=="Chạy theo phút" and d_val2 in hrs) else 8
                n_val2 = st.selectbox("Giờ bắt đầu:", hrs, index=idx_h)
            elif new_type == "Hàng ngày":
                hours = [f"{i:02d}:00" for i in range(24)]
                idx = hours.index(d_val1) if (d_type=="Hàng ngày" and d_val1 in hours) else 8
                n_val1 = st.selectbox("Giờ:", hours, index=idx)
            elif new_type == "Hàng tuần":
                hours = [f"{i:02d}:00" for i in range(24)]
                days = ["T2", "T3", "T4", "T5", "T6", "T7", "CN"]
                od = [x.strip() for x in d_val2.split(",")] if d_type=="Hàng tuần" else []
                sel_d = st.multiselect("Thứ:", days, default=[d for d in od if d in days])
                n_val1 = st.selectbox("Giờ:", hours, index=hours.index(d_val1) if (d_type=="Hàng tuần" and d_val1 in hours) else 8)
                n_val2 = ",".join(sel_d)
            elif new_type == "Hàng tháng":
                dates = [str(i) for i in range(1,32)]
                od = [x.strip() for x in d_val2.split(",")] if d_type=="Hàng tháng" else []
                sel_d = st.multiselect("Ngày:", dates, default=[d for d in od if d in dates])
                n_val1 = st.selectbox("Giờ:", [f"{i:02d}:00" for i in range(24)], index=8)
                n_val2 = ",".join(sel_d)

            if st.button("💾 Lưu Lịch"):
                if SCHED_COL_BLOCK in df_sched.columns: df_sched = df_sched[df_sched[SCHED_COL_BLOCK] != sel_blk]
                new_r = {SCHED_COL_BLOCK: sel_blk, SCHED_COL_TYPE: new_type, SCHED_COL_VAL1: n_val1, SCHED_COL_VAL2: n_val2}
                df_sched = pd.concat([df_sched, pd.DataFrame([new_r])], ignore_index=True)
                save_scheduler_config(df_sched, creds, uid, new_type, n_val1, n_val2)
                st.success("Đã lưu!"); time.sleep(1); st.rerun()

        with st.expander("⚙️ Manager"):
            new_b = st.text_input("New Name:")
            if st.button("➕ Add"):
                row = {c: "" for c in df_cfg.columns}; row[COL_BLOCK_NAME] = new_b; row[COL_STATUS] = "Chưa chốt & đang cập nhật"
                st.session_state['df_full_config'] = pd.concat([df_cfg, pd.DataFrame([row])], ignore_index=True)
                st.session_state['target_block_display'] = new_b; st.rerun()
            rn = st.text_input("Rename to:", value=sel_blk)
            if st.button("✏️ Rename") and rn != sel_blk:
                if rename_block_action(sel_blk, rn, creds, uid): st.cache_data.clear(); del st.session_state['df_full_config']; st.session_state['target_block_display'] = rn; st.rerun()
            if st.button("🗑️ Delete"): delete_block_direct(sel_blk, creds, uid); st.cache_data.clear(); del st.session_state['df_full_config']; st.rerun()
        st.divider(); 
        if st.button("📝 Note", use_container_width=True): show_note_popup(creds, blks, uid)
        if st.button("📚 Hướng dẫn", use_container_width=True): show_guide_popup()

    st.subheader(f"Config: {sel_blk}")
    curr_df = st.session_state['df_full_config'][st.session_state['df_full_config'][COL_BLOCK_NAME] == sel_blk].copy().reset_index(drop=True)
    if COL_COPY_FLAG not in curr_df.columns: curr_df.insert(0, COL_COPY_FLAG, False)
    if 'STT' not in curr_df.columns: curr_df.insert(1, 'STT', range(1, len(curr_df)+1))
    
    edt_df = st.data_editor(
        curr_df,
        column_order=[COL_COPY_FLAG, "STT", COL_STATUS, COL_WRITE_MODE, COL_DATA_RANGE, COL_MONTH, COL_SRC_LINK, COL_SRC_SHEET, COL_TGT_LINK, COL_TGT_SHEET, COL_FILTER, COL_HEADER, COL_RESULT, COL_LOG_ROW],
        column_config={
            COL_COPY_FLAG: st.column_config.CheckboxColumn("Copy", width="small", default=False),
            "STT": st.column_config.NumberColumn("STT", width="small", disabled=True),
            COL_STATUS: st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt & đang cập nhật", "Đã chốt"], required=True),
            COL_WRITE_MODE: st.column_config.SelectboxColumn("Cách ghi", options=["Ghi Đè", "Ghi Nối Tiếp"], default="Ghi Đè", required=True),
            COL_DATA_RANGE: st.column_config.TextColumn("Vùng lấy", width="small", default="Lấy hết"),
            COL_MONTH: st.column_config.TextColumn("Tháng", width="small"),
            COL_SRC_LINK: st.column_config.LinkColumn("Link nguồn", width="medium"), 
            COL_TGT_LINK: st.column_config.LinkColumn("Link đích", width="medium"),
            COL_FILTER: st.column_config.TextColumn("Điều kiện lọc", width="medium"),
            COL_HEADER: st.column_config.CheckboxColumn("Lấy Header?", default=False), 
            COL_RESULT: st.column_config.TextColumn("Kết quả", disabled=True),
            COL_LOG_ROW: st.column_config.TextColumn("Log Row", disabled=True),
            COL_BLOCK_NAME: None 
        }, use_container_width=True, num_rows="dynamic", key="edt_v92"
    )

    if edt_df[COL_COPY_FLAG].any():
        nw = []
        for i, r in edt_df.iterrows():
            rc = r.copy(); rc[COL_COPY_FLAG] = False; nw.append(rc)
            if r[COL_COPY_FLAG]: cp = r.copy(); cp[COL_COPY_FLAG] = False; nw.append(cp)
        st.session_state['df_full_config'] = pd.concat([st.session_state['df_full_config'][st.session_state['df_full_config'][COL_BLOCK_NAME] != sel_blk], pd.DataFrame(nw)], ignore_index=True)
        st.rerun()

    st.divider()
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        if st.button("▶️ RUN BLOCK", type="primary", use_container_width=True):
            save_block_config_to_sheet(edt_df, sel_blk, creds, uid)
            rows = []
            for i, r in edt_df.iterrows():
                if str(r.get(COL_STATUS,'')).strip() == "Chưa chốt & đang cập nhật":
                    r_dict = r.to_dict(); r_dict['_index'] = i; rows.append(r_dict)
            if not rows: st.warning("Không có dòng nào để chạy."); st.stop()
            st_cont = st.status(f"🚀 Đang chạy {sel_blk}...", expanded=True)
            ok, res, tot = process_pipeline_mixed(rows, uid, sel_blk, st_cont)
            if isinstance(res, dict):
                for i, r in edt_df.iterrows():
                    if i in res: 
                        edt_df.at[i, COL_RESULT] = res[i][0]; edt_df.at[i, COL_LOG_ROW] = res[i][1]
                save_block_config_to_sheet(edt_df, sel_blk, creds, uid)
                st_cont.update(label=f"Done! {tot} rows.", state="complete", expanded=False)
            else: st_cont.update(label="Hệ thống bận!", state="error", expanded=False)
            st.cache_data.clear(); time.sleep(1); st.rerun()
    
    with c2:
        if st.button("⏩ RUN ALL BLOCKS", use_container_width=True):
            full_df = st.session_state['df_full_config']
            all_blocks = full_df[COL_BLOCK_NAME].unique().tolist()
            if not all_blocks: st.warning("Không có khối nào."); st.stop()
            main_status = st.status("🚀 Khởi động chuỗi xử lý...", expanded=True)
            total_processed = 0
            for idx, blk in enumerate(all_blocks):
                main_status.write(f"⏳ [{idx+1}/{len(all_blocks)}] Đang xử lý: **{blk}**...")
                blk_df = full_df[full_df[COL_BLOCK_NAME] == blk].copy().reset_index(drop=True)
                rows_to_run = []
                for i, r in blk_df.iterrows():
                    if str(r.get(COL_STATUS,'')).strip() == "Chưa chốt & đang cập nhật":
                        r_dict = r.to_dict(); r_dict['_index'] = i; rows_to_run.append(r_dict)
                if not rows_to_run:
                    main_status.write(f"⚪ {blk}: Không có dòng active. Bỏ qua."); continue
                ok, res, tot = process_pipeline_mixed(rows_to_run, uid, blk, main_status)
                total_processed += tot
                if isinstance(res, dict):
                    has_change = False
                    for i, r in blk_df.iterrows():
                        if i in res:
                            blk_df.at[i, COL_RESULT] = res[i][0]; blk_df.at[i, COL_LOG_ROW] = res[i][1]; has_change = True
                    if has_change:
                        save_block_config_to_sheet(blk_df, blk, creds, uid); main_status.write(f"✅ {blk}: Xong ({tot} dòng).")
                    else: main_status.write(f"⚠️ {blk}: Không có phản hồi.")
                else: main_status.write(f"❌ {blk}: Lỗi hệ thống.")
            main_status.update(label=f"🎉 Hoàn tất! Tổng {total_processed} dòng.", state="complete", expanded=False)
            st.cache_data.clear(); st.toast("Done!", icon="🏁"); time.sleep(2); st.rerun()

    with c3:
        if st.button("🔍 Quét Quyền", use_container_width=True):
            with st.status("Checking...", expanded=True) as st_chk: check_permissions_ui(edt_df.to_dict('records'), creds, st_chk, uid)
    
    with c4:
        if st.button("💾 Save Config", use_container_width=True): save_block_config_to_sheet(edt_df, sel_blk, creds, uid); st.rerun()

    flush_logs(creds, force=True)
    st.divider(); st.caption("Logs")
    if st.button("Refresh Logs"): st.cache_data.clear()
    logs = fetch_activity_logs(creds, 50)
    if not logs.empty: st.dataframe(logs, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main_ui()
