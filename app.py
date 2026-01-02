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
from collections import defaultdict, Counter
from st_copy_to_clipboard import st_copy_to_clipboard

# ==========================================
# 1. CẤU HÌNH HỆ THỐNG
# ==========================================
st.set_page_config(page_title="Kinkin Tool 2.0 (V108 - Perf & Feat)", layout="wide", page_icon="⚡")

# 🟢 DANH SÁCH 5 BOT (User điền)
MY_BOT_LIST = [
    "getdulieu@kin-kin-477902.iam.gserviceaccount.com", # Bot 1
    "botnew@kinkin2.iam.gserviceaccount.com",          # Bot 2
    "kinkingetdulieu3@kinkin3.iam.gserviceaccount.com", # Bot 3
    "email_bot_4@gmail.com",                            # Bot 4
    "email_bot_5@gmail.com"                             # Bot 5
]

AUTHORIZED_USERS = {
    "admin2025": "Admin_Master",
    "team_hn": "Team_HaNoi",
    "team_hcm": "Team_HCM"
}

# Tên Sheet
SHEET_CONFIG_NAME = "luu_cau_hinh" 
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_ACTIVITY_NAME = "log_hanh_vi"
SHEET_LOCK_NAME = "sys_lock"
SHEET_SYS_CONFIG = "sys_config"
SHEET_NOTE_NAME = "database_ghi_chu"
SHEET_SYS_STATE = "sys_state"

# --- ĐỊNH NGHĨA CỘT ---
COL_BLOCK_NAME = "Block_Name"; COL_STATUS = "Trạng thái"; COL_WRITE_MODE = "Cach_Ghi"
COL_DATA_RANGE = "Vùng lấy dữ liệu"; COL_MONTH = "Tháng"; COL_SRC_LINK = "Link dữ liệu lấy dữ liệu"
COL_TGT_LINK = "Link dữ liệu đích"; COL_SRC_SHEET = "Tên sheet nguồn dữ liệu gốc"
COL_TGT_SHEET = "Tên sheet dữ liệu đích"; COL_RESULT = "Kết quả"; COL_LOG_ROW = "Dòng dữ liệu"
COL_FILTER = "Dieu_Kien_Loc"; COL_HEADER = "Lay_Header"; COL_COPY_FLAG = "Copy_Flag"

REQUIRED_COLS_CONFIG = [
    COL_BLOCK_NAME, COL_STATUS, COL_WRITE_MODE, COL_DATA_RANGE, COL_MONTH, 
    COL_SRC_LINK, COL_TGT_LINK, COL_TGT_SHEET, COL_SRC_SHEET, 
    COL_RESULT, COL_LOG_ROW, COL_FILTER, COL_HEADER
]

SCHED_COL_BLOCK = "Block_Name"; SCHED_COL_TYPE = "Loai_Lich"
SCHED_COL_VAL1 = "Thong_So_Chinh"; SCHED_COL_VAL2 = "Thong_So_Phu"
REQUIRED_COLS_SCHED = [SCHED_COL_BLOCK, SCHED_COL_TYPE, SCHED_COL_VAL1, SCHED_COL_VAL2]

NOTE_COL_ID = "ID"; NOTE_COL_BLOCK = "Tên Khối"; NOTE_COL_CONTENT = "Nội dung Note"
REQUIRED_COLS_NOTE = [NOTE_COL_ID, NOTE_COL_BLOCK, NOTE_COL_CONTENT]

# [V108] Thêm cột Thời điểm ghi
SYS_COL_LINK = "Src_Link"; SYS_COL_SHEET = "Src_Sheet"; SYS_COL_MONTH = "Month"
SYS_COL_TIME = "Thời điểm ghi"

DEFAULT_BLOCK_NAME = "Block_Mac_Dinh"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
LOG_BUFFER_SIZE = 5; LOG_FLUSH_INTERVAL = 10 

# ==========================================
# 2. AUTHENTICATION & BOT ENGINE
# ==========================================
def get_master_creds():
    try:
        raw = st.secrets["gcp_service_account"]
        info = json.loads(raw) if isinstance(raw, str) else dict(raw)
        if "private_key" in info: info["private_key"] = info["private_key"].replace("\\n", "\n")
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    except: return None

def get_bot_credentials_from_secrets(target_email):
    try:
        raw = st.secrets["gcp_service_account"]
        info = json.loads(raw) if isinstance(raw, str) else dict(raw)
        if info.get("client_email") == target_email:
            if "private_key" in info: info["private_key"] = info["private_key"].replace("\\n", "\n")
            return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    except: pass
    all_secs = st.secrets.to_dict() if hasattr(st.secrets, "to_dict") else dict(st.secrets)
    for key in all_secs:
        if key.startswith("gcp_service_account_"):
            try:
                raw = all_secs[key]
                info = json.loads(raw) if isinstance(raw, str) else dict(raw)
                if info.get("client_email") == target_email:
                    if "private_key" in info: info["private_key"] = info["private_key"].replace("\\n", "\n")
                    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            except: pass
    return None

def assign_bot_to_block(block_name):
    valid_bots = [b for b in MY_BOT_LIST if b.strip() and "@" in b]
    if not valid_bots: return "No_Bot_Configured"
    hash_val = sum(ord(c) for c in block_name)
    return valid_bots[hash_val % len(valid_bots)]

# --- STANDARD UTILS ---
def safe_api_call(func, *args, **kwargs):
    for i in range(5):
        try: return func(*args, **kwargs)
        except Exception as e:
            if "429" in str(e) or "quota" in str(e).lower(): time.sleep((2**i)+3)
            elif i==4: raise e
            else: time.sleep(2)
    return None

def safe_get_as_dataframe(wks, **kwargs): return safe_api_call(get_as_dataframe, wks, **kwargs)
def safe_set_with_dataframe(wks, df, **kwargs): return safe_api_call(set_with_dataframe, wks, df, **kwargs)
def get_sh_with_retry(creds, sid): gc = gspread.authorize(creds); return safe_api_call(gc.open_by_key, sid)

def extract_id(url):
    if not isinstance(url, str): return None
    try: return url.split("/d/")[1].split("/")[0]
    except: return None
def col_name_to_index(col):
    col = col.upper(); idx=0
    for c in col: idx = idx*26 + (ord(c)-ord('A'))+1
    return idx-1
def ensure_sheet_headers(wks, required_columns):
    try:
        if not wks.row_values(1): wks.append_row(required_columns)
    except: pass

# --- LOGGING ---
def init_log_buffer():
    if 'log_buffer' not in st.session_state: st.session_state['log_buffer'] = []
    if 'last_log_flush' not in st.session_state: st.session_state['last_log_flush'] = time.time()
def flush_logs(creds, force=False):
    buf = st.session_state.get('log_buffer', [])
    if (force or len(buf)>=LOG_BUFFER_SIZE) and buf:
        try:
            sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
            try: wks = sh.worksheet(SHEET_ACTIVITY_NAME)
            except: wks = sh.add_worksheet(SHEET_ACTIVITY_NAME, 1000, 4)
            safe_api_call(wks.append_rows, buf); st.session_state['log_buffer'] = []
        except: pass
def log_user_action_buffered(creds, user_id, action, status="", force_flush=False):
    init_log_buffer()
    st.session_state['log_buffer'].append([datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime("%d/%m/%Y %H:%M:%S"), user_id, action, status])
    flush_logs(creds, force=force_flush)

def detect_df_changes(df_old, df_new):
    if len(df_old) != len(df_new): return f"Thay đổi dòng: {len(df_old)} -> {len(df_new)}"
    changes = []
    ignore = [COL_BLOCK_NAME, COL_LOG_ROW, COL_RESULT, "STT", COL_COPY_FLAG, "_index"]
    cols = [c for c in df_new.columns if c not in ignore and c in df_old.columns]
    dfo = df_old.reset_index(drop=True); dfn = df_new.reset_index(drop=True)
    for i in range(len(dfo)):
        for c in cols:
            vo=str(dfo.at[i,c]).strip(); vn=str(dfn.at[i,c]).strip()
            if vo!=vn: changes.append(f"Dòng {i+1} [{c}]: {vo}->{vn}")
    return " | ".join(changes) if changes else "Không thay đổi"

# --- UTILS UI ---
def acquire_lock(creds, user_id):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_LOCK_NAME)
        except: wks = sh.add_worksheet(SHEET_LOCK_NAME, 10, 5); wks.update([["FALSE", "", ""]])
        val = wks.cell(2, 1).value; user = wks.cell(2, 2).value
        if val == "TRUE" and user != user_id: return False
        wks.update("A2:C2", [["TRUE", user_id, datetime.now().strftime("%d/%m/%Y %H:%M:%S")]])
        return True
    except: return False

def release_lock(creds, user_id):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_LOCK_NAME)
        if wks.cell(2, 2).value == user_id: wks.update("A2:C2", [["FALSE", "", ""]])
    except: pass

def load_notes_data(creds):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_NOTE_NAME)
        except: wks = sh.add_worksheet(SHEET_NOTE_NAME, rows=100, cols=5); ensure_sheet_headers(wks, REQUIRED_COLS_NOTE)
        df = safe_get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        return df.dropna(how='all') if not df.empty else pd.DataFrame(columns=REQUIRED_COLS_NOTE)
    except: return pd.DataFrame(columns=REQUIRED_COLS_NOTE)

def save_notes_data(df_notes, creds, user_id, block_name):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_NOTE_NAME)
        for i, row in df_notes.iterrows():
            if not row[NOTE_COL_ID]: df_notes.at[i, NOTE_COL_ID] = str(uuid.uuid4())[:8]
        safe_set_with_dataframe(wks, df_notes, row=1, col=1)
        log_user_action_buffered(creds, user_id, "Lưu Ghi Chú", f"Cập nhật note cho {block_name}", force_flush=True)
        return True
    except: return False

@st.dialog("📝 Note", width="large")
def show_note_popup(creds, all_blocks, user_id):
    if 'df_notes_temp' not in st.session_state: st.session_state['df_notes_temp'] = load_notes_data(creds)
    df = st.session_state['df_notes_temp']
    edt = st.data_editor(df, num_rows="dynamic", use_container_width=True,
        column_config={
            NOTE_COL_ID: st.column_config.TextColumn("ID", disabled=True, width="small"),
            NOTE_COL_BLOCK: st.column_config.SelectboxColumn("Khối", options=all_blocks, required=True),
            NOTE_COL_CONTENT: st.column_config.TextColumn("Nội dung", width="large")
        }, key="note_popup")
    if st.button("💾 Lưu Note", type="primary"):
        if save_notes_data(edt, creds, user_id, "All"): st.success("Đã lưu!"); time.sleep(1); st.rerun()

def load_scheduler_config(creds):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_SYS_CONFIG)
        except: wks = sh.add_worksheet(SHEET_SYS_CONFIG, 50, 5); wks.append_row(REQUIRED_COLS_SCHED)
        ensure_sheet_headers(wks, REQUIRED_COLS_SCHED)
        df = safe_get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        return df.dropna(how='all') if not df.empty else pd.DataFrame(columns=REQUIRED_COLS_SCHED)
    except: return pd.DataFrame(columns=REQUIRED_COLS_SCHED)

def save_scheduler_config(df_sched, creds, user_id, type_run, v1, v2):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_SYS_CONFIG)
        cols = REQUIRED_COLS_SCHED
        for c in cols:
            if c not in df_sched.columns: df_sched[c] = ""
        wks.clear(); safe_set_with_dataframe(wks, df_sched[cols].fillna(""), row=1, col=1)
        msg = f"Cài đặt: {type_run} | {v1} {v2}".strip()
        log_user_action_buffered(creds, user_id, "Cài Lịch Chạy", msg, force_flush=True)
        return True
    except: return False

def fetch_activity_logs(creds, limit=50):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_ACTIVITY_NAME)
        df = safe_get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        return df.tail(limit).iloc[::-1] if not df.empty else pd.DataFrame()
    except: return pd.DataFrame()

def write_detailed_log(creds, log_data_list):
    if not log_data_list: return
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_LOG_NAME)
        except: 
            wks = sh.add_worksheet(SHEET_LOG_NAME, rows=1000, cols=15)
            wks.append_row(["Thời gian", "Vùng lấy", "Tháng", "User", "Link Nguồn", "Link Đích", "Sheet Đích", "Sheet Nguồn", "Kết Quả", "Số Dòng", "Range", "Block"])
        
        cleaned = [[str(x) for x in row] for row in log_data_list]
        safe_api_call(wks.append_rows, cleaned)
    except: pass

# ==========================================
# 4. CORE ETL
# ==========================================
def apply_smart_filter_v90(df, filter_str, debug_container=None):
    if not filter_str or str(filter_str).strip().lower() in ['nan', 'none', 'null', '']: return df, None
    conditions = str(filter_str).split(';')
    current_df = df.copy()
    if debug_container: debug_container.markdown(f"**🔍 Lọc: {len(current_df)} dòng gốc**")
    for cond in conditions:
        fs = cond.strip()
        if not fs: continue 
        op_list = [" contains ", "==", "!=", ">=", "<=", ">", "<", "="]
        op = next((o for o in op_list if o in fs), None)
        if not op: return None, f"Lỗi cú pháp: '{fs}'"
        parts = fs.split(op, 1); col_raw = parts[0].strip().replace("`", "").replace("'", "").replace('"', ""); val_raw = parts[1].strip()
        val_clean = val_raw[1:-1] if (val_raw.startswith("'") or val_raw.startswith('"')) else val_raw
        
        real_col = next((c for c in current_df.columns if str(c).lower() == col_raw.lower()), None)
        if not real_col: return None, f"Không tìm thấy cột '{col_raw}'"
        
        try:
            series = current_df[real_col]
            if op == " contains ": current_df = current_df[series.astype(str).str.contains(val_clean, case=False, na=False)]
            else:
                # Logic so sánh
                is_dt = False
                try: 
                    s_dt = pd.to_datetime(series, dayfirst=True, errors='coerce'); v_dt = pd.to_datetime(val_clean, dayfirst=True)
                    if s_dt.notna().any(): is_dt = True
                except: pass
                
                is_num = False
                if not is_dt:
                    try: s_num = pd.to_numeric(series, errors='coerce'); v_num = float(val_clean); is_num = True
                    except: pass
                
                if is_dt:
                    if op==">": current_df=current_df[s_dt>v_dt]
                    elif op=="<": current_df=current_df[s_dt<v_dt]
                    elif op==">=": current_df=current_df[s_dt>=v_dt]
                    elif op=="<=": current_df=current_df[s_dt<=v_dt]
                    elif op in ["=","=="]: current_df=current_df[s_dt==v_dt]
                    elif op=="!=": current_df=current_df[s_dt!=v_dt]
                elif is_num:
                    if op==">": current_df=current_df[s_num>v_num]
                    elif op=="<": current_df=current_df[s_num<v_num]
                    elif op==">=": current_df=current_df[s_num>=v_num]
                    elif op=="<=": current_df=current_df[s_num<=v_num]
                    elif op in ["=","=="]: current_df=current_df[s_num==v_num]
                    elif op=="!=": current_df=current_df[s_num!=v_num]
                else:
                    s_str = series.astype(str).str.strip()
                    if op==">": current_df=current_df[s_str>str(val_clean)]
                    elif op=="<": current_df=current_df[s_str<str(val_clean)]
                    elif op==">=": current_df=current_df[s_str>=str(val_clean)]
                    elif op=="<=": current_df=current_df[s_str<=str(val_clean)]
                    elif op in ["=","=="]: current_df=current_df[s_str==str(val_clean)]
                    elif op=="!=": current_df=current_df[s_str!=str(val_clean)]
            if debug_container: debug_container.caption(f"👉 Lọc '{val_clean}' -> Còn {len(current_df)}")
        except Exception as e: return None, f"Lỗi '{fs}': {e}"
    return current_df, None

def fetch_data_v4(row_config, bot_creds, target_headers=None, status_container=None):
    link_src = str(row_config.get(COL_SRC_LINK, '')).strip()
    source_label = str(row_config.get(COL_SRC_SHEET, '')).strip()
    month_val = str(row_config.get(COL_MONTH, ''))
    raw_range = str(row_config.get(COL_DATA_RANGE, '')).strip()
    data_range_str = "Lấy hết" if raw_range.lower() in ['nan', 'none', 'null', '', 'lấy hết'] else raw_range
    raw_filter = str(row_config.get(COL_FILTER, '')).strip()
    if raw_filter.lower() in ['nan', 'none', 'null']: raw_filter = ""
    
    # [V108] Checkbox logic fix: Convert string/bool correctly
    h_val = row_config.get(COL_HEADER, False)
    include_header = str(h_val).strip().upper() == 'TRUE' if isinstance(h_val, str) else bool(h_val)
    
    sheet_id = extract_id(link_src)
    if not sheet_id: return None, sheet_id, "Link lỗi"
    
    try:
        sh_source = get_sh_with_retry(bot_creds, sheet_id)
        wks_source = sh_source.worksheet(source_label) if source_label else sh_source.sheet1
        data = safe_api_call(wks_source.get_all_values)
        if not data: return pd.DataFrame(), sheet_id, "Sheet trắng"

        header_row = data[0]; body_rows = data[1:]
        unique_headers = []
        seen = {}
        for col in header_row:
            if col in seen: seen[col] += 1; unique_headers.append(f"{col}_{seen[col]}")
            else: seen[col] = 0; unique_headers.append(col)
        
        df_working = pd.DataFrame(body_rows, columns=unique_headers)

        if target_headers:
            min_cols = min(len(df_working.columns), len(target_headers))
            rename_map = {df_working.columns[i]: target_headers[i] for i in range(min_cols)}
            df_working = df_working.rename(columns=rename_map).iloc[:, :len(target_headers)]

        if data_range_str != "Lấy hết" and ":" in data_range_str:
            try:
                s, e = data_range_str.split(":")
                s_idx = col_name_to_index(s.strip()); e_idx = col_name_to_index(e.strip())
                if s_idx >= 0: df_working = df_working.iloc[:, s_idx : e_idx + 1]
            except: pass

        if raw_filter:
            df_filtered, err = apply_smart_filter_v90(df_working, raw_filter, debug_container=status_container)
            if err: return None, sheet_id, f"⚠️ {err}"; 
            df_working = df_filtered

        if include_header:
            df_header_row = pd.DataFrame([df_working.columns.tolist()], columns=df_working.columns)
            df_final = pd.concat([df_header_row, df_working], ignore_index=True)
        else: df_final = df_working

        df_final = df_final.astype(str).replace(['nan', 'None', '<NA>', 'null'], '')
        
        # [V108] Thêm cột hệ thống: Link, Sheet, Month, Time
        df_final[SYS_COL_LINK] = link_src.strip()
        df_final[SYS_COL_SHEET] = source_label.strip()
        df_final[SYS_COL_MONTH] = month_val.strip()
        df_final[SYS_COL_TIME] = datetime.now().strftime("%d/%m/%Y") # New Column
        
        return df_final, sheet_id, "Thành công"
    except Exception as e: return None, sheet_id, f"Lỗi tải: {str(e)}"

def get_rows_to_delete_dynamic(wks, keys_to_delete, log_container):
    all_values = safe_api_call(wks.get_all_values)
    if not all_values: return []
    headers = all_values[0]
    try: 
        # Chỉ check 3 key chính để xóa (Time không dùng để định danh xóa)
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
    requests = [{"deleteDimension": {"range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": s-1, "endIndex": e}}} for s, e in ranges]
    for i in range(0, len(requests), 100):
        if log_container: log_container.write(f"✂️ Xóa batch {i//100 + 1}...")
        safe_api_call(sh.batch_update, {'requests': requests[i:i+100]})
        time.sleep(1)

def write_strict_sync_v2(tasks_list, target_link, target_sheet_name, bot_creds, log_container):
    result_map = {}; debug_data = [] 
    try:
        target_id = extract_id(target_link)
        if not target_id: return False, "Link lỗi", {}, []
        sh = get_sh_with_retry(bot_creds, target_id)
        real_sheet_name = str(target_sheet_name).strip() or "Tong_Hop_Data"
        log_container.write(f"📂 Đích: ...{target_link[-10:]} | Sheet: {real_sheet_name}")
        
        all_titles = [s.title for s in safe_api_call(sh.worksheets)]
        if real_sheet_name in all_titles: wks = sh.worksheet(real_sheet_name)
        else: wks = sh.add_worksheet(title=real_sheet_name, rows=1000, cols=20); log_container.write(f"✨ Tạo mới sheet: {real_sheet_name}")
        
        df_new_all = pd.DataFrame()
        for df, _, _, _ in tasks_list: df_new_all = pd.concat([df_new_all, df], ignore_index=True)
        if df_new_all.empty: return True, "No Data", {}, []

        existing_headers = safe_api_call(wks.row_values, 1)
        if not existing_headers:
            final_headers = df_new_all.columns.tolist()
            wks.update(range_name="A1", values=[final_headers])
            existing_headers = final_headers
            log_container.write("🆕 Tạo Header mới.")
        else:
            updated = existing_headers.copy(); added = False
            # [V108] Đảm bảo có đủ 4 cột hệ thống
            for col in [SYS_COL_LINK, SYS_COL_SHEET, SYS_COL_MONTH, SYS_COL_TIME]:
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
                time.sleep(2) 
        
        current_data = safe_api_call(wks.get_all_values)
        start_row = (len(current_data) + 1) if current_data else 1
        
        log_container.write(f"🚀 Ghi {len(df_aligned)} dòng mới (từ dòng {start_row})...")
        chunk_size = 5000
        new_vals = df_aligned.fillna('').values.tolist()
        for i in range(0, len(new_vals), chunk_size):
            safe_api_call(wks.append_rows, new_vals[i:i+chunk_size], value_input_option='USER_ENTERED')
            time.sleep(1)

        current_cursor = int(start_row)
        for df, src_link, r_idx, w_mode in tasks_list:
            count = len(df); rng_str = "0 dòng"
            if count > 0:
                end = current_cursor + count - 1; rng_str = f"{current_cursor} - {end}"; current_cursor += count
            result_map[r_idx] = ("Thành công", rng_str, count)
            debug_data.append({"File": src_link[-10:], "Mode": w_mode, "Start": current_cursor - count, "End": end if count >0 else 0, "Range Log": rng_str})
            
        return True, f"Cập nhật {len(df_aligned)} dòng", result_map, debug_data
    except Exception as e: return False, f"Lỗi Ghi: {str(e)}", {}, []

# --- CHECK PERMISSION ---
def verify_access_fast(url, creds):
    sid = extract_id(url)
    if not sid: return False, "Lỗi Link"
    try: get_sh_with_retry(creds, sid); return True, "OK"
    except: return False, "Chặn"

def check_permissions_ui(rows, creds, container, user_id):
    log_user_action_buffered(creds, user_id, "Quét Quyền", "Bắt đầu...", force_flush=False)
    src_links = set(); tgt_links = set()
    for r in rows:
        if "docs.google.com" in str(r.get(COL_SRC_LINK, '')): src_links.add(str(r.get(COL_SRC_LINK, '')).strip())
        if "docs.google.com" in str(r.get(COL_TGT_LINK, '')): tgt_links.add(str(r.get(COL_TGT_LINK, '')).strip())
    
    all_unique_links = list(src_links.union(tgt_links))
    if not all_unique_links: container.info("Không tìm thấy link nào."); return
    
    prog = container.progress(0); err_count = 0
    for i, link in enumerate(all_unique_links):
        prog.progress((i + 1) / len(all_unique_links)); time.sleep(0.1)
        ok, msg = verify_access_fast(link, creds)
        if not ok:
            err_count += 1; msgs = []
            if link in src_links: msgs.append("Link Nguồn: Cần quyền XEM")
            if link in tgt_links: msgs.append("Link Đích: Cần quyền SỬA")
            container.error(f"❌ {link}\n👉 {' & '.join(msgs)}")
    
    if err_count == 0: container.success("✅ Tuyệt vời! Bot đã có đủ quyền.")
    else: container.warning(f"⚠️ {err_count} link thiếu quyền.")
    log_user_action_buffered(creds, user_id, "Quét Quyền", f"Lỗi: {err_count}", force_flush=True)

def process_pipeline_mixed(rows_to_run, user_id, block_name_run, status_container, forced_bot=None):
    master_creds = get_master_creds()
    if not acquire_lock(master_creds, user_id): st.error("⚠️ Hệ thống bận!"); return False, {}, 0
    
    assigned_bot_email = forced_bot if forced_bot else assign_bot_to_block(block_name_run)
    log_user_action_buffered(master_creds, user_id, f"Chạy: {block_name_run}", f"Bot: {assigned_bot_email}", force_flush=True)
    
    try:
        bot_creds = get_bot_credentials_from_secrets(assigned_bot_email)
        if not bot_creds:
            st.error(f"❌ Không tìm thấy key cho {assigned_bot_email}. Check Secrets!"); return False, {}, 0

        grouped = defaultdict(list)
        for r in rows_to_run:
            if str(r.get(COL_STATUS, '')).strip() == "Chưa chốt & đang cập nhật":
                key = (str(r.get(COL_TGT_LINK, '')).strip(), str(r.get(COL_TGT_SHEET, '')).strip())
                grouped[key].append(r)
        
        final_res_map = {}; all_ok = True; total_rows = 0; log_ents = []
        all_debug_data = [] 
        tz = pytz.timezone('Asia/Ho_Chi_Minh'); now = datetime.now(tz).strftime("%d/%m/%Y %H:%M:%S")

        for idx, ((t_link, t_sheet), group_rows) in enumerate(grouped.items()):
            with status_container.expander(f"🤖 [{assigned_bot_email}] -> {t_sheet}", expanded=True):
                target_headers = []
                try:
                    tid = extract_id(t_link)
                    if tid:
                        sh_t = get_sh_with_retry(bot_creds, tid)
                        if t_sheet in [s.title for s in safe_api_call(sh_t.worksheets)]:
                            target_headers = safe_api_call(sh_t.worksheet(t_sheet).row_values, 1)
                except: pass

                tasks = []
                for i, r in enumerate(group_rows):
                    lnk = r.get(COL_SRC_LINK, ''); lbl = r.get(COL_SRC_SHEET, ''); row_idx = r.get('_index', -1)
                    w_mode = str(r.get(COL_WRITE_MODE, 'Ghi Đè')).strip()
                    if w_mode not in ["Ghi Đè", "Ghi Nối Tiếp"]: w_mode = "Ghi Đè"

                    msg = st.empty(); msg.write(f"⏳ Tải: {lnk[-10:]} ({lbl})...")
                    df, sid, m = fetch_data_v4(r, bot_creds, target_headers, status_container=msg)
                    time.sleep(0.5) # [V108] Reduced delay for speed
                    
                    if df is not None: 
                        count = len(df); msg.success(f"✅ OK: {count} dòng"); tasks.append((df, lnk, row_idx, w_mode)); total_rows += len(df)
                    else: 
                        msg.error(f"❌ Lỗi: {m}"); final_res_map[row_idx] = ("Lỗi tải", "", 0)
                    del df; gc.collect()

                if tasks:
                    ok, m, batch_res, batch_db = write_strict_sync_v2(tasks, t_link, t_sheet, bot_creds, st)
                    if not ok: st.error(m); all_ok = False
                    else: st.success(m)
                    final_res_map.update(batch_res); all_debug_data.extend(batch_db)
                    del tasks; gc.collect()
                
                for r in group_rows:
                    row_idx = r.get('_index', -1)
                    res_status, res_range, res_count = final_res_map.get(row_idx, ("Lỗi", "", 0))
                    log_ents.append([now, r.get(COL_DATA_RANGE), r.get(COL_MONTH), user_id, r.get(COL_SRC_LINK), t_link, t_sheet, r.get(COL_SRC_SHEET), res_status, res_count, res_range, block_name_run])
        
        write_detailed_log(master_creds, log_ents)
        if all_debug_data: st.dataframe(pd.DataFrame(all_debug_data))
        return all_ok, final_res_map, total_rows
    finally: release_lock(master_creds, user_id)

# ==========================================
# 5. LOGIN
# ==========================================
def check_login():
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
    if 'current_user_id' not in st.session_state: st.session_state['current_user_id'] = "Unknown"
    if "auto_key" in st.query_params and st.query_params["auto_key"] in AUTHORIZED_USERS:
        st.session_state['logged_in'] = True; st.session_state['current_user_id'] = AUTHORIZED_USERS[st.query_params["auto_key"]]; return True
    if st.session_state['logged_in']: return True
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.header("🛡️ Đăng nhập")
        pwd = st.text_input("Mật khẩu:", type="password")
        if st.button("Đăng Nhập", use_container_width=True):
            if pwd in AUTHORIZED_USERS:
                st.session_state['logged_in'] = True; st.session_state['current_user_id'] = AUTHORIZED_USERS[pwd]; st.rerun()
            else: st.error("Sai mật khẩu")
    return False

# ==========================================
# 6. CONFIG LOADER & SAVER
# ==========================================
@st.cache_data
def load_full_config(_creds):
    sh = get_sh_with_retry(_creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    ensure_sheet_headers(wks, REQUIRED_COLS_CONFIG)
    df = safe_get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
    
    if df is None or df.empty: return pd.DataFrame(columns=REQUIRED_COLS_CONFIG)
    
    df = df.dropna(how='all').replace(['nan', 'None', 'NaN', '<NA>'], '')
    df[COL_BLOCK_NAME] = df[COL_BLOCK_NAME].replace('', DEFAULT_BLOCK_NAME).fillna(DEFAULT_BLOCK_NAME)
    if COL_WRITE_MODE not in df.columns: df[COL_WRITE_MODE] = "Ghi Đè"
    
    # [V108] Checkbox logic: Convert "TRUE"/"FALSE" strings to Boolean
    if COL_HEADER in df.columns:
        df[COL_HEADER] = df[COL_HEADER].astype(str).str.upper().map({'TRUE': True, 'FALSE': False}).fillna(False)
    else:
        df[COL_HEADER] = False
        
    return df

def save_block_config_to_sheet(df_ui, blk_name, creds, uid):
    if not acquire_lock(creds, uid): st.error("Busy!"); return
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        
        # [V108] Optimization: Read once, update locally
        df_svr = safe_get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if df_svr is None or df_svr.empty: df_svr = pd.DataFrame(columns=REQUIRED_COLS_CONFIG)
        else: df_svr = df_svr.dropna(how='all').replace(['nan', 'None'], '')

        if COL_BLOCK_NAME not in df_svr.columns: df_svr[COL_BLOCK_NAME] = DEFAULT_BLOCK_NAME
        
        df_old_blk = df_svr[df_svr[COL_BLOCK_NAME] == blk_name].copy().reset_index(drop=True)
        df_new_blk = df_ui.copy().reset_index(drop=True)
        
        # [V108] Convert boolean checkbox back to string "TRUE"/"FALSE" for Google Sheets
        if COL_HEADER in df_new_blk.columns:
            df_new_blk[COL_HEADER] = df_new_blk[COL_HEADER].apply(lambda x: "TRUE" if x is True or str(x).lower()=='true' else "FALSE")

        # Cleanup UI cols
        ignore = ['STT', COL_COPY_FLAG, '_index', 'Che_Do_Ghi']
        for c in ignore: 
            if c in df_new_blk.columns: df_new_blk = df_new_blk.drop(columns=[c])
        
        # Merge
        df_oth = df_svr[df_svr[COL_BLOCK_NAME] != blk_name]
        df_fin = pd.concat([df_oth, df_new_blk], ignore_index=True).astype(str).replace(['nan', 'None'], '')
        
        wks.clear(); safe_set_with_dataframe(wks, df_fin, row=1, col=1)
        st.toast("Saved!", icon="💾")
    finally: release_lock(creds, uid)

# (Rename & Delete functions optimized similarly...)
def rename_block_action(old, new, creds, uid):
    if not acquire_lock(creds, uid): return False
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"]); wks = sh.worksheet(SHEET_CONFIG_NAME)
        df = safe_get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        df.loc[df[COL_BLOCK_NAME] == old, COL_BLOCK_NAME] = new
        wks.clear(); safe_set_with_dataframe(wks, df, row=1, col=1)
        log_user_action_buffered(creds, uid, "Rename", f"{old}->{new}", force_flush=True)
        return True
    finally: release_lock(creds, uid)

def delete_block_direct(blk, creds, uid):
    if not acquire_lock(creds, uid): return
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"]); wks = sh.worksheet(SHEET_CONFIG_NAME)
        df = safe_get_as_dataframe(wks, evaluate_formulas=True, dtype=str).dropna(how='all')
        df = df[df[COL_BLOCK_NAME] != blk]
        wks.clear(); safe_set_with_dataframe(wks, df, row=1, col=1)
        log_user_action_buffered(creds, uid, "Delete", blk, force_flush=True)
    finally: release_lock(creds, uid)

# ==========================================
# 7. MAIN UI
# ==========================================
def main_ui():
    init_log_buffer()
    if not check_login(): return
    uid = st.session_state['current_user_id']; master_creds = get_master_creds()
    
    # --- [HEADER] ---
    if 'df_full_config' not in st.session_state: st.session_state['df_full_config'] = load_full_config(master_creds)
    df_cfg = st.session_state['df_full_config']
    blks = df_cfg[COL_BLOCK_NAME].unique().tolist() if not df_cfg.empty else [DEFAULT_BLOCK_NAME]
    
    with st.sidebar:
        if st.button("🔄 Reload"): st.cache_data.clear(); st.session_state['df_full_config'] = load_full_config(master_creds); st.rerun()
        if 'target_block_display' not in st.session_state: st.session_state['target_block_display'] = blks[0]
        sel_blk = st.selectbox("Chọn Khối:", blks, index=blks.index(st.session_state['target_block_display']) if st.session_state['target_block_display'] in blks else 0)
        st.session_state['target_block_display'] = sel_blk

        if st.button("©️ Copy Block"):
             new_b = f"{sel_blk}_copy"
             bd = df_cfg[df_cfg[COL_BLOCK_NAME] == sel_blk].copy(); bd[COL_BLOCK_NAME] = new_b
             st.session_state['df_full_config'] = pd.concat([df_cfg, bd], ignore_index=True)
             save_block_config_to_sheet(bd, new_b, master_creds, uid); st.session_state['target_block_display'] = new_b; st.rerun()

        # SCHEDULER (Compact)
        with st.expander("⏰ Lịch chạy tự động"):
            df_sched = load_scheduler_config(master_creds)
            curr_row = df_sched[df_sched[SCHED_COL_BLOCK] == sel_blk] if SCHED_COL_BLOCK in df_sched.columns else pd.DataFrame()
            d_type = str(curr_row.iloc[0].get(SCHED_COL_TYPE, "Không chạy")) if not curr_row.empty else "Không chạy"
            d_val1 = str(curr_row.iloc[0].get(SCHED_COL_VAL1, "")) if not curr_row.empty else ""
            
            new_type = st.selectbox("Kiểu:", ["Không chạy", "Chạy theo phút", "Hàng ngày", "Hàng tuần", "Hàng tháng"], index=["Không chạy", "Chạy theo phút", "Hàng ngày", "Hàng tuần", "Hàng tháng"].index(d_type) if d_type in ["Không chạy", "Chạy theo phút", "Hàng ngày", "Hàng tuần", "Hàng tháng"] else 0)
            n_val1 = st.text_input("Tham số 1 (Phút/Giờ):", value=d_val1)
            n_val2 = st.text_input("Tham số 2 (Ngày/Thứ):", value=str(curr_row.iloc[0].get(SCHED_COL_VAL2, "")) if not curr_row.empty else "")
            
            if st.button("💾 Lưu Lịch"):
                if SCHED_COL_BLOCK in df_sched.columns: df_sched = df_sched[df_sched[SCHED_COL_BLOCK] != sel_blk]
                new_r = {SCHED_COL_BLOCK: sel_blk, SCHED_COL_TYPE: new_type, SCHED_COL_VAL1: n_val1, SCHED_COL_VAL2: n_val2}
                df_sched = pd.concat([df_sched, pd.DataFrame([new_r])], ignore_index=True)
                save_scheduler_config(df_sched, master_creds, uid, f"{new_type}")
                st.success("Saved!"); time.sleep(1); st.rerun()

        # MANAGER
        with st.expander("⚙️ Manager"):
            new_b = st.text_input("New Block:")
            if st.button("➕ Add"):
                row = {c: "" for c in df_cfg.columns}; row[COL_BLOCK_NAME] = new_b; row[COL_STATUS] = "Chưa chốt & đang cập nhật"; row[COL_HEADER] = False
                st.session_state['df_full_config'] = pd.concat([df_cfg, pd.DataFrame([row])], ignore_index=True)
                st.session_state['target_block_display'] = new_b; st.rerun()
            rn = st.text_input("Rename to:", value=sel_blk)
            if st.button("✏️ Rename") and rn != sel_blk:
                if rename_block_action(sel_blk, rn, master_creds, uid): st.cache_data.clear(); st.session_state['target_block_display'] = rn; st.rerun()
            if st.button("🗑️ Delete"): delete_block_direct(sel_blk, master_creds, uid); st.cache_data.clear(); st.rerun()
        
        st.divider()
        if st.button("📝 Note", use_container_width=True): show_note_popup(master_creds, blks, uid)
        if st.button("📚 HDSD", use_container_width=True): st.info("1 Block = 1 Bot. Hệ thống tự động phân tải.")

    assigned_bot = assign_bot_to_block(sel_blk)
    c_head_1, c_head_2 = st.columns([3, 1.5])
    with c_head_1: st.title("💎 Kinkin Tool 2.0 (V108)"); st.caption(f"User: {uid}")
    with c_head_2: st.info(f"🤖 **Bot phụ trách:**"); st.code(assigned_bot, language="text")

    # --- MAIN EDITOR ---
    st.subheader(f"Config: {sel_blk}")
    curr_df = st.session_state['df_full_config'][st.session_state['df_full_config'][COL_BLOCK_NAME] == sel_blk].copy().reset_index(drop=True)
    if COL_COPY_FLAG not in curr_df.columns: curr_df.insert(0, COL_COPY_FLAG, False)
    if 'STT' not in curr_df.columns: curr_df.insert(1, 'STT', range(1, len(curr_df)+1))

    # [V108] Checkbox Config
    edt_df = st.data_editor(
        curr_df,
        column_order=[COL_COPY_FLAG, "STT", COL_STATUS, COL_WRITE_MODE, COL_DATA_RANGE, COL_MONTH, COL_SRC_LINK, COL_SRC_SHEET, COL_TGT_LINK, COL_TGT_SHEET, COL_FILTER, COL_HEADER, COL_RESULT, COL_LOG_ROW],
        column_config={
            COL_STATUS: st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt & đang cập nhật", "Đã chốt"], required=True),
            COL_WRITE_MODE: st.column_config.SelectboxColumn("Cách ghi", options=["Ghi Đè", "Ghi Nối Tiếp"], default="Ghi Đè", required=True),
            COL_SRC_LINK: st.column_config.LinkColumn("Link nguồn", width="medium"),
            COL_TGT_LINK: st.column_config.LinkColumn("Link đích", width="medium"),
            COL_HEADER: st.column_config.CheckboxColumn("Lấy Header?", default=False, width="small"),
            "STT": st.column_config.NumberColumn("STT", width="small", disabled=True),
            COL_RESULT: st.column_config.TextColumn("Kết quả", disabled=True),
            COL_BLOCK_NAME: None 
        }, use_container_width=True, num_rows="dynamic", key="edt_v108"
    )

    if edt_df[COL_COPY_FLAG].any():
        nw = []
        for i, r in edt_df.iterrows():
            rc = r.copy(); rc[COL_COPY_FLAG] = False; nw.append(rc)
            if r[COL_COPY_FLAG]: cp = r.copy(); cp[COL_COPY_FLAG] = False; nw.append(cp)
        st.session_state['df_full_config'] = pd.concat([st.session_state['df_full_config'][st.session_state['df_full_config'][COL_BLOCK_NAME] != sel_blk], pd.DataFrame(nw)], ignore_index=True)
        st.rerun()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        if st.button("▶️ RUN BLOCK", type="primary", use_container_width=True):
            # 1. Lưu config trước khi chạy để đảm bảo dữ liệu mới nhất
            save_block_config_to_sheet(edt_df, sel_blk, master_creds, uid)
            
            rows = []
            for i, r in edt_df.iterrows():
                if str(r.get(COL_STATUS,'')).strip() == "Chưa chốt & đang cập nhật":
                    r_dict = r.to_dict(); r_dict['_index'] = i; rows.append(r_dict)
            
            if not rows: st.warning("Không có dòng nào để chạy."); st.stop()
            st_cont = st.status(f"🚀 Đang chạy {sel_blk} (Bot: {assigned_bot})...", expanded=True)
            
            ok, res, tot = process_pipeline_mixed(rows, uid, sel_blk, st_cont, forced_bot=assigned_bot)
            
            if isinstance(res, dict):
                # [V108] Cập nhật kết quả vào dataframe và lưu ngay lập tức
                for i, r in edt_df.iterrows():
                    if i in res: 
                        edt_df.at[i, COL_RESULT] = res[i][0]
                        edt_df.at[i, COL_LOG_ROW] = res[i][1] # Cập nhật Log Row
                
                # Lưu lại kết quả vào Google Sheet (bao gồm Log Row mới)
                save_block_config_to_sheet(edt_df, sel_blk, master_creds, uid)
                st_cont.update(label=f"Done! {tot} rows. Log Updated.", state="complete", expanded=False)
            else: st_cont.update(label="Lỗi!", state="error", expanded=False)
            
            st.cache_data.clear(); time.sleep(1); st.rerun()

    with c2:
        if st.button("⏩ RUN ALL BLOCKS", use_container_width=True):
            full_df = st.session_state['df_full_config']
            all_blocks = full_df[COL_BLOCK_NAME].unique().tolist()
            if not all_blocks: st.warning("Trống"); st.stop()
            
            main_st = st.status("🚀 Chạy toàn bộ...", expanded=True)
            total = 0
            for idx, blk in enumerate(all_blocks):
                blk_bot = assign_bot_to_block(blk)
                main_st.write(f"⏳ [{idx+1}/{len(all_blocks)}] {blk} -> {blk_bot}")
                
                # Logic lấy và chạy rows
                blk_df = full_df[full_df[COL_BLOCK_NAME] == blk].copy().reset_index(drop=True)
                rows_to_run = []
                for i, r in blk_df.iterrows():
                    if str(r.get(COL_STATUS,'')).strip() == "Chưa chốt & đang cập nhật":
                        r_dict = r.to_dict(); r_dict['_index'] = i; rows_to_run.append(r_dict)
                
                if rows_to_run:
                    ok, res, tot = process_pipeline_mixed(rows_to_run, uid, blk, main_st, forced_bot=blk_bot)
                    total += len(rows_to_run)
                    
                    # [V108] Cập nhật Log Row cho từng Block sau khi chạy xong
                    if isinstance(res, dict):
                        for i, r in blk_df.iterrows():
                            if i in res:
                                blk_df.at[i, COL_RESULT] = res[i][0]
                                blk_df.at[i, COL_LOG_ROW] = res[i][1]
                        # Lưu ngay trạng thái của block này xuống sheet
                        save_block_config_to_sheet(blk_df, blk, master_creds, uid)

            main_st.update(label="Hoàn tất!", state="complete", expanded=False)
            st.toast("Done Run All!"); time.sleep(2)

    with c3:
        if st.button("🔍 Quét Quyền", use_container_width=True):
            assigned_email = assign_bot_to_block(sel_blk)
            checking_creds = get_bot_credentials_from_secrets(assigned_email)
            with st.status(f"Đang dùng {assigned_email} để kiểm tra...", expanded=True) as st_chk:
                if checking_creds: check_permissions_ui(edt_df.to_dict('records'), checking_creds, st_chk, uid)
                else: st_chk.error(f"❌ Không tìm thấy Key cho {assigned_email}. Vui lòng kiểm tra Secrets!")

    with c4:
        if st.button("💾 Save Config", use_container_width=True):
            save_block_config_to_sheet(edt_df, sel_blk, master_creds, uid); st.rerun()

    flush_logs(master_creds, force=True)
    st.divider(); st.caption("Logs")
    if st.button("Refresh Logs"): st.cache_data.clear()
    logs = fetch_activity_logs(master_creds, 50)
    if not logs.empty: st.dataframe(logs, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main_ui()
