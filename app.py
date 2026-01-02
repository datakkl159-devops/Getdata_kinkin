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

# --- CẤU HÌNH ---
st.set_page_config(page_title="Kinkin Tool 2.0 (V99 Final)", layout="wide", page_icon="🤖")

AUTHORIZED_USERS = {"admin2025": "Admin", "team_hn": "HaNoi", "team_hcm": "HCM"}
BOT_EMAIL_DISPLAY = "System: Multi-Bot Cluster Active"

SHEET_CONFIG_NAME = "luu_cau_hinh"
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_ACTIVITY_NAME = "log_hanh_vi"
SHEET_LOCK_NAME = "sys_lock"
SHEET_SYS_CONFIG = "sys_config"
SHEET_NOTE_NAME = "database_ghi_chu"
SHEET_SYS_STATE = "sys_state"

# --- CỘT TIẾNG VIỆT ---
COL_BLOCK_NAME = "Block_Name"; COL_STATUS = "Trạng thái"; COL_WRITE_MODE = "Cach_Ghi"
COL_DATA_RANGE = "Vùng lấy dữ liệu"; COL_MONTH = "Tháng"; COL_SRC_LINK = "Link dữ liệu lấy dữ liệu"
COL_TGT_LINK = "Link dữ liệu đích"; COL_SRC_SHEET = "Tên sheet nguồn dữ liệu gốc"
COL_TGT_SHEET = "Tên sheet dữ liệu đích"; COL_RESULT = "Kết quả"; COL_LOG_ROW = "Dòng dữ liệu"
COL_FILTER = "Dieu_Kien_Loc"; COL_HEADER = "Lay_Header"; COL_COPY_FLAG = "Copy_Flag"
COL_ASSIGNED_BOT = "Bot_Phu_Trach" # [MỚI]

REQUIRED_COLS_CONFIG = [
    COL_BLOCK_NAME, COL_STATUS, COL_WRITE_MODE, COL_DATA_RANGE, COL_MONTH, 
    COL_SRC_LINK, COL_TGT_LINK, COL_TGT_SHEET, COL_SRC_SHEET, 
    COL_RESULT, COL_LOG_ROW, COL_FILTER, COL_HEADER, COL_ASSIGNED_BOT
]

SCHED_COL_BLOCK = "Block_Name"; SCHED_COL_TYPE = "Loai_Lich"
SCHED_COL_VAL1 = "Thong_So_Chinh"; SCHED_COL_VAL2 = "Thong_So_Phu"
REQUIRED_COLS_SCHED = [SCHED_COL_BLOCK, SCHED_COL_TYPE, SCHED_COL_VAL1, SCHED_COL_VAL2]

NOTE_COL_ID = "ID"; NOTE_COL_BLOCK = "Tên Khối"; NOTE_COL_CONTENT = "Nội dung Note"
REQUIRED_COLS_NOTE = [NOTE_COL_ID, NOTE_COL_BLOCK, NOTE_COL_CONTENT]

SYS_COL_LINK = "Src_Link"; SYS_COL_SHEET = "Src_Sheet"; SYS_COL_MONTH = "Month"
DEFAULT_BLOCK_NAME = "Block_Mac_Dinh"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
LOG_BUFFER_SIZE = 5; LOG_FLUSH_INTERVAL = 10

# --- AUTH & UTILS ---
def get_master_creds():
    raw = st.secrets["gcp_service_account"]
    info = json.loads(raw) if isinstance(raw, str) else dict(raw)
    if "private_key" in info: info["private_key"] = info["private_key"].replace("\\n", "\n")
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

def get_available_bots():
    """Lấy danh sách Bot và Email từ Secrets"""
    bots = {}
    try:
        # Bot 1
        if "gcp_service_account" in st.secrets:
            raw = st.secrets["gcp_service_account"]
            info = json.loads(raw) if isinstance(raw, str) else dict(raw)
            bots["Bot 1"] = info.get("client_email", "")
        # Bot phụ
        all_secs = st.secrets.to_dict() if hasattr(st.secrets, "to_dict") else dict(st.secrets)
        for key in all_secs:
            if key.startswith("gcp_service_account_"):
                idx = key.split("_")[-1]
                b_name = f"Bot {int(idx)+1}"
                raw = all_secs[key]
                info = json.loads(raw) if isinstance(raw, str) else dict(raw)
                bots[b_name] = info.get("client_email", "")
    except: pass
    return dict(sorted(bots.items()))

def get_bot_creds_by_name(name):
    if name == "Bot 1": return get_master_creds()
    try:
        idx = int(name.replace("Bot ", "")) - 1
        raw = st.secrets[f"gcp_service_account_{idx}"]
        info = json.loads(raw) if isinstance(raw, str) else dict(raw)
        if "private_key" in info: info["private_key"] = info["private_key"].replace("\\n", "\n")
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    except: return get_master_creds() # Fallback

def safe_api_call(func, *args, **kwargs):
    for i in range(5):
        try: return func(*args, **kwargs)
        except Exception as e:
            if "429" in str(e).lower() or "quota" in str(e).lower():
                time.sleep((2**i)+3); print(f"Wait API {i}...")
            elif i==4: raise e
            else: time.sleep(2)
    return None

def safe_get_as_dataframe(wks, **kwargs): return safe_api_call(get_as_dataframe, wks, **kwargs)
def safe_set_with_dataframe(wks, df, **kwargs): return safe_api_call(set_with_dataframe, wks, df, **kwargs)
def get_sh_with_retry(creds, sid): gc = gspread.authorize(creds); return safe_api_call(gc.open_by_key, sid)
def extract_id(url):
    try: return url.split("/d/")[1].split("/")[0]
    except: return None
def col_name_to_index(col):
    col = col.upper(); idx=0
    for c in col: idx = idx*26 + (ord(c)-ord('A'))+1
    return idx-1
def ensure_headers(wks, cols):
    try:
        if not wks.row_values(1): wks.append_row(cols)
    except: pass

# --- LOGGING ---
def init_log(): 
    if 'log_buffer' not in st.session_state: st.session_state['log_buffer'] = []
    if 'last_flush' not in st.session_state: st.session_state['last_flush'] = time.time()
def flush_logs(creds, force=False):
    buf = st.session_state.get('log_buffer', [])
    if (force or len(buf)>=LOG_BUFFER_SIZE) and buf:
        try:
            sh = get_sh_with_retry(creds, st.secrets["general"]["history_sheet_id"])
            try: wks = sh.worksheet(SHEET_ACTIVITY_NAME)
            except: wks = sh.add_worksheet(SHEET_ACTIVITY_NAME, 1000, 4)
            safe_api_call(wks.append_rows, buf)
            st.session_state['log_buffer'] = []
        except: pass
def log_action(creds, user, action, status=""):
    init_log()
    st.session_state['log_buffer'].append([datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime("%d/%m/%Y %H:%M:%S"), user, action, status])
    flush_logs(creds)

# --- CORE LOGIC (Rút gọn cho app.py, auto_job.py sẽ chứa logic full) ---
def check_login():
    if 'logged_in' not in st.session_state: st.session_state['logged_in']=False
    if st.session_state['logged_in']: return True
    if st.sidebar.text_input("Password", type="password") in AUTHORIZED_USERS:
        st.session_state['logged_in']=True; st.session_state['current_user_id']="Admin"; st.rerun()
    return False

# --- LOAD/SAVE CONFIG ---
@st.cache_data
def load_config(creds):
    sh = get_sh_with_retry(creds, st.secrets["general"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    ensure_headers(wks, REQUIRED_COLS_CONFIG)
    df = safe_get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
    if df.empty: return pd.DataFrame(columns=REQUIRED_COLS_CONFIG)
    df = df.dropna(how='all').replace(['nan','None','NaN'], '')
    
    # Auto Assign Bot
    bots = list(get_available_bots().keys())
    if COL_ASSIGNED_BOT not in df.columns: df[COL_ASSIGNED_BOT] = ""
    if bots:
        for i, r in df.iterrows():
            if not r[COL_ASSIGNED_BOT] or r[COL_ASSIGNED_BOT] not in bots:
                df.at[i, COL_ASSIGNED_BOT] = bots[i % len(bots)]
    return df

def save_config(df, creds):
    sh = get_sh_with_retry(creds, st.secrets["general"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    # Clean and Save
    df_clean = df.copy()
    for c in ['STT', COL_COPY_FLAG]: 
        if c in df_clean.columns: df_clean = df_clean.drop(columns=[c])
    wks.clear()
    safe_set_with_dataframe(wks, df_clean, row=1, col=1)
    st.toast("Đã lưu cấu hình!", icon="💾")

# --- UI ---
def main_ui():
    if not check_login(): return
    uid = st.session_state.get('current_user_id', 'User')
    creds = get_master_creds()
    
    st.title("💎 Kinkin Manager (V99 - Multi Bot)")
    
    bots_map = get_available_bots()
    with st.sidebar:
        st.write(f"Hello, {uid}")
        if st.button("Reload"): st.cache_data.clear(); st.rerun()
        
        st.divider()
        st.caption("Danh sách Bot:")
        for b, mail in bots_map.items():
            with st.expander(f"🤖 {b}"):
                st.code(mail)
                st.caption("Copy email để share quyền")

    # Load Data
    if 'df_cfg' not in st.session_state: st.session_state['df_cfg'] = load_config(creds)
    df = st.session_state['df_cfg']
    
    # Editor
    blocks = df[COL_BLOCK_NAME].unique().tolist() if not df.empty else ["New"]
    sel_blk = st.selectbox("Chọn Khối:", blocks)
    
    df_blk = df[df[COL_BLOCK_NAME] == sel_blk].copy().reset_index(drop=True)
    df_blk.insert(0, COL_COPY_FLAG, False); df_blk.insert(1, "STT", range(1, len(df_blk)+1))
    
    edited = st.data_editor(
        df_blk, num_rows="dynamic", use_container_width=True,
        column_order=[COL_COPY_FLAG, "STT", COL_ASSIGNED_BOT, COL_STATUS, COL_WRITE_MODE, COL_SRC_LINK, COL_SRC_SHEET, COL_TGT_LINK, COL_TGT_SHEET, COL_FILTER, COL_HEADER, COL_LOG_ROW],
        column_config={
            COL_ASSIGNED_BOT: st.column_config.SelectboxColumn("Bot", options=list(bots_map.keys()), required=True, width="small"),
            COL_SRC_LINK: st.column_config.LinkColumn("Nguồn", width="medium"),
            COL_TGT_LINK: st.column_config.LinkColumn("Đích", width="medium"),
            COL_COPY_FLAG: st.column_config.CheckboxColumn("Copy", width="small")
        }
    )
    
    if st.button("Lưu Cấu Hình", type="primary"):
        # Logic merge & save (Rút gọn)
        df_final = pd.concat([df[df[COL_BLOCK_NAME]!=sel_blk], edited], ignore_index=True)
        save_config(df_final, creds)
        st.cache_data.clear(); st.rerun()

if __name__ == "__main__":
    main_ui()
