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
from datetime import datetime, timedelta
from google.oauth2 import service_account
from collections import defaultdict, Counter
from st_copy_to_clipboard import st_copy_to_clipboard

# ==========================================
# 1. CẤU HÌNH HỆ THỐNG
# ==========================================
st.set_page_config(page_title="Kinkin Tool 2.0 (V108.3 - Full Guide)", layout="wide", page_icon="📘")

# 🟢 DANH SÁCH 5 BOT (User điền)
MY_BOT_LIST = [
    "kinkingetdulieu1@kinkin1.iam.gserviceaccount.com", # Bot 1
    "botnew@kinkin2.iam.gserviceaccount.com",          # Bot 2
    "kinkingetdulieu3@kinkin3.iam.gserviceaccount.com", # Bot 3
    "kinkingetdulieu4@kinkin4.iam.gserviceaccount.com", # Bot 4
    "kinkingetdulieu5@kinkin5.iam.gserviceaccount.com"  # Bot 5
]

AUTHORIZED_USERS = {
    "admin2025": "Sếp Thường",
    "team_hn": "Huyền KT",
    "team_hcm": "Admin"
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
        val = wks.cell(2, 1).value; user = wks.cell(2, 2).value; time_str = wks.cell(2, 3).value
        if val == "TRUE":
            try:
                if (datetime.now() - datetime.strptime(time_str, "%d/%m/%Y %H:%M:%S")).total_seconds() > 300: return False
            except: pass
            return True if user == user_id else False
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

# --- [V108.3] CẢI TIẾN HƯỚNG DẪN SỬ DỤNG (CHI TIẾT & CHUẨN XÁC) ---
@st.dialog("📘 CẨM NANG HƯỚNG DẪN SỬ DỤNG HỆ THỐNG", width="large")
def show_guide_popup():
    st.markdown("""
    Chào mừng bạn! Nếu đây là lần đầu bạn sử dụng Kinkin Tool, đừng lo lắng. Hãy đọc kỹ các bước dưới đây để vận hành trơn tru nhé.

    ### 1. Tool này dùng để làm gì?
    Đơn giản là: Bạn có nhiều file Google Sheet nằm rải rác (File Nguồn). Bạn muốn gom dữ liệu từ các file đó về một file tổng (File Đích). Tool này sẽ làm việc đó thay bạn hoàn toàn tự động.
    
    * **🤖 Bot làm việc thế nào?** Hệ thống có 5 con Bot. Khi bạn đặt tên cho một "Khối" công việc, hệ thống sẽ tự động chỉ định 1 con Bot riêng để phục vụ Khối đó (Ví dụ: Khối "Kế toán" luôn do Bot 1 làm, Khối "Nhân sự" luôn do Bot 2 làm). Điều này giúp công việc không bị chồng chéo.

    ---
    ### 2. Quy Trình 4 Bước Đơn Giản
    
    #### 🟢 Bước 1: Điền thông tin vào bảng
    Chọn một Khối ở menu bên trái, bảng cấu hình sẽ hiện ra. Bạn cần điền các cột sau:
    
    | Tên Cột | Giải thích bình dân | Ví dụ điền |
    | :--- | :--- | :--- |
    | **Trạng thái** | Phải chọn **"Chưa chốt..."** thì dòng này mới được chạy. Nếu chọn "Đã chốt", Tool sẽ bỏ qua. | `Chưa chốt...` |
    | **Cách ghi** | • **Ghi Đè:** Xóa cái cũ (của link nguồn này) đi, viết cái mới vào.<br>• **Ghi Nối Tiếp:** Cái cũ giữ nguyên, viết thêm cái mới xuống dưới đáy. | `Ghi Đè` |
    | **Vùng lấy** | Bạn muốn lấy dữ liệu từ cột nào đến cột nào? | `A:Z` (Lấy hết bảng)<br>`A:E` (Chỉ lấy cột A đến E) |
    | **Link nguồn** | Địa chỉ web của file chứa dữ liệu gốc. | `https://docs.google...` |
    | **Tên sheet** | Tên cái tab nhỏ bên dưới file Excel/Sheet mà bạn muốn lấy. | `Sheet1` hoặc `Data_Thang_3` |
    | **Điều kiện lọc** | *(Xem hướng dẫn chi tiết mục 3 bên dưới)* | `Doanh_thu > 0` |
    | **Lấy Header** | Tick ✅ nếu dòng 1 của file nguồn là tiêu đề cột và bạn muốn lấy nó. | ✅ |

    #### 🔐 Bước 2: Mở cửa cho Bot (Cấp quyền)
    Bot cũng giống người, muốn vào nhà (file) thì phải được mở cửa.
    1.  Nhìn lên góc trên bên phải màn hình, mục **🤖 Bot phụ trách**, copy địa chỉ Email ở đó.
    2.  Vào **File Nguồn** -> Nút Share -> Dán email Bot -> Chọn quyền **Viewer (Người xem)**.
    3.  Vào **File Đích** -> Nút Share -> Dán email Bot -> Chọn quyền **Editor (Người chỉnh sửa)**.
    
    #### 🚀 Bước 3: Bấm nút chạy
    * Bấm **`💾 Save Config`** để lưu lại những gì vừa điền.
    * Bấm **`▶️ RUN BLOCK`** để chạy thử. Tool sẽ tự động quét và báo lỗi nếu quên cấp quyền.

    #### 🔄 Bước 4: Xem kết quả (Quan trọng)
    * Chạy xong, bảng sẽ hiện chữ "Thành công" ở cột Kết quả.
    * **Lưu ý:** Nếu bạn thấy bảng chưa hiện số dòng mới, hãy bấm nút **`🔄 Reload`** màu trắng ở menu bên trái để làm mới màn hình.

    ---
    ### 3. Bí Kíp Điền "Điều Kiện Lọc" (Filter)
    Dùng để chỉ lấy những dòng dữ liệu bạn cần. 
    **Cấu trúc:** `[Tên Cột] [Toán tử] [Giá trị]`

    #### 📐 Các toán tử hỗ trợ:
    | Toán tử | Ý nghĩa | Ví dụ |
    | :--- | :--- | :--- |
    | `==` | Bằng chính xác | `Bo_phan == 'IT'` |
    | `!=` | Khác (Không bằng) | `Trang_thai != 'Hủy'` |
    | `>` | Lớn hơn | `Doanh_thu > 500000` |
    | `<` | Nhỏ hơn | `So_luong < 10` |
    | `>=` | Lớn hơn hoặc bằng | `Diem >= 5` |
    | `<=` | Nhỏ hơn hoặc bằng | `Tuoi <= 18` |
    | `contains` | Chứa từ khóa | `Dia_chi contains 'Hà Nội'` |

    #### 💡 Ví dụ cơ bản:
    * **1. Lọc Số:** `Doanh_thu > 1000000` hoặc `So_luong == 0`
    * **2. Lọc Chữ (Dùng nháy đơn):** `Ten == 'Lan'` hoặc `Trang_thai != 'Hủy'`
    * **3. Lọc Ngày (Dùng nháy đơn):** `Ngay_dat > '01/01/2025'`
    
    #### 🌟 CÁC TRƯỜNG HỢP ĐẶC BIỆT (Lọc 2-3 Giá Trị)
    Đây là phần quan trọng nhất để lọc dữ liệu nâng cao:

    | Nhu cầu | Cú pháp mẫu (Copy vào cột Dieu_Kien_Loc) | Giải thích chi tiết |
    | :--- | :--- | :--- |
    | **Lọc 1 trong 2 (HOẶC)** | `Phong_ban contains 'Kế toán|Nhân sự'` | Lấy dòng có chữ Kế toán **HOẶC** Nhân sự. Dùng dấu gạch đứng `|` để nối. |
    | **Lọc 1 trong 3 (HOẶC)** | `Trang_thai contains 'Chờ|Duyệt|Xong'` | Lấy dòng là Chờ, Duyệt **HOẶC** Xong. |
    | **Lọc chính xác 3 Mã** | `Ma_NV contains '^A01$|^B02$|^C03$'` | Thêm `^` (đầu) và `$` (cuối) để lấy chính xác mã, không lấy mã gần giống (VD: không lấy A01_New). |
    | **Lọc số trong khoảng** | `Gia >= 1000; Gia <= 5000` | Dùng dấu chấm phẩy `;` (nghĩa là **VÀ**). Lấy số >= 1000 **VÀ** <= 5000. |
    | **Lọc 2 điều kiện khác** | `Ton_kho > 0; Trang_thai == 'Done'` | Lấy dòng tồn kho dương **VÀ** đã làm xong. |
    | **Lọc ngày (Khoảng)** | `Ngay >= '01/01/2025'; Ngay <= '31/01/2025'` | Lấy dữ liệu trong tháng 1. |
    | **Lọc ngày (Động)** | `Ngay >= 'TODAY-1'` | Lấy từ hôm qua (`TODAY-1`) đến nay (`TODAY`). Tự động nhảy ngày. |
    | **Lọc loại trừ** | `Trang_thai != 'Hủy'; Trang_thai != 'Lỗi'` | Lấy tất cả, **TRỪ** dòng Hủy và dòng Lỗi. |

    #### 💡 Lưu ý cú pháp:
    1. **Dấu ngăn cách:** Dấu `;` nghĩa là **VÀ** (Phải thỏa mãn cả hai).
    2. **Dấu gạch đứng:** Dấu `|` (trên phím Enter) nghĩa là **HOẶC** (Cái này hoặc cái kia).
    3. **Dấu nháy:** Chữ và Ngày tháng bắt buộc để trong dấu nháy đơn `' '`.

    ---
    ### 4. Logic Điền Dữ Liệu (Khi vào File Đích)
    Đây là cách Tool xử lý khi đổ dữ liệu vào File Đích của bạn:

    #### 🆕 Trường hợp 1: File Đích là file trắng (Chưa có gì)
    * Tool sẽ tự động tạo dòng tiêu đề (Header) dựa trên File Nguồn.
    * Dữ liệu được điền bình thường.

    #### 🔁 Trường hợp 2: File Đích ĐÃ CÓ dữ liệu cũ
    Tool sẽ tôn trọng cấu trúc của File Đích hiện tại.
    * **Nếu Tiêu Đề TRÙNG KHỚP:** Quá tuyệt! Dữ liệu sẽ được điền thẳng hàng, thẳng lối.
    * **Nếu Tiêu Đề KHÁC NHAU:**
        * ⛔ **Tool sẽ KHÔNG chạy về dữ liệu.**
        * *Lời khuyên:* Hãy đảm bảo tên cột (dòng 1) ở File Nguồn và File Đích phải giống hệt nhau để tránh lỗi lệch cột.

    #### 🛡️ Cột Hệ Thống
    Để giúp bạn quản lý, Tool luôn tự động thêm 4 cột này vào cuối file đích:
    1.  `Src_Link`: Dữ liệu này lấy từ link nào?
    2.  `Src_Sheet`: Lấy từ sheet nào?
    3.  `Month`: Dữ liệu của tháng mấy?
    4.  `Thời điểm ghi`: Dữ liệu này được Bot cập nhật vào giờ nào, ngày nào?
    """)

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
# --- [NEW] HÀM XỬ LÝ NGÀY ĐỘNG ---
def parse_dynamic_date(val_str):
    """Biến đổi TODAY-1, YESTERDAY thành ngày cụ thể"""
    if not isinstance(val_str, str): return val_str
    
    # Chuẩn hóa chuỗi (xóa khoảng trắng, dấu nháy)
    val_upper = val_str.strip().upper().replace(" ", "").replace("'", "").replace('"', "")
    
    # Lấy ngày hôm nay (0h sáng) theo giờ VN
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Xử lý TODAY
    if "TODAY" in val_upper:
        calc_part = val_upper.replace("TODAY()", "").replace("TODAY", "")
        if not calc_part: return now # Là TODAY
        try:
            days = int(calc_part) # Python hiểu -1 là trừ 1 ngày
            return now + timedelta(days=days)
        except: pass

    # Xử lý YESTERDAY
    if val_upper == "YESTERDAY": return now - timedelta(days=1)
    
    return val_str # Trả về nguyên gốc nếu không phải biến động

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
        
        parts = fs.split(op, 1)
        col_raw = parts[0].strip().replace("`", "").replace("'", "").replace('"', "")
        val_raw = parts[1].strip()
        
        # [MỚI] Xử lý ngày động (VD: TODAY-1) trước khi lọc
        val_resolved = parse_dynamic_date(val_raw)
        
        # Làm sạch giá trị chuỗi (bỏ dấu nháy bao quanh)
        val_clean = val_raw[1:-1] if (isinstance(val_raw, str) and (val_raw.startswith("'") or val_raw.startswith('"'))) else val_raw
        
        real_col = next((c for c in current_df.columns if str(c).lower() == col_raw.lower()), None)
        if not real_col: return None, f"Không tìm thấy cột '{col_raw}'"
        
        try:
            series = current_df[real_col]
            if op == " contains ": 
                current_df = current_df[series.astype(str).str.contains(val_clean, case=False, na=False)]
            else:
                # Logic so sánh
                is_dt = False
                v_dt = None
                
                # Check 1: Nếu giá trị so sánh là datetime (do hàm parse_dynamic_date trả về)
                if isinstance(val_resolved, datetime):
                    is_dt = True
                    # Bỏ múi giờ để so sánh với dữ liệu trong Sheet (thường không có múi giờ)
                    v_dt = pd.to_datetime(val_resolved).tz_localize(None)
                else:
                    # Check 2: Thử parse string thường
                    try: 
                        s_dt = pd.to_datetime(series, dayfirst=True, errors='coerce')
                        v_dt_try = pd.to_datetime(val_clean, dayfirst=True)
                        if s_dt.notna().any() and pd.notna(v_dt_try): 
                            is_dt = True
                            v_dt = v_dt_try
                    except: pass
                
                is_num = False
                if not is_dt:
                    try: s_num = pd.to_numeric(series, errors='coerce'); v_num = float(val_clean); is_num = True
                    except: pass
                
                if is_dt:
                    # Chuyển cột series sang datetime
                    s_dt = pd.to_datetime(series, dayfirst=True, errors='coerce')
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
                    val_str_cmp = str(val_clean)
                    if op==">": current_df=current_df[s_str>val_str_cmp]
                    elif op=="<": current_df=current_df[s_str<val_str_cmp]
                    elif op==">=": current_df=current_df[s_str>=val_str_cmp]
                    elif op=="<=": current_df=current_df[s_str<=val_str_cmp]
                    elif op in ["=","=="]: current_df=current_df[s_str==val_str_cmp]
                    elif op=="!=": current_df=current_df[s_str!=val_str_cmp]
            
            if debug_container: debug_container.caption(f"👉 Lọc '{val_clean}' ({op}) -> Còn {len(current_df)}")
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
    """
    V110.1: Quét toàn bộ sheet (Deep Scan) để tìm dòng cần xóa.
    Khắc phục lỗi dừng quét khi gặp header lặp lại hoặc dòng trống giữa chừng.
    """
    try:
        # 1. Lấy toàn bộ dữ liệu thô (List of Lists) - Cách nhanh nhất
        all_values = safe_api_call(wks.get_all_values)
        if not all_values or len(all_values) < 2: return []
        
        # 2. Tìm dòng tiêu đề CHÍNH (thường là dòng 1) để xác định vị trí cột
        # Chúng ta chỉ tìm tiêu đề ở 10 dòng đầu tiên để tránh nhầm lẫn
        header_row_idx = -1
        headers = []
        
        for i in range(min(10, len(all_values))):
            row_lower = [str(c).strip().lower() for c in all_values[i]]
            if SYS_COL_LINK.lower() in row_lower and SYS_COL_SHEET.lower() in row_lower:
                header_row_idx = i
                headers = row_lower
                break
        
        if header_row_idx == -1:
            if log_container: log_container.warning("⚠️ Không tìm thấy dòng tiêu đề hệ thống (Src_Link...). Không thể xóa.")
            return []

        # 3. Xác định chỉ số cột (Index)
        try:
            idx_link = headers.index(SYS_COL_LINK.lower())
            idx_sheet = headers.index(SYS_COL_SHEET.lower())
            idx_month = headers.index(SYS_COL_MONTH.lower())
        except ValueError:
            return []

        rows_to_delete = []
        
        # 4. QUÉT TOÀN BỘ (Deep Scan) từ ngay sau dòng header chính
        # Không dùng break, quét đến tận dòng cuối cùng
        total_rows = len(all_values)
        
        for i in range(header_row_idx + 1, total_rows):
            row = all_values[i]
            
            # Xử lý an toàn nếu dòng dữ liệu bị thiếu cột (ngắn hơn header)
            if len(row) <= max(idx_link, idx_sheet, idx_month):
                continue # Bỏ qua dòng lỗi format
                
            # Lấy giá trị và làm sạch (strip)
            val_link = str(row[idx_link]).strip()
            val_sheet = str(row[idx_sheet]).strip()
            val_month = str(row[idx_month]).strip()
            
            # Kiểm tra: Nếu dòng này là một dòng Header lặp lại (do copy paste cũ)
            # Thì nó sẽ có giá trị là "Src_Link", "Src_Sheet"... -> Không khớp Key (URL) -> Không bị xóa
            # Nếu bạn muốn xóa luôn cả dòng header thừa đó, hãy báo tôi.
            # Hiện tại logic là: Chỉ xóa dòng có DỮ LIỆU trùng khớp.
            
            if (val_link, val_sheet, val_month) in keys_to_delete:
                rows_to_delete.append(i + 1) # +1 vì gspread dùng index bắt đầu từ 1

        return rows_to_delete

    except Exception as e:
        print(f"Lỗi Deep Scan: {e}")
        return []

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
        
        # 1. Kết nối Sheet (Tạo mới nếu chưa có)
        all_titles = [s.title for s in safe_api_call(sh.worksheets)]
        if real_sheet_name in all_titles: wks = sh.worksheet(real_sheet_name)
        else: wks = sh.add_worksheet(title=real_sheet_name, rows=1000, cols=20)
        
        # 2. Xử lý Header
        existing_headers = safe_api_call(wks.row_values, 1)
        if not existing_headers:
            # Sheet trắng -> Tạo header mới từ dữ liệu đầu tiên
            if not tasks_list: return True, "No Data", {}, []
            first_df = tasks_list[0][0]
            final_headers = first_df.columns.tolist()
            wks.update(range_name="A1", values=[final_headers])
            existing_headers = final_headers
        else:
            # Sheet đã có -> Bổ sung cột hệ thống nếu thiếu
            updated = existing_headers.copy(); added = False
            for col in [SYS_COL_LINK, SYS_COL_SHEET, SYS_COL_MONTH, SYS_COL_TIME]:
                if col not in updated: updated.append(col); added = True
            if added: 
                wks.update(range_name="A1", values=[updated])
                existing_headers = updated

        # 3. Chuẩn bị dữ liệu
        final_df_to_write = pd.DataFrame()
        keys_to_delete = set() # Chứa danh sách các key cần xóa (cho Ghi Đè)

        for df, src_link, row_idx, w_mode in tasks_list:
            if df.empty: continue
            
            # Luôn gom dữ liệu vào danh sách chờ ghi (Cho cả Ghi Đè và Nối Tiếp)
            final_df_to_write = pd.concat([final_df_to_write, df], ignore_index=True)
            
            # LOGIC QUAN TRỌNG TẠI ĐÂY:
            if w_mode == "Ghi Đè":
                # Nếu là Ghi Đè -> Thêm key này vào danh sách "Sổ Đen" để xóa dữ liệu cũ đi
                l_key = str(df[SYS_COL_LINK].iloc[0]).strip()
                s_key = str(df[SYS_COL_SHEET].iloc[0]).strip()
                m_key = str(df[SYS_COL_MONTH].iloc[0]).strip()
                keys_to_delete.add((l_key, s_key, m_key))
            
            # Nếu là "Ghi Nối Tiếp" -> Không làm gì cả (Không thêm vào keys_to_delete)
            # Code sẽ tự động bỏ qua bước xóa và chỉ thực hiện bước Ghi ở dưới.

        # 4. Thực hiện XÓA (Chỉ chạy nếu có task Ghi Đè)
        if keys_to_delete:
            log_container.write(f"🔍 Đang quét dữ liệu cũ để Ghi Đè...")
            rows_to_del = get_rows_to_delete_dynamic(wks, keys_to_delete, log_container)
            
            if rows_to_del:
                log_container.write(f"✂️ Đang xóa {len(rows_to_del)} dòng cũ...")
                batch_delete_rows(sh, wks.id, rows_to_del, log_container)
                log_container.write("✅ Đã xóa xong. Dữ liệu cũ đã được đẩy lên.")
                # Bắt buộc nghỉ để Google cập nhật lại index dòng sau khi xóa
                time.sleep(3) 
            else:
                log_container.write("ℹ️ Không tìm thấy dữ liệu cũ để xóa (Ghi mới hoàn toàn).")

        # 5. Thực hiện GHI (Append xuống dòng cuối cùng)
        if not final_df_to_write.empty:
            # Sắp xếp cột cho khớp với file đích
            df_aligned = pd.DataFrame()
            for col in existing_headers:
                df_aligned[col] = final_df_to_write[col] if col in final_df_to_write.columns else ""
            
            # Xác định dòng bắt đầu ghi (để log hiển thị)
            # Lấy lại số dòng hiện tại sau khi đã xóa (nếu có)
            current_vals = safe_api_call(wks.get_all_values)
            start_row_idx = len(current_vals) + 1 if current_vals else 1
            
            log_container.write(f"🚀 Đang ghi {len(df_aligned)} dòng mới từ dòng {start_row_idx}...")
            
            chunk_size = 5000
            new_vals = df_aligned.fillna('').values.tolist()
            for i in range(0, len(new_vals), chunk_size):
                safe_api_call(wks.append_rows, new_vals[i:i+chunk_size], value_input_option='USER_ENTERED')
                time.sleep(1)
            
            # Tính toán log trả về cho giao diện
            current_cursor = int(start_row_idx)
            for df, src_link, row_idx, w_mode in tasks_list:
                count = len(df)
                if count > 0:
                    end = current_cursor + count - 1
                    rng_str = f"{current_cursor} - {end}"
                    current_cursor += count
                else:
                    rng_str = "0 dòng"
                
                result_map[row_idx] = ("Thành công", rng_str, count)
                debug_data.append({"File": src_link[-10:], "Mode": w_mode})

        return True, "Hoàn tất", result_map, debug_data

    except Exception as e: 
        return False, f"Lỗi Ghi: {str(e)}", {}, []
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
# --- [ĐOẠN CODE MAIN_UI ĐÃ SỬA LỖI & LOGIC] ---
def main_ui():
    init_log_buffer()
    if not check_login(): return
    uid = st.session_state['current_user_id']; master_creds = get_master_creds()
    
    # --- HEADER ---
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

        # --- SCHEDULER (ĐÃ SỬA LỖI) ---
        with st.expander("⏰ Lịch chạy tự động", expanded=True):
            df_sched = load_scheduler_config(master_creds)
            curr_row = df_sched[df_sched[SCHED_COL_BLOCK] == sel_blk] if SCHED_COL_BLOCK in df_sched.columns else pd.DataFrame()
            d_type = str(curr_row.iloc[0].get(SCHED_COL_TYPE, "Không chạy")) if not curr_row.empty else "Không chạy"
            d_val1 = str(curr_row.iloc[0].get(SCHED_COL_VAL1, "")) if not curr_row.empty else ""
            d_val2 = str(curr_row.iloc[0].get(SCHED_COL_VAL2, "")) if not curr_row.empty else ""
            
            if d_type != "Không chạy": st.info(f"✅ {d_type} | {d_val1} {d_val2}")
            else: st.info("⚪ Chưa cài đặt")

            opts = ["Không chạy", "Chạy theo phút", "Hàng ngày", "Hàng tuần", "Hàng tháng"]
            new_type = st.selectbox("Kiểu:", opts, index=opts.index(d_type) if d_type in opts else 0)
            n_val1 = d_val1; n_val2 = d_val2
            
            if new_type == "Chạy theo phút":
                v = int(d_val1) if d_val1.isdigit() else 60
                n_val1 = str(st.slider("Cứ bao nhiêu phút chạy 1 lần?", 30, 180, max(30, v), 10))
                n_val2 = "" # [Fixed] Không cần giờ bắt đầu, chạy ngay khi đến hạn
            
            elif new_type == "Hàng ngày":
                hrs = [f"{i:02d}:00" for i in range(24)]; idx = hrs.index(d_val1) if d_val1 in hrs else 8
                n_val1 = st.selectbox("Chạy vào lúc mấy giờ:", hrs, index=idx)
                n_val2 = ""
            
            elif new_type == "Hàng tuần":
                days = ["T2", "T3", "T4", "T5", "T6", "T7", "CN"]; od = [x.strip() for x in d_val2.split(",")]
                sel_d = st.multiselect("Chọn các Thứ:", days, default=[d for d in od if d in days])
                hrs = [f"{i:02d}:00" for i in range(24)]; n_val1 = st.selectbox("Chạy vào lúc mấy giờ:", hrs)
                n_val2 = ",".join(sel_d)
            
            elif new_type == "Hàng tháng":
                dates = [str(i) for i in range(1,32)]; od = [x.strip() for x in d_val2.split(",")]
                sel_d = st.multiselect("Chọn các Ngày:", dates, default=[d for d in od if d in dates])
                hrs = [f"{i:02d}:00" for i in range(24)]; n_val1 = st.selectbox("Chạy vào lúc mấy giờ:", hrs)
                n_val2 = ",".join(sel_d)

            if st.button("💾 Lưu Lịch"):
                if SCHED_COL_BLOCK in df_sched.columns: df_sched = df_sched[df_sched[SCHED_COL_BLOCK] != sel_blk]
                new_r = {SCHED_COL_BLOCK: sel_blk, SCHED_COL_TYPE: new_type, SCHED_COL_VAL1: n_val1, SCHED_COL_VAL2: n_val2}
                df_sched = pd.concat([df_sched, pd.DataFrame([new_r])], ignore_index=True)
                # [Fixed] Truyền đúng 6 tham số
                save_scheduler_config(df_sched, master_creds, uid, new_type, n_val1, n_val2)
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
        if st.button("📚 HDSD", use_container_width=True): show_guide_popup()

    assigned_bot = assign_bot_to_block(sel_blk)
    c_head_1, c_head_2 = st.columns([3, 1.5])
    with c_head_1: st.title("💎 Kinkin Tool 2.0 (V109)"); st.caption(f"User: {uid}")
    with c_head_2: st.info(f"🤖 **Bot phụ trách:**"); st.code(assigned_bot, language="text")

    # --- MAIN EDITOR ---
    st.subheader(f"Config: {sel_blk}")
    curr_df = st.session_state['df_full_config'][st.session_state['df_full_config'][COL_BLOCK_NAME] == sel_blk].copy().reset_index(drop=True)
    if COL_COPY_FLAG not in curr_df.columns: curr_df.insert(0, COL_COPY_FLAG, False)
    if 'STT' not in curr_df.columns: curr_df.insert(1, 'STT', range(1, len(curr_df)+1))

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
        }, use_container_width=True, num_rows="dynamic", key="edt_v109"
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
            save_block_config_to_sheet(edt_df, sel_blk, master_creds, uid)
            rows = []
            for i, r in edt_df.iterrows():
                if str(r.get(COL_STATUS,'')).strip() == "Chưa chốt & đang cập nhật":
                    r_dict = r.to_dict(); r_dict['_index'] = i; rows.append(r_dict)
            if not rows: st.warning("Không có dòng nào để chạy."); st.stop()
            st_cont = st.status(f"🚀 Đang chạy {sel_blk} (Bot: {assigned_bot})...", expanded=True)
            ok, res, tot = process_pipeline_mixed(rows, uid, sel_blk, st_cont, forced_bot=assigned_bot)
            if isinstance(res, dict):
                for i, r in edt_df.iterrows():
                    if i in res: edt_df.at[i, COL_RESULT] = res[i][0]; edt_df.at[i, COL_LOG_ROW] = res[i][1]
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
            
            # --- [ĐOẠN CỐT LÕI ĐƯỢC CẢI TIẾN] ---
            for idx, blk in enumerate(all_blocks):
                # 1. Xác định Bot
                blk_bot = assign_bot_to_block(blk)
                main_st.write(f"⏳ [{idx+1}/{len(all_blocks)}] Xử lý: **{blk}** (Bot: {blk_bot})...")
                
                # 2. Lấy dữ liệu cấu hình của khối
                blk_df = full_df[full_df[COL_BLOCK_NAME] == blk].copy().reset_index(drop=True)
                rows_to_run = []
                for i, r in blk_df.iterrows():
                    if str(r.get(COL_STATUS,'')).strip() == "Chưa chốt & đang cập nhật":
                        r_dict = r.to_dict(); r_dict['_index'] = i; rows_to_run.append(r_dict)
                
                if rows_to_run:
                    # 3. Chạy xử lý
                    ok, res, tot = process_pipeline_mixed(rows_to_run, uid, blk, main_st, forced_bot=blk_bot)
                    total += len(rows_to_run)
                    
                    # 4. Lưu kết quả ngay lập tức
                    if isinstance(res, dict):
                        for i, r in blk_df.iterrows():
                            if i in res:
                                blk_df.at[i, COL_RESULT] = res[i][0]
                                blk_df.at[i, COL_LOG_ROW] = res[i][1]
                        save_block_config_to_sheet(blk_df, blk, master_creds, uid)
                    
                    # --- [QUAN TRỌNG NHẤT] ---
                    # Nghỉ 5 giây để Google Sheets kịp cập nhật index trước khi qua khối mới
                    # Tránh việc khối sau đọc nhầm dữ liệu của khối trước
                    main_st.write("💤 Đang đợi Google cập nhật dữ liệu...")
                    time.sleep(5) 
                    gc.collect() # Dọn dẹp bộ nhớ RAM cho nhẹ máy
            # -------------------------------------

            main_st.update(label="Hoàn tất!", state="complete", expanded=False)
            st.toast("Done Run All!"); time.sleep(2)

    with c3:
        if st.button("🔍 Quét Quyền", use_container_width=True):
            assigned_email = assign_bot_to_block(sel_blk)
            checking_creds = get_bot_credentials_from_secrets(assigned_email)
            with st.status(f"Đang dùng {assigned_email} để kiểm tra...", expanded=True) as st_chk:
                if checking_creds: check_permissions_ui(edt_df.to_dict('records'), checking_creds, st_chk, uid)
                else: st_chk.error(f"❌ Không tìm thấy Key cho {assigned_email}. Vui lòng kiểm tra Secrets!")

    # ... (Các đoạn code bên trên giữ nguyên) ...

    # ... (Các cột c1, c2, c3 giữ nguyên) ...

    with c4:
        if st.button("💾 Save Config", use_container_width=True):
            # BƯỚC 1: Lưu dữ liệu cấu hình vào Sheet Config
            # Hàm này đã có logic acquire_lock bên trong
            save_block_config_to_sheet(edt_df, sel_blk, master_creds, uid)
            
            # BƯỚC 2: Ghi log hành vi (Quan trọng: force_flush=True)
            # Ghi rõ user nào, làm gì, vào thời gian nào
            action_detail = f"Cập nhật cấu hình cho khối: {sel_blk}"
            log_user_action_buffered(master_creds, uid, "Lưu Cấu Hình", action_detail, force_flush=True)
            
            # BƯỚC 3: Xóa Cache và Thông báo
            # Xóa cache để đảm bảo lần tải lại trang sau sẽ thấy dữ liệu mới nhất
            st.cache_data.clear()
            
            st.toast("✅ Đã lưu cấu hình & Ghi nhận hành vi!", icon="💾")
            
            # BƯỚC 4: Rerun
            # Nghỉ 1 nhịp ngắn để Toast kịp hiện và Gspread kịp đóng kết nối
            time.sleep(1.0) 
            st.rerun()

    # --- PHẦN HIỂN THỊ LOG Ở CUỐI TRANG ---
    # Đảm bảo flush những log còn sót lại trong buffer (nếu có)
    flush_logs(master_creds, force=False) 
    
    st.divider()
    st.caption("Logs hành vi hệ thống")
    
    # Thêm key="refresh_logs_bottom" để tránh lỗi Duplicate Widget ID với nút Reload ở sidebar
    if st.button("Refresh Logs", key="refresh_logs_bottom"): 
        st.cache_data.clear()
        st.rerun()
    
    # Tải và hiển thị log
    try:
        logs = fetch_activity_logs(master_creds, 50)
        if not logs.empty: 
            st.dataframe(logs, use_container_width=True, hide_index=True)
        else:
            st.info("Chưa có dữ liệu log hành vi.")
    except Exception as e:
        st.error(f"Không thể tải logs: {str(e)}")

# if __name__ == "__main__": ... (Giữ nguyên)

if __name__ == "__main__":
    main_ui()


















