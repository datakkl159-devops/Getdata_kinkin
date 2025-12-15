import streamlit as st
import pandas as pd
import polars as pl
import requests
import io
import concurrent.futures
import time
import gspread
import json
from gspread_dataframe import get_as_dataframe
from datetime import datetime, timedelta
from google.oauth2 import service_account
import google.auth.transport.requests
import pytz
from collections import defaultdict

# --- CẤU HÌNH ---
st.set_page_config(page_title="Tool Quản Lý Data", layout="wide")

AUTHORIZED_USERS = {
    "admin2024": "Admin_Master",
    "team_hn": "Team_HaNoi",
    "team_hcm": "Team_HCM"
}

BOT_EMAIL_DISPLAY = "getdulieu@kin-kin-477902.iam.gserviceaccount.com"
SHEET_CONFIG_NAME = "luu_cau_hinh" 
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_LOCK_NAME = "sys_lock"

COL_LINK_SRC = "Link file nguồn"
COL_LABEL_SRC = "Sheet nguồn"
COL_MONTH_SRC = "Tháng chốt"

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- HÀM AUTH ---
def check_login():
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
    if 'current_user_id' not in st.session_state: st.session_state['current_user_id'] = "Unknown"

    if "auto_key" in st.query_params:
        key = st.query_params["auto_key"]
        if key in AUTHORIZED_USERS:
            st.session_state['logged_in'] = True
            st.session_state['current_user_id'] = AUTHORIZED_USERS[key]
            return True

    if st.session_state['logged_in']:
        if st.session_state['current_user_id'] == "Unknown": st.session_state['logged_in'] = False
        else: return True

    st.header("🔒 Đăng nhập hệ thống")
    pwd = st.text_input("Nhập mật khẩu truy cập:", type="password")
    if st.button("Đăng Nhập"):
        if pwd in AUTHORIZED_USERS:
            st.session_state['logged_in'] = True
            st.session_state['current_user_id'] = AUTHORIZED_USERS[pwd]
            st.toast(f"Xin chào {AUTHORIZED_USERS[pwd]}!", icon="👋")
            time.sleep(0.5)
            st.rerun()
        else: st.error("Mật khẩu không đúng!")
    return False

def get_creds():
    raw_creds = st.secrets["gcp_service_account"]
    if isinstance(raw_creds, str):
        try: creds_info = json.loads(raw_creds)
        except: return None
    else: creds_info = dict(raw_creds)
    if "private_key" in creds_info: creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
    return service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)

def extract_id(url):
    if not isinstance(url, str): return None
    if "docs.google.com" in url:
        try: return url.split("/d/")[1].split("/")[0]
        except: return None
    return None

# --- LOCKING SYSTEM ---
def get_system_lock(creds):
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_LOCK_NAME)
        except: 
            wks = sh.add_worksheet(SHEET_LOCK_NAME, rows=10, cols=5)
            wks.update([["is_locked", "user", "time_start"], ["FALSE", "", ""]])
            return False, "", ""
        val = wks.cell(2, 1).value
        user = wks.cell(2, 2).value
        time_str = wks.cell(2, 3).value
        if val == "TRUE":
            try:
                if (datetime.now() - datetime.strptime(time_str, "%d/%m/%Y %H:%M:%S")).total_seconds() > 1800: return False, "", ""
            except: pass
            return True, user, time_str
        return False, "", ""
    except: return False, "", ""

def set_system_lock(creds, user_id, lock=True):
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_LOCK_NAME)
        except: wks = sh.add_worksheet(SHEET_LOCK_NAME, rows=10, cols=5)
        now_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        wks.update("A2:C2", [["TRUE", user_id, now_str]] if lock else [["FALSE", "", ""]])
    except: pass

def write_detailed_log(creds, history_sheet_id, log_data_list):
    if not log_data_list: return
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(history_sheet_id)
        try: wks = sh.worksheet(SHEET_LOG_NAME)
        except: 
            wks = sh.add_worksheet(SHEET_LOG_NAME, rows=1000, cols=10)
            wks.append_row(["Ngày & giờ get dữ liệu", "Ngày chốt", "Tháng", "Nhân sự get", "Link nguồn", "Link đích", "Sheet Đích", "Sheet nguồn lấy dữ liệu", "Trạng Thái", "Số Dòng Đã Lấy"])
        wks.append_rows(log_data_list)
    except Exception as e: print(f"Lỗi log: {e}")

# --- CORE LOGIC (FIXED DELETE) ---
def fetch_single_csv_safe(row_config, token):
    if not isinstance(row_config, dict): return None, "Lỗi Config", "Lỗi Config"
    link_src = str(row_config.get('Link dữ liệu lấy dữ liệu', ''))
    source_label = str(row_config.get('Tên sheet nguồn dữ liệu gốc', '')).strip()
    month_val = str(row_config.get('Tháng', ''))
    sheet_id = extract_id(link_src)
    if not sheet_id: return None, sheet_id, "Link lỗi"
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            df = pl.read_csv(io.BytesIO(response.content), infer_schema_length=0)
            df = df.with_columns([
                pl.lit(link_src).cast(pl.Utf8).alias(COL_LINK_SRC),
                pl.lit(source_label).cast(pl.Utf8).alias(COL_LABEL_SRC),
                pl.lit(month_val).cast(pl.Utf8).alias(COL_MONTH_SRC)
            ])
            return df, sheet_id, "Thành công"
        return None, sheet_id, f"Lỗi HTTP {response.status_code}"
    except Exception as e: return None, sheet_id, str(e)

def smart_update_safe(df_new_updates, target_link, target_sheet_name, creds, links_to_remove):
    try:
        gc = gspread.authorize(creds)
        target_id = extract_id(target_link)
        if not target_id: return False, "Link đích lỗi"
        
        sh = gc.open_by_key(target_id) # <--- ĐỐI TƯỢNG SPREADSHEET
        real_sheet_name = str(target_sheet_name).strip()
        if not real_sheet_name: real_sheet_name = "Tong_Hop_Data"
        
        try: wks = sh.worksheet(real_sheet_name) # <--- ĐỐI TƯỢNG WORKSHEET
        except: wks = sh.add_worksheet(title=real_sheet_name, rows=1000, cols=20)
        
        token = creds.token 
        if not token:
            import google.auth.transport.requests
            auth_req = google.auth.transport.requests.Request()
            creds.refresh(auth_req)
            token = creds.token

        # 1. DELETE OLD
        existing_headers = []
        try: existing_headers = wks.row_values(1)
        except: pass

        if existing_headers:
            try: link_col_idx = existing_headers.index(COL_LINK_SRC) + 1
            except ValueError: link_col_idx = None
            
            if link_col_idx:
                col_values = wks.col_values(link_col_idx)
                rows_to_delete = []
                for i, val in enumerate(col_values):
                    if val in links_to_remove: rows_to_delete.append(i + 1)
                
                if rows_to_delete:
                    rows_to_delete.sort()
                    ranges = []
                    start = rows_to_delete[0]; end = start
                    for r in rows_to_delete[1:]:
                        if r == end + 1: end = r
                        else: ranges.append((start, end)); start = r; end = r
                    ranges.append((start, end))
                    
                    delete_reqs = []
                    for start, end in reversed(ranges):
                        delete_reqs.append({
                            "deleteDimension": {
                                "range": {
                                    "sheetId": wks.id,
                                    "dimension": "ROWS",
                                    "startIndex": start - 1,
                                    "endIndex": end
                                }
                            }
                        })
                    
                    if delete_reqs:
                        # FIX LỖI Ở ĐÂY: DÙNG sh.batch_update THAY VÌ wks.batch_update
                        sh.batch_update({'requests': delete_reqs}) 
                        time.sleep(1)

        # 2. APPEND NEW
        if not df_new_updates.is_empty():
            pdf = df_new_updates.to_pandas().fillna('')
            data_values = pdf.values.tolist()
            if not existing_headers:
                headers = pdf.columns.tolist()
                wks.append_row(headers)
            
            BATCH_SIZE = 5000
            total_rows = len(data_values)
            for i in range(0, total_rows, BATCH_SIZE):
                chunk = data_values[i : i + BATCH_SIZE]
                wks.append_rows(chunk)
                time.sleep(1)
            return True, f"Sheet '{real_sheet_name}': +{total_rows} dòng (Append)."
            
        return True, f"Sheet '{real_sheet_name}': Đã làm sạch (nếu có)."

    except Exception as e: return False, f"Lỗi Update: {str(e)}"

def process_pipeline(rows_to_run, user_id):
    creds = get_creds()
    is_locked, locking_user, lock_time = get_system_lock(creds)
    if is_locked and locking_user != user_id:
        return False, f"HỆ THỐNG ĐANG BẬN! {locking_user} đang chạy từ {lock_time}."
    
    set_system_lock(creds, user_id, lock=True)
    try:
        auth_req = google.auth.transport.requests.Request() 
        creds.refresh(auth_req)
        token = creds.token
        
        grouped_tasks = defaultdict(list)
        for row in rows_to_run:
            t_link = row.get('Link dữ liệu đích', '')
            t_sheet = str(row.get('Tên sheet dữ liệu đích', '')).strip()
            if not t_sheet: t_sheet = "Tong_Hop_Data"
            grouped_tasks[(t_link, t_sheet)].append(row)

        final_messages = []
        all_success = True
        log_entries = []
        tz_vn = pytz.timezone('Asia/Ho_Chi_Minh')
        time_now = datetime.now(tz_vn).strftime("%d/%m/%Y %H:%M:%S")

        for (target_link, target_sheet), group_rows in grouped_tasks.items():
            if not target_link: continue
            results = []
            links_remove = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(fetch_single_csv_safe, row, token): row for row in group_rows}
                for future in concurrent.futures.as_completed(futures):
                    row = futures[future]
                    df, sid, status = future.result()
                    src_link = row.get('Link dữ liệu lấy dữ liệu', '')
                    log_row = [time_now, str(row.get('Ngày chốt', '')), str(row.get('Tháng', '')), user_id, src_link, target_link, target_sheet, row.get('Tên sheet nguồn dữ liệu gốc', ''), status, str(df.height) if df is not None else "0"]
                    log_entries.append(log_row)
                    if df is not None:
                        results.append(df)
                        links_remove.append(src_link)
            
            if results or links_remove:
                if results: df_new = pl.concat(results, how="vertical", rechunk=True)
                else: df_new = pl.DataFrame()
                
                success, msg = smart_update_safe(df_new, target_link, target_sheet, creds, links_remove)
                final_messages.append(msg)
                if not success: all_success = False
            else:
                final_messages.append(f"Sheet '{target_sheet}': Không tải được dữ liệu.")
                all_success = False
                
        history_id = st.secrets["gcp_service_account"]["history_sheet_id"]
        write_detailed_log(creds, history_id, log_entries)
        return all_success, " | ".join(final_messages)
    finally:
        set_system_lock(creds, user_id, lock=False)

# --- UI CHÍNH ---
def main_ui():
    if not check_login(): return
    user_id = st.session_state['current_user_id']
    st.title(f"⚙️ Tool Quản Lý Data (User: {user_id})")
    creds = get_creds()

    def load_conf(creds):
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        df = df.dropna(how='all')
        df = df[df['Link dữ liệu lấy dữ liệu'].astype(str).str.len() > 5]
        for col in ['Chọn', 'STT']:
            if col in df.columns: df = df.drop(columns=[col])
        rename_map = {'Tên sheet dữ liệu': 'Tên sheet dữ liệu đích', 'Tên nguồn (Nhãn)': 'Tên sheet nguồn dữ liệu gốc'}
        for old, new in rename_map.items():
            if old in df.columns and new not in df.columns: df = df.rename(columns={old: new})
        if 'Trạng thái' not in df.columns: df['Trạng thái'] = "Chưa chốt & đang cập nhật"
        else: df['Trạng thái'] = df['Trạng thái'].apply(lambda x: "Đã chốt" if str(x).strip() in ["Đã chốt", "Đã cập nhật", "TRUE"] else "Chưa chốt & đang cập nhật")
        if 'Ngày chốt' in df.columns: df['Ngày chốt'] = pd.to_datetime(df['Ngày chốt'], errors='coerce').dt.date
        for c in ['Tên sheet dữ liệu đích', 'Tên sheet nguồn dữ liệu gốc', 'Hành động']:
            if c not in df.columns: df[c] = ""
        df.insert(0, 'STT', range(1, len(df) + 1))
        return df

    def save_conf(df_ui, creds):
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        df_save = df_ui.copy()
        if 'STT' in df_save.columns: df_save = df_save.drop(columns=['STT'])
        if 'Tên sheet dữ liệu đích' in df_save.columns: df_save['Tên sheet dữ liệu đích'] = df_save['Tên sheet dữ liệu đích'].astype(str).str.strip()
        if 'Ngày chốt' in df_save.columns: df_save['Ngày chốt'] = df_save['Ngày chốt'].astype(str).replace({'NaT': '', 'nan': '', 'None': ''})
        wks.clear()
        wks.update([df_save.columns.tolist()] + df_save.fillna('').values.tolist())
        st.toast("✅ Đã lưu cấu hình!", icon="💾")

    def man_scan(df):
        errs = []
        for idx, row in df.iterrows():
            link = str(row.get('Link dữ liệu lấy dữ liệu', ''))
            if "docs.google.com" in link:
                ok, msg = verify_access_fast(link, creds)
                if not ok: errs.append(f"Dòng {row.get('STT')} (Nguồn): {msg}")
        return errs

    if 'df_config' not in st.session_state:
        with st.spinner("Đang tải..."): st.session_state['df_config'] = load_conf(creds)

    col_order = ["STT", "Trạng thái", "Ngày chốt", "Tháng", "Link dữ liệu lấy dữ liệu", "Link dữ liệu đích", "Tên sheet dữ liệu đích", "Tên sheet nguồn dữ liệu gốc", "Hành động"]
    edited_df = st.data_editor(st.session_state['df_config'], column_order=col_order, column_config={"STT": st.column_config.NumberColumn("STT", disabled=True, width="small"), "Trạng thái": st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt & đang cập nhật", "Đã chốt"], required=True, width="medium"), "Ngày chốt": st.column_config.DateColumn("Ngày chốt", format="DD/MM/YYYY"), "Link dữ liệu lấy dữ liệu": st.column_config.TextColumn("Link Nguồn", width="medium"), "Link dữ liệu đích": st.column_config.TextColumn("Link Đích", width="medium"), "Hành động": st.column_config.TextColumn("Kết quả", disabled=True)}, use_container_width=True, hide_index=True, key="editor")

    if not edited_df.equals(st.session_state['df_config']):
        edited_df = edited_df.reset_index(drop=True)
        edited_df['STT'] = range(1, len(edited_df) + 1)
        for idx, row in edited_df.iterrows():
            if row['Trạng thái'] == "Chưa chốt & đang cập nhật": edited_df.at[idx, 'Hành động'] = "Sẽ chạy"
            else: edited_df.at[idx, 'Hành động'] = ""
        st.session_state['df_config'] = edited_df
        st.rerun()

    st.divider()
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        wks_sys = sh.worksheet("sys_config")
        data_conf = wks_sys.get_all_values()
        saved_hour = 8; saved_freq = "1 ngày/1 lần"
        for r in data_conf:
            if r[0] == "run_hour": saved_hour = int(r[1])
            if r[0] == "run_freq": saved_freq = r[1]
    except: pass

    st.subheader("⏰ Cài Đặt Tự Động")
    c1, c2, c3 = st.columns(3)
    with c1: new_freq = st.selectbox("Tần suất:", ["1 ngày/1 lần", "1 tuần/1 lần", "1 tháng/1 lần"], index=["1 ngày/1 lần", "1 tuần/1 lần", "1 tháng/1 lần"].index(saved_freq))
    with c2: new_hour = st.slider("Giờ chạy (VN):", 0, 23, value=saved_hour)
    with c3:
        st.write("")
        if st.button("Lưu Cài Đặt"):
            wks_sys.update("B2", str(saved_hour)); wks_sys.update("B3", saved_freq)
            st.toast("Đã lưu!", icon="✅")

    st.divider()
    col_run, col_scan, col_save = st.columns([3, 1, 1])
    with col_run:
        if st.button("▶️ CẬP NHẬT DỮ LIỆU (Chưa chốt)", type="primary"):
            is_locked, locking_user, lock_time = get_system_lock(creds)
            if is_locked and locking_user != user_id:
                st.error(f"❌ {locking_user} đang chạy. Vui lòng đợi.")
            else:
                rows_run = edited_df[edited_df['Trạng thái'] == "Chưa chốt & đang cập nhật"].to_dict('records')
                if not rows_run: st.warning("⚠️ Không có dòng nào chưa chốt.")
                else:
                    with st.status(f"Đang xử lý {len(rows_run)} nguồn...", expanded=True):
                        success, msg = process_pipeline(rows_run, user_id)
                        if success:
                            st.success(f"Kết quả: {msg}")
                            for idx, row in edited_df.iterrows():
                                if row['Trạng thái'] == "Chưa chốt & đang cập nhật":
                                    edited_df.at[idx, 'Hành động'] = "Vừa xong"
                            save_conf(edited_df, creds)
                            st.session_state['df_config'] = edited_df
                            time.sleep(1)
                            st.rerun()
                        else: st.error(msg)

    with col_scan:
        if st.button("🔍 Quét Quyền"):
            errs = man_scan(edited_df)
            if errs: st.error(f"Có {len(errs)} link lỗi!"); st.write(errs)
            else: st.toast("Link OK!", icon="✨")

    with col_save:
        if st.button("💾 Lưu Cấu Hình"):
            save_conf(edited_df, creds)

if __name__ == "__main__":
    main_ui()
