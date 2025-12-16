import streamlit as st
import pandas as pd
import polars as pl
import requests
import io
import time
import gspread
import json
from gspread_dataframe import get_as_dataframe
from datetime import datetime
from google.oauth2 import service_account
import google.auth.transport.requests
import pytz
from collections import defaultdict

# --- 1. CẤU HÌNH HỆ THỐNG ---
st.set_page_config(page_title="Tool Quản Lý Data Multi-Block", layout="wide")

AUTHORIZED_USERS = {
    "admin2024": "Admin_Master",
    "team_hn": "Team_HaNoi",
    "team_hcm": "Team_HCM"
}

SHEET_CONFIG_NAME = "luu_cau_hinh" 
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_LOCK_NAME = "sys_lock"
SHEET_SYS_CONFIG = "sys_config"

COL_LINK_SRC = "Link file nguồn"
COL_LABEL_SRC = "Sheet nguồn"
COL_MONTH_SRC = "Tháng chốt"
COL_BLOCK_NAME = "Block_Name"

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
DEFAULT_BLOCK_NAME = "Block_Mac_Dinh"

# --- 2. HÀM CƠ BẢN ---
def check_login():
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
    if 'current_user_id' not in st.session_state: st.session_state['current_user_id'] = "Unknown"
    if "auto_key" in st.query_params:
        key = st.query_params["auto_key"]
        if key in AUTHORIZED_USERS:
            st.session_state['logged_in'] = True; st.session_state['current_user_id'] = AUTHORIZED_USERS[key]; return True
    if st.session_state['logged_in']: return True
    st.header("🔒 Đăng nhập hệ thống"); pwd = st.text_input("Nhập mật khẩu truy cập:", type="password")
    if st.button("Đăng Nhập"):
        if pwd in AUTHORIZED_USERS:
            st.session_state['logged_in'] = True; st.session_state['current_user_id'] = AUTHORIZED_USERS[pwd]
            st.toast(f"Xin chào {AUTHORIZED_USERS[pwd]}!", icon="👋"); time.sleep(0.5); st.rerun()
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

def get_system_lock(creds):
    try:
        gc = gspread.authorize(creds); sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_LOCK_NAME)
        except: wks = sh.add_worksheet(SHEET_LOCK_NAME, rows=10, cols=5); wks.update([["is_locked", "user", "time_start"], ["FALSE", "", ""]]); return False, "", ""
        val = wks.cell(2, 1).value; user = wks.cell(2, 2).value; time_str = wks.cell(2, 3).value
        if val == "TRUE":
            try:
                if (datetime.now() - datetime.strptime(time_str, "%d/%m/%Y %H:%M:%S")).total_seconds() > 1800: return False, "", ""
            except: pass
            return True, user, time_str
        return False, "", ""
    except: return False, "", ""

def set_system_lock(creds, user_id, lock=True):
    try:
        gc = gspread.authorize(creds); sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_LOCK_NAME)
        except: wks = sh.add_worksheet(SHEET_LOCK_NAME, rows=10, cols=5)
        now_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        wks.update("A2:C2", [["TRUE", user_id, now_str]] if lock else [["FALSE", "", ""]])
    except: pass

def write_detailed_log(creds, history_sheet_id, log_data_list):
    if not log_data_list: return
    try:
        gc = gspread.authorize(creds); sh = gc.open_by_key(history_sheet_id)
        try: wks = sh.worksheet(SHEET_LOG_NAME)
        except: wks = sh.add_worksheet(SHEET_LOG_NAME, rows=1000, cols=11); wks.append_row(["Ngày & giờ get dữ liệu", "Ngày chốt", "Tháng", "Nhân sự get", "Link nguồn", "Link đích", "Sheet Đích", "Sheet nguồn lấy dữ liệu", "Trạng Thái", "Số Dòng Đã Lấy", "Dòng dữ liệu"])
        wks.append_rows(log_data_list)
    except: pass

def verify_access_fast(url, creds):
    sheet_id = extract_id(url)
    if not sheet_id: return False, "Link lỗi/Sai định dạng"
    try: gc = gspread.authorize(creds); gc.open_by_key(sheet_id); return True, "OK"
    except gspread.exceptions.SpreadsheetNotFound: return False, "❌ Không tìm thấy file"
    except gspread.exceptions.APIError as e: return False, "⛔ Chưa cấp quyền (403)" if "403" in str(e) else f"❌ Lỗi API: {e}"
    except Exception as e: return False, f"❌ Lỗi: {e}"

# --- 3. CORE LOGIC (ĐÃ CẬP NHẬT ĐỂ ĐẾM DÒNG) ---
def fetch_single_csv_safe(row_config, creds, token):
    if not isinstance(row_config, dict): return None, "Lỗi Config", "Lỗi Config"
    link_src = str(row_config.get('Link dữ liệu lấy dữ liệu', '')); source_label = str(row_config.get('Tên sheet nguồn dữ liệu gốc', '')).strip(); month_val = str(row_config.get('Tháng', '')); sheet_id = extract_id(link_src)
    if not sheet_id: return None, sheet_id, "Link lỗi"
    df = None; status_msg = ""; target_gid = None
    try:
        gc = gspread.authorize(creds); sh_source = gc.open_by_key(sheet_id)
        if source_label:
            try: wks_source = sh_source.worksheet(source_label); target_gid = wks_source.id
            except: return None, sheet_id, f"❌ Không tìm thấy sheet: '{source_label}'"
        else: wks_source = sh_source.sheet1; target_gid = wks_source.id
    except Exception as e: return None, sheet_id, f"Lỗi truy cập file nguồn: {str(e)}"
    if target_gid is not None:
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={target_gid}"; headers = {'Authorization': f'Bearer {token}'}
        try:
            response = requests.get(url, headers=headers, timeout=20)
            if response.status_code == 200: df = pl.read_csv(io.BytesIO(response.content), infer_schema_length=0); status_msg = f"Thành công"
        except: pass
    if df is None or df.is_empty():
        try:
            data = wks_source.get_all_values()
            if data and len(data) > 0: df = pl.DataFrame(data[1:], schema=data[0], orient="row"); df = df.select(pl.all().cast(pl.Utf8)); status_msg = f"Thành công"
            else: status_msg = "Sheet rỗng"
        except Exception as e: return None, sheet_id, f"Lỗi tải data: {str(e)}"
    if df is not None and not df.is_empty():
        df = df.with_columns([pl.lit(link_src).cast(pl.Utf8).alias(COL_LINK_SRC), pl.lit(source_label).cast(pl.Utf8).alias(COL_LABEL_SRC), pl.lit(month_val).cast(pl.Utf8).alias(COL_MONTH_SRC)])
        return df, sheet_id, status_msg
    return None, sheet_id, "Không lấy được dữ liệu"

def scan_realtime_row_ranges(target_link, target_sheet_name, creds):
    results = {}
    try:
        gc = gspread.authorize(creds); target_id = extract_id(target_link)
        if not target_id: return {}
        sh = gc.open_by_key(target_id); real_sheet_name = str(target_sheet_name).strip() if str(target_sheet_name).strip() else "Tong_Hop_Data"
        try: wks = sh.worksheet(real_sheet_name)
        except: return {}
        all_data = wks.get_all_values()
        if not all_data: return {}
        try: link_col_idx = all_data[0].index(COL_LINK_SRC)
        except ValueError: return {} 
        temp_map = {}
        for i, row in enumerate(all_data[1:], start=2):
            if len(row) > link_col_idx:
                link_val = row[link_col_idx]
                if link_val:
                    if link_val not in temp_map: temp_map[link_val] = [i, i]
                    else: temp_map[link_val][1] = i 
        for link, (start, end) in temp_map.items(): results[link] = f"{start} - {end}"
    except: pass
    return results

def smart_update_safe(tasks_list, target_link, target_sheet_name, creds):
    added_rows_count = 0 # Đếm số dòng thêm mới
    try:
        gc = gspread.authorize(creds); target_id = extract_id(target_link)
        if not target_id: return False, "Link đích lỗi", 0
        sh = gc.open_by_key(target_id); real_sheet_name = str(target_sheet_name).strip() if str(target_sheet_name).strip() else "Tong_Hop_Data"
        try: wks = sh.worksheet(real_sheet_name)
        except: wks = sh.add_worksheet(title=real_sheet_name, rows=1000, cols=20)
        
        links_to_remove = [t[1] for t in tasks_list]
        existing_headers = []
        try: existing_headers = wks.row_values(1)
        except: pass
        if existing_headers:
            try: 
                link_col_idx = existing_headers.index(COL_LINK_SRC) + 1; col_values = wks.col_values(link_col_idx); rows_to_delete = []
                for i, val in enumerate(col_values):
                    if i > 0 and val in links_to_remove: rows_to_delete.append(i + 1)
                if rows_to_delete:
                    rows_to_delete.sort(); ranges = []; start = rows_to_delete[0]; end = start
                    for r in rows_to_delete[1:]:
                        if r == end + 1: end = r
                        else: ranges.append((start, end)); start = r; end = r
                    ranges.append((start, end)); delete_reqs = []
                    for start, end in reversed(ranges): delete_reqs.append({"deleteDimension": {"range": {"sheetId": wks.id, "dimension": "ROWS", "startIndex": start - 1, "endIndex": end}}})
                    if delete_reqs: sh.batch_update({'requests': delete_reqs}); time.sleep(1)
            except: pass

        dfs_to_concat = []; all_new_cols = set()
        for t in tasks_list: all_new_cols.update(t[0].columns)
        all_new_cols = list(all_new_cols)

        if not existing_headers: final_headers = all_new_cols; wks.append_row(final_headers); existing_headers = final_headers
        else:
            missing = [c for c in all_new_cols if c not in existing_headers]
            if missing: wks.resize(cols=len(existing_headers) + len(missing)); final_headers = existing_headers + missing; wks.update(range_name="A1", values=[final_headers]); existing_headers = final_headers
            else: final_headers = existing_headers

        for df, src_link in tasks_list:
            pdf = df.to_pandas().fillna(''); pdf_aligned = pdf.reindex(columns=final_headers, fill_value="")
            dfs_to_concat.append(pdf_aligned)

        if dfs_to_concat:
            final_pdf = pd.concat(dfs_to_concat, ignore_index=True); data_values = final_pdf.values.tolist(); BATCH_SIZE = 5000
            added_rows_count = len(data_values) # Cập nhật số dòng
            for i in range(0, len(data_values), BATCH_SIZE): chunk = data_values[i : i + BATCH_SIZE]; wks.append_rows(chunk); time.sleep(1)
            return True, "Thành công", added_rows_count
        return True, "Thành công (Không có data mới)", 0
    except Exception as e: return False, f"Lỗi Ghi: {str(e)}", 0

def process_pipeline(rows_to_run, user_id):
    # Stats Tracking
    total_sources = 0
    total_rows_added = 0
    start_time = time.time()
    
    creds = get_creds(); is_locked, locking_user, lock_time = get_system_lock(creds)
    if is_locked and locking_user != user_id: return False, f"HỆ THỐNG ĐANG BẬN! {locking_user} đang chạy từ {lock_time}.", {}
    set_system_lock(creds, user_id, lock=True)
    try:
        import google.auth.transport.requests; auth_req = google.auth.transport.requests.Request(); creds.refresh(auth_req); token = creds.token
        grouped_tasks = defaultdict(list)
        for row in rows_to_run:
            t_link = row.get('Link dữ liệu đích', ''); t_sheet = str(row.get('Tên sheet dữ liệu đích', '')).strip()
            if not t_sheet: t_sheet = "Tong_Hop_Data"
            grouped_tasks[(t_link, t_sheet)].append(row)

        global_results_map = {}; all_success = True; log_entries = []; tz_vn = pytz.timezone('Asia/Ho_Chi_Minh'); time_now = datetime.now(tz_vn).strftime("%d/%m/%Y %H:%M:%S")

        for (target_link, target_sheet), group_rows in grouped_tasks.items():
            if not target_link: continue
            tasks_list = []
            for row in group_rows:
                df, sid, status = fetch_single_csv_safe(row, creds, token); src_link = row.get('Link dữ liệu lấy dữ liệu', '')
                if df is not None: tasks_list.append((df, src_link)); total_sources += 1
                else: global_results_map[src_link] = ("Lỗi tải/Quyền", ""); log_entries.append([time_now, str(row.get('Ngày chốt', '')), str(row.get('Tháng', '')), user_id, src_link, target_link, target_sheet, row.get('Tên sheet nguồn dữ liệu gốc', ''), "Lỗi tải", "0", ""])

            msg_update = ""; success_update = True; added_count = 0
            if tasks_list:
                success_update, msg_update, added_count = smart_update_safe(tasks_list, target_link, target_sheet, creds)
                total_rows_added += added_count
                if not success_update: all_success = False
            
            realtime_ranges = scan_realtime_row_ranges(target_link, target_sheet, creds)
            for link, rng in realtime_ranges.items():
                if link not in global_results_map: global_results_map[link] = ("Cập nhật lại", rng)
                else: global_results_map[link] = (global_results_map[link][0], rng)

            for row in group_rows:
                s_link = row.get('Link dữ liệu lấy dữ liệu', ''); status_str = "Thành công" if success_update else f"Lỗi: {msg_update}"; final_range = realtime_ranges.get(s_link, "")
                if any(t[1] == s_link for t in tasks_list) or (s_link in global_results_map and "Lỗi" in global_results_map[s_link][0]):
                    height = "0"
                    for df, sl in tasks_list:
                        if sl == s_link: height = str(df.height)
                    log_entries.append([time_now, str(row.get('Ngày chốt', '')), str(row.get('Tháng', '')), user_id, s_link, target_link, target_sheet, row.get('Tên sheet nguồn dữ liệu gốc', ''), status_str, height, final_range])
                    global_results_map[s_link] = (status_str, final_range)
        
        write_detailed_log(creds, st.secrets["gcp_service_account"]["history_sheet_id"], log_entries)
        
        end_time = time.time()
        elapsed_time = round(end_time - start_time, 2)
        
        # Return Stats dictionary
        run_stats = {
            "sources": total_sources,
            "rows": total_rows_added,
            "time": elapsed_time
        }
        
        return all_success, global_results_map, run_stats
    finally: set_system_lock(creds, user_id, lock=False)

# --- 4. GIAO DIỆN CHÍNH ---
def main_ui():
    if not check_login(): return
    user_id = st.session_state['current_user_id']
    st.title(f"⚙️ Tool Quản Lý Data Đa Khối (User: {user_id})")
    creds = get_creds()

    def load_data(creds):
        gc = gspread.authorize(creds); sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME); df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str); df = df.dropna(how='all')
        rename_map = {'Tên sheet dữ liệu': 'Tên sheet dữ liệu đích', 'Tên nguồn (Nhãn)': 'Tên sheet nguồn dữ liệu gốc', 'Link file nguồn': 'Link dữ liệu lấy dữ liệu', 'Link file đích': 'Link dữ liệu đích'}
        for old, new in rename_map.items():
            if old in df.columns and new not in df.columns: df = df.rename(columns={old: new})
        required_cols = ['Block_Name', 'Trạng thái', 'Ngày chốt', 'Tháng', 'Link dữ liệu lấy dữ liệu', 'Link dữ liệu đích', 'Tên sheet dữ liệu đích', 'Tên sheet nguồn dữ liệu gốc', 'Kết quả', 'Dòng dữ liệu']
        for c in required_cols:
            if c not in df.columns: df[c] = ""
        df['Block_Name'] = df['Block_Name'].fillna(DEFAULT_BLOCK_NAME).replace('', DEFAULT_BLOCK_NAME)
        if 'STT' in df.columns: df = df.drop(columns=['STT'])
        df.insert(0, 'STT', range(1, len(df) + 1))
        
        try: wks_sys = sh.worksheet(SHEET_SYS_CONFIG)
        except: wks_sys = sh.add_worksheet(SHEET_SYS_CONFIG, rows=20, cols=3)
        df_sys = get_as_dataframe(wks_sys, evaluate_formulas=True, dtype=str)
        if 'Block_Name' not in df_sys.columns: wks_sys.clear(); wks_sys.update([['Block_Name', 'Run_Hour', 'Run_Freq']]); df_sys = pd.DataFrame(columns=['Block_Name', 'Run_Hour', 'Run_Freq'])
        if DEFAULT_BLOCK_NAME not in df_sys['Block_Name'].values: new_row = pd.DataFrame([{'Block_Name': DEFAULT_BLOCK_NAME, 'Run_Hour': '8', 'Run_Freq': 'Hàng ngày'}]); df_sys = pd.concat([df_sys, new_row], ignore_index=True); wks_sys.append_row([DEFAULT_BLOCK_NAME, '8', 'Hàng ngày'])
        df_sys = df_sys.dropna(subset=['Block_Name'])
        return df, df_sys

    def save_data(df_ui, creds):
        gc = gspread.authorize(creds); sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME); df_save = df_ui.copy()
        if 'STT' in df_save.columns: df_save = df_save.drop(columns=['STT'])
        wks.clear(); wks.update([df_save.columns.tolist()] + df_save.fillna('').values.tolist())
        st.toast("✅ Đã lưu cấu hình dữ liệu!", icon="💾")

    def save_sys_config(df_sys, creds):
        gc = gspread.authorize(creds); sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_SYS_CONFIG); wks.clear(); wks.update([df_sys.columns.tolist()] + df_sys.fillna('').values.tolist())
        st.toast("✅ Đã lưu cấu hình khối!", icon="💾")

    if 'df_config' not in st.session_state or 'df_sys' not in st.session_state:
        with st.spinner("Đang tải dữ liệu..."): d_conf, d_sys = load_data(creds); st.session_state['df_config'] = d_conf; st.session_state['df_sys'] = d_sys

    with st.expander("🛠️ Quản lý Khối (Thêm/Xóa)", expanded=False):
        c_add, c_del = st.columns([2, 1])
        with c_add:
            with st.form("add_block_form", clear_on_submit=True):
                new_block_name = st.text_input("Tên khối mới:")
                if st.form_submit_button("Thêm Khối"):
                    if new_block_name and new_block_name not in st.session_state['df_sys']['Block_Name'].values:
                        new_row = pd.DataFrame([{'Block_Name': new_block_name, 'Run_Hour': '8', 'Run_Freq': 'Hàng ngày'}]); st.session_state['df_sys'] = pd.concat([st.session_state['df_sys'], new_row], ignore_index=True); save_sys_config(st.session_state['df_sys'], creds); st.rerun()
                    elif new_block_name: st.warning("Tên khối đã tồn tại.")

    if st.button("🚀 CHẠY TẤT CẢ CÁC KHỐI (Tuần tự)", type="primary"):
        blocks = st.session_state['df_sys']['Block_Name'].unique(); progress_bar = st.progress(0); status_text = st.empty()
        total_s = 0; total_r = 0; total_t = 0
        for i, b_name in enumerate(blocks):
            status_text.text(f"Đang xử lý Khối: {b_name}..."); df_curr = st.session_state['df_config']
            rows_run = df_curr[(df_curr['Block_Name'] == b_name) & (df_curr['Trạng thái'] != "Đã chốt")].to_dict('records')
            rows_run = [r for r in rows_run if len(str(r.get('Link dữ liệu lấy dữ liệu', ''))) > 5]
            if rows_run:
                ok, res_map, stats = process_pipeline(rows_run, f"{user_id}_ALL_RUN")
                total_s += stats.get('sources', 0); total_r += stats.get('rows', 0); total_t += stats.get('time', 0)
                for idx, row in df_curr.iterrows():
                    if row['Block_Name'] == b_name and row.get('Link dữ liệu lấy dữ liệu', '') in res_map:
                        msg, rng = res_map[row.get('Link dữ liệu lấy dữ liệu')]
                        if row['Trạng thái'] != "Đã chốt": st.session_state['df_config'].at[idx, 'Kết quả'] = msg
                        st.session_state['df_config'].at[idx, 'Dòng dữ liệu'] = rng
            progress_bar.progress((i + 1) / len(blocks))
        save_data(st.session_state['df_config'], creds); status_text.text("✅ Đã chạy xong tất cả!"); 
        st.success(f"🎉 TỔNG KẾT: Xử lý {total_s} nguồn | Thêm +{total_r} dòng | Tổng thời gian: {round(total_t, 2)}s")
        time.sleep(3); st.rerun()

    all_blocks = st.session_state['df_sys']['Block_Name'].unique()
    for block_name in all_blocks:
        try: block_sys_info = st.session_state['df_sys'][st.session_state['df_sys']['Block_Name'] == block_name].iloc[0]
        except: continue
        cur_hour = int(block_sys_info.get('Run_Hour', 8)); cur_freq = block_sys_info.get('Run_Freq', 'Hàng ngày')
        with st.container(border=True):
            c_head, c_btn = st.columns([3, 1])
            with c_head: st.subheader(f"📦 Khối: {block_name}" + (" (Mặc định)" if block_name == DEFAULT_BLOCK_NAME else ""))
            with c_btn:
                if block_name != DEFAULT_BLOCK_NAME:
                    if st.button("🗑️ Xóa Khối", key=f"del_{block_name}"):
                        st.session_state['df_sys'] = st.session_state['df_sys'][st.session_state['df_sys']['Block_Name'] != block_name]; save_sys_config(st.session_state['df_sys'], creds); st.rerun()
            c_freq, c_hour, c_save_sche = st.columns([2, 2, 1])
            with c_freq: new_f = st.selectbox("Tần suất:", ["Hàng ngày", "Hàng tuần", "Hàng tháng"], index=["Hàng ngày", "Hàng tuần", "Hàng tháng"].index(cur_freq) if cur_freq in ["Hàng ngày", "Hàng tuần", "Hàng tháng"] else 0, key=f"freq_{block_name}")
            with c_hour: new_h = st.slider("Giờ chạy (VN):", 0, 23, value=cur_hour, key=f"hour_{block_name}")
            with c_save_sche: 
                st.write(""); 
                if st.button("Lưu giờ", key=f"save_h_{block_name}"):
                    idx = st.session_state['df_sys'].index[st.session_state['df_sys']['Block_Name'] == block_name].tolist()[0]
                    st.session_state['df_sys'].at[idx, 'Run_Hour'] = str(new_h); st.session_state['df_sys'].at[idx, 'Run_Freq'] = new_f
                    save_sys_config(st.session_state['df_sys'], creds); st.toast(f"Đã lưu lịch cho khối {block_name}")

            df_block_view = st.session_state['df_config'][st.session_state['df_config']['Block_Name'] == block_name].copy()
            col_order = ["STT", "Trạng thái", "Ngày chốt", "Tháng", "Link dữ liệu lấy dữ liệu", "Link dữ liệu đích", "Tên sheet dữ liệu đích", "Tên sheet nguồn dữ liệu gốc", "Kết quả", "Dòng dữ liệu"]
            edited_block_df = st.data_editor(df_block_view, column_order=col_order, column_config={"STT": st.column_config.NumberColumn("STT", disabled=True, width="small"), "Trạng thái": st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt & đang cập nhật", "Đã chốt"], required=True), "Ngày chốt": st.column_config.DateColumn("Ngày chốt", format="DD/MM/YYYY"), "Kết quả": st.column_config.TextColumn("Kết quả", disabled=True), "Dòng dữ liệu": st.column_config.TextColumn("Dòng Dữ Liệu", disabled=True)}, use_container_width=True, hide_index=True, num_rows="dynamic", key=f"editor_{block_name}")
            if not edited_block_df.equals(df_block_view):
                edited_block_df['Block_Name'] = block_name
                df_main_no_block = st.session_state['df_config'][st.session_state['df_config']['Block_Name'] != block_name]
                st.session_state['df_config'] = pd.concat([df_main_no_block, edited_block_df], ignore_index=True); st.session_state['df_config'].reset_index(drop=True, inplace=True); st.session_state['df_config']['STT'] = range(1, len(st.session_state['df_config']) + 1)
                save_data(st.session_state['df_config'], creds); st.rerun()

            c_run_b, c_scan_b = st.columns([1, 1])
            with c_run_b:
                if st.button(f"▶️ Chạy Khối '{block_name}'", key=f"run_{block_name}"):
                    rows_run = edited_block_df[edited_block_df['Trạng thái'] != "Đã chốt"].to_dict('records')
                    rows_run = [r for r in rows_run if len(str(r.get('Link dữ liệu lấy dữ liệu', ''))) > 5]
                    if not rows_run: st.warning("Không có dòng nào 'Chưa chốt'.")
                    else:
                        with st.spinner(f"Đang xử lý khối {block_name}..."):
                            ok, res_map, stats = process_pipeline(rows_run, f"{user_id}_{block_name}")
                            if res_map:
                                for idx, row in st.session_state['df_config'].iterrows():
                                    if row['Block_Name'] == block_name and row.get('Link dữ liệu lấy dữ liệu', '') in res_map:
                                        msg, rng = res_map[row.get('Link dữ liệu lấy dữ liệu')]
                                        if row['Trạng thái'] != "Đã chốt": st.session_state['df_config'].at[idx, 'Kết quả'] = msg
                                        st.session_state['df_config'].at[idx, 'Dòng dữ liệu'] = rng
                                save_data(st.session_state['df_config'], creds)
                                st.success(f"✅ Xong! Xử lý: {stats['sources']} nguồn | Thêm: +{stats['rows']} dòng | Hết: {stats['time']}s")
                                time.sleep(2); st.rerun()
            with c_scan_b:
                if st.button(f"🔍 Quét Quyền '{block_name}'", key=f"scan_{block_name}"):
                    errs = []
                    for _, row in edited_block_df.iterrows():
                        link_src = str(row.get('Link dữ liệu lấy dữ liệu', ''))
                        if "docs.google.com" in link_src:
                            ok, msg = verify_access_fast(link_src, creds)
                            if not ok: errs.append((row.get('STT'), "Nguồn", link_src, f"{msg} -> Cần quyền XEM"))
                        link_tgt = str(row.get('Link dữ liệu đích', ''))
                        if "docs.google.com" in link_tgt:
                            ok, msg = verify_access_fast(link_tgt, creds)
                            if not ok: errs.append((row.get('STT'), "Đích", link_tgt, f"{msg} -> Cần quyền SỬA"))
                    if errs:
                        st.error(f"Phát hiện lỗi ở khối {block_name}:")
                        for stt, typ, lk, m in errs: st.markdown(f"- Dòng {stt} [{typ}]: {m}")
                    else: st.success(f"Khối {block_name} OK! (Nguồn: Đã có quyền XEM, Đích: Đã có quyền XEM/SỬA)")

if __name__ == "__main__":
    main_ui()
