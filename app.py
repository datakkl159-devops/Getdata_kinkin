import streamlit as st
import pandas as pd
import polars as pl
import requests
import io
import time
import random
import gspread
import json
import re
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

# EMAIL BOT
BOT_EMAIL_DISPLAY = "getdulieu@kin-kin-477902.iam.gserviceaccount.com"

# Tên các Sheet
SHEET_CONFIG_NAME = "luu_cau_hinh" 
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_LOCK_NAME = "sys_lock"
SHEET_SYS_CONFIG = "sys_config"
SHEET_LOG_GITHUB = "log_chay_auto_github"

# Cột hệ thống
COL_LINK_SRC = "Link file nguồn"
COL_LABEL_SRC = "Sheet nguồn"
COL_MONTH_SRC = "Tháng chốt"
COL_BLOCK_NAME = "Block_Name"
COL_DATA_RANGE = "Vùng lấy dữ liệu"
DEFAULT_BLOCK_NAME = "Block_Mac_Dinh"

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- 2. HÀM HỖ TRỢ & HƯỚNG DẪN SỬ DỤNG ---
def col_name_to_index(col_name):
    col_name = col_name.upper()
    index = 0
    for char in col_name:
        index = index * 26 + (ord(char) - ord('A')) + 1
    return index - 1

@st.dialog("📘 TÀI LIỆU HƯỚNG DẪN SỬ DỤNG TOOL QUẢN LÝ DATA", width="large")
def show_guide_popup():
    st.markdown("""
    ### 1. Tổng Quan Hệ Thống
    Công cụ này giúp tự động hóa quy trình lấy dữ liệu từ nhiều file Google Sheets nguồn (Source) và tổng hợp vào các file đích (Target).
    
    ### 2. Giới Hạn & Cách Xử Lý Dữ Liệu Lớn
    | Trạng Thái | Số Dòng Dữ Liệu | Khuyến Nghị Thao Tác |
    | :--- | :--- | :--- |
    | ✅ **An Toàn** | **< 200.000 dòng** | Chạy bình thường. Dùng được `🚀 CHẠY TẤT CẢ`. |
    | ⚠️ **Mạo Hiểm** | **> 300.000 dòng** | Chỉ bấm `▶️ CHẠY KHỐI`. Tránh chạy tất cả để không tràn bộ nhớ. |
    | ⛔ **Không Thể** | **> 500.000 dòng** | Phải tách file đích hoặc giới hạn vùng lấy cột (VD: A:E). |

    ### 3. Các Bước Thao Tác
    * **Bước 1:** Đăng nhập và chọn Khối làm việc bên trái.
    * **Bước 2:** Nhập cấu hình vào bảng (Link nguồn, Link đích, Tên sheet...).
    * **Bước 3 - Quan Trọng:** Cấp quyền cho Bot.
        1. Copy email Bot: `%s`
        2. Share quyền **Viewer** cho file Nguồn.
        3. Share quyền **Editor** cho file Đích.
    * **Bước 4:** Bấm **Lưu** rồi bấm **Chạy**. Tool sẽ tự động kiểm tra quyền trước khi chạy.
    """ % BOT_EMAIL_DISPLAY)

def check_login():
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
    if 'current_user_id' not in st.session_state: st.session_state['current_user_id'] = "Unknown"
    if "auto_key" in st.query_params:
        key = st.query_params["auto_key"]
        if key in AUTHORIZED_USERS:
            st.session_state['logged_in'] = True; st.session_state['current_user_id'] = AUTHORIZED_USERS[key]; return True
    if st.session_state['logged_in']: return True
    st.header("🔒 Đăng nhập hệ thống")
    pwd = st.text_input("Nhập mật khẩu truy cập:", type="password")
    if st.button("Đăng Nhập"):
        if pwd in AUTHORIZED_USERS:
            st.session_state['logged_in'] = True; st.session_state['current_user_id'] = AUTHORIZED_USERS[pwd]; st.rerun()
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

def get_sh_with_retry(creds, sheet_id_or_key):
    gc = gspread.authorize(creds)
    max_retries = 3
    for i in range(max_retries):
        try: return gc.open_by_key(sheet_id_or_key)
        except Exception as e:
            if i == max_retries - 1: raise e
            time.sleep((2 ** i) + random.random()) 
    return None

def extract_id(url):
    if not isinstance(url, str): return None
    if "docs.google.com" in url:
        try: return url.split("/d/")[1].split("/")[0]
        except: return None
    return None

# --- 3. HỆ THỐNG KHÓA & LOG ---
def get_system_lock(creds):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
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
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_LOCK_NAME)
        except: wks = sh.add_worksheet(SHEET_LOCK_NAME, rows=10, cols=5)
        now_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        wks.update("A2:C2", [["TRUE", user_id, now_str]] if lock else [["FALSE", "", ""]])
    except: pass

def write_detailed_log(creds, history_sheet_id, log_data_list):
    if not log_data_list: return
    try:
        sh = get_sh_with_retry(creds, history_sheet_id)
        try: wks = sh.worksheet(SHEET_LOG_NAME)
        except: 
            wks = sh.add_worksheet(SHEET_LOG_NAME, rows=1000, cols=15)
            wks.append_row([
                "Ngày & giờ get dữ liệu", "Vùng lấy dữ liệu", "Tháng", "Nhân sự get", 
                "Link nguồn", "Link đích", "Sheet Đích", "Sheet nguồn lấy dữ liệu", 
                "Trạng Thái", "Số Dòng Đã Lấy", "Dòng dữ liệu cập nhật", "Chạy từ khối"
            ])
        wks.append_rows(log_data_list)
    except Exception as e: print(f"Lỗi log: {e}")

# --- 4. HÀM KIỂM TRA QUYỀN TRƯỚC KHI CHẠY (QUAN TRỌNG) ---
def verify_access_fast(url, creds):
    sheet_id = extract_id(url)
    if not sheet_id: return False, "Link lỗi/Sai định dạng"
    try:
        get_sh_with_retry(creds, sheet_id)
        return True, "OK"
    except Exception as e: return False, f"Lỗi: {e}"

def check_permissions_strict(rows_to_run, creds):
    """
    Hàm kiểm tra quyền NGHIÊM NGẶT trước khi chạy.
    Trả về: (True, None) nếu OK hết.
    Trả về: (False, list_errors) nếu có lỗi.
    """
    errs = []
    # Dùng set để tránh check lại cùng 1 link nhiều lần
    checked_links = {} 

    for row in rows_to_run:
        # Check Nguồn
        raw_s = row.get('Link dữ liệu lấy dữ liệu', '')
        link_src = str(raw_s[0]).strip() if isinstance(raw_s, list) and raw_s else str(raw_s).strip()
        
        if "docs.google.com" in link_src:
            if link_src not in checked_links:
                checked_links[link_src] = verify_access_fast(link_src, creds)
            
            is_ok, msg = checked_links[link_src]
            if not is_ok:
                errs.append(f"❌ Nguồn: {msg} -> {link_src}")

        # Check Đích
        raw_t = row.get('Link dữ liệu đích', '')
        link_tgt = str(raw_t[0]).strip() if isinstance(raw_t, list) and raw_t else str(raw_t).strip()
        
        if "docs.google.com" in link_tgt:
            if link_tgt not in checked_links:
                checked_links[link_tgt] = verify_access_fast(link_tgt, creds)
            
            is_ok, msg = checked_links[link_tgt]
            if not is_ok:
                errs.append(f"❌ Đích: {msg} -> {link_tgt}")
    
    if errs: return False, errs
    return True, []

# --- 5. TẢI DATA & XỬ LÝ ---
def fetch_data_preserve_columns(row_config, creds):
    if not isinstance(row_config, dict): return None, "Lỗi Config", "Lỗi Config"
    link_src = str(row_config.get('Link dữ liệu lấy dữ liệu', '')).strip()
    source_label = str(row_config.get('Tên sheet nguồn dữ liệu gốc', '')).strip()
    month_val = str(row_config.get('Tháng', ''))
    data_range_str = str(row_config.get(COL_DATA_RANGE, 'Lấy hết')).strip()
    if not data_range_str or data_range_str.lower() == 'nan': data_range_str = "Lấy hết"
    
    sheet_id = extract_id(link_src)
    if not sheet_id: return None, sheet_id, "Link lỗi"
    
    df = None
    status_msg = ""
    
    try:
        sh_source = get_sh_with_retry(creds, sheet_id)
        wks_source = None
        if source_label:
            try: wks_source = sh_source.worksheet(source_label)
            except: return None, sheet_id, f"❌ Không tìm thấy sheet: '{source_label}'"
        else: wks_source = sh_source.sheet1
            
        data = wks_source.get_all_values()
        
        if data and len(data) > 0:
            headers = data[0]
            rows = data[1:]
            
            if not rows:
                status_msg = "Sheet rỗng (Chỉ có tiêu đề)"
                df = pd.DataFrame(columns=headers)
            else:
                df = pd.DataFrame(rows, columns=headers)
            
            if data_range_str != "Lấy hết" and ":" in data_range_str:
                try:
                    start_col_str, end_col_str = data_range_str.split(":")
                    start_idx = col_name_to_index(start_col_str.strip())
                    end_idx = col_name_to_index(end_col_str.strip())
                    if start_idx >= 0 and end_idx >= start_idx:
                        end_idx = min(end_idx, len(df.columns) - 1)
                        df = df.iloc[:, start_idx : end_idx + 1]
                except Exception as e: print(f"Lỗi cắt cột: {e}")

            df = df.astype(str).replace(['nan', 'None', '<NA>', 'null'], '')
            status_msg = "Thành công"
        else:
            status_msg = "Sheet trắng tinh"
            
    except Exception as e:
        return None, sheet_id, f"Lỗi tải data: {str(e)}"

    if df is not None:
        df[COL_LINK_SRC] = link_src
        df[COL_LABEL_SRC] = source_label
        df[COL_MONTH_SRC] = month_val
        return df, sheet_id, status_msg
        
    return None, sheet_id, "Không lấy được dữ liệu"

def scan_realtime_row_ranges(target_link, target_sheet_name, creds):
    results = {}
    try:
        target_id = extract_id(target_link)
        if not target_id: return {}
        sh = get_sh_with_retry(creds, target_id)
        real_sheet_name = str(target_sheet_name).strip()
        if not real_sheet_name: real_sheet_name = "Tong_Hop_Data"
        try: wks = sh.worksheet(real_sheet_name)
        except: return {}
        all_data = wks.get_all_values()
        if not all_data: return {}
        headers = all_data[0]
        try: link_col_idx = headers.index(COL_LINK_SRC)
        except ValueError: return {} 
        temp_map = {} 
        for i, row in enumerate(all_data[1:], start=2):
            if len(row) > link_col_idx:
                link_val = row[link_col_idx]
                if link_val:
                    if link_val not in temp_map: temp_map[link_val] = [i, i]
                    else: temp_map[link_val][1] = i 
        for link, (start, end) in temp_map.items():
            results[link] = f"{start} - {end}"
    except Exception as e: print(f"Lỗi scan: {e}"); return {}
    return results

def smart_update_safe(tasks_list, target_link, target_sheet_name, creds):
    try:
        target_id = extract_id(target_link)
        if not target_id: return False, "Link đích lỗi"
        sh = get_sh_with_retry(creds, target_id)
        real_sheet_name = str(target_sheet_name).strip()
        if not real_sheet_name: real_sheet_name = "Tong_Hop_Data"
        try: wks = sh.worksheet(real_sheet_name)
        except: wks = sh.add_worksheet(title=real_sheet_name, rows=1000, cols=20)
        
        links_to_remove = [t[1] for t in tasks_list if t[1] and len(str(t[1])) > 5]
        
        existing_headers = []
        try: existing_headers = wks.row_values(1)
        except: pass
        
        if existing_headers and links_to_remove:
            try: 
                link_col_idx = existing_headers.index(COL_LINK_SRC) + 1
                col_values = wks.col_values(link_col_idx)
                rows_to_delete = []
                for i, val in enumerate(col_values):
                    if i > 0 and str(val).strip() in links_to_remove: 
                        rows_to_delete.append(i + 1)
                
                if rows_to_delete:
                    rows_to_delete.sort()
                    ranges = []; start = rows_to_delete[0]; end = start
                    for r in rows_to_delete[1:]:
                        if r == end + 1: end = r
                        else: ranges.append((start, end)); start = r; end = r
                    ranges.append((start, end))
                    delete_reqs = []
                    for start, end in reversed(ranges):
                        delete_reqs.append({"deleteDimension": {"range": {"sheetId": wks.id, "dimension": "ROWS", "startIndex": start - 1, "endIndex": end}}})
                    if delete_reqs:
                        sh.batch_update({'requests': delete_reqs})
                        time.sleep(1)
            except ValueError: pass

        if not existing_headers:
            first_df = tasks_list[0][0]
            if first_df is not None and not first_df.empty:
                final_headers = first_df.columns.tolist()
                wks.append_row(final_headers)
                existing_headers = final_headers
            else: return True, "Không có dữ liệu nguồn để tạo header"
        else:
            final_headers = existing_headers
            all_new_cols = []
            for t in tasks_list:
                if t[0] is not None: all_new_cols.extend(t[0].columns.tolist())
            seen = set(existing_headers)
            cols_to_add = [x for x in all_new_cols if x not in seen and not seen.add(x)]
            if cols_to_add:
                wks.resize(cols=len(existing_headers) + len(cols_to_add))
                final_headers = existing_headers + cols_to_add
                wks.update(range_name="A1", values=[final_headers])

        data_to_append = []
        for df, src_link in tasks_list:
            if df is not None and not df.empty:
                df_aligned = df.reindex(columns=final_headers, fill_value="")
                data_to_append.extend(df_aligned.values.tolist())

        if data_to_append:
            BATCH_SIZE = 5000
            for i in range(0, len(data_to_append), BATCH_SIZE):
                chunk = data_to_append[i : i + BATCH_SIZE]
                wks.append_rows(chunk)
                time.sleep(1)
            return True, f"Thành công (+{len(data_to_append)} dòng)"
            
        return True, "Thành công (Không có data mới)"
    except Exception as e: return False, f"Lỗi Ghi: {str(e)}"

# --- 6. PIPELINE ---
def process_pipeline(rows_to_run, user_id, block_name_run):
    creds = get_creds()
    is_locked, locking_user, lock_time = get_system_lock(creds)
    if is_locked and locking_user != user_id and "AutoAll" not in user_id:
        return False, f"HỆ THỐNG ĐANG BẬN! {locking_user} đang chạy từ {lock_time}.", 0
    
    set_system_lock(creds, user_id, lock=True)
    try:
        grouped_tasks = defaultdict(list)
        total_fetched_rows = 0
        
        for row in rows_to_run:
            raw_t = row.get('Link dữ liệu đích', '')
            t_link = str(raw_t[0]).strip() if isinstance(raw_t, list) and raw_t else str(raw_t).strip()
            row['Link dữ liệu đích'] = t_link 

            raw_s = row.get('Link dữ liệu lấy dữ liệu', '')
            s_link = str(raw_s[0]).strip() if isinstance(raw_s, list) and raw_s else str(raw_s).strip()
            row['Link dữ liệu lấy dữ liệu'] = s_link 

            t_sheet = str(row.get('Tên sheet dữ liệu đích', '')).strip()
            if not t_sheet: t_sheet = "Tong_Hop_Data"
            
            if COL_DATA_RANGE not in row or not row[COL_DATA_RANGE]:
                row[COL_DATA_RANGE] = "Lấy hết"

            grouped_tasks[(t_link, t_sheet)].append(row)

        global_results_map = {} 
        all_success = True
        log_entries = []
        tz_vn = pytz.timezone('Asia/Ho_Chi_Minh')
        time_now = datetime.now(tz_vn).strftime("%d/%m/%Y %H:%M:%S")

        for (target_link, target_sheet), group_rows in grouped_tasks.items():
            if not target_link: continue
            
            tasks_list = []
            for row in group_rows:
                df, sid, status = fetch_data_preserve_columns(row, creds)
                src_link = row['Link dữ liệu lấy dữ liệu']
                
                if df is not None:
                    tasks_list.append((df, src_link))
                    total_fetched_rows += len(df)
                else:
                    global_results_map[src_link] = ("Lỗi tải/Quyền", "")
                    log_entries.append([
                        time_now, str(row.get(COL_DATA_RANGE, 'Lấy hết')), str(row.get('Tháng', '')), 
                        user_id, src_link, target_link, target_sheet,
                        row.get('Tên sheet nguồn dữ liệu gốc', ''), "Lỗi tải", "0", "", block_name_run
                    ])

            msg_update = ""
            success_update = True
            if tasks_list:
                success_update, msg_update = smart_update_safe(tasks_list, target_link, target_sheet, creds)
                if not success_update: all_success = False
            
            realtime_ranges = scan_realtime_row_ranges(target_link, target_sheet, creds)
            
            for link, rng in realtime_ranges.items():
                if link not in global_results_map: global_results_map[link] = ("Cập nhật lại", rng)
                else:
                    current_msg = global_results_map[link][0]
                    global_results_map[link] = (current_msg, rng)

            for row in group_rows:
                s_link = row['Link dữ liệu lấy dữ liệu']
                status_str = "Thành công" if success_update else f"Lỗi: {msg_update}"
                final_range = realtime_ranges.get(s_link, "")
                
                if any(t[1] == s_link for t in tasks_list) or (s_link in global_results_map and "Lỗi" in global_results_map[s_link][0]):
                    height = "0"
                    for df, sl in tasks_list:
                        if sl == s_link: height = str(len(df))

                    log_entries.append([
                        time_now, str(row.get(COL_DATA_RANGE, 'Lấy hết')), str(row.get('Tháng', '')),
                        user_id, s_link, target_link, target_sheet,
                        row.get('Tên sheet nguồn dữ liệu gốc', ''), 
                        status_str, height, final_range, block_name_run
                    ])
                    global_results_map[s_link] = (status_str, final_range)
        
        history_id = st.secrets["gcp_service_account"]["history_sheet_id"]
        write_detailed_log(creds, history_id, log_entries)
        return all_success, global_results_map, total_fetched_rows
    finally:
        set_system_lock(creds, user_id, lock=False)

# --- 7. QUẢN LÝ BLOCK & QUÉT QUYỀN ---
def man_scan(df):
    creds = get_creds()
    errs = []
    for idx, row in df.iterrows():
        raw_s = row.get('Link dữ liệu lấy dữ liệu', '')
        link_src = str(raw_s[0]).strip() if isinstance(raw_s, list) and raw_s else str(raw_s).strip()
        if "docs.google.com" in link_src:
            ok, msg = verify_access_fast(link_src, creds)
            if not ok: errs.append((row.get('STT'), "Nguồn (Xem)", link_src, "Chưa cấp quyền xem"))
        
        raw_t = row.get('Link dữ liệu đích', '')
        link_tgt = str(raw_t[0]).strip() if isinstance(raw_t, list) and raw_t else str(raw_t).strip()
        if "docs.google.com" in link_tgt:
            ok, msg = verify_access_fast(link_tgt, creds)
            if not ok: errs.append((row.get('STT'), "Đích (Chỉnh sửa)", link_tgt, "Chưa cấp quyền sửa"))
    return errs

def load_full_config(creds):
    sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
    df = df.dropna(how='all')
    
    rename_map = {
        'Tên sheet dữ liệu': 'Tên sheet dữ liệu đích', 'Tên nguồn (Nhãn)': 'Tên sheet nguồn dữ liệu gốc',
        'Link file nguồn': 'Link dữ liệu lấy dữ liệu', 'Link file đích': 'Link dữ liệu đích'
    }
    for old, new in rename_map.items():
        if old in df.columns: df = df.rename(columns={old: new})
    
    required_cols = ['Trạng thái', COL_DATA_RANGE, 'Tháng', 'Link dữ liệu lấy dữ liệu', 'Link dữ liệu đích', 'Tên sheet dữ liệu đích', 'Tên sheet nguồn dữ liệu gốc', 'Kết quả', 'Dòng dữ liệu', COL_BLOCK_NAME]
    for c in required_cols:
        if c not in df.columns: df[c] = ""
    
    df[COL_BLOCK_NAME] = df[COL_BLOCK_NAME].replace('', DEFAULT_BLOCK_NAME).fillna(DEFAULT_BLOCK_NAME)
    df[COL_DATA_RANGE] = df[COL_DATA_RANGE].replace('', 'Lấy hết').fillna('Lấy hết')
    
    if 'Trạng thái' in df.columns:
        df['Trạng thái'] = df['Trạng thái'].apply(lambda x: "Đã chốt" if str(x).strip() in ["Đã chốt", "Đã cập nhật", "TRUE"] else "Chưa chốt & đang cập nhật")
    
    if 'STT' in df.columns: df = df.drop(columns=['STT'])
    return df

def delete_block_from_server(block_name, creds):
    sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
    df = df[df[COL_BLOCK_NAME] != block_name]
    wks.clear()
    wks.update([df.columns.tolist()] + df.fillna('').values.tolist())
    st.toast(f"🗑️ Đã xóa khối '{block_name}' khỏi hệ thống!", icon="✅")

def save_block_config(df_current_ui, current_block_name, creds):
    sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    
    df_full_server = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
    df_full_server = df_full_server.dropna(how='all')
    if COL_BLOCK_NAME not in df_full_server.columns: df_full_server[COL_BLOCK_NAME] = DEFAULT_BLOCK_NAME
    df_full_server[COL_BLOCK_NAME] = df_full_server[COL_BLOCK_NAME].replace('', DEFAULT_BLOCK_NAME).fillna(DEFAULT_BLOCK_NAME)
    
    df_other_blocks = df_full_server[df_full_server[COL_BLOCK_NAME] != current_block_name]
    
    df_to_save = df_current_ui.copy()
    if 'STT' in df_to_save.columns: df_to_save = df_to_save.drop(columns=['STT'])
    df_to_save[COL_BLOCK_NAME] = current_block_name 
    
    target_cols = [COL_BLOCK_NAME, 'Trạng thái', COL_DATA_RANGE, 'Tháng', 'Link dữ liệu lấy dữ liệu', 'Link dữ liệu đích', 'Tên sheet dữ liệu đích', 'Dòng dữ liệu', 'Kết quả', 'Tên sheet nguồn dữ liệu gốc']
    
    df_final = pd.concat([df_other_blocks, df_to_save], ignore_index=True)
    df_final = df_final.astype(str).replace(['nan', 'None', '<NA>'], '')
    
    for c in target_cols:
        if c not in df_final.columns: df_final[c] = ""
    
    df_final = df_final[target_cols]
    wks.clear()
    wks.update([df_final.columns.tolist()] + df_final.values.tolist())
    st.toast(f"✅ Đã lưu cấu hình khối: {current_block_name}!", icon="💾")

def save_full_config_direct(df_full, creds):
    sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    
    target_cols = [COL_BLOCK_NAME, 'Trạng thái', COL_DATA_RANGE, 'Tháng', 'Link dữ liệu lấy dữ liệu', 'Link dữ liệu đích', 'Tên sheet dữ liệu đích', 'Dòng dữ liệu', 'Kết quả', 'Tên sheet nguồn dữ liệu gốc']
    
    df_full = df_full.astype(str).replace(['nan', 'None', '<NA>'], '')
    for c in target_cols:
        if c not in df_full.columns: df_full[c] = ""
    
    df_full = df_full[target_cols]
    wks.clear()
    wks.update([df_full.columns.tolist()] + df_full.values.tolist())

def load_sys_schedule(creds):
    try:
        sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_SYS_CONFIG)
        except: 
            wks = sh.add_worksheet(SHEET_SYS_CONFIG, rows=20, cols=5)
            wks.append_row([COL_BLOCK_NAME, "Run_Hour", "Run_Freq"])
        
        df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if COL_BLOCK_NAME not in df.columns: 
            wks.clear(); wks.append_row([COL_BLOCK_NAME, "Run_Hour", "Run_Freq"])
            df = pd.DataFrame(columns=[COL_BLOCK_NAME, "Run_Hour", "Run_Freq"])
            
        return df.dropna(how='all')
    except: return pd.DataFrame(columns=[COL_BLOCK_NAME, "Run_Hour", "Run_Freq"])

def save_sys_schedule(df_schedule, creds):
    sh = get_sh_with_retry(creds, st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_SYS_CONFIG)
    wks.clear()
    wks.update([df_schedule.columns.tolist()] + df_schedule.fillna('').values.tolist())

# --- 8. GIAO DIỆN CHÍNH ---
def main_ui():
    if not check_login(): return
    user_id = st.session_state['current_user_id']
    creds = get_creds()
    
    st.title(f"⚙️ Tool Quản Lý Data (User: {user_id})")
    
    with st.sidebar:
        st.header("📦 Quản Lý Khối")
        if 'df_full_config' not in st.session_state:
            with st.spinner("Đang tải dữ liệu..."): st.session_state['df_full_config'] = load_full_config(creds)
            
        unique_blocks = st.session_state['df_full_config'][COL_BLOCK_NAME].unique().tolist()
        if not unique_blocks: unique_blocks = [DEFAULT_BLOCK_NAME]
        selected_block = st.selectbox("Chọn Khối làm việc:", unique_blocks, key="sb_block_select")
        
        st.divider()
        new_block_input = st.text_input("Tên khối mới:")
        if st.button("➕ Thêm Khối Mới"):
            if new_block_input and new_block_input not in unique_blocks:
                st.session_state['df_full_config'] = pd.concat([
                    st.session_state['df_full_config'],
                    pd.DataFrame([{COL_BLOCK_NAME: new_block_input, 'Trạng thái': 'Chưa chốt & đang cập nhật', COL_DATA_RANGE: 'Lấy hết'}])
                ], ignore_index=True)
                st.success(f"Đã thêm {new_block_input}")
                st.rerun()
            elif new_block_input in unique_blocks: st.warning("Tên khối đã tồn tại!")
        
        if st.button("🗑️ Xóa Khối Hiện Tại", type="primary"):
            if len(unique_blocks) <= 1: st.error("Không thể xóa khối cuối cùng!")
            else:
                delete_block_from_server(selected_block, creds)
                del st.session_state['df_full_config']
                time.sleep(1)
                st.rerun()
        
        st.divider()
        if st.button("📘 Tài liệu Hướng Dẫn"):
            show_guide_popup()

    st.subheader(f"Danh sách Job của khối: {selected_block}")
    
    df_display = st.session_state['df_full_config'][st.session_state['df_full_config'][COL_BLOCK_NAME] == selected_block].copy()
    df_display = df_display.reset_index(drop=True)
    df_display.insert(0, 'STT', range(1, len(df_display) + 1))
    
    col_order = ["STT", "Trạng thái", COL_DATA_RANGE, "Tháng", "Link dữ liệu lấy dữ liệu", "Link dữ liệu đích", "Tên sheet dữ liệu đích", "Tên sheet nguồn dữ liệu gốc", "Kết quả", "Dòng dữ liệu"]
    
    edited_df = st.data_editor(
        df_display,
        column_order=col_order,
        column_config={
            "STT": st.column_config.NumberColumn("STT", disabled=True, width="small"),
            "Trạng thái": st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt & đang cập nhật", "Đã chốt"], required=True),
            COL_DATA_RANGE: st.column_config.TextColumn("Vùng lấy dữ liệu", width="small", help="VD: A:D hoặc để trống (Lấy hết)"),
            "Link dữ liệu lấy dữ liệu": st.column_config.TextColumn("Link Nguồn", width="medium"),
            "Link dữ liệu đích": st.column_config.TextColumn("Link Đích", width="medium"),
            "Kết quả": st.column_config.TextColumn("Kết quả", disabled=True),
            "Dòng dữ liệu": st.column_config.TextColumn("Dòng Dữ Liệu", disabled=True),
        },
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        key=f"editor_{selected_block}"
    )

    st.divider()
    st.markdown(f"**⏰ Cài Đặt Hẹn Giờ (Block: {selected_block})**")
    if 'df_sys_schedule' not in st.session_state: st.session_state['df_sys_schedule'] = load_sys_schedule(creds)
    df_sch = st.session_state['df_sys_schedule']
    row_sch = df_sch[df_sch[COL_BLOCK_NAME] == selected_block]
    cur_hour = 8; cur_freq = "Hàng ngày"
    if not row_sch.empty:
        try: cur_hour = int(row_sch.iloc[0]['Run_Hour']); cur_freq = str(row_sch.iloc[0]['Run_Freq'])
        except: pass

    c1, c2, c3 = st.columns(3)
    list_freq = ["Hàng ngày", "Hàng tuần", "Hàng tháng"]
    with c1: new_freq = st.selectbox("Tần suất:", list_freq, index=list_freq.index(cur_freq) if cur_freq in list_freq else 0)
    with c2: new_hour = st.slider("Giờ chạy (VN):", 0, 23, value=cur_hour)
    with c3:
        st.write("")
        if st.button("Lưu Hẹn Giờ"):
            new_row = {COL_BLOCK_NAME: selected_block, "Run_Hour": str(new_hour), "Run_Freq": new_freq}
            df_sch = df_sch[df_sch[COL_BLOCK_NAME] != selected_block]
            df_sch = pd.concat([df_sch, pd.DataFrame([new_row])], ignore_index=True)
            save_sys_schedule(df_sch, creds)
            st.session_state['df_sys_schedule'] = df_sch
            st.toast("✅ Đã lưu lịch chạy!", icon="⏰")

    st.divider()
    col_run_block, col_run_all, col_scan, col_save = st.columns([2, 2, 1, 1])
    
    with col_run_block:
        if st.button(f"▶️ CHẠY KHỐI: {selected_block}", type="primary"):
            rows_run = edited_df[edited_df['Trạng thái'] == "Chưa chốt & đang cập nhật"].to_dict('records')
            rows_run = [r for r in rows_run if len(str(r.get('Link dữ liệu lấy dữ liệu', ''))) > 5]
            
            if not rows_run: 
                st.warning("⚠️ Không có dòng chưa chốt.")
            else:
                # KIỂM TRA QUYỀN TRƯỚC KHI CHẠY (CHẶN NẾU LỖI)
                with st.spinner("Đang kiểm tra quyền truy cập..."):
                    ok_check, err_list = check_permissions_strict(rows_run, creds)
                
                if not ok_check:
                    st.error("Lỗi không ghi được dữ liệu vì bạn chưa cấp quyền vui lòng cấp quyền cho bot dưới đây")
                    st.code(BOT_EMAIL_DISPLAY, language="text")
                    st.write("**Chi tiết lỗi:**")
                    for err in err_list: st.error(err)
                else:
                    # NẾU QUYỀN OK MỚI CHẠY
                    with st.status(f"Đang xử lý {len(rows_run)} nguồn...", expanded=True):
                        start_t = time.time()
                        all_ok, results_map, total_rows = process_pipeline(rows_run, user_id, selected_block) 
                        elapsed = time.time() - start_t
                        
                        if isinstance(results_map, str): st.error(results_map)
                        elif results_map:
                            st.success(f"✅ Xong. Tổng {total_rows} dòng. Hết {elapsed:.2f}s")
                            for idx, row in edited_df.iterrows():
                                raw_s = row.get('Link dữ liệu lấy dữ liệu', '')
                                s_link = str(raw_s[0]).strip() if isinstance(raw_s, list) and raw_s else str(raw_s).strip()
                                if s_link in results_map:
                                    msg, rng = results_map[s_link]
                                    if row['Trạng thái'] == "Chưa chốt & đang cập nhật": edited_df.at[idx, 'Kết quả'] = msg
                                    edited_df.at[idx, 'Dòng dữ liệu'] = rng
                            save_block_config(edited_df, selected_block, creds)
                            del st.session_state['df_full_config']
                            time.sleep(1); st.rerun()

    with col_run_all:
        if st.button("🚀 CHẠY TẤT CẢ CÁC KHỐI"):
            with st.status("Đang chạy toàn bộ hệ thống...", expanded=True) as status:
                full_df = st.session_state['df_full_config'].copy()
                all_blocks = full_df[COL_BLOCK_NAME].unique()
                total_all = 0; start_all = time.time()
                
                for blk in all_blocks:
                    status.write(f"⏳ Đang xử lý khối: **{blk}**...")
                    block_mask = (full_df[COL_BLOCK_NAME] == blk) & (full_df['Trạng thái'] == "Chưa chốt & đang cập nhật")
                    rows_blk = full_df[block_mask].to_dict('records')
                    
                    if rows_blk:
                        # KIỂM TRA QUYỀN (Nếu lỗi thì bỏ qua khối này, chạy khối tiếp)
                        ok_check, err_list = check_permissions_strict(rows_blk, creds)
                        if not ok_check:
                            status.write(f"❌ Khối {blk} bị bỏ qua do lỗi quyền (Xem chi tiết log).")
                            continue

                        _, results_map, rows_count = process_pipeline(rows_blk, f"{user_id} (AutoAll)", blk)
                        total_all += rows_count
                        
                        if results_map:
                            for idx, row in full_df[block_mask].iterrows():
                                raw_s = row.get('Link dữ liệu lấy dữ liệu', '')
                                s_link = str(raw_s[0]).strip() if isinstance(raw_s, list) and raw_s else str(raw_s).strip()
                                if s_link in results_map:
                                    msg, rng = results_map[s_link]
                                    full_df.at[idx, 'Kết quả'] = msg
                                    full_df.at[idx, 'Dòng dữ liệu'] = rng
                        
                        status.write(f"✅ Xong khối {blk} (+{rows_count} dòng).")
                    else: status.write(f"⚪ Khối {blk} không có dữ liệu cần chạy.")

                status.write("💾 Đang lưu cập nhật trạng thái...")
                save_full_config_direct(full_df, creds)
                st.session_state['df_full_config'] = full_df
                
                status.update(label=f"Đã xong! Tổng {total_all} dòng.", state="complete", expanded=False)
                st.toast(f"Xong tất cả! Tổng {total_all} dòng. {time.time()-start_all:.2f}s", icon="🏁")
                time.sleep(1); st.rerun()

    with col_scan:
        if st.button("🔍 Quét Quyền"):
            errs = man_scan(edited_df) 
            if errs:
                st.error("Lỗi không ghi được dữ liệu vì bạn chưa cấp quyền vui lòng cấp quyền cho bot dưới đây")
                st.code(BOT_EMAIL_DISPLAY, language="text")
                st.write("**Chi tiết lỗi:**")
                for stt, l_type, link, msg in errs:
                    st.markdown(f"- **Dòng {stt} [{l_type}]**: {msg} -> [Link]({link})")
            else: st.success("✅ Quyền OK. Bot đã đọc/ghi được.")

    with col_save:
        if st.button("💾 Lưu"):
            save_block_config(edited_df, selected_block, creds)
            del st.session_state['df_full_config']
            st.rerun()

if __name__ == "__main__":
    main_ui()
