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
st.set_page_config(page_title="Tool Quản Lý Data", layout="wide")

AUTHORIZED_USERS = {
    "admin2024": "Admin_Master",
    "team_hn": "Team_HaNoi",
    "team_hcm": "Team_HCM"
}

BOT_EMAIL_DISPLAY = "getdulieu@kin-kin-477902.iam.gserviceaccount.com"

# Tên các Sheet
SHEET_CONFIG_NAME = "luu_cau_hinh" 
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_LOCK_NAME = "sys_lock"
SHEET_SYS_CONFIG = "sys_config"
SHEET_LOG_GITHUB = "log_chay_auto_github"

# Tên 3 cột hệ thống tự động thêm vào file đích
COL_LINK_SRC = "Link file nguồn"
COL_LABEL_SRC = "Sheet nguồn"
COL_MONTH_SRC = "Tháng chốt"

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- 2. HÀM XÁC THỰC & KẾT NỐI ---
def check_login():
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
    if 'current_user_id' not in st.session_state: st.session_state['current_user_id'] = "Unknown"

    if "auto_key" in st.query_params:
        key = st.query_params["auto_key"]
        if key in AUTHORIZED_USERS:
            st.session_state['logged_in'] = True
            st.session_state['current_user_id'] = AUTHORIZED_USERS[key]
            return True

    if st.session_state['logged_in']: return True

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
    if "private_key" in creds_info: 
        creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
    return service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)

def extract_id(url):
    if not isinstance(url, str): return None
    if "docs.google.com" in url:
        try: return url.split("/d/")[1].split("/")[0]
        except: return None
    return None

# --- 3. HỆ THỐNG KHÓA & LOG ---
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
            wks = sh.add_worksheet(SHEET_LOG_NAME, rows=1000, cols=11)
            # Thêm cột "Dòng dữ liệu" vào Log
            wks.append_row(["Ngày & giờ get dữ liệu", "Ngày chốt", "Tháng", "Nhân sự get", "Link nguồn", "Link đích", "Sheet Đích", "Sheet nguồn lấy dữ liệu", "Trạng Thái", "Số Dòng Đã Lấy", "Dòng dữ liệu"])
        wks.append_rows(log_data_list)
    except Exception as e: print(f"Lỗi log: {e}")

# --- 4. HÀM QUÉT QUYỀN ---
def verify_access_fast(url, creds):
    sheet_id = extract_id(url)
    if not sheet_id: return False, "Link lỗi/Sai định dạng"
    try:
        gc = gspread.authorize(creds)
        gc.open_by_key(sheet_id)
        return True, "OK"
    except gspread.exceptions.SpreadsheetNotFound:
        return False, "❌ Không tìm thấy file"
    except gspread.exceptions.APIError as e:
        if "403" in str(e): return False, "⛔ Chưa cấp quyền (403)"
        return False, f"❌ Lỗi API: {e}"
    except Exception as e: return False, f"❌ Lỗi: {e}"

# --- 5. LOGIC XỬ LÝ DỮ LIỆU ---
def fetch_single_csv_safe(row_config, creds, token):
    if not isinstance(row_config, dict): return None, "Lỗi Config", "Lỗi Config"
    link_src = str(row_config.get('Link dữ liệu lấy dữ liệu', ''))
    source_label = str(row_config.get('Tên sheet nguồn dữ liệu gốc', '')).strip()
    month_val = str(row_config.get('Tháng', ''))
    sheet_id = extract_id(link_src)
    
    if not sheet_id: return None, sheet_id, "Link lỗi"
    
    df = None
    status_msg = ""
    target_gid = None

    # Tìm GID
    try:
        gc = gspread.authorize(creds)
        sh_source = gc.open_by_key(sheet_id)
        if source_label:
            try:
                wks_source = sh_source.worksheet(source_label)
                target_gid = wks_source.id
            except gspread.exceptions.WorksheetNotFound:
                return None, sheet_id, f"❌ Không tìm thấy sheet: '{source_label}'"
        else:
            wks_source = sh_source.sheet1
            target_gid = wks_source.id
    except Exception as e:
        return None, sheet_id, f"Lỗi truy cập file nguồn: {str(e)}"

    # Tải Data
    if target_gid is not None:
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={target_gid}"
        headers = {'Authorization': f'Bearer {token}'}
        try:
            response = requests.get(url, headers=headers, timeout=20)
            if response.status_code == 200:
                df = pl.read_csv(io.BytesIO(response.content), infer_schema_length=0)
                status_msg = f"Thành công"
        except: pass

    if df is None or df.is_empty():
        try:
            data = wks_source.get_all_values()
            if data and len(data) > 0:
                headers = data[0]
                rows = data[1:]
                if rows:
                    df = pl.DataFrame(rows, schema=headers, orient="row")
                    df = df.select(pl.all().cast(pl.Utf8))
                    status_msg = f"Thành công"
                else: status_msg = "Sheet rỗng"
            else: status_msg = "Sheet rỗng"
        except Exception as e:
            return None, sheet_id, f"Lỗi tải data: {str(e)}"

    # Thêm 3 cột hệ thống
    if df is not None and not df.is_empty():
        df = df.with_columns([
            pl.lit(link_src).cast(pl.Utf8).alias(COL_LINK_SRC),
            pl.lit(source_label).cast(pl.Utf8).alias(COL_LABEL_SRC),
            pl.lit(month_val).cast(pl.Utf8).alias(COL_MONTH_SRC)
        ])
        return df, sheet_id, status_msg
        
    return None, sheet_id, "Không lấy được dữ liệu"

# --- HÀM GHI & TÍNH DÒNG CHI TIẾT (QUAN TRỌNG) ---
def smart_update_safe(tasks_list, target_link, target_sheet_name, creds):
    # tasks_list: Danh sách chứa (DataFrame, Source_Link)
    try:
        gc = gspread.authorize(creds)
        target_id = extract_id(target_link)
        if not target_id: return False, "Link đích lỗi", {}
        
        sh = gc.open_by_key(target_id)
        real_sheet_name = str(target_sheet_name).strip()
        if not real_sheet_name: real_sheet_name = "Tong_Hop_Data"
        
        try: wks = sh.worksheet(real_sheet_name)
        except: wks = sh.add_worksheet(title=real_sheet_name, rows=1000, cols=20)
        
        # Tạo danh sách các link nguồn cần xóa
        links_to_remove = [t[1] for t in tasks_list]

        # 1. LẤY HEADER & XÓA CŨ
        existing_headers = []
        try: existing_headers = wks.row_values(1)
        except: pass
        
        if existing_headers:
            try: 
                link_col_idx = existing_headers.index(COL_LINK_SRC) + 1
                col_values = wks.col_values(link_col_idx)
                
                rows_to_delete = []
                for i, val in enumerate(col_values):
                    # Bỏ qua header (i=0)
                    if i > 0 and val in links_to_remove: 
                        rows_to_delete.append(i + 1)
                
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
                        sh.batch_update({'requests': delete_reqs})
                        time.sleep(1)
            except ValueError: pass

        # 2. TÍNH TOÁN VỊ TRÍ & GỘP DATA
        range_results_map = {} # Lưu kết quả dòng: {link: "100-200"}
        
        # Lấy số dòng hiện tại sau khi xóa
        try: current_rows = len(wks.col_values(1))
        except: current_rows = 0
        
        # Con trỏ bắt đầu ghi
        current_pointer = current_rows + 1
        
        # Danh sách DF để gộp
        dfs_to_concat = []
        
        # Căn chỉnh Header trước
        # Lấy tất cả cột từ tất cả file mới
        all_new_cols = set()
        for t in tasks_list:
            all_new_cols.update(t[0].columns)
        all_new_cols = list(all_new_cols)

        # Update Header Sheet nếu thiếu
        if not existing_headers:
            final_headers = all_new_cols
            wks.append_row(final_headers)
            existing_headers = final_headers
            current_pointer = 2 # Nếu mới tạo header thì data bắt đầu từ dòng 2
        else:
            missing = [c for c in all_new_cols if c not in existing_headers]
            if missing:
                wks.resize(cols=len(existing_headers) + len(missing))
                final_headers = existing_headers + missing
                wks.update(range_name="A1", values=[final_headers])
                existing_headers = final_headers
            else:
                final_headers = existing_headers

        # Duyệt qua từng task để tính dòng và chuẩn hóa
        for df, src_link in tasks_list:
            # Chuyển về Pandas và Reindex theo Header chuẩn
            pdf = df.to_pandas().fillna('')
            pdf_aligned = pdf.reindex(columns=final_headers, fill_value="")
            
            row_count = len(pdf_aligned)
            start_r = current_pointer
            end_r = start_r + row_count - 1
            
            # Lưu kết quả dòng cho link này
            range_results_map[src_link] = f"{start_r} - {end_r}"
            
            # Cập nhật con trỏ
            current_pointer += row_count
            
            # Thêm vào danh sách chờ ghi
            dfs_to_concat.append(pdf_aligned)

        # 3. GHI MỘT LẦN (BATCH WRITE)
        if dfs_to_concat:
            final_pdf = pd.concat(dfs_to_concat, ignore_index=True)
            data_values = final_pdf.values.tolist()
            
            BATCH_SIZE = 5000
            total_rows = len(data_values)
            for i in range(0, total_rows, BATCH_SIZE):
                chunk = data_values[i : i + BATCH_SIZE]
                wks.append_rows(chunk)
                time.sleep(1)
            
            return True, "Thành công", range_results_map
            
        return True, "Thành công (Không có data mới)", {}

    except Exception as e: return False, f"Lỗi Ghi: {str(e)}", {}

def process_pipeline(rows_to_run, user_id):
    creds = get_creds()
    is_locked, locking_user, lock_time = get_system_lock(creds)
    if is_locked and locking_user != user_id:
        return False, f"HỆ THỐNG ĐANG BẬN! {locking_user} đang chạy từ {lock_time}."
    
    set_system_lock(creds, user_id, lock=True)
    try:
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

        # Map lưu kết quả trả về: {Key (Link nguồn): (Message, Range)}
        results_map = {} 
        all_success = True
        log_entries = []
        tz_vn = pytz.timezone('Asia/Ho_Chi_Minh')
        time_now = datetime.now(tz_vn).strftime("%d/%m/%Y %H:%M:%S")

        for (target_link, target_sheet), group_rows in grouped_tasks.items():
            if not target_link: continue
            
            # Danh sách các task cần xử lý cho Sheet đích này
            # Format: [(DataFrame, SourceLink), (DataFrame, SourceLink)...]
            tasks_list = []
            
            for row in group_rows:
                df, sid, status = fetch_single_csv_safe(row, creds, token)
                src_link = row.get('Link dữ liệu lấy dữ liệu', '')
                
                if df is not None:
                    tasks_list.append((df, src_link))
                else:
                    # Lỗi ngay từ lúc tải
                    results_map[src_link] = ("Lỗi tải/Quyền", "")
                    # Ghi log lỗi
                    log_entries.append([
                        time_now, str(row.get('Ngày chốt', '')), str(row.get('Tháng', '')),
                        user_id, src_link, target_link, target_sheet,
                        row.get('Tên sheet nguồn dữ liệu gốc', ''), "Lỗi tải", "0", ""
                    ])

            if tasks_list:
                # Gửi cả danh sách vào hàm update để nó tự tính toán dòng
                success, msg, range_map = smart_update_safe(tasks_list, target_link, target_sheet, creds)
                
                # Cập nhật kết quả cho từng row
                for df, s_link in tasks_list:
                    # Tìm row config tương ứng để lấy thông tin ghi log
                    original_row = next((r for r in group_rows if r.get('Link dữ liệu lấy dữ liệu') == s_link), {})
                    
                    rng = range_map.get(s_link, "")
                    status_str = "Thành công" if success else f"Lỗi Ghi: {msg}"
                    
                    # Ghi log
                    log_entries.append([
                        time_now, str(original_row.get('Ngày chốt', '')), str(original_row.get('Tháng', '')),
                        user_id, s_link, target_link, target_sheet,
                        original_row.get('Tên sheet nguồn dữ liệu gốc', ''), 
                        status_str,
                        str(df.height),
                        rng # Cột Dòng dữ liệu
                    ])
                    
                    results_map[s_link] = (msg if not success else "Thành công", rng)
                
                if not success: all_success = False
            else:
                # Nếu không có task nào thành công trong nhóm này
                if not results_map: all_success = False
        
        history_id = st.secrets["gcp_service_account"]["history_sheet_id"]
        write_detailed_log(creds, history_id, log_entries)
        
        return all_success, results_map

    finally:
        set_system_lock(creds, user_id, lock=False)

# --- 6. GIAO DIỆN CHÍNH ---
def main_ui():
    if not check_login(): return
    user_id = st.session_state['current_user_id']
    st.title(f"⚙️ Tool Quản Lý Data (User: {user_id})")
    
    scan_result_placeholder = st.container()
    creds = get_creds()

    def load_conf(creds):
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        df = df.dropna(how='all')
        
        rename_map = {
            'Tên sheet dữ liệu': 'Tên sheet dữ liệu đích', 
            'Tên nguồn (Nhãn)': 'Tên sheet nguồn dữ liệu gốc',
            'Link file nguồn': 'Link dữ liệu lấy dữ liệu',
            'Link file đích': 'Link dữ liệu đích'
        }
        for old, new in rename_map.items():
            if old in df.columns and new not in df.columns: df = df.rename(columns={old: new})
        
        required_cols = ['Trạng thái', 'Ngày chốt', 'Tháng', 'Link dữ liệu lấy dữ liệu', 'Link dữ liệu đích', 'Tên sheet dữ liệu đích', 'Tên sheet nguồn dữ liệu gốc', 'Kết quả', 'Dòng dữ liệu']
        for c in required_cols:
            if c not in df.columns: df[c] = ""
            
        if 'Trạng thái' in df.columns:
            df['Trạng thái'] = df['Trạng thái'].apply(lambda x: "Đã chốt" if str(x).strip() in ["Đã chốt", "Đã cập nhật", "TRUE"] else "Chưa chốt & đang cập nhật")
        if 'Ngày chốt' in df.columns: 
            df['Ngày chốt'] = pd.to_datetime(df['Ngày chốt'], errors='coerce').dt.date

        if 'STT' in df.columns: df = df.drop(columns=['STT'])
        df.insert(0, 'STT', range(1, len(df) + 1))
        return df

    def save_conf(df_ui, creds):
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        df_save = df_ui.copy()
        if 'STT' in df_save.columns: df_save = df_save.drop(columns=['STT'])
        if 'Ngày chốt' in df_save.columns: df_save['Ngày chốt'] = df_save['Ngày chốt'].astype(str).replace({'NaT': '', 'nan': '', 'None': ''})
        wks.clear()
        wks.update([df_save.columns.tolist()] + df_save.fillna('').values.tolist())
        st.toast("✅ Đã lưu cấu hình!", icon="💾")

    def man_scan(df):
        errs = []
        for idx, row in df.iterrows():
            link_src = str(row.get('Link dữ liệu lấy dữ liệu', ''))
            if "docs.google.com" in link_src:
                ok, msg = verify_access_fast(link_src, creds)
                if not ok: errs.append((row.get('STT'), "Nguồn", link_src, f"{msg} -> Cần quyền XEM"))
            
            link_tgt = str(row.get('Link dữ liệu đích', ''))
            if "docs.google.com" in link_tgt:
                ok, msg = verify_access_fast(link_tgt, creds)
                if not ok: errs.append((row.get('STT'), "Đích", link_tgt, f"{msg} -> Cần quyền SỬA"))
        return errs

    if 'df_config' not in st.session_state:
        with st.spinner("Đang tải dữ liệu..."): st.session_state['df_config'] = load_conf(creds)

    cols_to_fix = ["Link dữ liệu lấy dữ liệu", "Link dữ liệu đích"]
    if 'df_config' in st.session_state and st.session_state['df_config'] is not None:
        for col in cols_to_fix:
            if col in st.session_state['df_config'].columns:
                st.session_state['df_config'][col] = st.session_state['df_config'][col].apply(
                    lambda x: ", ".join(map(str, x)) if isinstance(x, list) else (str(x) if pd.notna(x) else "")
                )

    col_order = ["STT", "Trạng thái", "Ngày chốt", "Tháng", "Link dữ liệu lấy dữ liệu", "Link dữ liệu đích", "Tên sheet dữ liệu đích", "Tên sheet nguồn dữ liệu gốc", "Kết quả", "Dòng dữ liệu"]
    
    edited_df = st.data_editor(
        st.session_state['df_config'],
        column_order=col_order,
        column_config={
            "STT": st.column_config.NumberColumn("STT", disabled=True, width="small"),
            "Trạng thái": st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt & đang cập nhật", "Đã chốt"], required=True, width="medium"),
            "Ngày chốt": st.column_config.DateColumn("Ngày chốt", format="DD/MM/YYYY"),
            "Link dữ liệu lấy dữ liệu": st.column_config.TextColumn("Link Nguồn", width="medium"),
            "Link dữ liệu đích": st.column_config.TextColumn("Link Đích", width="medium"),
            "Kết quả": st.column_config.TextColumn("Kết quả", disabled=True),
            "Dòng dữ liệu": st.column_config.TextColumn("Dòng Dữ Liệu", disabled=True),
        },
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        key="editor"
    )

    if not edited_df.equals(st.session_state['df_config']):
        edited_df = edited_df.reset_index(drop=True)
        edited_df['STT'] = range(1, len(edited_df) + 1)
        if 'Trạng thái' in edited_df.columns:
            edited_df['Trạng thái'] = edited_df['Trạng thái'].fillna("Chưa chốt & đang cập nhật").replace("", "Chưa chốt & đang cập nhật")
        st.session_state['df_config'] = edited_df
        st.rerun()

    st.divider()

    saved_hour = 8
    saved_freq = "1 ngày/1 lần"
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks_sys = sh.worksheet(SHEET_SYS_CONFIG)
        except: 
            wks_sys = sh.add_worksheet(SHEET_SYS_CONFIG, rows=5, cols=2)
            wks_sys.update([["run_hour", "8"], ["run_freq", "1 ngày/1 lần"]])

        data_conf = wks_sys.get_all_values()
        for r in data_conf:
            if r and len(r) > 1:
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
            try:
                wks_sys.update("A1:B1", [["run_hour", str(new_hour)]])
                wks_sys.update("A2:B2", [["run_freq", new_freq]])
                st.toast("✅ Đã lưu cài đặt!", icon="💾")
                time.sleep(1)
                st.rerun()
            except Exception as e: st.error(f"Lỗi lưu: {e}")

    col_run, col_scan, col_save = st.columns([3, 1, 1])
    
    with col_run:
        if st.button("▶️ CẬP NHẬT DỮ LIỆU (Chưa chốt)", type="primary"):
            rows_run = edited_df[edited_df['Trạng thái'] == "Chưa chốt & đang cập nhật"].to_dict('records')
            rows_run = [r for r in rows_run if len(str(r.get('Link dữ liệu lấy dữ liệu', ''))) > 5]
            
            if not rows_run: st.warning("⚠️ Không có dòng nào chưa chốt.")
            else:
                with st.status(f"Đang xử lý {len(rows_run)} nguồn...", expanded=True):
                    # all_ok: Boolean
                    # results_map: {link_nguon: (Message, RangeString)}
                    all_ok, results_map = process_pipeline(rows_run, user_id)
                    
                    if results_map:
                        st.success("Đã chạy xong.")
                        # Cập nhật kết quả lên bảng
                        for idx, row in edited_df.iterrows():
                            s_link = row.get('Link dữ liệu lấy dữ liệu', '')
                            if s_link in results_map:
                                msg, rng = results_map[s_link]
                                edited_df.at[idx, 'Kết quả'] = msg
                                edited_df.at[idx, 'Dòng dữ liệu'] = rng
                        
                        # Lưu lại ngay lập tức để hiển thị cho lần sau
                        save_conf(edited_df, creds)
                        st.session_state['df_config'] = edited_df
                        time.sleep(1)
                        st.rerun()
                    else: st.error("Có lỗi xảy ra.")

    with col_scan:
        if st.button("🔍 Quét Quyền"):
            errs = man_scan(edited_df)
            with scan_result_placeholder:
                if errs:
                    st.error(f"❌ Phát hiện {len(errs)} lỗi quyền!")
                    st.code(BOT_EMAIL_DISPLAY, language="text")
                    for stt, l_type, link, msg in errs:
                        st.markdown(f"- **Dòng {stt} [{l_type}]**: [Link]({link}) | {msg}")
                else:
                    st.success("✅ Tất cả Link Nguồn (Xem) và Đích (Sửa) đều OK.")

    with col_save:
        if st.button("💾 Lưu Cấu Hình"):
            save_conf(edited_df, creds)

if __name__ == "__main__":
    main_ui()
