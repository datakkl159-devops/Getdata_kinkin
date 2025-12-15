import streamlit as st
import pandas as pd
import polars as pl
import requests
import io
import concurrent.futures
import time
import gspread
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
SHEET_LOCK_NAME = "sys_lock"  # Sheet dùng để khóa hệ thống

# --- TÊN 3 CỘT QUẢN LÝ ---
COL_LINK_SRC = "Link file nguồn"
COL_LABEL_SRC = "Sheet nguồn"
COL_MONTH_SRC = "Tháng chốt"

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- AUTH ---
def check_login():
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
    if "auto_key" in st.query_params:
        if st.query_params["auto_key"] in AUTHORIZED_USERS:
            st.session_state['logged_in'] = True
            return True
    if not st.session_state['logged_in']:
        st.header("🔒 Đăng nhập hệ thống")
        pwd = st.text_input("Nhập mật khẩu truy cập:", type="password")
        if st.button("Đăng Nhập"):
            if pwd in AUTHORIZED_USERS:
                st.session_state['logged_in'] = True
                st.rerun()
            else: st.error("Mật khẩu không đúng!")
        return False
    return True

def get_creds():
    creds_info = dict(st.secrets["gcp_service_account"])
    if "private_key" in creds_info:
        creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
    return service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)

def extract_id(url):
    if url and "docs.google.com" in str(url):
        try: return url.split("/d/")[1].split("/")[0]
        except: return None
    return None

# --- HÀM LOCK SYSTEM (QUAN TRỌNG) ---
def get_system_lock(creds):
    """
    Kiểm tra xem hệ thống có đang bị khóa không.
    Trả về: (is_locked, user_locking, lock_time_str)
    Logic timeout: Nếu khóa quá 30 phút coi như khóa chết -> Cho phép chạy đè.
    """
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_LOCK_NAME)
        except: 
            wks = sh.add_worksheet(SHEET_LOCK_NAME, rows=10, cols=5)
            wks.update([["is_locked", "user", "time_start"], ["FALSE", "", ""]])
            return False, "", ""
        
        val = wks.cell(2, 1).value # Ô A2: Trạng thái khóa
        user = wks.cell(2, 2).value # Ô B2: Người đang khóa
        time_str = wks.cell(2, 3).value # Ô C2: Thời gian bắt đầu
        
        if val == "TRUE":
            # Kiểm tra Timeout (30 phút)
            try:
                lock_time = datetime.strptime(time_str, "%d/%m/%Y %H:%M:%S")
                diff = datetime.now() - lock_time
                if diff.total_seconds() > 1800: # 30 phút
                    return False, "", "" # Coi như hết hạn khóa
            except: pass # Lỗi format ngày tháng -> coi như không khóa
            
            return True, user, time_str
        return False, "", ""
    except: return False, "", ""

def set_system_lock(creds, user_id, lock=True):
    """
    Lock hoặc Unlock hệ thống.
    """
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_LOCK_NAME)
        except: wks = sh.add_worksheet(SHEET_LOCK_NAME, rows=10, cols=5)
        
        now_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        if lock:
            wks.update("A2:C2", [["TRUE", user_id, now_str]])
        else:
            wks.update("A2:C2", [["FALSE", "", ""]])
    except: pass

# --- HÀM LOG CHI TIẾT ---
def write_detailed_log(creds, history_sheet_id, log_data_list):
    if not log_data_list: return
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(history_sheet_id)
        try: wks = sh.worksheet(SHEET_LOG_NAME)
        except: 
            wks = sh.add_worksheet(SHEET_LOG_NAME, rows=1000, cols=10)
            headers = ["Ngày & giờ get dữ liệu", "Ngày chốt", "Tháng", "Nhân sự get", "Link nguồn", "Link đích", "Sheet Đích", "Sheet nguồn lấy dữ liệu", "Trạng Thái", "Số Dòng Đã Lấy"]
            wks.append_row(headers)
        wks.append_rows(log_data_list)
    except Exception as e: print(f"Lỗi log: {e}")

# --- LOAD CONFIG ---
def load_history_config(creds):
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        df = df.dropna(how='all')
        df = df[df['Link dữ liệu lấy dữ liệu'].str.len() > 5] 
        for col in ['Chọn', 'STT']:
            if col in df.columns: df = df.drop(columns=[col])
        rename_map = {'Tên sheet dữ liệu': 'Tên sheet dữ liệu đích', 'Tên nguồn (Nhãn)': 'Tên sheet nguồn dữ liệu gốc'}
        for old, new in rename_map.items():
            if old in df.columns and new not in df.columns: df = df.rename(columns={old: new})
        if 'Trạng thái' not in df.columns: df['Trạng thái'] = "Chưa cập nhật"
        else: df['Trạng thái'] = df['Trạng thái'].apply(lambda x: "Đã cập nhật" if str(x).strip() in ["Đã cập nhật", "Đã chốt", "TRUE"] else "Chưa cập nhật")
        if 'Ngày chốt' in df.columns: df['Ngày chốt'] = pd.to_datetime(df['Ngày chốt'], errors='coerce').dt.date
        for c in ['Tên sheet dữ liệu đích', 'Tên sheet nguồn dữ liệu gốc', 'Hành động']:
            if c not in df.columns: df[c] = ""
        df.insert(0, 'STT', range(1, len(df) + 1))
        return df
    except: return None

def save_history_config(df_ui, creds):
    try:
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
    except Exception as e: st.error(f"Lỗi lưu: {e}")

# --- QUÉT QUYỀN ---
def verify_access_fast(url, creds):
    sheet_id = extract_id(url)
    if not sheet_id: return False, "Link lỗi"
    try:
        gc = gspread.authorize(creds)
        gc.open_by_key(sheet_id)
        return True, "OK"
    except gspread.exceptions.APIError as e:
        if "403" in str(e): return False, "⛔ Chưa cấp quyền (403)"
        return False, f"❌ Lỗi: {e}"
    except Exception as e: return False, f"❌ Lỗi mạng: {e}"

def manual_scan(df):
    creds = get_creds()
    errors = []
    with st.spinner("Đang quét..."):
        for idx, row in df.iterrows():
            link_src = row.get('Link dữ liệu lấy dữ liệu', '')
            link_dst = row.get('Link dữ liệu đích', '')
            if link_src and "docs.google.com" in str(link_src):
                ok, msg = verify_access_fast(link_src, creds)
                if not ok: errors.append(f"Dòng {row.get('STT', idx+1)} (Nguồn): {msg}")
            if link_dst and "docs.google.com" in str(link_dst):
                ok, msg = verify_access_fast(link_dst, creds)
                if not ok: errors.append(f"Dòng {row.get('STT', idx+1)} (Đích): {msg}")
    return errors

# --- CORE LOGIC (XÓA CŨ - CHÈN MỚI) ---
def fetch_single_csv_safe(row_config, token):
    link_src = row_config.get('Link dữ liệu lấy dữ liệu', '')
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
        sh = gc.open_by_key(target_id)
        
        real_sheet_name = str(target_sheet_name).strip()
        if not real_sheet_name: real_sheet_name = "Tong_Hop_Data"
        
        try: wks = sh.worksheet(real_sheet_name)
        except: wks = sh.add_worksheet(title=real_sheet_name, rows=1000, cols=20)
        
        token = creds.token 
        if not token:
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
        except: pass

        # --- LOGIC XÓA VÀ CHÈN (Requirement: Xóa hẳn các dòng trùng link nguồn) ---
        if not df_current.is_empty():
            if COL_LINK_SRC in df_current.columns:
                # XÓA: Lọc GIỮ LẠI những dòng KHÔNG nằm trong danh sách link cần cập nhật
                df_keep = df_current.filter(~pl.col(COL_LINK_SRC).is_in(links_to_remove))
            else:
                df_keep = df_current 
        else:
            df_keep = pl.DataFrame()

        # CHÈN: Nối đuôi dữ liệu mới vào
        if not df_new_updates.is_empty():
            df_final = pl.concat([df_keep, df_new_updates], how="diagonal")
        else:
            df_final = df_keep

        # Sắp xếp cột: Đưa 3 cột quản lý xuống cuối
        all_cols = df_final.columns
        data_cols = [c for c in all_cols if c not in [COL_LINK_SRC, COL_LABEL_SRC, COL_MONTH_SRC]]
        final_order = data_cols + [COL_LINK_SRC, COL_LABEL_SRC, COL_MONTH_SRC]
        final_cols = [c for c in final_order if c in df_final.columns]
        df_final = df_final.select(final_cols)

        pdf = df_final.to_pandas().fillna('')
        wks.clear()
        wks.update([pdf.columns.tolist()] + pdf.values.tolist())
        return True, f"Sheet '{real_sheet_name}': OK {len(pdf)} dòng."
    except Exception as e: return False, str(e)

def process_pipeline(rows_to_run, user_id):
    creds = get_creds()
    
    # --- CHECK LOCK ---
    is_locked, locking_user, lock_time = get_system_lock(creds)
    if is_locked and locking_user != user_id: # Nếu bị khóa bởi người khác
        return False, f"HỆ THỐNG ĐANG BẬN! {locking_user} đang chạy từ {lock_time}. Vui lòng thử lại sau."
    
    # --- SET LOCK ---
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
                    
                    log_row = [
                        time_now, str(row.get('Ngày chốt', '')), str(row.get('Tháng', '')),
                        user_id, src_link, target_link, target_sheet,
                        row.get('Tên sheet nguồn dữ liệu gốc', ''), status,
                        str(df.height) if df is not None else "0"
                    ]
                    log_entries.append(log_row)

                    if df is not None:
                        results.append(df)
                        links_remove.append(src_link)
            
            if results:
                df_new = pl.concat(results, how="vertical", rechunk=True)
                success, msg = smart_update_safe(df_new, target_link, target_sheet, creds, links_remove)
                final_messages.append(msg)
                if not success: all_success = False
            else:
                final_messages.append(f"Sheet '{target_sheet}': Lỗi tải data.")
                all_success = False
                
        history_id = st.secrets["gcp_service_account"]["history_sheet_id"]
        write_detailed_log(creds, history_id, log_entries)
        
        return all_success, " | ".join(final_messages)
        
    finally:
        # --- RELEASE LOCK (Bắt buộc mở khóa dù lỗi) ---
        set_system_lock(creds, user_id, lock=False)

# --- UI CHÍNH ---
def main_ui():
    user_id = st.session_state.get('current_user_id', 'Unknown')
    st.title(f"⚙️ Tool Quản Lý Data (User: {user_id})")
    creds = get_creds()

    # --- CHECK LOCK TRẠNG THÁI HIỂN THỊ ---
    is_locked, locking_user, lock_time = get_system_lock(creds)
    if is_locked and locking_user != user_id:
        st.warning(f"⚠️ **HỆ THỐNG ĐANG BẬN!** Người dùng **{locking_user}** đang xử lý dữ liệu (Bắt đầu: {lock_time}). Vui lòng đợi họ làm xong.")
        st.stop() # Dừng không cho làm gì cả

    if 'df_config' not in st.session_state:
        with st.spinner("Đang tải..."):
            st.session_state['df_config'] = load_history_config(creds)

    if 'scan_errors' in st.session_state and st.session_state['scan_errors']:
        st.error(f"⚠️ Có {len(st.session_state['scan_errors'])} link lỗi!")
        for err in st.session_state['scan_errors']: st.write(f"- {err}")
        c1, c2 = st.columns([3,1])
        with c1:
            st.markdown(f"**👉 COPY Email Robot:**")
            st.code(BOT_EMAIL_DISPLAY, language="text")
        st.divider()

    col_order = ["STT", "Trạng thái", "Ngày chốt", "Tháng", "Link dữ liệu lấy dữ liệu", "Link dữ liệu đích", "Tên sheet dữ liệu đích", "Tên sheet nguồn dữ liệu gốc", "Hành động"]
    
    edited_df = st.data_editor(
        st.session_state['df_config'],
        column_order=col_order,
        column_config={
            "STT": st.column_config.NumberColumn("STT", disabled=True, width="small"),
            "Trạng thái": st.column_config.SelectboxColumn("Trạng thái", options=["Chưa cập nhật", "Đã cập nhật"], required=True, width="small"),
            "Ngày chốt": st.column_config.DateColumn("Ngày chốt", format="DD/MM/YYYY"),
            "Link dữ liệu lấy dữ liệu": st.column_config.TextColumn("Link Nguồn", width="medium"),
            "Link dữ liệu đích": st.column_config.TextColumn("Link Đích", width="medium"),
            "Hành động": st.column_config.TextColumn("Kết quả", disabled=True),
        },
        use_container_width=True,
        hide_index=True,
        key="editor"
    )

    if not edited_df.equals(st.session_state['df_config']):
        edited_df = edited_df.reset_index(drop=True)
        edited_df['STT'] = range(1, len(edited_df) + 1)
        for idx, row in edited_df.iterrows():
            if row['Trạng thái'] == "Chưa cập nhật": edited_df.at[idx, 'Hành động'] = "Sẽ chạy"
            else: edited_df.at[idx, 'Hành động'] = ""
        st.session_state['df_config'] = edited_df
        st.rerun()

    st.divider()

    st.subheader("⏰ Cài Đặt Tự Động")
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks_sys = sh.worksheet("sys_config")
        except: 
            wks_sys = sh.add_worksheet("sys_config", rows=10, cols=5)
            wks_sys.update([["setting_name", "value"], ["run_hour", "8"], ["run_freq", "1 ngày/1 lần"]])
        
        data_conf = wks_sys.get_all_values()
        saved_hour = 8
        saved_freq = "1 ngày/1 lần"
        for r in data_conf:
            if r[0] == "run_hour": saved_hour = int(r[1])
            if r[0] == "run_freq": saved_freq = r[1]
    except: pass

    c1, c2, c3 = st.columns(3)
    with c1:
        new_freq = st.selectbox("Tần suất:", ["1 ngày/1 lần", "1 tuần/1 lần", "1 tháng/1 lần"], 
                                index=["1 ngày/1 lần", "1 tuần/1 lần", "1 tháng/1 lần"].index(saved_freq))
    with c2:
        new_hour = st.slider("Giờ chạy (VN):", 0, 23, value=saved_hour)
    with c3:
        st.write("")
        if st.button("Lưu Cài Đặt"):
            try:
                cell_h = wks_sys.find("run_hour")
                if cell_h: wks_sys.update_cell(cell_h.row, cell_h.col + 1, str(new_hour))
                else: wks_sys.append_row(["run_hour", str(new_hour)])
                cell_f = wks_sys.find("run_freq")
                if cell_f: wks_sys.update_cell(cell_f.row, cell_f.col + 1, str(new_freq))
                else: wks_sys.append_row(["run_freq", str(new_freq)])
                st.toast("Đã lưu!", icon="✅")
            except: st.error("Lỗi lưu")

    st.divider()

    col_run, col_scan, col_save = st.columns([3, 1, 1])
    
    with col_run:
        if st.button("▶️ CẬP NHẬT DỮ LIỆU (Chưa cập nhật)", type="primary"):
            # CHECK LOCK LẦN NỮA TRONG TRƯỜNG HỢP VỪA BẤM THÌ CÓ NGƯỜI KHÁC VÀO
            is_locked, locking_user, lock_time = get_system_lock(creds)
            if is_locked and locking_user != user_id:
                st.error(f"❌ Chậm chân rồi! {locking_user} vừa mới chiếm quyền điều khiển.")
                st.rerun()
            else:
                rows_run = edited_df[edited_df['Trạng thái'] == "Chưa cập nhật"].to_dict('records')
                if not rows_run:
                    st.warning("⚠️ Không có dòng nào 'Chưa cập nhật'.")
                else:
                    with st.status(f"Đang xử lý {len(rows_run)} nguồn...", expanded=True):
                        success, msg = process_pipeline(rows_run, user_id)
                        if success:
                            st.success(f"Kết quả: {msg}")
                            for idx, row in edited_df.iterrows():
                                if row['Trạng thái'] == "Chưa cập nhật":
                                    edited_df.at[idx, 'Trạng thái'] = "Đã cập nhật"
                                    edited_df.at[idx, 'Hành động'] = "Vừa xong"
                            save_history_config(edited_df, creds)
                            st.session_state['df_config'] = edited_df
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(msg)

    with col_scan:
        if st.button("🔍 Quét All Quyền"):
            errors = manual_scan(edited_df)
            st.session_state['scan_errors'] = errors
            if not errors: st.toast("✅ Link OK!", icon="✨")
            else: st.toast(f"⚠️ Phát hiện {len(errors)} link lỗi!", icon="🚨")
            st.rerun()

    with col_save:
        if st.button("💾 Lưu Cấu Hình"):
            save_history_config(edited_df, creds)

if __name__ == "__main__":
    if check_login():
        main_ui()
