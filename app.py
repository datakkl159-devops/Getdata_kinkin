import streamlit as st
import pandas as pd
import polars as pl
import requests
import io
import concurrent.futures
import time
import gspread
from gspread_dataframe import get_as_dataframe
from datetime import datetime
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

# --- TÊN 3 CỘT QUẢN LÝ ---
COL_LINK_SRC = "Link file nguồn"
COL_LABEL_SRC = "Sheet nguồn"
COL_MONTH_SRC = "Tháng chốt"

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- HÀM HỖ TRỢ ---
def check_login():
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
    
    if "auto_key" in st.query_params:
        key = st.query_params["auto_key"]
        if key in AUTHORIZED_USERS:
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

def load_history_config(creds):
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        
        df = df.dropna(how='all')
        df = df[df['Link dữ liệu lấy dữ liệu'].str.len() > 5] 
        
        # Đổi tên cột cũ sang mới nếu cần
        rename_map = {
            'Tên sheet dữ liệu': 'Tên sheet dữ liệu đích',
            'Tên nguồn (Nhãn)': 'Tên sheet nguồn dữ liệu gốc',
            'Trạng thái': 'Chọn'
        }
        for old, new in rename_map.items():
            if old in df.columns and new not in df.columns:
                df = df.rename(columns={old: new})

        if 'Chọn' in df.columns:
            df['Chọn'] = df['Chọn'].apply(lambda x: True if str(x).upper() == "TRUE" else False)
        else:
            df['Chọn'] = False
            
        if 'Ngày chốt' in df.columns:
            df['Ngày chốt'] = pd.to_datetime(df['Ngày chốt'], errors='coerce').dt.date

        if 'Tên sheet dữ liệu đích' not in df.columns: df['Tên sheet dữ liệu đích'] = ""
        if 'Tên sheet nguồn dữ liệu gốc' not in df.columns: df['Tên sheet nguồn dữ liệu gốc'] = ""

        return df
    except: return None

def save_history_config(df_ui, creds):
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        wks = sh.worksheet(SHEET_CONFIG_NAME)
        
        df_save = df_ui.copy()
        # Chuẩn hóa string trước khi lưu để đẹp data
        if 'Tên sheet dữ liệu đích' in df_save.columns:
            df_save['Tên sheet dữ liệu đích'] = df_save['Tên sheet dữ liệu đích'].astype(str).str.strip()

        if 'Chọn' in df_save.columns:
            df_save['Chọn'] = df_save['Chọn'].apply(lambda x: "TRUE" if x else "FALSE")
            
        if 'Ngày chốt' in df_save.columns:
            df_save['Ngày chốt'] = df_save['Ngày chốt'].astype(str).replace({'NaT': '', 'nan': '', 'None': ''})

        wks.clear()
        wks.update([df_save.columns.tolist()] + df_save.fillna('').values.tolist())
        st.toast("✅ Đã lưu cấu hình!", icon="💾")
    except Exception as e: st.error(f"Lỗi lưu: {e}")

# --- CORE LOGIC ---
def fetch_single_csv_safe(row_config, token):
    link_src = row_config.get('Link dữ liệu lấy dữ liệu', '')
    # Cắt khoảng trắng tên nguồn
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
            
            cols_to_drop = [c for c in df.columns if c in [COL_LINK_SRC, COL_LABEL_SRC, COL_MONTH_SRC]]
            if cols_to_drop: df = df.drop(cols_to_drop)

            df = df.with_columns([
                pl.lit(link_src).cast(pl.Utf8).alias(COL_LINK_SRC),
                pl.lit(source_label).cast(pl.Utf8).alias(COL_LABEL_SRC),
                pl.lit(month_val).cast(pl.Utf8).alias(COL_MONTH_SRC)
            ])
            return df, sheet_id, "Thành công"
        return None, sheet_id, "Lỗi HTTP"
    except Exception as e: return None, sheet_id, str(e)

def smart_update_safe(df_new_updates, target_link, target_sheet_name, creds, links_to_remove):
    try:
        gc = gspread.authorize(creds)
        target_id = extract_id(target_link)
        sh = gc.open_by_key(target_id)
        
        # 1. XỬ LÝ TÊN SHEET ĐÍCH (TRIM SPACE)
        real_sheet_name = str(target_sheet_name).strip()
        if not real_sheet_name: real_sheet_name = "Tong_Hop_Data" # Mặc định nếu để trống
        
        # 2. TẠO SHEET NẾU CHƯA CÓ
        try: 
            wks = sh.worksheet(real_sheet_name)
        except: 
            # Nếu chưa có thì tạo mới
            wks = sh.add_worksheet(title=real_sheet_name, rows=1000, cols=20)
        
        token = creds.token 
        if not token:
            auth_req = google.auth.transport.requests.Request()
            creds.refresh(auth_req)
            token = creds.token

        # 3. ĐỌC DỮ LIỆU CŨ (NẾU CÓ)
        export_url = f"https://docs.google.com/spreadsheets/d/{target_id}/export?format=csv&gid={wks.id}"
        headers = {'Authorization': f'Bearer {token}'}
        
        df_current = pl.DataFrame()
        try:
            r = requests.get(export_url, headers=headers)
            if r.status_code == 200:
                df_current = pl.read_csv(io.BytesIO(r.content), infer_schema_length=0)
        except: pass

        # 4. GIỮ LẠI DỮ LIỆU CỦA LINK KHÁC (ĐỂ VIẾT TIẾP XUỐNG DƯỚI)
        if not df_current.is_empty():
            rename_map = {}
            for col in df_current.columns:
                if col.strip() in ["Link Nguồn", "Link URL nguồn"]: rename_map[col] = COL_LINK_SRC
            if rename_map: df_current = df_current.rename(rename_map)

            if COL_LINK_SRC in df_current.columns:
                # Chỉ xóa những dòng thuộc về Link đang cập nhật (để thay bằng bản mới)
                # Giữ nguyên các dòng khác
                df_keep = df_current.filter(~pl.col(COL_LINK_SRC).is_in(links_to_remove))
            else:
                df_keep = df_current
        else:
            df_keep = pl.DataFrame()

        # 5. GỘP (APPEND)
        if not df_new_updates.is_empty():
            df_final = pl.concat([df_keep, df_new_updates], how="diagonal")
        else:
            df_final = df_keep

        # 6. SẮP XẾP CỘT
        all_cols = df_final.columns
        data_cols = [c for c in all_cols if c not in [COL_LINK_SRC, COL_LABEL_SRC, COL_MONTH_SRC]]
        final_order = data_cols + [COL_LINK_SRC, COL_LABEL_SRC, COL_MONTH_SRC]
        final_cols = [c for c in final_order if c in df_final.columns]
        df_final = df_final.select(final_cols)

        # 7. GHI LẠI
        pdf = df_final.to_pandas().fillna('')
        data_values = pdf.values.tolist()
        wks.clear()
        wks.update([pdf.columns.tolist()] + data_values)
        return True, f"Sheet '{real_sheet_name}': OK {len(pdf)} dòng."

    except Exception as e: return False, str(e)

def process_pipeline(rows_to_run, user_id):
    creds = get_creds()
    auth_req = google.auth.transport.requests.Request() 
    creds.refresh(auth_req)
    token = creds.token
    
    # GOM NHÓM: Xử lý khoảng trắng ngay khi gom nhóm
    grouped_tasks = defaultdict(list)
    for row in rows_to_run:
        t_link = row.get('Link dữ liệu đích', '')
        # Cắt khoảng trắng tên sheet đích để gom nhóm chính xác
        t_sheet = str(row.get('Tên sheet dữ liệu đích', '')).strip()
        if not t_sheet: t_sheet = "Tong_Hop_Data"
        
        key = (t_link, t_sheet)
        grouped_tasks[key].append(row)

    final_messages = []
    all_success = True

    for (target_link, target_sheet), group_rows in grouped_tasks.items():
        if not target_link: continue
        
        results = []
        links_remove = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(fetch_single_csv_safe, row, token): row for row in group_rows}
            for future in concurrent.futures.as_completed(futures):
                row = futures[future]
                df, sid, status = future.result()
                if df is not None:
                    results.append(df)
                    links_remove.append(row.get('Link dữ liệu lấy dữ liệu'))
        
        if results:
            df_new = pl.concat(results, how="vertical", rechunk=True)
            success, msg = smart_update_safe(df_new, target_link, target_sheet, creds, links_remove)
            final_messages.append(msg)
            if not success: all_success = False
        else:
            final_messages.append(f"Sheet '{target_sheet}': Không có data.")
            all_success = False
            
    return all_success, " | ".join(final_messages)

# --- UI CHÍNH ---
def main_ui():
    user_id = st.session_state.get('current_user_id', 'Unknown')
    st.title(f"⚙️ Tool Quản Lý Data (User: {user_id})")
    creds = get_creds()

    if 'df_config' not in st.session_state:
        with st.spinner("Đang tải..."):
            st.session_state['df_config'] = load_history_config(creds)

    col_order = ["Chọn", "Ngày chốt", "Tháng", "Link dữ liệu lấy dữ liệu", "Link dữ liệu đích", "Tên sheet dữ liệu đích", "Tên sheet nguồn dữ liệu gốc", "Hành động"]
    
    edited_df = st.data_editor(
        st.session_state['df_config'],
        column_order=col_order,
        column_config={
            "Chọn": st.column_config.CheckboxColumn("Chọn", width="small"),
            "Ngày chốt": st.column_config.DateColumn("Ngày chốt", format="DD/MM/YYYY"),
            "Hành động": st.column_config.TextColumn("Hành động", disabled=True),
            "Tên sheet dữ liệu đích": st.column_config.TextColumn("Tên sheet dữ liệu đích", help="Tự động tạo sheet nếu chưa có"),
        },
        use_container_width=True,
        hide_index=True,
        key="editor"
    )

    if not edited_df.equals(st.session_state['df_config']):
        for idx, row in edited_df.iterrows():
            if row['Chọn']: edited_df.at[idx, 'Hành động'] = "Sẽ chạy"
            else: edited_df.at[idx, 'Hành động'] = ""
        st.session_state['df_config'] = edited_df
        st.rerun()

    st.divider()
    # ... (Phần Cài đặt lịch giữ nguyên code cũ, vì không đổi logic) ...
    # Để code gọn, tôi giữ phần Cài đặt lịch và Nút chạy như bản v6.0
    # Vì logic ở trên đã xử lý phần process_pipeline rồi.
    
    # 2. CÀI ĐẶT LỊCH CHẠY
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

    col_run, col_save = st.columns([4, 1])
    with col_run:
        if st.button("▶️ CHẠY NGAY (Dòng được chọn)", type="primary"):
            rows_run = edited_df[edited_df['Chọn'] == True].to_dict('records')
            if not rows_run:
                st.warning("Chưa chọn dòng nào!")
            else:
                with st.status("Đang xử lý...", expanded=True):
                    success, msg = process_pipeline(rows_run, user_id)
                    if success:
                        st.success(f"Kết quả: {msg}")
                        for idx, row in edited_df.iterrows():
                            if row['Chọn']:
                                edited_df.at[idx, 'Chọn'] = False
                                edited_df.at[idx, 'Hành động'] = "Đã xong"
                        save_history_config(edited_df, creds)
                        st.session_state['df_config'] = edited_df
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(msg)

    with col_save:
        if st.button("💾 Lưu Cấu Hình"):
            save_history_config(edited_df, creds)

if __name__ == "__main__":
    if check_login():
        main_ui()
