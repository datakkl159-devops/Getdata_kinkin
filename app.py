import streamlit as st
import pandas as pd
import polars as pl
import requests
import io
import concurrent.futures
import time
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from datetime import datetime
from google.oauth2 import service_account
import google.auth.transport.requests

# --- 1. CẤU HÌNH HỆ THỐNG ---
st.set_page_config(page_title="Tool Quản Lý Data (ID Logic)", layout="wide")

AUTHORIZED_USERS = {
    "admin2024": "Admin_Master",
    "team_hn": "Team_HaNoi",
    "team_hcm": "Team_HCM"
}

BOT_EMAIL_DISPLAY = "getdulieu@kin-kin-477902.iam.gserviceaccount.com"
SHEET_CONFIG_NAME = "luu_cau_hinh" 
SHEET_LOG_NAME = "log_lanthucthi"

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- 2. HÀM HỖ TRỢ & BẢO MẬT ---
def check_login():
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
        st.session_state['current_user_id'] = None
    
    if "auto_key" in st.query_params:
        key = st.query_params["auto_key"]
        if key in AUTHORIZED_USERS:
            st.session_state['logged_in'] = True
            st.session_state['current_user_id'] = AUTHORIZED_USERS[key]
            return True

    if not st.session_state['logged_in']:
        st.header("🔒 Đăng nhập hệ thống")
        pwd = st.text_input("Nhập mật khẩu truy cập:", type="password")
        if st.button("Đăng Nhập"):
            if pwd in AUTHORIZED_USERS:
                st.session_state['logged_in'] = True
                st.session_state['current_user_id'] = AUTHORIZED_USERS[pwd]
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

# --- 3. QUẢN LÝ LOG & HISTORY ---
def log_batch_to_sheet(creds, log_rows):
    history_id = st.secrets["gcp_service_account"].get("history_sheet_id")
    if not history_id or not log_rows: return
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(history_id)
        try: wks = sh.worksheet(SHEET_LOG_NAME)
        except: 
            wks = sh.add_worksheet(title=SHEET_LOG_NAME, rows=1000, cols=10)
            wks.append_row(["Thời gian", "Người thực hiện", "Nguồn", "Đích", "Trạng thái", "Chi tiết"])
        wks.append_rows(log_rows)
    except: pass

def load_history_config(creds, current_user_id):
    history_id = st.secrets["gcp_service_account"].get("history_sheet_id")
    if not history_id: return None
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(history_id)
        try: wks = sh.worksheet(SHEET_CONFIG_NAME)
        except: return None
        
        df_all = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if df_all.empty or 'User_ID' not in df_all.columns: return None
            
        df_user = df_all[df_all['User_ID'] == current_user_id].copy()
        if 'User_ID' in df_user.columns: df_user = df_user.drop(columns=['User_ID'])
        df_user = df_user.fillna("")
        
        if 'Trạng thái' in df_user.columns:
            df_user['Trạng thái'] = df_user['Trạng thái'].apply(lambda x: "Chưa chốt" if x == "" or pd.isna(x) else x)
            
        return df_user
    except: return None

def save_history_config(df_ui, creds, current_user_id):
    history_id = st.secrets["gcp_service_account"].get("history_sheet_id")
    if not history_id: return
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(history_id)
        try: wks = sh.worksheet(SHEET_CONFIG_NAME)
        except: wks = sh.add_worksheet(title=SHEET_CONFIG_NAME, rows=100, cols=20)
            
        try: df_all = get_as_dataframe(wks, dtype=str)
        except: df_all = pd.DataFrame()
        
        df_new = df_ui.copy()
        df_new['User_ID'] = current_user_id
        
        final_df = df_new
        if not df_all.empty and 'User_ID' in df_all.columns:
            df_others = df_all[df_all['User_ID'] != current_user_id]
            final_df = pd.concat([df_others, df_new], ignore_index=True)
            
        wks.clear()
        final_df = final_df.fillna('')
        wks.update([final_df.columns.tolist()] + final_df.values.tolist())
        st.toast(f"✅ Đã lưu cấu hình!", icon="💾")
    except Exception as e: st.error(f"Lỗi lưu: {e}")

# --- 4. CORE ENGINE (LOGIC MỚI: DÙNG ID ĐỂ XÓA) ---
def verify_access_fast(url, creds):
    sheet_id = extract_id(url)
    if not sheet_id: return False, "Link sai"
    try:
        gc = gspread.authorize(creds)
        gc.open_by_key(sheet_id)
        return True, "✅ Sẵn sàng"
    except gspread.exceptions.APIError as e:
        if "403" in str(e): return False, "⛔ Thiếu quyền"
        return False, "❌ Lỗi khác"
    except: return False, "❌ Lỗi mạng"

def fetch_single_csv_with_id(row_config, token):
    link_src = row_config.get('Link dữ liệu lấy dữ liệu', '')
    display_label = row_config.get('Tên nguồn (Nhãn)', '') # Dùng để hiển thị cho đẹp
    
    # LẤY ID TỪ LINK -> ĐÂY LÀ KHÓA CHÍNH ĐỂ XÓA
    sheet_id = extract_id(link_src)
    
    if not sheet_id:
        return None, sheet_id, "Link lỗi"

    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            df = pl.read_csv(io.BytesIO(response.content), infer_schema_length=0)
            
            # THÊM 2 CỘT QUAN TRỌNG:
            # 1. System_Source_ID: Chứa ID file (Dùng để code xóa dữ liệu cũ chính xác)
            # 2. Tên_Nguồn: Chứa Nhãn (Dùng để sếp đọc báo cáo cho dễ hiểu)
            
            df = df.with_columns([
                pl.lit(sheet_id).alias("System_Source_ID"), # Cột Kỹ thuật
                pl.lit(display_label).alias("Tên_Nguồn")    # Cột Hiển thị
            ])
            return df, sheet_id, "Thành công"
        return None, sheet_id, "Lỗi tải HTTP"
    except Exception as e: return None, sheet_id, str(e)

def smart_update_by_id(df_new_updates, target_link, creds, ids_to_remove):
    """
    Logic xóa dựa trên System_Source_ID (ID của Link)
    """
    try:
        gc = gspread.authorize(creds)
        target_id = extract_id(target_link)
        sh = gc.open_by_key(target_id)
        try: wks = sh.worksheet("Tong_Hop_Data")
        except: wks = sh.get_worksheet(0)
        
        # 1. Đọc file đích
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

        # 2. Lọc bỏ dữ liệu cũ DỰA TRÊN ID (Chính xác 100%)
        if not df_current.is_empty():
            if "System_Source_ID" in df_current.columns:
                # Giữ lại những dòng mà ID KHÔNG nằm trong danh sách đang chạy
                df_keep = df_current.filter(~pl.col("System_Source_ID").is_in(ids_to_remove))
            else:
                # Nếu file đích chưa có cột ID (Lần đầu chạy tool mới), giữ nguyên hoặc xóa hết?
                # Để an toàn, coi như chưa có gì để lọc, ta nối thêm vào (hoặc user tự clear lần đầu)
                # Tốt nhất: Nếu chưa có cột ID, ta coi như đây là file trắng của tool này -> Giữ nguyên.
                df_keep = df_current 
        else:
            df_keep = pl.DataFrame()

        # 3. Gộp
        if not df_new_updates.is_empty():
            # Align schema if needed (thường Polars tự lo nếu cột khớp)
            df_final = pl.concat([df_keep, df_new_updates], how="diagonal")
        else:
            df_final = df_keep

        # 4. Ghi
        wks.clear()
        pdf = df_final.to_pandas().fillna('')
        data_to_write = [pdf.columns.tolist()] + pdf.values.tolist()
        wks.update(data_to_write)
        
        return True, f"Cập nhật thành công. (Đã thay thế data của {len(ids_to_remove)} ID nguồn)"

    except Exception as e: return False, str(e)

def process_pipeline_smart(rows_to_process, user_id):
    creds = get_creds()
    auth_req = google.auth.transport.requests.Request() 
    creds.refresh(auth_req)
    token = creds.token
    
    results_df = []
    ids_processing = [] # Danh sách ID cần xóa
    log_entries = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    target_link = rows_to_process[0]['Link dữ liệu đích']
    
    # 1. Tải dữ liệu
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_row = {executor.submit(fetch_single_csv_with_id, row, token): row for row in rows_to_process}
        for future in concurrent.futures.as_completed(future_to_row):
            row = future_to_row[future]
            label = row.get('Tên nguồn (Nhãn)', 'Unknown')
            df, sheet_id, status = future.result()
            
            if df is not None and sheet_id:
                results_df.append(df)
                ids_processing.append(sheet_id) # Lưu ID để tí nữa xóa data cũ của ID này
                log_entries.append([timestamp, user_id, label, target_link, "Tải OK", f"ID: {sheet_id} - {df.height} dòng"])
            else:
                log_entries.append([timestamp, user_id, label, target_link, "Lỗi Tải", status])

    # 2. Update Đích
    final_msg = ""
    success = False
    
    if results_df:
        df_new = pl.concat(results_df, how="vertical", rechunk=True)
        # GỌI HÀM UPDATE THEO ID
        success, msg = smart_update_by_id(df_new, target_link, creds, ids_processing)
        final_msg = msg
    else:
        final_msg = "Không tải được dữ liệu nào"

    # Log tổng
    log_entries.append([timestamp, user_id, "TỔNG HỢP", target_link, "Hoàn tất" if success else "Thất bại", final_msg])
    log_batch_to_sheet(creds, log_entries)
    
    return success, final_msg

# --- 5. GIAO DIỆN CHÍNH ---
def main_ui():
    user_id = st.session_state.get('current_user_id', 'Unknown')
    st.title(f"⚙️ Tool Quản Lý Data (Smart ID Logic)")
    
    # LOAD CONFIG
    if 'df_config' not in st.session_state:
        creds = get_creds()
        with st.spinner("⏳ Tải cấu hình..."):
            df = load_history_config(creds, user_id)
        
        default_data = {
            "Trạng thái": ["Chưa chốt", "Chưa chốt"],
            "Tiến độ": ["", ""],
            "Ngày chốt": [datetime.now().date(), datetime.now().date()],
            "Tháng": ["12/2025", "12/2025"],
            "Link dữ liệu lấy dữ liệu": ["", ""],
            "Link dữ liệu đích": ["", ""],
            "Tên sheet dữ liệu": ["Sheet1", "Sheet1"],
            "Tên nguồn (Nhãn)": ["CN Hà Nội", "CN HCM"] # Chỉ dùng để hiển thị
        }
        
        if df is not None and not df.empty:
            for k in default_data.keys():
                if k not in df.columns: df[k] = ""
            st.session_state['df_config'] = df[list(default_data.keys())]
        else:
            st.session_state['df_config'] = pd.DataFrame(default_data)

    st.info("💡 **Logic Mới:** Hệ thống dùng **ID của Link Nguồn** để xóa dữ liệu cũ và cập nhật mới. Tên nhãn chỉ để hiển thị.")

    # EDITOR
    edited_df = st.data_editor(
        st.session_state['df_config'],
        num_rows="dynamic",
        column_config={
            "Trạng thái": st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt", "Đã chốt"], required=True, width="small"),
            "Tiến độ": st.column_config.TextColumn("Tiến độ", disabled=True),
            "Link dữ liệu lấy dữ liệu": st.column_config.TextColumn("Link Nguồn (ID)", width="medium", required=True),
            "Link dữ liệu đích": st.column_config.TextColumn("Link Đích", width="medium"),
            "Tên nguồn (Nhãn)": st.column_config.TextColumn("Tên Hiển Thị", required=True),
            "Ngày chốt": st.column_config.DateColumn("Ngày chốt", format="DD/MM/YYYY"),
        },
        use_container_width=True,
        key="editor"
    )

    if not edited_df.equals(st.session_state['df_config']):
        for idx, row in edited_df.iterrows():
            if row['Trạng thái'] == "Chưa chốt": edited_df.at[idx, 'Tiến độ'] = "⏳ Chờ chạy"
            elif row['Trạng thái'] == "Đã chốt" and "Đã" not in str(row['Tiến độ']): edited_df.at[idx, 'Tiến độ'] = "✅ Đã xong"
        st.session_state['df_config'] = edited_df
        st.rerun()

    # BUTTON
    st.divider()
    col_run, col_save = st.columns([4, 1])
    
    with col_run:
        if st.button("▶️ CẬP NHẬT DỮ LIỆU (CHƯA CHỐT)", type="primary"):
            rows_to_run = edited_df[edited_df['Trạng thái'] == "Chưa chốt"].to_dict('records')
            
            if not rows_to_run:
                st.warning("⚠️ Không có dòng 'Chưa chốt'.")
            else:
                target_link = rows_to_run[0]['Link dữ liệu đích']
                if not target_link:
                    st.error("❌ Thiếu Link Đích.")
                    st.stop()

                with st.status("🚀 Đang xử lý theo ID...", expanded=True) as status:
                    st.write(f"Đang cập nhật {len(rows_to_run)} nguồn...")
                    
                    # UI Update
                    for idx, row in edited_df.iterrows():
                        if row['Trạng thái'] == "Chưa chốt": edited_df.at[idx, 'Tiến độ'] = "🔄 Processing..."
                    st.session_state['df_config'] = edited_df
                    
                    # RUN
                    success, msg = process_pipeline_smart(rows_to_run, user_id)
                    
                    if success:
                        status.update(label="Hoàn tất!", state="complete", expanded=False)
                        st.success(f"🎉 {msg}")
                        st.balloons()
                        
                        # Done
                        for idx, row in edited_df.iterrows():
                            if row['Trạng thái'] == "Chưa chốt":
                                edited_df.at[idx, 'Trạng thái'] = "Đã chốt"
                                edited_df.at[idx, 'Tiến độ'] = "✅ Đã cập nhật"
                        
                        creds = get_creds()
                        save_history_config(edited_df, creds, user_id)
                        st.session_state['df_config'] = edited_df
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(f"❌ Lỗi: {msg}")

    with col_save:
        if st.button("💾 Lưu Cấu Hình"):
            creds = get_creds()
            save_history_config(edited_df, creds, user_id)

if __name__ == "__main__":
    if check_login():
        main_ui()
