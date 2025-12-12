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
st.set_page_config(page_title="Tool Xử Lý Data (Log System)", layout="wide")

# --- DANH SÁCH TÀI KHOẢN ---
AUTHORIZED_USERS = {
    "admin2024": "Admin_Master",
    "team_hn_1": "Team_HaNoi",
    "team_hcm_1": "Team_HCM",
    "auto_bot_key": "hẹn giờ tự động" # Key dự phòng cho automation sau này
}

BOT_EMAIL_DISPLAY = "getdulieu@kin-kin-477902.iam.gserviceaccount.com"
SHEET_CONFIG_NAME = "luu_cau_hinh" 
SHEET_LOG_NAME = "log_lanthucthi" # Tên tab lưu log

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- 2. HÀM HỖ TRỢ & BẢO MẬT ---
def check_login():
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
        st.session_state['current_user_id'] = None
        
    # Hỗ trợ login qua URL params (cho automation sau này nếu cần)
    query_params = st.query_params
    if "auto_key" in query_params:
        key = query_params["auto_key"]
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
                user_id = AUTHORIZED_USERS[pwd]
                st.session_state['current_user_id'] = user_id
                st.toast(f"Xin chào: {user_id}", icon="👋")
                time.sleep(1)
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

# --- 3. HỆ THỐNG LOGGING (MỚI) ---
def log_execution(creds, user_id, action_type, status, details=""):
    """
    Ghi lại lịch sử chạy vào sheet 'log_lanthucthi'
    """
    history_id = st.secrets["gcp_service_account"].get("history_sheet_id")
    if not history_id: return

    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(history_id)
        
        # Tìm hoặc tạo tab log
        try:
            wks = sh.worksheet(SHEET_LOG_NAME)
        except gspread.WorksheetNotFound:
            wks = sh.add_worksheet(title=SHEET_LOG_NAME, rows=1000, cols=10)
            # Tạo header nếu mới tạo
            wks.append_row(["Thời gian", "Người thực hiện", "Hành động", "Trạng thái", "Chi tiết"])
            
        # Chuẩn bị dữ liệu log
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_row = [timestamp, user_id, action_type, status, details]
        
        # Ghi nối tiếp vào cuối sheet (append)
        wks.append_row(log_row)
        
    except Exception as e:
        print(f"Lỗi ghi log: {e}") 
        # Không show lỗi log ra UI để tránh làm phiền user, chỉ in ra console

# --- 4. QUẢN LÝ LỊCH SỬ CẤU HÌNH ---
def load_history_config(creds, current_user_id):
    history_id = st.secrets["gcp_service_account"].get("history_sheet_id")
    if not history_id: return None
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(history_id)
        try: wks = sh.worksheet(SHEET_CONFIG_NAME)
        except: return None
        
        df_all = get_as_dataframe(wks, evaluate_formulas=True)
        if df_all.empty or 'User_ID' not in df_all.columns: return None
            
        df_user = df_all[df_all['User_ID'] == current_user_id].copy()
        
        if 'Hành động' in df_user.columns:
            df_user['Hành động'] = df_user['Hành động'].astype(str).str.upper() == 'TRUE'
        if 'Ngày chốt' in df_user.columns:
            df_user['Ngày chốt'] = pd.to_datetime(df_user['Ngày chốt'], errors='coerce').dt.date
            
        if 'User_ID' in df_user.columns:
            df_user = df_user.drop(columns=['User_ID'])
            
        return df_user
    except: return None

def save_history_config(df_current_ui, creds, current_user_id):
    history_id = st.secrets["gcp_service_account"].get("history_sheet_id")
    if not history_id: return
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(history_id)
        try:
            wks = sh.worksheet(SHEET_CONFIG_NAME)
            df_all_existing = get_as_dataframe(wks, evaluate_formulas=True)
        except gspread.WorksheetNotFound:
            wks = sh.add_worksheet(title=SHEET_CONFIG_NAME, rows=100, cols=20)
            df_all_existing = pd.DataFrame()
            
        df_new = df_current_ui.copy()
        df_new['User_ID'] = current_user_id 
        
        if 'Hành động' in df_new.columns:
            df_new['Hành động'] = df_new['Hành động'].apply(lambda x: "TRUE" if x else "FALSE")
        if 'Ngày chốt' in df_new.columns:
            df_new['Ngày chốt'] = df_new['Ngày chốt'].astype(str)
            
        final_df = df_new
        if not df_all_existing.empty and 'User_ID' in df_all_existing.columns:
            df_others = df_all_existing[df_all_existing['User_ID'] != current_user_id].copy()
            if 'Ngày chốt' in df_others.columns: df_others['Ngày chốt'] = df_others['Ngày chốt'].astype(str)
            if 'Hành động' in df_others.columns: df_others['Hành động'] = df_others['Hành động'].astype(str).str.upper()
            final_df = pd.concat([df_others, df_new], ignore_index=True)
            
        wks.clear()
        final_df = final_df.fillna('')
        data_to_write = [final_df.columns.tolist()] + final_df.values.tolist()
        wks.update(data_to_write)
        st.toast(f"✅ Đã lưu cấu hình: {current_user_id}", icon="💾")
    except Exception as e:
        st.error(f"❌ LỖI LƯU: {e}")

# --- 5. CORE ENGINE (COPY 1:1) ---
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

def fetch_single_csv_raw(row_config, token):
    sheet_id = extract_id(row_config['Link dữ liệu lấy dữ liệu'])
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            df = pl.read_csv(io.BytesIO(response.content), infer_schema_length=0)
            return df
        return None
    except: return None

def write_to_google_sheet(df, target_link, creds):
    try:
        gc = gspread.authorize(creds)
        target_id = extract_id(target_link)
        sh = gc.open_by_key(target_id)
        try: wks = sh.worksheet("Tong_Hop_Data")
        except: wks = sh.get_worksheet(0) 
        
        wks.clear()
        pdf = df.to_pandas().fillna('')
        data_to_write = [pdf.columns.tolist()] + pdf.values.tolist()
        wks.update(data_to_write)
        return True, f"Đã ghi {len(data_to_write)} dòng vào: {sh.title}"
    except Exception as e: return False, str(e)

def process_pipeline_raw(selected_rows):
    creds = get_creds()
    auth_req = google.auth.transport.requests.Request() 
    creds.refresh(auth_req)
    token = creds.token
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_row = {executor.submit(fetch_single_csv_raw, row, token): row for row in selected_rows}
        for future in concurrent.futures.as_completed(future_to_row):
            data = future.result()
            if data is not None: results.append(data)
    
    if results:
        df_big = pl.concat(results, how="vertical", rechunk=True)
        return df_big
    return None

# --- 6. GIAO DIỆN CHÍNH ---
def main_ui():
    user_id = st.session_state.get('current_user_id', 'Unknown')
    st.title(f"⚙️ Tool Xử Lý Data (User: {user_id})")
    
    # 1. LOAD
    if 'df_config' not in st.session_state:
        creds = get_creds()
        with st.spinner(f"⏳ Đang tải cấu hình..."):
            df_history = load_history_config(creds, user_id)
        
        if df_history is not None and not df_history.empty:
            expected_cols = ["Hành động", "Ngày chốt", "Tháng", "Link dữ liệu lấy dữ liệu", 
                             "Link dữ liệu đích", "Tên sheet dữ liệu", "Tên nguồn (Nhãn)", "Trạng thái"]
            for col in expected_cols:
                if col not in df_history.columns:
                    df_history[col] = "" if col != "Hành động" else False
            st.session_state['df_config'] = df_history[expected_cols]
        else:
            data = {
                "Hành động": [False, False], 
                "Ngày chốt": [datetime.now().date(), datetime.now().date()],
                "Tháng": ["12/2025", "12/2025"],
                "Link dữ liệu lấy dữ liệu": ["", ""],
                "Link dữ liệu đích": ["", ""],
                "Tên sheet dữ liệu": ["Sheet1", "Sheet1"],
                "Tên nguồn (Nhãn)": ["KV_HaNoi", "KV_HCM"],
                "Trạng thái": ["", ""]
            }
            st.session_state['df_config'] = pd.DataFrame(data)

    st.info("💡 Chế độ: **Giữ nguyên bản (Copy 1:1)**. Dữ liệu sẽ được LOG lại khi chạy.")

    # 2. EDITOR
    edited_df = st.data_editor(
        st.session_state['df_config'],
        num_rows="dynamic",
        column_config={
            "Hành động": st.column_config.CheckboxColumn("Chọn", width="small"),
            "Link dữ liệu lấy dữ liệu": st.column_config.TextColumn("Link Nguồn", width="medium"),
            "Link dữ liệu đích": st.column_config.TextColumn("Link Đích", width="medium"),
            "Trạng thái": st.column_config.TextColumn("Trạng thái", disabled=True, width="medium"),
            "Ngày chốt": st.column_config.DateColumn("Ngày chốt", format="DD/MM/YYYY"),
        },
        use_container_width=True,
        key="editor"
    )

    # 3. AUTO CHECK
    if not edited_df.equals(st.session_state['df_config']):
        try:
            creds = get_creds()
            for index, row in edited_df.iterrows():
                link_src = row['Link dữ liệu lấy dữ liệu']
                link_dst = row['Link dữ liệu đích']
                new_status_parts = []
                if link_src and "docs.google.com" in str(link_src):
                    ok, msg = verify_access_fast(link_src, creds)
                    if not ok: new_status_parts.append(f"Nguồn: {msg}")
                if link_dst and "docs.google.com" in str(link_dst):
                    ok, msg = verify_access_fast(link_dst, creds)
                    if not ok: new_status_parts.append(f"Đích: {msg}")
                
                if new_status_parts: edited_df.at[index, 'Trạng thái'] = " | ".join(new_status_parts)
                elif (link_src or link_dst): edited_df.at[index, 'Trạng thái'] = "✅ Sẵn sàng"
                else: edited_df.at[index, 'Trạng thái'] = ""

            st.session_state['df_config'] = edited_df
            st.rerun() 
        except: pass

    # WARNING
    error_rows = edited_df[edited_df['Trạng thái'].astype(str).str.contains("Thiếu quyền", na=False)]
    if not error_rows.empty:
        st.divider()
        st.error(f"⚠️ Có {len(error_rows)} dòng chưa cấp quyền!")
        c1, c2 = st.columns([3, 1])
        with c1:
            st.markdown(f"**👉 COPY Email Robot:**")
            st.code(BOT_EMAIL_DISPLAY, language="text")
        with c2:
            st.warning("Share quyền Editor xong nhớ sửa nhẹ bảng.")

    # 4. BUTTONS
    st.divider()
    col_run, col_save = st.columns([4, 1])
    
    with col_run:
        if st.button("▶️ TỔNG HỢP & GHI DATA", type="primary"):
            selected_rows = edited_df[edited_df["Hành động"] == True].to_dict('records')
            creds = get_creds()
            
            # --- START LOGGING START ---
            log_execution(creds, user_id, "Bắt đầu chạy", "Đang xử lý", f"Số nguồn: {len(selected_rows)}")
            
            # Save Config
            with st.spinner("💾 Đang lưu cấu hình..."):
                save_history_config(edited_df, creds, user_id)
            
            # Validate
            has_error = any("Thiếu quyền" in str(row.get('Trạng thái', '')) for row in selected_rows)
            if has_error:
                st.error("❌ Cấp quyền trước khi chạy!")
                log_execution(creds, user_id, "Chạy", "Lỗi", "Thiếu quyền truy cập")
                st.stop()
            if not selected_rows:
                st.warning("⚠️ Chọn ít nhất 1 dòng.")
            else:
                target_link = selected_rows[0]['Link dữ liệu đích']
                if not target_link:
                    st.error("❌ Thiếu Link Đích.")
                    st.stop()

                with st.status("🚀 Đang chạy...", expanded=True) as status:
                    st.write(f"1. Đang tải {len(selected_rows)} nguồn...")
                    df_result = process_pipeline_raw(selected_rows)
                    
                    if df_result is not None:
                        st.write(f"✅ Tải xong {df_result.height:,} dòng. Đang ghi đè...")
                        success, msg = write_to_google_sheet(df_result, target_link, creds)
                        
                        if success:
                            status.update(label="Xong!", state="complete", expanded=False)
                            st.success(f"🎉 {msg}")
                            st.balloons()
                            
                            # --- LOG SUCCESS ---
                            log_execution(creds, user_id, "Ghi Data", "Thành công", f"Ghi {df_result.height} dòng vào {extract_id(target_link)}")
                            
                            buffer = io.BytesIO()
                            df_result.write_excel(buffer)
                            st.download_button("📥 Tải Backup .xlsx", buffer.getvalue(), "Backup.xlsx")
                        else: 
                            st.error(f"❌ Lỗi ghi: {msg}")
                            # --- LOG FAIL WRITE ---
                            log_execution(creds, user_id, "Ghi Data", "Thất bại", msg)
                    else: 
                        st.error("❌ Lỗi tải nguồn.")
                        # --- LOG FAIL READ ---
                        log_execution(creds, user_id, "Đọc Data", "Thất bại", "Không tải được file nguồn")
                    
    with col_save:
        if st.button("💾 Lưu Cấu Hình"):
            creds = get_creds()
            save_history_config(edited_df, creds, user_id)

if __name__ == "__main__":
    if check_login():
        main_ui()
