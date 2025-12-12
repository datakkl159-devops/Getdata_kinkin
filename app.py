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
st.set_page_config(page_title="Tool Xử Lý Data (Log Chi Tiết)", layout="wide")

AUTHORIZED_USERS = {
    "admin2024": "Admin_Master",
    "team_hn_1": "Team_HaNoi",
    "team_hcm_1": "Team_HCM",
    "auto_bot": "Hẹn giờ tự động"
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

# --- 3. HỆ THỐNG LOGGING (CẬP NHẬT 10 CỘT) ---
def log_batch_to_sheet(creds, log_rows):
    """
    Ghi log với đúng 10 cột yêu cầu.
    """
    history_id = st.secrets["gcp_service_account"].get("history_sheet_id")
    if not history_id or not log_rows: return

    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(history_id)
        
        try:
            wks = sh.worksheet(SHEET_LOG_NAME)
        except gspread.WorksheetNotFound:
            wks = sh.add_worksheet(title=SHEET_LOG_NAME, rows=1000, cols=10)
            # HEADER CHUẨN 10 CỘT
            wks.append_row([
                "Thời gian chạy lấy dữ liệu", 
                "Ngày chốt", 
                "Tháng", 
                "Người thực hiện", 
                "Link Nguồn", 
                "Link Đích", 
                "Tên sheet dữ liệu", 
                "Tên nguồn(nhãn)", 
                "Trạng thái", 
                "Chi tiết lỗi"
            ])
            
        wks.append_rows(log_rows)
        
    except Exception as e:
        print(f"Lỗi ghi log: {e}")

# --- 4. QUẢN LÝ LỊCH SỬ ---
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
        st.error(f"❌ Lỗi lưu cấu hình: {e}")

# --- 5. CORE ENGINE (XỬ LÝ DỮ LIỆU) ---
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

def fetch_single_csv_raw_with_status(row_config, token):
    link_src = row_config['Link dữ liệu lấy dữ liệu']
    sheet_id = extract_id(link_src)
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            df = pl.read_csv(io.BytesIO(response.content), infer_schema_length=0)
            return df, "Thành công", f"Tải {df.height} dòng"
        return None, "Thất bại", f"Lỗi HTTP {response.status_code}"
    except Exception as e:
        return None, "Thất bại", str(e)

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
        return True, f"Ghi {len(data_to_write)} dòng"
    except Exception as e: return False, str(e)

def process_pipeline_and_collect_logs(selected_rows, user_id):
    creds = get_creds()
    auth_req = google.auth.transport.requests.Request() 
    creds.refresh(auth_req)
    token = creds.token
    
    results_df = []
    log_entries = []
    target_link = selected_rows[0]['Link dữ liệu đích']
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 1. TẢI DỮ LIỆU
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Map future với row config để lấy thông tin log
        future_to_row = {executor.submit(fetch_single_csv_raw_with_status, row, token): row for row in selected_rows}
        
        for future in concurrent.futures.as_completed(future_to_row):
            row = future_to_row[future] # Lấy thông tin dòng config
            
            # Lấy kết quả chạy
            try:
                df, status, msg = future.result()
            except Exception as e:
                df, status, msg = None, "Lỗi hệ thống", str(e)
            
            if df is not None:
                results_df.append(df)
            
            # --- TẠO DÒNG LOG CHUẨN 10 CỘT ---
            # 1. Thời gian, 2. Ngày chốt, 3. Tháng, 4. Người thực hiện
            # 5. Link Nguồn, 6. Link Đích, 7. Tên sheet, 8. Tên nguồn, 9. Trạng thái, 10. Chi tiết
            
            log_row = [
                timestamp,
                str(row.get('Ngày chốt', '')),
                str(row.get('Tháng', '')),
                user_id,
                row.get('Link dữ liệu lấy dữ liệu', ''),
                target_link,
                row.get('Tên sheet dữ liệu', ''),
                row.get('Tên nguồn (Nhãn)', ''),
                status,
                msg
            ]
            log_entries.append(log_row)
    
    # 2. GỘP VÀ GHI
    final_status = "Thất bại"
    final_msg = "Không có dữ liệu nguồn"
    df_big = None
    
    if results_df:
        try:
            df_big = pl.concat(results_df, how="vertical", rechunk=True)
            success, write_msg = write_to_google_sheet(df_big, target_link, creds)
            
            if success: 
                final_status = "Hoàn tất"
                final_msg = write_msg
            else:
                final_status = "Lỗi Ghi"
                final_msg = write_msg
                
            # Log dòng tổng hợp (Optional - Nếu muốn ghi nhận bước Ghi Đích)
            log_entries.append([
                timestamp, 
                "---", "---", 
                user_id, 
                "TỔNG HỢP CÁC NGUỒN", 
                target_link, 
                "Tong_Hop_Data", 
                "ALL", 
                "Thành công" if success else "Lỗi Ghi", 
                final_msg
            ])
                
        except Exception as e:
            final_status = "Lỗi Gộp"
            final_msg = str(e)
            
    # 3. GHI LOG VÀO SHEET
    log_batch_to_sheet(creds, log_entries)
    
    return df_big, final_status, final_msg

# --- 6. GIAO DIỆN CHÍNH ---
def main_ui():
    user_id = st.session_state.get('current_user_id', 'Unknown')
    st.title(f"⚙️ Tool Xử Lý Data (User: {user_id})")
    
    # LOAD
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

    st.info("💡 Chế độ: **Giữ nguyên bản (Copy 1:1)**. Tự động ghi Log chi tiết 10 cột.")

    # EDITOR
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

    # AUTO CHECK
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

    # BUTTONS
    st.divider()
    col_run, col_save = st.columns([4, 1])
    
    with col_run:
        if st.button("▶️ TỔNG HỢP & GHI DATA", type="primary"):
            selected_rows = edited_df[edited_df["Hành động"] == True].to_dict('records')
            creds = get_creds()
            
            with st.spinner("💾 Đang lưu cấu hình..."):
                save_history_config(edited_df, creds, user_id)
            
            has_error = any("Thiếu quyền" in str(row.get('Trạng thái', '')) for row in selected_rows)
            if has_error:
                st.error("❌ Cấp quyền trước khi chạy!")
                st.stop()
            if not selected_rows:
                st.warning("⚠️ Chọn ít nhất 1 dòng.")
            else:
                target_link = selected_rows[0]['Link dữ liệu đích']
                if not target_link:
                    st.error("❌ Thiếu Link Đích.")
                    st.stop()

                with st.status("🚀 Đang chạy và ghi log...", expanded=True) as status:
                    st.write(f"Đang xử lý {len(selected_rows)} nguồn...")
                    
                    df_result, final_status, final_msg = process_pipeline_and_collect_logs(selected_rows, user_id)
                    
                    if final_status == "Hoàn tất":
                        status.update(label="Xong!", state="complete", expanded=False)
                        st.success(f"🎉 {final_msg}")
                        st.balloons()
                        buffer = io.BytesIO()
                        df_result.write_excel(buffer)
                        st.download_button("📥 Tải Backup .xlsx", buffer.getvalue(), "Backup.xlsx")
                    else:
                        status.update(label="Lỗi!", state="error", expanded=False)
                        st.error(f"❌ {final_msg}")
                    
    with col_save:
        if st.button("💾 Lưu Cấu Hình"):
            creds = get_creds()
            save_history_config(edited_df, creds, user_id)

if __name__ == "__main__":
    if check_login():
        main_ui()
