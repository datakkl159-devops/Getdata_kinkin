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

# --- 1. CẤU HÌNH HỆ THỐNG ---
st.set_page_config(page_title="Tool Quản Lý Data", layout="wide")

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

# --- 3. LOGGING ---
def log_batch_to_sheet(creds, log_rows):
    history_id = st.secrets["gcp_service_account"].get("history_sheet_id")
    if not history_id or not log_rows: return
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(history_id)
        try: wks = sh.worksheet(SHEET_LOG_NAME)
        except: 
            wks = sh.add_worksheet(title=SHEET_LOG_NAME, rows=1000, cols=10)
            wks.append_row(["Thời gian", "Ngày chốt", "Tháng", "Người thực hiện", "Link Nguồn", "Link Đích", "Tên sheet", "Tên nguồn", "Trạng thái", "Chi tiết"])
        wks.append_rows(log_rows)
    except: pass

# --- 4. LOAD & SAVE HISTORY ---
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
        
        # Fix Type
        if 'Ngày chốt' in df_user.columns:
            df_user['Ngày chốt'] = pd.to_datetime(df_user['Ngày chốt'], errors='coerce').dt.date
        if 'Trạng thái' in df_user.columns:
            df_user['Trạng thái'] = df_user['Trạng thái'].apply(lambda x: "Chưa chốt" if pd.isna(x) or str(x).strip() == "" else x)
        if 'Hành động' in df_user.columns:
            df_user['Hành động'] = df_user['Hành động'].fillna("")
            
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
        
        for idx, row in df_new.iterrows():
            if row['Trạng thái'] == "Đã chốt":
                df_new.at[idx, 'Hành động'] = "Đã cập nhật"
            else:
                df_new.at[idx, 'Hành động'] = "Xóa & Cập nhật"

        if 'Ngày chốt' in df_new.columns:
            df_new['Ngày chốt'] = df_new['Ngày chốt'].astype(str).replace({'NaT': '', 'nan': '', 'None': ''})

        final_df = df_new
        if not df_all.empty and 'User_ID' in df_all.columns:
            df_others = df_all[df_all['User_ID'] != current_user_id]
            final_df = pd.concat([df_others, df_new], ignore_index=True)
            
        wks.clear()
        final_df = final_df.fillna('')
        wks.update([final_df.columns.tolist()] + final_df.values.tolist())
        st.toast(f"✅ Đã lưu cấu hình!", icon="💾")
    except Exception as e: st.error(f"Lỗi lưu: {e}")

# --- 5. CORE ENGINE & CHECK QUYỀN ---
def verify_access_fast(url, creds):
    """Check quyền truy cập, trả về True/False và Message"""
    if not url or "docs.google.com" not in str(url): return True, "" # Bỏ qua nếu chưa nhập link
    
    sheet_id = extract_id(url)
    if not sheet_id: return False, "Link sai định dạng"
    try:
        gc = gspread.authorize(creds)
        gc.open_by_key(sheet_id)
        return True, "OK"
    except gspread.exceptions.APIError as e:
        if "403" in str(e): return False, "⛔ Chưa cấp quyền (403)"
        return False, f"❌ Lỗi khác: {e}"
    except Exception as e: return False, f"❌ Lỗi mạng: {e}"

def fetch_single_csv_with_id(row_config, token):
    link_src = row_config.get('Link dữ liệu lấy dữ liệu', '')
    display_label = row_config.get('Tên nguồn (Nhãn)', '')
    sheet_id = extract_id(link_src)
    if not sheet_id: return None, sheet_id, "Link lỗi"

    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            df = pl.read_csv(io.BytesIO(response.content), infer_schema_length=0)
            df = df.with_columns([
                pl.lit(sheet_id).alias("System_Source_ID"), 
                pl.lit(display_label).alias("Tên_Nguồn")
            ])
            return df, sheet_id, "Thành công"
        return None, sheet_id, "Lỗi HTTP"
    except Exception as e: return None, sheet_id, str(e)

def smart_update_by_id(df_new_updates, target_link, creds, ids_to_remove):
    try:
        gc = gspread.authorize(creds)
        target_id = extract_id(target_link)
        sh = gc.open_by_key(target_id)
        try: wks = sh.worksheet("Tong_Hop_Data")
        except: wks = sh.get_worksheet(0)
        
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

        if not df_current.is_empty():
            if "System_Source_ID" in df_current.columns:
                df_keep = df_current.filter(~pl.col("System_Source_ID").is_in(ids_to_remove))
            else:
                df_keep = df_current 
        else:
            df_keep = pl.DataFrame()

        if not df_new_updates.is_empty():
            df_final = pl.concat([df_keep, df_new_updates], how="diagonal")
        else:
            df_final = df_keep

        wks.clear()
        pdf = df_final.to_pandas().fillna('')
        wks.update([pdf.columns.tolist()] + pdf.values.tolist())
        return True, f"Cập nhật thành công {len(ids_to_remove)} nguồn."

    except Exception as e: return False, str(e)

def process_pipeline_smart(rows_to_process, user_id):
    creds = get_creds()
    auth_req = google.auth.transport.requests.Request() 
    creds.refresh(auth_req)
    token = creds.token
    
    results_df = []
    ids_processing = []
    log_entries = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    target_link = rows_to_process[0]['Link dữ liệu đích']
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_row = {executor.submit(fetch_single_csv_with_id, row, token): row for row in rows_to_process}
        for future in concurrent.futures.as_completed(future_to_row):
            row = future_to_row[future]
            label = row.get('Tên nguồn (Nhãn)', 'Unknown')
            df, sheet_id, status = future.result()
            
            d_log = row.get('Ngày chốt', '')
            log_date = d_log.strftime("%d/%m/%Y") if isinstance(d_log, (datetime, pd.Timestamp)) else str(d_log)

            log_row = [
                timestamp, log_date, str(row.get('Tháng', '')),
                user_id, row.get('Link dữ liệu lấy dữ liệu', ''), target_link,
                row.get('Tên sheet dữ liệu', ''), label, status, ""
            ]
            
            if df is not None and sheet_id:
                results_df.append(df)
                ids_processing.append(sheet_id)
                log_row[-1] = f"Tải {df.height} dòng"
            else:
                log_row[-2] = "Thất bại"
                log_row[-1] = "Lỗi tải"
            log_entries.append(log_row)

    success = False
    final_msg = ""
    if results_df:
        df_new = pl.concat(results_df, how="vertical", rechunk=True)
        success, msg = smart_update_by_id(df_new, target_link, creds, ids_processing)
        final_msg = msg
    else:
        final_msg = "Không tải được dữ liệu nào"

    log_entries.append([timestamp, "---", "---", user_id, "TỔNG HỢP", target_link, "Tong_Hop_Data", "ALL", "Hoàn tất" if success else "Thất bại", final_msg])
    log_batch_to_sheet(creds, log_entries)
    return success, final_msg

# --- 6. GIAO DIỆN CHÍNH ---
def main_ui():
    user_id = st.session_state.get('current_user_id', 'Unknown')
    st.title(f"⚙️ Tool Quản Lý Data (User: {user_id})")
    
    # 1. LOAD CONFIG
    if 'df_config' not in st.session_state:
        creds = get_creds()
        with st.spinner("⏳ Tải cấu hình..."):
            df = load_history_config(creds, user_id)
        
        col_order = ["Ngày chốt", "Tháng", "Link dữ liệu lấy dữ liệu", "Link dữ liệu đích", "Tên sheet dữ liệu", "Tên nguồn (Nhãn)", "Trạng thái", "Hành động"]
        
        # Init Error State
        st.session_state['scan_errors'] = []

        if df is not None and not df.empty:
            for col in col_order:
                if col not in df.columns: 
                    df[col] = "Chưa chốt" if col == "Trạng thái" else ""
            st.session_state['df_config'] = df[col_order]
        else:
            data = {c: [] for c in col_order}
            data["Ngày chốt"] = [datetime.now().date()]
            data["Trạng thái"] = ["Chưa chốt"]
            data["Hành động"] = ["Xóa & Cập nhật"]
            st.session_state['df_config'] = pd.DataFrame(data)

    st.info("💡 **Logic:** Tự động quét lỗi link (403) ngay khi bạn nhập.")

    # 2. KHU VỰC HIỂN THỊ LỖI (QUAN TRỌNG: ĐẶT TRÊN CÙNG)
    if 'scan_errors' in st.session_state and st.session_state['scan_errors']:
        st.error(f"⚠️ Phát hiện {len(st.session_state['scan_errors'])} link chưa được cấp quyền!")
        for err in st.session_state['scan_errors']:
            st.write(f"- {err}")
        
        c1, c2 = st.columns([3,1])
        with c1:
            st.markdown(f"**👉 COPY Email Robot này và Share quyền Editor:**")
            st.code(BOT_EMAIL_DISPLAY, language="text")
        st.divider()

    # 3. EDITOR
    edited_df = st.data_editor(
        st.session_state['df_config'],
        num_rows="dynamic",
        column_config={
            "Ngày chốt": st.column_config.DateColumn("Ngày chốt", format="DD/MM/YYYY"),
            "Trạng thái": st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt", "Đã chốt"], required=True, width="small"),
            "Hành động": st.column_config.TextColumn("Hành động", disabled=True),
            "Link dữ liệu lấy dữ liệu": st.column_config.TextColumn("Link Nguồn", width="medium"),
            "Link dữ liệu đích": st.column_config.TextColumn("Link Đích", width="medium"),
        },
        use_container_width=True,
        key="editor"
    )

    # 4. LOGIC QUÉT LỖI TỰ ĐỘNG (AUTO SCAN ON CHANGE)
    if not edited_df.equals(st.session_state['df_config']):
        
        # Cập nhật UI Hành động
        for idx, row in edited_df.iterrows():
            if row['Trạng thái'] == "Chưa chốt": edited_df.at[idx, 'Hành động'] = "Xóa & Cập nhật"
            elif row['Trạng thái'] == "Đã chốt": edited_df.at[idx, 'Hành động'] = "Đã cập nhật"
        
        # --- BẮT ĐẦU QUÉT LỖI ---
        creds = get_creds()
        scan_errors = []
        
        for idx, row in edited_df.iterrows():
            link_src = row.get('Link dữ liệu lấy dữ liệu', '')
            link_dst = row.get('Link dữ liệu đích', '')
            
            # Check Nguồn
            if link_src and "docs.google.com" in str(link_src):
                ok, msg = verify_access_fast(link_src, creds)
                if not ok: scan_errors.append(f"Dòng {idx+1} (Nguồn): {msg}")
            
            # Check Đích
            if link_dst and "docs.google.com" in str(link_dst):
                ok, msg = verify_access_fast(link_dst, creds)
                if not ok: scan_errors.append(f"Dòng {idx+1} (Đích): {msg}")

        # Lưu lỗi và cập nhật state -> Rerun để hiện lỗi lên trên cùng
        st.session_state['scan_errors'] = scan_errors
        st.session_state['df_config'] = edited_df
        st.rerun()

    # 5. BUTTONS
    st.divider()
    col_run, col_save = st.columns([4, 1])
    
    with col_run:
        if st.button("▶️ CẬP NHẬT DỮ LIỆU (CHƯA CHỐT)", type="primary"):
            # Chặn nếu còn lỗi
            if st.session_state.get('scan_errors'):
                st.error("❌ Không thể chạy vì còn Link chưa cấp quyền. Vui lòng xử lý lỗi bên trên!")
            else:
                rows_to_run = edited_df[edited_df['Trạng thái'] == "Chưa chốt"].to_dict('records')
                
                if not rows_to_run:
                    st.warning("⚠️ Không có dòng 'Chưa chốt' nào.")
                else:
                    target_link = rows_to_run[0]['Link dữ liệu đích']
                    if not target_link:
                        st.error("❌ Thiếu Link Đích.")
                        st.stop()

                    with st.status("🚀 Đang xử lý...", expanded=True) as status:
                        st.write(f"Cập nhật {len(rows_to_run)} nguồn...")
                        
                        # Update UI -> Running
                        for idx, row in edited_df.iterrows():
                            if row['Trạng thái'] == "Chưa chốt": edited_df.at[idx, 'Hành động'] = "🔄 Đang chạy..."
                        st.session_state['df_config'] = edited_df
                        
                        success, msg = process_pipeline_smart(rows_to_run, user_id)
                        
                        if success:
                            status.update(label="Hoàn tất!", state="complete", expanded=False)
                            st.success(f"🎉 {msg}")
                            st.balloons()
                            
                            # Done
                            for idx, row in edited_df.iterrows():
                                if row['Trạng thái'] == "Chưa chốt":
                                    edited_df.at[idx, 'Trạng thái'] = "Đã chốt"
                                    edited_df.at[idx, 'Hành động'] = "Đã cập nhật"
                            
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
