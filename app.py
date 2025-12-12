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
st.set_page_config(page_title="Tool Xử Lý Data (Copy 1:1)", layout="wide")
PASSWORD_ACCESS = "admin2024" # Mật khẩu truy cập
# Email Robot (Dùng để hiển thị cho người dùng copy share quyền)
BOT_EMAIL_DISPLAY = "getdulieu@kin-kin-477902.iam.gserviceaccount.com"

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets', 
    'https://www.googleapis.com/auth/drive'
]

# --- 2. HÀM HỖ TRỢ & BẢO MẬT ---
def check_login():
    """Kiểm tra mật khẩu đăng nhập"""
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
    if not st.session_state['logged_in']:
        st.header("🔒 Đăng nhập hệ thống")
        pwd = st.text_input("Mật khẩu:", type="password")
        if st.button("Đăng Nhập"):
            if pwd == PASSWORD_ACCESS:
                st.session_state['logged_in'] = True
                st.rerun()
            else: st.error("Sai mật khẩu!")
        return False
    return True

def get_creds():
    """Lấy Credentials và tự sửa lỗi định dạng Private Key"""
    creds_info = dict(st.secrets["gcp_service_account"])
    if "private_key" in creds_info:
        # Sửa lỗi xuống dòng thường gặp trong Streamlit Secrets
        creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
    return service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)

def extract_id(url):
    """Lấy ID Google Sheet từ Link"""
    if url and "docs.google.com" in str(url):
        try: return url.split("/d/")[1].split("/")[0]
        except: return None
    return None

# --- 3. QUẢN LÝ LỊCH SỬ (AUTO SAVE/LOAD) ---
def load_history_config(creds):
    """Đọc cấu hình cũ từ Sheet Lịch sử"""
    # Lấy ID Sheet History từ Secrets (Nếu có cấu hình)
    history_id = st.secrets["gcp_service_account"].get("history_sheet_id")
    if not history_id: return None
    
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(history_id)
        wks = sh.get_worksheet(0)
        df = get_as_dataframe(wks, evaluate_formulas=True)
        # Loại bỏ dòng trống
        df = df.dropna(how='all')
        # Chuyển cột Hành động về dạng checkbox (True/False)
        if 'Hành động' in df.columns: df['Hành động'] = df['Hành động'].astype(bool)
        return df
    except: return None

def save_history_config(df, creds):
    """Lưu cấu hình hiện tại vào Sheet Lịch sử"""
    history_id = st.secrets["gcp_service_account"].get("history_sheet_id")
    if not history_id: return
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(history_id)
        wks = sh.get_worksheet(0)
        wks.clear()
        set_with_dataframe(wks, df)
    except: pass

# --- 4. HÀM KIỂM TRA QUYỀN (AUTO CHECK) ---
def verify_access_fast(url, creds):
    """Kiểm tra nhanh xem Robot có vào được file không"""
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

# --- 5. HÀM TẢI & GHI DỮ LIỆU (CORE ENGINE) ---
def fetch_single_csv_raw(row_config, token):
    """
    Tải CSV và giữ nguyên bản 100% (Raw Text).
    Không thêm cột, không sửa kiểu dữ liệu.
    """
    sheet_id = extract_id(row_config['Link dữ liệu lấy dữ liệu'])
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
    headers = {'Authorization': f'Bearer {token}'}
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            # infer_schema_length=0 -> Đọc tất cả là TEXT để không bị mất số 0 ở đầu
            df = pl.read_csv(io.BytesIO(response.content), infer_schema_length=0)
            return df
        return None
    except: return None

def write_to_google_sheet(df, target_link, creds):
    """Ghi đè dữ liệu vào Sheet Đích"""
    try:
        gc = gspread.authorize(creds)
        target_id = extract_id(target_link)
        sh = gc.open_by_key(target_id)
        
        # Thử lấy tab Tong_Hop_Data, nếu không có lấy tab đầu tiên
        try: wks = sh.worksheet("Tong_Hop_Data")
        except: wks = sh.get_worksheet(0) 
        
        wks.clear() # Xóa sạch dữ liệu cũ
        
        # Chuyển Polars -> Pandas -> List
        pdf = df.to_pandas().fillna('') # Giữ nguyên ô trống
        data_to_write = [pdf.columns.tolist()] + pdf.values.tolist()
        
        # Batch Update (Ghi 1 lần)
        wks.update(data_to_write)
        return True, f"Đã ghi nguyên bản {len(data_to_write)} dòng vào sheet: {sh.title}"
    except Exception as e: return False, str(e)

def process_pipeline_raw(selected_rows):
    """Luồng xử lý đa luồng"""
    creds = get_creds()
    # Fix lỗi TypeError request cũ
    auth_req = google.auth.transport.requests.Request() 
    creds.refresh(auth_req)
    token = creds.token
    
    results = []
    # Tải song song 10 file
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_row = {executor.submit(fetch_single_csv_raw, row, token): row for row in selected_rows}
        for future in concurrent.futures.as_completed(future_to_row):
            data = future.result()
            if data is not None: results.append(data)
    
    if results:
        # Gộp dọc (Các file phải có tiêu đề cột giống nhau)
        df_big = pl.concat(results, how="vertical", rechunk=True)
        return df_big
    return None

# --- 6. GIAO DIỆN CHÍNH (UI) ---
def main_ui():
    st.title("⚙️ Tool Tổng Hợp Data (Chế độ Copy 1:1)")
    
    # 1. LOAD CONFIG TỪ LỊCH SỬ
    if 'df_config' not in st.session_state:
        creds = get_creds()
        with st.spinner("⏳ Đang tải cấu hình cũ..."):
            df_history = load_history_config(creds)
            
        if df_history is not None and not df_history.empty:
            # Đảm bảo đủ cột
            expected_cols = ["Hành động", "Ngày chốt", "Tháng", "Link dữ liệu lấy dữ liệu", 
                             "Link dữ liệu đích", "Tên sheet dữ liệu", "Tên nguồn (Nhãn)", "Trạng thái"]
            for col in expected_cols:
                if col not in df_history.columns:
                    df_history[col] = "" if col != "Hành động" else False
            st.session_state['df_config'] = df_history[expected_cols]
            st.toast("Đã khôi phục cấu hình cũ!", icon="✅")
        else:
            # Mặc định nếu chưa có lịch sử
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

    st.info(f"💡 Hệ thống sẽ **Copy Nguyên Bản** dữ liệu từ các Link Nguồn và Gộp vào Link Đích. Dữ liệu cũ trong Link Đích sẽ bị xóa.")

    # 2. HIỂN THỊ BẢNG NHẬP LIỆU
    edited_df = st.data_editor(
        st.session_state['df_config'],
        num_rows="dynamic",
        column_config={
            "Hành động": st.column_config.CheckboxColumn("Chọn", width="small"),
            "Link dữ liệu lấy dữ liệu": st.column_config.TextColumn("Link Lấy Dữ Liệu (Nguồn)", width="medium"),
            "Link dữ liệu đích": st.column_config.TextColumn("Link Đích (Ghi vào)", width="medium"),
            "Trạng thái": st.column_config.TextColumn("Trạng thái", disabled=True, width="medium"),
        },
        use_container_width=True,
        key="editor"
    )

    # 3. LOGIC TỰ ĐỘNG CHECK QUYỀN
    if not edited_df.equals(st.session_state['df_config']):
        try:
            creds = get_creds()
            for index, row in edited_df.iterrows():
                link_src = row['Link dữ liệu lấy dữ liệu']
                link_dst = row['Link dữ liệu đích']
                new_status_parts = []
                
                # Check Nguồn
                if link_src and "docs.google.com" in str(link_src):
                    ok, msg = verify_access_fast(link_src, creds)
                    if not ok: new_status_parts.append(f"Nguồn: {msg}")
                
                # Check Đích
                if link_dst and "docs.google.com" in str(link_dst):
                    ok, msg = verify_access_fast(link_dst, creds)
                    if not ok: new_status_parts.append(f"Đích: {msg}")
                
                # Update Status Text
                if new_status_parts: edited_df.at[index, 'Trạng thái'] = " | ".join(new_status_parts)
                elif (link_src or link_dst): edited_df.at[index, 'Trạng thái'] = "✅ Sẵn sàng"
                else: edited_df.at[index, 'Trạng thái'] = ""

            st.session_state['df_config'] = edited_df
            st.rerun() 
        except Exception as e:
            st.error(f"Lỗi: {e}")

    # 4. HIỂN THỊ CẢNH BÁO NẾU THIẾU QUYỀN
    error_rows = edited_df[edited_df['Trạng thái'].astype(str).str.contains("Thiếu quyền", na=False)]
    if not error_rows.empty:
        st.divider()
        st.error(f"⚠️ Có {len(error_rows)} dòng chưa cấp quyền!")
        c1, c2 = st.columns([3, 1])
        with c1:
            st.markdown(f"**👉 COPY Email Robot này và Share quyền Editor:**")
            st.code(BOT_EMAIL_DISPLAY, language="text")
        with c2:
            st.write("")
            st.warning("Share xong nhớ sửa nhẹ 1 ký tự trong bảng để check lại.")

    # 5. KHU VỰC NÚT BẤM
    st.divider()
    col_run, col_save = st.columns([4, 1])
    
    with col_run:
        if st.button("▶️ TỔNG HỢP & GHI DATA", type="primary"):
            selected_rows = edited_df[edited_df["Hành động"] == True].to_dict('records')
            
            # Tự động lưu lịch sử trước khi chạy
            with st.spinner("💾 Đang lưu cấu hình..."):
                creds = get_creds()
                save_history_config(edited_df, creds)
            
            # Validate
            has_error = any("Thiếu quyền" in str(row.get('Trạng thái', '')) for row in selected_rows)
            if has_error:
                st.error("❌ Vui lòng cấp quyền (Share Email) trước khi chạy!")
                st.stop()
            if not selected_rows:
                st.warning("⚠️ Chọn ít nhất 1 dòng để chạy.")
            else:
                target_link = selected_rows[0]['Link dữ liệu đích']
                if not target_link:
                    st.error("❌ Dòng đầu tiên thiếu Link Đích.")
                    st.stop()

                # CHẠY PROCESS
                with st.status("🚀 Đang xử lý...", expanded=True) as status:
                    st.write(f"1. Đang tải {len(selected_rows)} nguồn (Raw Mode)...")
                    df_result = process_pipeline_raw(selected_rows)
                    
                    if df_result is not None:
                        st.write(f"✅ Tải xong {df_result.height:,} dòng. Đang ghi vào Sheet đích...")
                        creds = get_creds()
                        success, msg = write_to_google_sheet(df_result, target_link, creds)
                        
                        if success:
                            status.update(label="Hoàn tất!", state="complete", expanded=False)
                            st.success(f"🎉 {msg}")
                            st.balloons()
                            # Nút tải backup
                            buffer = io.BytesIO()
                            df_result.write_excel(buffer)
                            st.download_button("📥 Tải Backup .xlsx", buffer.getvalue(), "Backup.xlsx")
                        else: st.error(f"❌ Lỗi ghi: {msg}")
                    else: st.error("❌ Không tải được dữ liệu nguồn.")
                    
    with col_save:
        if st.button("💾 Lưu Cấu Hình"):
            creds = get_creds()
            save_history_config(edited_df, creds)
            st.toast("Đã lưu lịch sử cấu hình!", icon="✅")

if __name__ == "__main__":
    if check_login():
        main_ui()
