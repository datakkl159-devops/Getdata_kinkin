import streamlit as st
import pandas as pd
import polars as pl
import requests
import io
import concurrent.futures
import time
import gspread
from datetime import datetime
from google.oauth2 import service_account

# --- 1. CẤU HÌNH HỆ THỐNG ---
st.set_page_config(page_title="Tool Xử Lý Data", layout="wide")
PASSWORD_ACCESS = "admin2024" 
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets', 
    'https://www.googleapis.com/auth/drive'
]

# --- 2. HÀM HỖ TRỢ & BẢO MẬT ---
def check_login():
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
    if not st.session_state['logged_in']:
        st.header("🔒 Đăng nhập hệ thống")
        pwd = st.text_input("Mật khẩu:", type="password")
        if st.button("Đăng Nhập"):
            if pwd == PASSWORD_ACCESS:
                st.session_state['logged_in'] = True
                st.rerun()
            else:
                st.error("Sai mật khẩu!")
        return False
    return True

def get_creds():
    return service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=SCOPES
    )

def extract_id(url):
    """Lấy ID từ link Google Sheet"""
    if url and "docs.google.com" in str(url):
        try:
            return url.split("/d/")[1].split("/")[0]
        except:
            return None
    return url

# --- 3. HÀM TẢI DỮ LIỆU TỪ "LINK LẤY DỮ LIỆU" ---
def fetch_single_csv(row_config, token):
    # Lấy ID từ cột "Link dữ liệu lấy dữ liệu" như yêu cầu
    sheet_id = extract_id(row_config['Link dữ liệu lấy dữ liệu'])
    
    # Lấy thông tin metadata để gán nhãn
    target_label = row_config['Tên nguồn (Nhãn)'] 
    date_close = str(row_config['Ngày chốt'])
    month_close = str(row_config['Tháng'])
    
    # Mặc định lấy gid=0 (Tab đầu tiên)
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
    headers = {'Authorization': f'Bearer {token}'}
    
    try:
        response = requests.get(url, headers=headers, timeout=20)
        if response.status_code == 200:
            # Polars đọc bytes (nhanh gấp 10 lần Pandas)
            df = pl.read_csv(io.BytesIO(response.content), infer_schema_length=0)
            
            # Thêm các cột định danh nguồn gốc
            df = df.with_columns([
                pl.lit(target_label).alias("Nguồn_Dữ_Liệu"),
                pl.lit(date_close).alias("Ngày_Chốt"),
                pl.lit(month_close).alias("Tháng_Data")
            ])
            return df
        return None
    except:
        return None

# --- 4. HÀM GHI VÀO "LINK DỮ LIỆU ĐÍCH" ---
def write_to_google_sheet(df, target_link, creds):
    """
    Ghi dữ liệu vào Sheet Đích.
    Sẽ xóa sạch dữ liệu cũ trong tab 'Tong_Hop_Data' và ghi mới.
    """
    try:
        gc = gspread.authorize(creds)
        target_id = extract_id(target_link)
        sh = gc.open_by_key(target_id)
        
        # Thử lấy tab có tên 'Tong_Hop_Data', nếu không có thì lấy tab đầu tiên
        try:
            wks = sh.worksheet("Tong_Hop_Data")
        except:
            wks = sh.get_worksheet(0) 
            
        # Xóa dữ liệu cũ
        wks.clear()
        
        # Chuyển đổi Polars -> Pandas -> List
        pdf = df.to_pandas()
        pdf = pdf.fillna('') # Xử lý ô trống
        
        # Chuẩn bị dữ liệu (Header + Rows)
        data_to_write = [pdf.columns.tolist()] + pdf.values.tolist()
        
        # Ghi vào Sheet (Batch Update)
        wks.update(data_to_write)
        return True, f"Đã ghi thành công {len(data_to_write)} dòng vào sheet: {sh.title}"
        
    except Exception as e:
        return False, f"Lỗi ghi Sheet: {str(e)}"

# --- 5. LUỒNG XỬ LÝ CHÍNH ---
def process_pipeline(selected_rows):
    creds = get_creds()
    auth_req = requests.Request()
    creds.refresh(auth_req)
    token = creds.token
    
    # 1. TẢI DỮ LIỆU (Song Song)
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_row = {executor.submit(fetch_single_csv, row, token): row for row in selected_rows}
        for future in concurrent.futures.as_completed(future_to_row):
            data = future.result()
            if data is not None:
                results.append(data)
    
    if results:
        # 2. GỘP DỮ LIỆU
        df_big = pl.concat(results, how="diagonal", rechunk=True)
        
        # 3. LÀM SẠCH (Logic chuẩn hóa)
        if "Thành tiền" in df_big.columns:
            df_big = df_big.with_columns(
                pl.col("Thành tiền").str.replace_all(",", "").cast(pl.Int64, strict=False)
            )
            
        # Sắp xếp cột ưu tiên đưa lên đầu
        priority_cols = ["Ngày_Chốt", "Tháng_Data", "Nguồn_Dữ_Liệu", "Mã đơn hàng", "Thành tiền"]
        other_cols = [c for c in df_big.columns if c not in priority_cols]
        final_cols = [c for c in priority_cols if c in df_big.columns] + other_cols
        
        df_final = df_big.select(final_cols)

        return df_final
    return None

# --- 6. GIAO DIỆN CHÍNH (UI) ---
def main_ui():
    st.title("⚙️ Hệ Thống Xử Lý & Đẩy Data Tập Trung")
    
    # --- KHỞI TẠO BẢNG CONFIG ---
    if 'df_config' not in st.session_state:
        # Cập nhật tên cột đúng như yêu cầu
        data = {
            "Hành động": [False, False], 
            "Ngày chốt": [datetime.now().date(), datetime.now().date()],
            "Tháng": ["12/2025", "12/2025"],
            "Link dữ liệu lấy dữ liệu": ["https://docs.google.com/spreadsheets/d/...", ""], # Cột Nguồn
            "Link dữ liệu đích": ["https://docs.google.com/spreadsheets/d/...", ""],         # Cột Đích
            "Tên sheet dữ liệu": ["Sheet1", "Sheet1"],
            "Tên nguồn (Nhãn)": ["KV_HaNoi", "KV_HCM"],
            "Trạng thái": ["-", "-"]
        }
        st.session_state['df_config'] = pd.DataFrame(data)

    # --- HIỂN THỊ BẢNG (DATA EDITOR) ---
    st.info("💡 Tích chọn các file cần gộp. Dữ liệu sau xử lý sẽ được ghi vào **Link dữ liệu đích** (lấy ở dòng đầu tiên được chọn).")
    
    edited_df = st.data_editor(
        st.session_state['df_config'],
        num_rows="dynamic",
        column_config={
            "Hành động": st.column_config.CheckboxColumn("Chọn chạy", default=False),
            "Ngày chốt": st.column_config.DateColumn("Ngày chốt", format="DD/MM/YYYY"),
            "Tháng": st.column_config.TextColumn("Tháng"),
            "Link dữ liệu lấy dữ liệu": st.column_config.LinkColumn("Link lấy dữ liệu (Nguồn)"),
            "Link dữ liệu đích": st.column_config.LinkColumn("Link dữ liệu đích (Ghi vào)"),
            "Tên sheet dữ liệu": st.column_config.TextColumn("Tên sheet nguồn"),
            "Tên nguồn (Nhãn)": st.column_config.TextColumn("Nhãn Nguồn (VD: CN Hà Nội)"),
            "Trạng thái": st.column_config.TextColumn("Trạng thái", disabled=True),
        },
        use_container_width=True,
        key="editor"
    )

    # --- NÚT BẤM ---
    st.write("---")
    col1, col2 = st.columns([1, 4])
    with col1:
        btn_run = st.button("▶️ TỔNG HỢP & GHI DATA", type="primary", use_container_width=True)

    if btn_run:
        selected_rows = edited_df[edited_df["Hành động"] == True].to_dict('records')
        
        if not selected_rows:
            st.warning("⚠️ Vui lòng chọn ít nhất 1 dòng để chạy.")
        else:
            # Lấy Link đích từ dòng đầu tiên được chọn
            target_link = selected_rows[0]['Link dữ liệu đích']
            
            # Kiểm tra link đích có hợp lệ không
            if not target_link or "docs.google.com" not in str(target_link):
                st.error("❌ Link dữ liệu đích (dòng đầu tiên) không hợp lệ!")
                st.stop()

            with st.status("🚀 Đang thực thi...", expanded=True) as status:
                st.write(f"1. Đang tải dữ liệu từ {len(selected_rows)} nguồn...")
                start_time = time.time()
                
                # Bước 1: Tổng hợp
                df_result = process_pipeline(selected_rows)
                
                if df_result is not None:
                    st.write(f"✅ Tổng hợp xong: **{df_result.height:,} dòng**. (Mất {time.time()-start_time:.2f}s)")
                    
                    # Bước 2: Ghi vào Sheet đích
                    st.write(f"2. Đang ghi dữ liệu vào Sheet đích...")
                    st.caption(f"Target: {target_link}")
                    
                    creds = get_creds()
                    success, msg = write_to_google_sheet(df_result, target_link, creds)
                    
                    if success:
                        status.update(label="Hoàn tất!", state="complete", expanded=False)
                        st.success(f"🎉 {msg}")
                        st.balloons()
                        
                        # Backup file
                        buffer = io.BytesIO()
                        df_result.write_excel(buffer)
                        st.download_button("📥 Tải File Backup (.xlsx)", buffer.getvalue(), "Backup_Data.xlsx")
                    else:
                        st.error(f"❌ Lỗi khi ghi vào Sheet: {msg}")
                else:
                    st.error("❌ Không tải được dữ liệu nguồn. Kiểm tra lại Link hoặc Quyền truy cập.")

if __name__ == "__main__":
    if check_login():
        main_ui()
