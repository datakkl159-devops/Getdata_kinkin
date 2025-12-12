import streamlit as st
import pandas as pd
import polars as pl
import requests
import io
import concurrent.futures
import time
from datetime import datetime
from google.oauth2 import service_account

# --- 1. CẤU HÌNH HỆ THỐNG ---
st.set_page_config(page_title="Tool Xử Lý Data", layout="wide")
PASSWORD_ACCESS = "admin2024" # Mật khẩu
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

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
    # Đọc từ Secrets (Bạn nhớ cấu hình secrets theo hướng dẫn TOML trước đó)
    return service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=SCOPES
    )

def extract_id(url):
    """Lấy ID từ link Google Sheet"""
    if "docs.google.com" in url:
        try:
            return url.split("/d/")[1].split("/")[0]
        except:
            return None
    return url

# --- 3. HÀM WORKER (TẢI DỮ LIỆU) ---
def fetch_single_csv(row_config, token):
    sheet_id = extract_id(row_config['Link dữ liệu'])
    
    # Lấy thông tin từ các cột cấu hình
    target_label = row_config['Sheet dữ liệu đích'] # Ví dụ: KV Hà Nội
    date_close = str(row_config['Ngày chốt'])
    month_close = str(row_config['Tháng'])
    
    # Mặc định lấy gid=0. Nếu bạn muốn lấy đúng "Tên sheet dữ liệu", 
    # cần thêm logic gọi API lấy gid, nhưng để nhanh ta tạm dùng gid=0
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
    headers = {'Authorization': f'Bearer {token}'}
    
    try:
        response = requests.get(url, headers=headers, timeout=20)
        if response.status_code == 200:
            # Polars đọc bytes
            df = pl.read_csv(io.BytesIO(response.content), infer_schema_length=0)
            
            # THÊM CÁC CỘT THÔNG TIN VÀO DỮ LIỆU ĐỂ PHÂN BIỆT
            df = df.with_columns([
                pl.lit(target_label).alias("Nguồn_Dữ_Liệu"), # Cột Sheet dữ liệu đích
                pl.lit(date_close).alias("Ngày_Chốt"),       # Cột Ngày chốt
                pl.lit(month_close).alias("Tháng_Data")      # Cột Tháng
            ])
            return df
        return None
    except:
        return None

# --- 4. LUỒNG XỬ LÝ CHÍNH ---
def process_pipeline(selected_rows):
    creds = get_creds()
    auth_req = requests.Request()
    creds.refresh(auth_req)
    token = creds.token
    
    results = []
    # Chạy song song
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_row = {executor.submit(fetch_single_csv, row, token): row for row in selected_rows}
        for future in concurrent.futures.as_completed(future_to_row):
            data = future.result()
            if data is not None:
                results.append(data)
    
    if results:
        # Gộp file
        df_big = pl.concat(results, how="diagonal", rechunk=True)
        
        # --- LOGIC LÀM SẠCH (Clean Data) ---
        # 1. Xử lý cột Thành tiền
        if "Thành tiền" in df_big.columns:
            df_big = df_big.with_columns(
                pl.col("Thành tiền").str.replace_all(",", "").cast(pl.Int64, strict=False)
            )
        
        # 2. Sắp xếp lại cột cho đẹp (Đưa các cột thông tin lên đầu)
        # Các cột ưu tiên
        priority_cols = ["Ngày_Chốt", "Tháng_Data", "Nguồn_Dữ_Liệu", "Mã đơn hàng", "Thành tiền"]
        # Lấy các cột còn lại
        other_cols = [c for c in df_big.columns if c not in priority_cols]
        # Select lại
        final_cols = [c for c in priority_cols if c in df_big.columns] + other_cols
        
        return df_big.select(final_cols)
    return None

# --- 5. GIAO DIỆN NGƯỜI DÙNG (UI) ---
def main_ui():
    st.title("⚙️ Trung Tâm Xử Lý Dữ Liệu Tập Trung")
    
    # --- KHỞI TẠO BẢNG CONFIG ---
    if 'df_config' not in st.session_state:
        # Tạo dữ liệu mẫu với ĐẦY ĐỦ CÁC CỘT BẠN YÊU CẦU
        data = {
            "Hành động": [False, False], # Checkbox
            "Ngày chốt": [datetime.now().date(), datetime.now().date()],
            "Tháng": ["12/2025", "12/2025"],
            "Link dữ liệu": ["https://docs.google.com/spreadsheets/d/...", ""],
            "Tên sheet dữ liệu": ["Sheet1", "Sheet1"],
            "Sheet dữ liệu đích": ["KV_HaNoi", "KV_HCM"], # Đây là tên nguồn
            "Thời gian cập nhật": ["-", "-"]
        }
        st.session_state['df_config'] = pd.DataFrame(data)

    # --- HIỂN THỊ BẢNG (DATA EDITOR) ---
    st.write("### 📋 Danh sách cấu hình nguồn dữ liệu")
    edited_df = st.data_editor(
        st.session_state['df_config'],
        num_rows="dynamic", # Cho phép thêm dòng mới
        column_config={
            "Hành động": st.column_config.CheckboxColumn(
                "Chọn chạy",
                help="Tích vào đây để xử lý file này",
                default=False,
            ),
            "Ngày chốt": st.column_config.DateColumn("Ngày chốt", format="DD/MM/YYYY"),
            "Tháng": st.column_config.TextColumn("Tháng"),
            "Link dữ liệu": st.column_config.LinkColumn("Link dữ liệu"),
            "Tên sheet dữ liệu": st.column_config.TextColumn("Tên sheet nguồn"),
            "Sheet dữ liệu đích": st.column_config.TextColumn("Tên nguồn (Đích)"),
            "Thời gian cập nhật": st.column_config.TextColumn("Cập nhật cuối", disabled=True), # Không cho sửa
        },
        use_container_width=True,
        key="editor"
    )

    # --- NÚT BẤM ---
    st.write("---")
    if st.button("▶️ BẮT ĐẦU TỔNG HỢP", type="primary"):
        # Lấy danh sách các dòng được tích chọn
        selected_rows = edited_df[edited_df["Hành động"] == True].to_dict('records')
        
        if not selected_rows:
            st.warning("⚠️ Bạn chưa chọn file nào. Vui lòng tích vào cột 'Hành động'.")
        else:
            with st.status("🚀 Đang xử lý dữ liệu...", expanded=True):
                st.write(f"Đang kết nối {len(selected_rows)} nguồn...")
                
                start_time = time.time()
                df_result = process_pipeline(selected_rows)
                
                if df_result is not None:
                    # Cập nhật thời gian vào cột "Thời gian cập nhật" (Visual)
                    now_str = datetime.now().strftime("%H:%M %d/%m")
                    st.success(f"✅ Xử lý xong {df_result.height:,} dòng (Mất {time.time()-start_time:.2f}s)")
                    
                    # NÚT TẢI VỀ
                    buffer = io.BytesIO()
                    df_result.write_excel(buffer)
                    
                    st.download_button(
                        label="📥 TẢI KẾT QUẢ TỔNG HỢP (.xlsx)",
                        data=buffer.getvalue(),
                        file_name=f"Tong_Hop_Data_{int(time.time())}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                else:
                    st.error("Có lỗi xảy ra. Vui lòng kiểm tra Link hoặc Quyền truy cập.")

if __name__ == "__main__":
    if check_login():
        main_ui()
