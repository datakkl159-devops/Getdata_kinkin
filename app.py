import streamlit as st
import pandas as pd
import polars as pl
import requests
import io
import concurrent.futures
import time
from datetime import datetime
from google.oauth2 import service_account

# --- CẤU HÌNH ---
st.set_page_config(page_title="Tool Xử Lý Data 500k", layout="wide")
PASSWORD_ACCESS = "admin2024" # MẬT KHẨU ĐỂ VÀO TOOL
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- HÀM LOGIN ---
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

# --- HÀM KẾT NỐI GOOGLE ---
def get_creds():
    # Đọc thông tin từ Secrets trên Streamlit Cloud
    return service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=SCOPES
    )

def extract_id(url):
    """Lấy ID file từ link"""
    if "docs.google.com" in url:
        try: return url.split("/d/")[1].split("/")[0]
        except: return None
    return url

# --- HÀM TẢI 1 FILE (WORKER) ---
def fetch_single_csv(row_config, token):
    sheet_id = extract_id(row_config['Link dữ liệu'])
    name_source = row_config['Tên Sheet/Chi Nhánh']
    
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
    headers = {'Authorization': f'Bearer {token}'}
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            # Polars đọc CSV siêu tốc
            df = pl.read_csv(io.BytesIO(response.content), infer_schema_length=0)
            
            # Thêm cột nguồn để phân biệt file nào
            df = df.with_columns([
                pl.lit(name_source).alias("Nguồn_Gốc"),
                pl.lit(row_config['Ngày chốt']).alias("Ngày_Data")
            ])
            return df
        return None
    except:
        return None

# --- LUỒNG XỬ LÝ CHÍNH (ĐA LUỒNG) ---
def process_pipeline(selected_rows):
    creds = get_creds()
    auth_req = requests.Request()
    creds.refresh(auth_req)
    token = creds.token
    
    results = []
    # Tải song song tối đa 10 file cùng lúc
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_row = {executor.submit(fetch_single_csv, row, token): row for row in selected_rows}
        for future in concurrent.futures.as_completed(future_to_row):
            data = future.result()
            if data is not None:
                results.append(data)
    
    if results:
        # Gộp tất cả thành 1 file lớn
        df_big = pl.concat(results, how="diagonal", rechunk=True)
        
        # --- LOGIC LÀM SẠCH DỮ LIỆU ---
        # 1. Chọn cột cần giữ lại (Sửa tên cột theo đúng file của bạn)
        cols_keep = ["Ngày chốt", "Mã đơn hàng", "Thành tiền", "Mã nhân viên bán hàng", "Nguồn_Gốc"]
        existing_cols = [c for c in cols_keep if c in df_big.columns]
        df_clean = df_big.select(existing_cols)
        
        # 2. Xử lý cột Thành tiền (Xóa dấu phẩy, chuyển thành số)
        if "Thành tiền" in df_clean.columns:
            df_clean = df_clean.with_columns(
                pl.col("Thành tiền").str.replace_all(",", "").cast(pl.Int64, strict=False)
            )
            
        return df_clean
    return None

# --- GIAO DIỆN ---
def main():
    st.title("🚀 Tool Xử Lý Data (Engine: Polars)")
    
    # Tạo bảng Config mặc định
    if 'df_config' not in st.session_state:
        st.session_state['df_config'] = pd.DataFrame({
            "Chọn": [False, False],
            "Tên Sheet/Chi Nhánh": ["KV Hà Nội", "KV HCM"],
            "Link dữ liệu": ["https://docs.google.com/spreadsheets/d/...", ""],
            "Ngày chốt": [datetime.now().date(), datetime.now().date()]
        })

    # Hiển thị bảng nhập liệu
    edited_df = st.data_editor(
        st.session_state['df_config'],
        num_rows="dynamic",
        column_config={
            "Chọn": st.column_config.CheckboxColumn("Chạy?", default=False),
            "Link dữ liệu": st.column_config.LinkColumn("Link Google Sheet")
        },
        use_container_width=True
    )
    
    if st.button("▶️ CHẠY TỔNG HỢP", type="primary"):
        rows_to_run = edited_df[edited_df["Chọn"] == True].to_dict('records')
        
        if not rows_to_run:
            st.warning("Vui lòng tích chọn ít nhất 1 dòng!")
        else:
            with st.status("Đang xử lý dữ liệu...", expanded=True):
                start = time.time()
                df_result = process_pipeline(rows_to_run)
                
                if df_result is not None:
                    st.success(f"✅ Xong! Tổng: {df_result.height:,} dòng ({time.time()-start:.2f}s)")
                    
                    # Nút tải file
                    buffer = io.BytesIO()
                    df_result.write_excel(buffer)
                    st.download_button(
                        label="📥 Tải Kết Quả (.xlsx)",
                        data=buffer.getvalue(),
                        file_name="Ket_qua_xu_ly.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                else:
                    st.error("Lỗi tải dữ liệu. Kiểm tra Link hoặc Quyền chia sẻ.")

if __name__ == "__main__":
    if check_login():
        main()