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
# EMAIL ROBOT CỦA BẠN
BOT_EMAIL_DISPLAY = "getdulieu@kin-kin-477902.iam.gserviceaccount.com"

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets', 
    'https://www.googleapis.com/auth/drive'
]

# --- 2. HÀM HỖ TRỢ CƠ BẢN ---
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
    if url and "docs.google.com" in str(url):
        try:
            return url.split("/d/")[1].split("/")[0]
        except:
            return None
    return None

# --- 3. HÀM KIỂM TRA QUYỀN (Core Logic) ---
def verify_access_fast(url, creds):
    """
    Hàm này kiểm tra nhanh xem Robot có vào được file không.
    Trả về: (True/False, Message Ngắn gọn)
    """
    sheet_id = extract_id(url)
    if not sheet_id:
        return False, "Link sai"
    
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
        return True, "✅ Sẵn sàng"
    except gspread.exceptions.APIError as e:
        if "403" in str(e):
            return False, "⛔ Thiếu quyền" # Keyword quan trọng để lọc lỗi
        return False, "❌ Lỗi khác"
    except:
        return False, "❌ Lỗi mạng"

# --- 4. HÀM TẢI & XỬ LÝ DỮ LIỆU (Giữ nguyên logic cũ) ---
def fetch_single_csv(row_config, token):
    sheet_id = extract_id(row_config['Link dữ liệu lấy dữ liệu'])
    target_label = row_config['Tên nguồn (Nhãn)'] 
    date_close = str(row_config['Ngày chốt'])
    month_close = str(row_config['Tháng'])
    
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
    headers = {'Authorization': f'Bearer {token}'}
    
    try:
        response = requests.get(url, headers=headers, timeout=20)
        if response.status_code == 200:
            df = pl.read_csv(io.BytesIO(response.content), infer_schema_length=0)
            df = df.with_columns([
                pl.lit(target_label).alias("Nguồn_Dữ_Liệu"),
                pl.lit(date_close).alias("Ngày_Chốt"),
                pl.lit(month_close).alias("Tháng_Data")
            ])
            return df
        return None
    except:
        return None

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
    except Exception as e:
        return False, str(e)

def process_pipeline(selected_rows):
    creds = get_creds()
    auth_req = requests.Request()
    creds.refresh(auth_req)
    token = creds.token
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_row = {executor.submit(fetch_single_csv, row, token): row for row in selected_rows}
        for future in concurrent.futures.as_completed(future_to_row):
            data = future.result()
            if data is not None: results.append(data)
    
    if results:
        df_big = pl.concat(results, how="diagonal", rechunk=True)
        if "Thành tiền" in df_big.columns:
            df_big = df_big.with_columns(
                pl.col("Thành tiền").str.replace_all(",", "").cast(pl.Int64, strict=False)
            )
        # Sắp xếp cột
        priority_cols = ["Ngày_Chốt", "Tháng_Data", "Nguồn_Dữ_Liệu", "Mã đơn hàng", "Thành tiền"]
        other_cols = [c for c in df_big.columns if c not in priority_cols]
        final_cols = [c for c in priority_cols if c in df_big.columns] + other_cols
        return df_big.select(final_cols)
    return None

# --- 5. GIAO DIỆN CHÍNH (LOGIC TỰ ĐỘNG) ---
def main_ui():
    st.title("⚙️ Hệ Thống Xử Lý & Đẩy Data Tập Trung")
    
    # 1. Khởi tạo dữ liệu
    if 'df_config' not in st.session_state:
        data = {
            "Hành động": [False, False], 
            "Ngày chốt": [datetime.now().date(), datetime.now().date()],
            "Tháng": ["12/2025", "12/2025"],
            "Link dữ liệu lấy dữ liệu": ["", ""],
            "Link dữ liệu đích": ["", ""],
            "Tên sheet dữ liệu": ["Sheet1", "Sheet1"],
            "Tên nguồn (Nhãn)": ["KV_HaNoi", "KV_HCM"],
            "Trạng thái": ["", ""] # Để trống ban đầu
        }
        st.session_state['df_config'] = pd.DataFrame(data)

    st.info("💡 Nhập Link vào bảng -> Hệ thống sẽ **tự động kiểm tra quyền**. Nếu thấy '⛔ Thiếu quyền', hãy share cho Email Robot bên dưới.")

    # 2. Hiển thị bảng (Data Editor)
    edited_df = st.data_editor(
        st.session_state['df_config'],
        num_rows="dynamic",
        column_config={
            "Hành động": st.column_config.CheckboxColumn("Chọn", width="small"),
            "Link dữ liệu lấy dữ liệu": st.column_config.TextColumn("Link Nguồn (Copy vào đây)", width="medium"),
            "Link dữ liệu đích": st.column_config.TextColumn("Link Đích (Ghi vào)", width="medium"),
            "Trạng thái": st.column_config.TextColumn("Trạng thái (Auto)", disabled=True, width="medium"),
            # Các cột khác ẩn bớt hoặc config gọn lại nếu muốn
        },
        use_container_width=True,
        key="editor" 
        # Key quan trọng để bắt sự kiện thay đổi
    )

    # 3. LOGIC TỰ ĐỘNG KIỂM TRA (Auto Check)
    # So sánh xem người dùng có vừa nhập/sửa gì không
    if not edited_df.equals(st.session_state['df_config']):
        
        # Lấy Credentials 1 lần dùng chung
        creds = get_creds()
        missing_permission_found = False
        
        # Duyệt qua từng dòng để kiểm tra Link
        for index, row in edited_df.iterrows():
            link_src = row['Link dữ liệu lấy dữ liệu']
            link_dst = row['Link dữ liệu đích']
            current_status = row['Trạng thái']
            
            new_status_parts = []
            
            # Kiểm tra Link Nguồn
            if link_src and "docs.google.com" in str(link_src):
                # Chỉ kiểm tra lại nếu chưa có trạng thái OK hoặc link vừa đổi
                # Để đơn giản và chính xác, ta kiểm tra lại khi có trigger thay đổi
                ok, msg = verify_access_fast(link_src, creds)
                if not ok: 
                    new_status_parts.append(f"Nguồn: {msg}")
                    missing_permission_found = True
            
            # Kiểm tra Link Đích
            if link_dst and "docs.google.com" in str(link_dst):
                ok, msg = verify_access_fast(link_dst, creds)
                if not ok: 
                    new_status_parts.append(f"Đích: {msg}")
                    missing_permission_found = True
            
            # Cập nhật cột Trạng thái
            if new_status_parts:
                edited_df.at[index, 'Trạng thái'] = " | ".join(new_status_parts)
            elif (link_src or link_dst):
                edited_df.at[index, 'Trạng thái'] = "✅ Sẵn sàng"
            else:
                edited_df.at[index, 'Trạng thái'] = ""

        # Cập nhật lại Session State để giao diện hiển thị Trạng thái mới
        st.session_state['df_config'] = edited_df
        st.rerun() # Load lại trang để hiện chữ ✅ hoặc ⛔ ngay lập tức

    # 4. HIỂN THỊ CẢNH BÁO NẾU THIẾU QUYỀN
    # Quét trong bảng hiện tại xem có dòng nào bị lỗi không
    error_rows = edited_df[edited_df['Trạng thái'].astype(str).str.contains("Thiếu quyền", na=False)]
    
    if not error_rows.empty:
        st.divider()
        st.error(f"⚠️ Phát hiện **{len(error_rows)} dòng** chưa cấp quyền cho Robot!")
        
        col_warn_1, col_warn_2 = st.columns([3, 1])
        with col_warn_1:
            st.markdown(f"**Copy email này và Share quyền Editor cho các file bị lỗi:**")
            st.code(BOT_EMAIL_DISPLAY, language="text")
        with col_warn_2:
            st.warning("Sau khi Share xong, hãy sửa nhẹ 1 ký tự bất kỳ trong bảng rồi Enter để hệ thống check lại.")

    # 5. NÚT CHẠY (Giữ nguyên)
    st.divider()
    if st.button("▶️ TỔNG HỢP & GHI DATA", type="primary"):
        selected_rows = edited_df[edited_df["Hành động"] == True].to_dict('records')
        
        # Kiểm tra chặn nếu còn lỗi
        has_error = any("Thiếu quyền" in str(row.get('Trạng thái', '')) for row in selected_rows)
        if has_error:
            st.error("❌ Không thể chạy vì có file được chọn đang '⛔ Thiếu quyền'. Vui lòng cấp quyền trước.")
            st.stop()
            
        if not selected_rows:
            st.warning("⚠️ Chọn ít nhất 1 dòng để chạy.")
        else:
            target_link = selected_rows[0]['Link dữ liệu đích']
            if not target_link:
                st.error("❌ Dòng đầu tiên chưa có Link Đích.")
                st.stop()

            with st.status("🚀 Đang thực thi...", expanded=True) as status:
                st.write(f"1. Tải dữ liệu từ {len(selected_rows)} nguồn...")
                start_time = time.time()
                df_result = process_pipeline(selected_rows)
                
                if df_result is not None:
                    st.write(f"✅ Tải xong: **{df_result.height:,} dòng**. Ghi vào Sheet đích...")
                    creds = get_creds()
                    success, msg = write_to_google_sheet(df_result, target_link, creds)
                    
                    if success:
                        status.update(label="Hoàn tất!", state="complete", expanded=False)
                        st.success(f"🎉 {msg}")
                        st.balloons()
                        buffer = io.BytesIO()
                        df_result.write_excel(buffer)
                        st.download_button("📥 Tải Backup (.xlsx)", buffer.getvalue(), "Backup_Data.xlsx")
                    else:
                        st.error(f"❌ Lỗi ghi: {msg}")
                else:
                    st.error("❌ Lỗi tải dữ liệu nguồn.")

if __name__ == "__main__":
    if check_login():
        main_ui()
