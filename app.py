import streamlit as st
import pandas as pd
import utils
import time
from datetime import datetime

st.set_page_config(page_title="Tool Quản Lý Khối Dữ Liệu", layout="wide")

# CSS để ẩn nút deploy
st.markdown("""<style>.stDeployButton {display:none;}</style>""", unsafe_allow_html=True)

# --- INIT DATABASE ---
# Chạy 1 lần để đảm bảo DB đủ sheet
if 'db_checked' not in st.session_state:
    utils.init_database_if_needed()
    st.session_state.db_checked = True

# --- LOGIN ---
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False

if not st.session_state.logged_in:
    st.title("🔐 Đăng nhập hệ thống")
    user = st.text_input("Username")
    pwd = st.text_input("Password", type="password")
    if st.button("Đăng nhập"):
        try:
            ws = utils.get_db_worksheet("users")
            df = pd.DataFrame(ws.get_all_records())
            if not df.empty and ((df['Username'] == user) & (df['Password'] == pwd)).any():
                st.session_state.logged_in = True
                st.session_state.username = user
                st.rerun()
            else:
                st.error("Sai thông tin!")
        except: st.error("Lỗi kết nối DB. Vui lòng kiểm tra file config.")
    st.stop()

# --- MAIN UI ---
user = st.session_state.username
st.sidebar.title(f"👤 {user}")
if st.sidebar.button("Đăng xuất"):
    st.session_state.logged_in = False
    st.rerun()

st.title("🎛️ Quản Lý Các Khối Dữ Liệu (Parallel Blocks)")

# Load Config
ws_config = utils.get_db_worksheet("luu_cau_hinh")
all_data = ws_config.get_all_records()
df_config = pd.DataFrame(all_data)

# 1. FORM THÊM KHỐI MỚI (TỰ ĐỘNG XÓA SAU KHI THÊM)
with st.expander("➕ THÊM KHỐI MỚI", expanded=False):
    with st.form("add_block_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        b_name = c1.text_input("Tên Khối (Block Name)")
        b_month = c2.text_input("Tháng (VD: 12/2024)")
        b_freq = c3.selectbox("Hẹn Giờ", ["Hàng ngày", "Hàng tuần", "Hàng tháng"])
        
        c4, c5 = st.columns(2)
        link_src = c4.text_input("Link Nguồn")
        sheet_src = c4.text_input("Tên Sheet Nguồn")
        link_dst = c5.text_input("Link Đích")
        sheet_dst = c5.text_input("Tên Sheet Đích")
        
        if st.form_submit_button("Lưu Khối"):
            if b_name and link_src and link_dst:
                new_row = [
                    user, b_name, "Chưa chốt & đang cập nhật",
                    link_src, sheet_src, link_dst, sheet_dst,
                    b_freq, "", "", 0, b_month, "0 - 0"
                ]
                ws_config.append_row(new_row)
                st.success(f"Đã thêm khối: {b_name}")
                time.sleep(1)
                st.rerun()
            else:
                st.warning("Vui lòng nhập đủ thông tin!")

st.divider()

# 2. NÚT CHẠY TẤT CẢ
col_run_all, _ = st.columns([1, 4])
with col_run_all:
    if st.button("🚀 CHẠY TẤT CẢ (Chưa chốt)", type="primary"):
        is_locked, locker = utils.check_lock()
        if is_locked and locker != user:
            st.error(f"Hệ thống đang bận bởi: {locker}")
        else:
            utils.set_lock("LOCKED", user)
            
            # Lọc các khối của user này
            my_blocks = df_config[(df_config['Username'] == user) & 
                                  (df_config['Status'] == "Chưa chốt & đang cập nhật")]
            
            if my_blocks.empty:
                st.info("Không có khối nào cần chạy.")
            else:
                progress_bar = st.progress(0)
                status_area = st.empty()
                
                for idx, (index, row) in enumerate(my_blocks.iterrows()):
                    status_area.markdown(f"**⏳ Đang xử lý: {row['Block_Name']}...**")
                    
                    ok, msg, count, rng = utils.process_single_block(row, user)
                    
                    # Update DB (Cần cộng 2 vì header và 0-based index)
                    # Giả sử đúng thứ tự cột trong DB
                    if ok:
                        ws_config.update_cell(index + 2, 9, datetime.now().strftime("%Y-%m-%d")) # Last Run
                        ws_config.update_cell(index + 2, 11, count) # Số dòng
                        ws_config.update_cell(index + 2, 13, rng) # Realtime Range
                    
                    progress_bar.progress((idx + 1) / len(my_blocks))
                
                status_area.success("✅ Hoàn tất chạy tất cả!")
                utils.set_lock("UNLOCKED", "")
                time.sleep(2)
                st.rerun()

# 3. HIỂN THỊ CÁC KHỐI (DẠNG KHỐI RIÊNG BIỆT)
st.subheader("Danh Sách Các Khối")

# Lấy lại data mới nhất để hiển thị
all_data_fresh = ws_config.get_all_records()
df_fresh = pd.DataFrame(all_data_fresh)
user_blocks = df_fresh[df_fresh['Username'] == user]

if user_blocks.empty:
    st.info("Chưa có khối nào. Hãy tạo mới ở trên.")

for index, row in user_blocks.iterrows():
    # Tạo Container riêng cho từng khối (Giao diện tách biệt)
    with st.container(border=True):
        c_head, c_body, c_action = st.columns([3, 4, 2])
        
        with c_head:
            st.markdown(f"### 📦 {row['Block_Name']}")
            st.caption(f"Lịch: {row['Tan_Suat_Hen_Gio']}")
            status = row['Status']
            color = "orange" if status == "Chưa chốt & đang cập nhật" else "green"
            st.markdown(f"Trạng thái: :{color}[{status}]")

        with c_body:
            st.write(f"**Nguồn:** ...{str(row['Link_Nguon'])[-15:]} | Sheet: `{row['Sheet_Nguon']}`")
            st.write(f"**Đích:** ...{str(row['Link_Dich'])[-15:]} | Sheet: `{row['Sheet_Dich']}`")
            st.write(f"📊 Dữ liệu: **{row['So_Dong_Du_Lieu']}** dòng | Vị trí: `{row['Realtime_Range']}`")
            st.caption(f"Cập nhật lần cuối: {row['Last_Run']}")

        with c_action:
            # Nút Chạy Riêng
            if st.button("▶️ Chạy Khối Này", key=f"run_{index}"):
                is_locked, locker = utils.check_lock()
                if is_locked:
                    st.error(f"Locked by {locker}")
                else:
                    utils.set_lock("LOCKED", user)
                    with st.spinner("Đang xử lý..."):
                        ok, msg, count, rng = utils.process_single_block(row, user)
                        if ok:
                            ws_config.update_cell(index + 2, 9, datetime.now().strftime("%Y-%m-%d"))
                            ws_config.update_cell(index + 2, 11, count)
                            ws_config.update_cell(index + 2, 13, rng)
                            st.toast(f"Xong! {msg}", icon="✅")
                        else:
                            st.error(msg)
                    utils.set_lock("UNLOCKED", "")
                    time.sleep(1)
                    st.rerun()
            
            # Nút Xóa
            if st.button("🗑️ Xóa Khối", key=f"del_{index}"):
                ws_config.delete_rows(index + 2)
                st.warning("Đã xóa!")
                time.sleep(1)
                st.rerun()
