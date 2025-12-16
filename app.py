import streamlit as st
import pandas as pd
import polars as pl
import requests
import io
import time
import gspread
import json
from gspread_dataframe import get_as_dataframe
from datetime import datetime
from google.oauth2 import service_account
import google.auth.transport.requests
import pytz
from collections import defaultdict

# --- 1. CẤU HÌNH HỆ THỐNG ---
st.set_page_config(page_title="Tool Quản Lý Data Multi-Block", layout="wide")

AUTHORIZED_USERS = {
    "admin2024": "Admin_Master",
    "team_hn": "Team_HaNoi",
    "team_hcm": "Team_HCM"
}

# Tên các Sheet
SHEET_CONFIG_NAME = "luu_cau_hinh" 
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_LOCK_NAME = "sys_lock"
SHEET_SYS_CONFIG = "sys_config"
SHEET_LOG_GITHUB = "log_chay_auto_github"

# Cột hệ thống
COL_LINK_SRC = "Link file nguồn"
COL_LABEL_SRC = "Sheet nguồn"
COL_MONTH_SRC = "Tháng chốt"
COL_BLOCK_NAME = "Block_Name" # Cột định danh khối
DEFAULT_BLOCK_NAME = "Block_Mac_Dinh"

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- GIỮ NGUYÊN CÁC HÀM XÁC THỰC & LOGIC XỬ LÝ CŨ (check_login, get_creds, v.v...) ---
# (Để tiết kiệm không gian, tôi chỉ viết lại phần Logic UI và Quản lý Block thay đổi)

def check_login():
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
    if 'current_user_id' not in st.session_state: st.session_state['current_user_id'] = "Unknown"
    if "auto_key" in st.query_params:
        key = st.query_params["auto_key"]
        if key in AUTHORIZED_USERS:
            st.session_state['logged_in'] = True; st.session_state['current_user_id'] = AUTHORIZED_USERS[key]; return True
    if st.session_state['logged_in']: return True
    st.header("🔒 Đăng nhập hệ thống")
    pwd = st.text_input("Nhập mật khẩu truy cập:", type="password")
    if st.button("Đăng Nhập"):
        if pwd in AUTHORIZED_USERS:
            st.session_state['logged_in'] = True; st.session_state['current_user_id'] = AUTHORIZED_USERS[pwd]
            st.rerun()
        else: st.error("Mật khẩu không đúng!")
    return False

def get_creds():
    raw_creds = st.secrets["gcp_service_account"]
    if isinstance(raw_creds, str): creds_info = json.loads(raw_creds)
    else: creds_info = dict(raw_creds)
    if "private_key" in creds_info: creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
    return service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)

def extract_id(url):
    if not isinstance(url, str): return None
    if "docs.google.com" in url:
        try: return url.split("/d/")[1].split("/")[0]
        except: return None
    return None

# --- INCLUDE CÁC HÀM CŨ: get_system_lock, set_system_lock, write_detailed_log, verify_access_fast, fetch_single_csv_safe, scan_realtime_row_ranges, smart_update_safe, process_pipeline ---
# (Bạn hãy copy lại nguyên văn các hàm này từ code cũ vào đây, chúng vẫn hoạt động tốt)
# Lưu ý: Hàm process_pipeline cần sửa nhẹ để log đúng block name nếu cần, nhưng logic cốt lõi giữ nguyên.

# ... [Chèn code các hàm logic cũ vào đây] ... 
# Để code chạy được, tôi giả lập lại hàm process_pipeline ở mức gọi, bạn dùng code cũ nhé.
# Dưới đây là logic MỚI cho phần quản lý BLOCK.

# --- HÀM QUẢN LÝ CẤU HÌNH THEO BLOCK ---
def load_full_config(creds):
    """Tải toàn bộ cấu hình (tất cả các block)"""
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
    df = df.dropna(how='all')
    
    # Chuẩn hóa cột
    rename_map = {
        'Tên sheet dữ liệu': 'Tên sheet dữ liệu đích', 'Tên nguồn (Nhãn)': 'Tên sheet nguồn dữ liệu gốc',
        'Link file nguồn': 'Link dữ liệu lấy dữ liệu', 'Link file đích': 'Link dữ liệu đích'
    }
    for old, new in rename_map.items():
        if old in df.columns: df = df.rename(columns={old: new})
    
    required_cols = ['Trạng thái', 'Ngày chốt', 'Tháng', 'Link dữ liệu lấy dữ liệu', 'Link dữ liệu đích', 'Tên sheet dữ liệu đích', 'Tên sheet nguồn dữ liệu gốc', 'Kết quả', 'Dòng dữ liệu', COL_BLOCK_NAME]
    for c in required_cols:
        if c not in df.columns: df[c] = ""
        
    # Xử lý dữ liệu
    df[COL_BLOCK_NAME] = df[COL_BLOCK_NAME].replace('', DEFAULT_BLOCK_NAME).fillna(DEFAULT_BLOCK_NAME)
    if 'Trạng thái' in df.columns:
        df['Trạng thái'] = df['Trạng thái'].apply(lambda x: "Đã chốt" if str(x).strip() in ["Đã chốt", "Đã cập nhật", "TRUE"] else "Chưa chốt & đang cập nhật")
    
    # Tạo STT giả để hiển thị
    if 'STT' in df.columns: df = df.drop(columns=['STT'])
    # Không tạo STT ở đây, tạo sau khi lọc block
    return df

def save_block_config(df_current_ui, current_block_name, creds):
    """Lưu cấu hình: Chỉ cập nhật các dòng thuộc Block hiện tại, giữ nguyên Block khác"""
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_CONFIG_NAME)
    
    # 1. Tải lại dữ liệu gốc từ sheet để đảm bảo không mất dữ liệu của Block khác
    df_full_server = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
    df_full_server = df_full_server.dropna(how='all')
    if COL_BLOCK_NAME not in df_full_server.columns: df_full_server[COL_BLOCK_NAME] = DEFAULT_BLOCK_NAME
    df_full_server[COL_BLOCK_NAME] = df_full_server[COL_BLOCK_NAME].replace('', DEFAULT_BLOCK_NAME).fillna(DEFAULT_BLOCK_NAME)
    
    # 2. Xóa các dòng cũ của Block hiện tại trong df server
    df_other_blocks = df_full_server[df_full_server[COL_BLOCK_NAME] != current_block_name]
    
    # 3. Chuẩn bị dữ liệu mới từ UI
    df_to_save = df_current_ui.copy()
    if 'STT' in df_to_save.columns: df_to_save = df_to_save.drop(columns=['STT'])
    df_to_save[COL_BLOCK_NAME] = current_block_name # Gán đúng tên block
    
    # 4. Gộp lại
    df_final = pd.concat([df_other_blocks, df_to_save], ignore_index=True)
    
    # 5. Ghi đè lên Sheet
    wks.clear()
    wks.update([df_final.columns.tolist()] + df_final.fillna('').values.tolist())
    st.toast(f"✅ Đã lưu cấu hình khối: {current_block_name}!", icon="💾")

# --- HÀM QUẢN LÝ LỊCH CHẠY (SYS_CONFIG) ---
def load_sys_schedule(creds):
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
        try: wks = sh.worksheet(SHEET_SYS_CONFIG)
        except: 
            wks = sh.add_worksheet(SHEET_SYS_CONFIG, rows=20, cols=5)
            wks.append_row([COL_BLOCK_NAME, "Run_Hour", "Run_Freq"])
        
        df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if COL_BLOCK_NAME not in df.columns: 
            # Migration từ version cũ
            wks.clear(); wks.append_row([COL_BLOCK_NAME, "Run_Hour", "Run_Freq"])
            df = pd.DataFrame(columns=[COL_BLOCK_NAME, "Run_Hour", "Run_Freq"])
            
        return df.dropna(how='all')
    except: return pd.DataFrame(columns=[COL_BLOCK_NAME, "Run_Hour", "Run_Freq"])

def save_sys_schedule(df_schedule, creds):
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
    wks = sh.worksheet(SHEET_SYS_CONFIG)
    wks.clear()
    wks.update([df_schedule.columns.tolist()] + df_schedule.fillna('').values.tolist())

# --- 6. GIAO DIỆN CHÍNH (ĐÃ NÂNG CẤP) ---
def main_ui():
    if not check_login(): return
    user_id = st.session_state['current_user_id']
    creds = get_creds()
    
    st.title(f"⚙️ Tool Quản Lý Data (User: {user_id})")
    
    # --- A. SIDEBAR: QUẢN LÝ KHỐI (BLOCKS) ---
    with st.sidebar:
        st.header("📦 Quản Lý Khối")
        
        # Load dữ liệu full để lấy danh sách block
        if 'df_full_config' not in st.session_state:
            with st.spinner("Đang tải dữ liệu..."): st.session_state['df_full_config'] = load_full_config(creds)
            
        unique_blocks = st.session_state['df_full_config'][COL_BLOCK_NAME].unique().tolist()
        if not unique_blocks: unique_blocks = [DEFAULT_BLOCK_NAME]
        
        # Chọn Block
        selected_block = st.selectbox("Chọn Khối làm việc:", unique_blocks, key="sb_block_select")
        
        st.divider()
        # Thêm Block Mới
        new_block_input = st.text_input("Tên khối mới:")
        if st.button("➕ Thêm Khối Mới"):
            if new_block_input and new_block_input not in unique_blocks:
                st.session_state['df_full_config'] = pd.concat([
                    st.session_state['df_full_config'],
                    pd.DataFrame([{COL_BLOCK_NAME: new_block_input, 'Trạng thái': 'Chưa chốt & đang cập nhật'}]) # Tạo dòng mồi
                ], ignore_index=True)
                st.success(f"Đã thêm {new_block_input}")
                st.rerun()
            elif new_block_input in unique_blocks: st.warning("Tên khối đã tồn tại!")
        
        # Xóa Block
        if st.button("🗑️ Xóa Khối Hiện Tại", type="primary"):
            if len(unique_blocks) <= 1: st.error("Không thể xóa khối cuối cùng!")
            else:
                # Xóa trong session state và lưu luôn
                df_remain = st.session_state['df_full_config'][st.session_state['df_full_config'][COL_BLOCK_NAME] != selected_block]
                save_block_config(df_remain, "TEMP_DELETE", creds) # Hàm save logic hơi khác chút, nên ta gọi trực tiếp save full
                
                # Manual save full override
                gc = gspread.authorize(creds)
                sh = gc.open_by_key(st.secrets["gcp_service_account"]["history_sheet_id"])
                wks = sh.worksheet(SHEET_CONFIG_NAME)
                wks.clear()
                wks.update([df_remain.columns.tolist()] + df_remain.fillna('').values.tolist())
                
                del st.session_state['df_full_config']
                st.rerun()

    # --- B. MAIN AREA: HIỂN THỊ DỮ LIỆU CỦA KHỐI ĐANG CHỌN ---
    st.subheader(f"Danh sách Job của khối: {selected_block}")
    
    # Lọc dữ liệu hiển thị
    df_display = st.session_state['df_full_config'][st.session_state['df_full_config'][COL_BLOCK_NAME] == selected_block].copy()
    df_display = df_display.reset_index(drop=True)
    df_display.insert(0, 'STT', range(1, len(df_display) + 1)) # STT nội bộ khối
    
    col_order = ["STT", "Trạng thái", "Ngày chốt", "Tháng", "Link dữ liệu lấy dữ liệu", "Link dữ liệu đích", "Tên sheet dữ liệu đích", "Tên sheet nguồn dữ liệu gốc", "Kết quả", "Dòng dữ liệu"]
    
    edited_df = st.data_editor(
        df_display,
        column_order=col_order,
        column_config={
            "STT": st.column_config.NumberColumn("STT", disabled=True, width="small"),
            "Trạng thái": st.column_config.SelectboxColumn("Trạng thái", options=["Chưa chốt & đang cập nhật", "Đã chốt"], required=True),
            "Link dữ liệu lấy dữ liệu": st.column_config.TextColumn("Link Nguồn", width="medium"),
            "Kết quả": st.column_config.TextColumn("Kết quả", disabled=True),
            "Dòng dữ liệu": st.column_config.TextColumn("Dòng Dữ Liệu", disabled=True),
        },
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        key=f"editor_{selected_block}"
    )

    # --- C. CÀI ĐẶT HẸN GIỜ CHO KHỐI NÀY ---
    st.divider()
    st.markdown(f"**⏰ Cài Đặt Hẹn Giờ (Block: {selected_block})**")
    
    # Load Sys Config
    if 'df_sys_schedule' not in st.session_state: st.session_state['df_sys_schedule'] = load_sys_schedule(creds)
    df_sch = st.session_state['df_sys_schedule']
    
    # Lấy cấu hình của Block hiện tại
    row_sch = df_sch[df_sch[COL_BLOCK_NAME] == selected_block]
    cur_hour = 8; cur_freq = "Hàng ngày"
    
    if not row_sch.empty:
        try: cur_hour = int(row_sch.iloc[0]['Run_Hour'])
        except: pass
        cur_freq = str(row_sch.iloc[0]['Run_Freq'])

    c1, c2, c3 = st.columns(3)
    list_freq = ["Hàng ngày", "Hàng tuần", "Hàng tháng"]
    with c1: new_freq = st.selectbox("Tần suất:", list_freq, index=list_freq.index(cur_freq) if cur_freq in list_freq else 0)
    with c2: new_hour = st.slider("Giờ chạy (VN):", 0, 23, value=cur_hour)
    with c3:
        st.write("")
        if st.button("Lưu Hẹn Giờ"):
            # Update vào df_sch
            new_row = {COL_BLOCK_NAME: selected_block, "Run_Hour": str(new_hour), "Run_Freq": new_freq}
            
            # Xóa cũ insert mới vào df_sch local
            df_sch = df_sch[df_sch[COL_BLOCK_NAME] != selected_block]
            df_sch = pd.concat([df_sch, pd.DataFrame([new_row])], ignore_index=True)
            
            save_sys_schedule(df_sch, creds)
            st.session_state['df_sys_schedule'] = df_sch
            st.toast("✅ Đã lưu lịch chạy!", icon="⏰")

    # --- D. THANH CÔNG CỤ (ACTION BAR) ---
    st.divider()
    col_run_block, col_run_all, col_scan, col_save = st.columns([2, 2, 1, 1])
    
    # Nút 1: CHẠY KHỐI HIỆN TẠI
    with col_run_block:
        if st.button(f"▶️ CHẠY KHỐI: {selected_block}", type="primary"):
            rows_run = edited_df[edited_df['Trạng thái'] == "Chưa chốt & đang cập nhật"].to_dict('records')
            rows_run = [r for r in rows_run if len(str(r.get('Link dữ liệu lấy dữ liệu', ''))) > 5]
            
            if not rows_run: st.warning("⚠️ Không có dòng chưa chốt trong khối này.")
            else:
                with st.status(f"Đang xử lý {len(rows_run)} nguồn của {selected_block}...", expanded=True):
                    # Gọi hàm process_pipeline cũ (cần import từ logic cũ)
                    # Lưu ý: Hàm này cần trả về results_map để update UI
                    # Ở đây giả định bạn đã định nghĩa lại process_pipeline như file cũ
                    all_ok, results_map = process_pipeline(rows_run, user_id) 
                    
                    if results_map:
                        st.success("Xong.")
                        # Cập nhật UI
                        for idx, row in edited_df.iterrows():
                            s_link = row.get('Link dữ liệu lấy dữ liệu', '')
                            if s_link in results_map:
                                msg, rng = results_map[s_link]
                                if row['Trạng thái'] == "Chưa chốt & đang cập nhật": edited_df.at[idx, 'Kết quả'] = msg
                                edited_df.at[idx, 'Dòng dữ liệu'] = rng
                        
                        # Lưu
                        save_block_config(edited_df, selected_block, creds)
                        # Reload full config để đồng bộ
                        del st.session_state['df_full_config']
                        time.sleep(1); st.rerun()

    # Nút 2: CHẠY TẤT CẢ (RUN ALL)
    with col_run_all:
        if st.button("🚀 CHẠY TẤT CẢ CÁC KHỐI"):
            with st.status("Đang chạy toàn bộ hệ thống...", expanded=True) as status:
                full_df = st.session_state['df_full_config']
                all_blocks_list = full_df[COL_BLOCK_NAME].unique()
                
                for blk in all_blocks_list:
                    status.write(f"⏳ Đang chạy khối: **{blk}**...")
                    # Lấy dòng chưa chốt của khối này
                    rows_blk = full_df[(full_df[COL_BLOCK_NAME] == blk) & (full_df['Trạng thái'] == "Chưa chốt & đang cập nhật")].to_dict('records')
                    rows_blk = [r for r in rows_blk if len(str(r.get('Link dữ liệu lấy dữ liệu', ''))) > 5]
                    
                    if rows_blk:
                        process_pipeline(rows_blk, f"{user_id} (AutoAll)")
                        status.write(f"✅ Xong khối {blk}.")
                    else:
                        status.write(f"⚪ Khối {blk} không có dữ liệu cần chạy.")
                
                status.update(label="Đã hoàn thành chạy tất cả!", state="complete", expanded=False)
                st.toast("Đã chạy xong tất cả!", icon="🏁")

    # Nút 3: QUÉT QUYỀN (CHỈ KHỐI HIỆN TẠI)
    with col_scan:
        if st.button("🔍 Quét Quyền"):
            # Hàm man_scan từ code cũ
            errs = man_scan(edited_df) 
            if errs: st.error(f"{len(errs)} lỗi quyền.")
            else: st.success("Quyền OK.")

    # Nút 4: LƯU CẤU HÌNH (CHỈ KHỐI HIỆN TẠI)
    with col_save:
        if st.button("💾 Lưu"):
            save_block_config(edited_df, selected_block, creds)
            # Update session state
            del st.session_state['df_full_config'] # Xóa cache để load lại cái mới
            st.rerun()

if __name__ == "__main__":
    main_ui()
