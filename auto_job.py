import pandas as pd
import gspread
import json
import os
import time
from datetime import datetime
from google.oauth2 import service_account
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# --- CẤU HÌNH ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
SHEET_CONFIG_NAME = "luu_cau_hinh"
SHEET_LOG_NAME = "log_hanh_vi"
SHEET_HISTORY_ID = "ID_FILE_SHEET_CUA_BAN_O_DAY" # <--- Thay ID sheet của bạn vào đây

# --- 1. KẾT NỐI GOOGLE ---
def get_creds():
    # Lấy key từ biến môi trường GitHub Secrets
    creds_json = os.environ.get('GCP_SERVICE_ACCOUNT')
    if not creds_json:
        raise ValueError("❌ Chưa cấu hình Secret GCP_SERVICE_ACCOUNT trên GitHub!")
    info = json.loads(creds_json)
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

def get_gc():
    return gspread.authorize(get_creds())

# --- 2. LOGIC CHỌN KHỐI (QUAN TRỌNG NHẤT) ---
def get_next_block_to_run(gc):
    sh = gc.open_by_key(SHEET_HISTORY_ID)
    
    # A. Lấy danh sách các khối CẦN chạy (Active)
    try:
        wks_cfg = sh.worksheet(SHEET_CONFIG_NAME)
        df_cfg = get_as_dataframe(wks_cfg, evaluate_formulas=True, dtype=str)
        # Lọc những khối đang "Chưa chốt"
        active_blocks = df_cfg[df_cfg['Trạng thái'] == 'Chưa chốt & đang cập nhật']['Block_Name'].unique().tolist()
        active_blocks = [b for b in active_blocks if b and b.strip()] # Bỏ dòng trống
        active_blocks.sort() # Sắp xếp để thứ tự luôn cố định: A -> B -> C
    except:
        print("⚠️ Lỗi đọc Config hoặc không có khối nào Active.")
        return None

    if not active_blocks:
        print("⚪ Không có khối nào cần chạy.")
        return None

    # B. Xem lịch sử lần chạy gần nhất
    last_block = None
    try:
        wks_log = sh.worksheet(SHEET_LOG_NAME)
        # Lấy 5 dòng cuối để check
        logs = wks_log.get_all_values()[-5:] 
        # Tìm ngược từ dưới lên xem dòng nào là "Auto_Runner" chạy
        for row in reversed(logs):
            # Giả sử cột 2 là User, cột 3 là Action (Block Name)
            # Cấu trúc log: [Time, User, Action, Status]
            if len(row) > 2 and row[1] == "Auto_Runner": 
                if "Chạy Khối:" in row[2]:
                    last_block = row[2].replace("Chạy Khối: ", "").strip()
                    break
    except:
        pass # Chưa có log thì mặc định chạy khối đầu tiên

    # C. Thuật toán "Tiếp sức" (Round Robin)
    if last_block and last_block in active_blocks:
        current_index = active_blocks.index(last_block)
        next_index = (current_index + 1) % len(active_blocks) # Quay vòng về 0 nếu hết
        next_block = active_blocks[next_index]
        print(f"🔄 Lần trước chạy: {last_block}. Tiếp theo -> {next_block}")
    else:
        next_block = active_blocks[0] # Chạy khối đầu tiên nếu mới tinh
        print(f"🚀 Khởi động lần đầu -> {next_block}")

    return next_block

# --- 3. HÀM XỬ LÝ DATA (Rút gọn từ app.py) ---
def run_block_logic(block_name, gc):
    print(f"▶️ Đang xử lý khối: {block_name}...")
    
    # ... (Copy phần logic fetch_data_v4 và write_strict_sync_v2 từ app.py vào đây) ...
    # Lưu ý: Vì chạy trên GitHub không có giao diện, hãy thay các lệnh st.write() bằng print()
    
    # Giả lập xử lý xong
    time.sleep(2) 
    print(f"✅ Đã xong khối {block_name}")
    return True

# --- 4. GHI LOG HỆ THỐNG ---
def log_action(gc, action, status):
    try:
        sh = gc.open_by_key(SHEET_HISTORY_ID)
        wks = sh.worksheet(SHEET_LOG_NAME)
        now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        wks.append_row([now, "Auto_Runner", action, status])
    except:
        print("Lỗi ghi log")

# --- MAIN ---
if __name__ == "__main__":
    try:
        gc_client = get_gc()
        
        # 1. Tìm người kế nhiệm
        target_block = get_next_block_to_run(gc_client)
        
        if target_block:
            # 2. Ghi log bắt đầu
            log_action(gc_client, f"Chạy Khối: {target_block}", "Đang chạy...")
            
            # 3. Chạy xử lý thật
            success = run_block_logic(target_block, gc_client)
            
            # 4. Ghi log kết thúc
            status = "Thành công" if success else "Có lỗi"
            log_action(gc_client, f"Kết thúc: {target_block}", status)
        else:
            print("💤 Không có việc gì làm.")
            
    except Exception as e:
        print(f"❌ CRITICAL ERROR: {str(e)}")
