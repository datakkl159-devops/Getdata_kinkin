import gspread
import pandas as pd
import time
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

# --- KẾT NỐI ---
def get_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    return gspread.authorize(creds)

def get_db_sheet(sheet_name):
    client = get_client()
    # THAY 'DATABASE_MASTER' BẰNG TÊN FILE GOOGLE SHEET CỦA BẠN
    sh = client.open("DATABASE_MASTER") 
    return sh.worksheet(sheet_name)

# --- LOCK HỆ THỐNG ---
def check_sys_lock():
    try:
        ws = get_db_sheet("sys_lock")
        status = ws.acell('A1').value
        user = ws.acell('B1').value
        lock_time_str = ws.acell('C1').value
        
        # Nếu lock quá 30 phút thì tự mở (tránh treo)
        if status == 'LOCKED' and lock_time_str:
            lock_time = datetime.strptime(lock_time_str, "%Y-%m-%d %H:%M:%S")
            if (datetime.now() - lock_time).seconds > 1800:
                set_sys_lock("UNLOCKED", "Auto-Release")
                return False, ""
        
        return status == 'LOCKED', user
    except:
        return False, ""

def set_sys_lock(status, user):
    ws = get_db_sheet("sys_lock")
    ws.update_acell('A1', status)
    ws.update_acell('B1', user)
    ws.update_acell('C1', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# --- LOGIC XỬ LÝ DỮ LIỆU (CORE ETL) ---
def process_block(block_row, user_running, is_auto=False):
    client = get_client()
    start_time = time.time()
    
    # Chọn sheet log tùy theo môi trường
    log_sheet_name = "log_chay_auto_github" if is_auto else "log_lanthucthi"
    
    try:
        # 1. Mở Nguồn & Đích
        try:
            sh_src = client.open_by_url(block_row['Link_Nguon'])
            ws_src = sh_src.worksheet(block_row['Sheet_Nguon'])
            
            sh_dest = client.open_by_url(block_row['Link_Dich'])
            ws_dest = sh_dest.worksheet(block_row['Sheet_Dich'])
        except Exception as e:
            return False, f"Lỗi quyền hoặc không tìm thấy Sheet: {str(e)}"

        # 2. Đọc dữ liệu
        data_src = ws_src.get_all_records()
        df_src = pd.DataFrame(data_src)
        
        if df_src.empty:
            return True, "Nguồn không có dữ liệu"

        # 3. Chuẩn bị dữ liệu mới (Thêm 3 cột bắt buộc)
        df_src['Link file nguồn'] = block_row['Link_Nguon']
        df_src['Sheet nguồn'] = block_row['Sheet_Nguon']
        df_src['Tháng chốt'] = datetime.now().strftime("%m/%Y") # Hoặc lấy từ logic khác

        # 4. Đọc đích & Xử lý xóa cũ (Logic Traceability)
        try:
            data_dest = ws_dest.get_all_records()
            df_dest = pd.DataFrame(data_dest)
        except:
            df_dest = pd.DataFrame()

        # Nếu file đích có dữ liệu và có cột truy vết
        if not df_dest.empty and 'Link file nguồn' in df_dest.columns:
            # GIỮ LẠI các dòng KHÔNG PHẢI của nguồn này (Xóa cũ)
            df_dest_kept = df_dest[df_dest['Link file nguồn'] != block_row['Link_Nguon']]
        else:
            df_dest_kept = df_dest

        # Gộp: Dữ liệu cũ (đã lọc) + Dữ liệu mới
        # Chuyển đổi sang string để tránh lỗi JSON khi đẩy lên Sheet
        df_final = pd.concat([df_dest_kept, df_src], ignore_index=True).astype(str)

        # 5. Ghi đè vào đích
        ws_dest.clear()
        ws_dest.update([df_final.columns.values.tolist()] + df_final.values.tolist())

        # 6. Ghi Log
        duration = round(time.time() - start_time, 2)
        log_entry = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            user_running, block_row['Block_Name'],
            block_row['Link_Nguon'], block_row['Sheet_Nguon'],
            block_row['Link_Dich'], block_row['Sheet_Dich'],
            "Thành công", len(df_src), duration
        ]
        
        # Mở db log và ghi
        db_master = client.open("DATABASE_MASTER") # Thay tên file DB của bạn
        ws_log = db_master.worksheet(log_sheet_name)
        ws_log.append_row(log_entry)

        return True, f"Xong: +{len(df_src)} dòng ({duration}s)"

    except Exception as e:
        # Ghi log lỗi
        try:
            db_master = client.open("DATABASE_MASTER")
            ws_log = db_master.worksheet(log_sheet_name)
            ws_log.append_row([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user_running, block_row['Block_Name'], "ERR", "ERR", "ERR", "ERR", f"Lỗi: {str(e)}", 0, 0])
        except:
            pass
        return False, f"Lỗi: {str(e)}"
