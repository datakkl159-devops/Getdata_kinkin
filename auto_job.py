import pandas as pd
import gspread
import json
import os
import time
import re
from datetime import datetime, timedelta
import pytz
from google.oauth2 import service_account
from gspread_dataframe import get_as_dataframe

# --- CẤU HÌNH ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
SHEET_CONFIG_NAME = "luu_cau_hinh"
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_SYS_CONFIG = "sys_config"
SHEET_ID = os.environ.get("HISTORY_SHEET_ID")
TZ_VN = pytz.timezone('Asia/Ho_Chi_Minh') # Múi giờ Việt Nam

MY_BOT_LIST = [
    "kinkingetdulieu1@kinkin1.iam.gserviceaccount.com",
    "botnew@kinkin2.iam.gserviceaccount.com",
    "kinkingetdulieu3@kinkin3.iam.gserviceaccount.com",
    "kinkingetdulieu4@kinkin4.iam.gserviceaccount.com",
    "kinkingetdulieu5@kinkin5.iam.gserviceaccount.com"
]

# --- 1. AUTH ---
def get_bot_creds_by_index(idx):
    env_name = "GCP_SERVICE_ACCOUNT" if idx == 0 else f"GCP_SERVICE_ACCOUNT_{idx}"
    json_str = os.environ.get(env_name)
    if not json_str: return None
    return service_account.Credentials.from_service_account_info(json.loads(json_str), scopes=SCOPES)

def get_bot_creds_by_email(target_email):
    try:
        idx = MY_BOT_LIST.index(target_email)
        return get_bot_creds_by_index(idx)
    except: return get_bot_creds_by_index(0)

def assign_bot_to_block(block_name):
    valid_bots = [b for b in MY_BOT_LIST if b.strip()]
    if not valid_bots: return None
    hash_val = sum(ord(c) for c in block_name)
    return valid_bots[hash_val % len(valid_bots)]

# --- 2. LOGIC KIỂM TRA LỊCH (SMART SCHEDULE) ---
def parse_weekday(day_str):
    """Map T2->0, CN->6"""
    map_day = {'T2':0, 'T3':1, 'T4':2, 'T5':3, 'T6':4, 'T7':5, 'CN':6}
    return map_day.get(str(day_str).upper().strip(), -1)

def is_block_due(block_name, sched_df, last_run_time):
    """Kiểm tra xem đã đến giờ chạy chưa"""
    now = datetime.now(TZ_VN)
    
    # Tìm cấu hình lịch của block này
    row = sched_df[sched_df['Block_Name'] == block_name]
    if row.empty: 
        print(f"⚪ {block_name}: Không có lịch -> Bỏ qua (Chỉ chạy tay).")
        return False # Không có lịch thì không chạy tự động
    
    row = row.iloc[0]
    l_type = str(row.get('Loai_Lich', '')).strip()
    val1 = str(row.get('Thong_So_Chinh', '')).strip() # Phút / Giờ
    val2 = str(row.get('Thong_So_Phu', '')).strip()   # Thứ / Ngày
    
    if l_type == "Không chạy": return False

    # A. CHẠY THEO PHÚT (Interval)
    if l_type == "Chạy theo phút":
        if not last_run_time: return True # Chưa chạy bao giờ -> Chạy ngay
        try:
            interval_min = int(val1)
            delta = now - last_run_time
            minutes_passed = delta.total_seconds() / 60
            if minutes_passed >= interval_min:
                print(f"🟢 {block_name}: Đã qua {int(minutes_passed)}p (Set: {interval_min}p) -> CHẠY.")
                return True
            else:
                print(f"⏳ {block_name}: Mới qua {int(minutes_passed)}p -> Đợi.")
                return False
        except: return False

    # B. HÀNG NGÀY / HÀNG TUẦN (Fixed Time)
    # Logic: Giờ hiện tại có trùng giờ cài đặt không? (Cron 30p quét 1 lần)
    # Để tránh chạy lặp lại trong cùng 1 giờ, kiểm tra xem hôm nay đã chạy chưa.
    
    target_hour = -1
    try: target_hour = int(val1.split(':')[0])
    except: return False

    # Check Giờ: Máy ảo chạy phút 5 và 35. 
    # Nếu cài 08:00, máy sẽ chạy lúc 08:05 hoặc 08:35. Cả 2 đều thỏa mãn now.hour == 8.
    if now.hour != target_hour: return False 

    # Check đã chạy hôm nay chưa?
    if last_run_time and last_run_time.date() == now.date():
        print(f"✅ {block_name}: Hôm nay đã chạy rồi -> Đợi mai.")
        return False

    if l_type == "Hàng ngày":
        print(f"🟢 {block_name}: Đúng giờ hàng ngày -> CHẠY.")
        return True

    if l_type == "Hàng tuần":
        target_days = [parse_weekday(d) for d in val2.split(',')]
        if now.weekday() in target_days:
            print(f"🟢 {block_name}: Đúng thứ, đúng giờ -> CHẠY.")
            return True
    
    return False

# --- 3. TÌM VIỆC ---
def get_jobs_to_run(gc_master):
    sh = gc_master.open_by_key(SHEET_ID)
    
    # Lấy list Active
    wks_cfg = sh.worksheet(SHEET_CONFIG_NAME)
    df_cfg = get_as_dataframe(wks_cfg, evaluate_formulas=True, dtype=str)
    active_blocks = df_cfg[df_cfg['Trạng thái'].str.contains('Chưa chốt', na=False, case=False)]['Block_Name'].unique().tolist()
    active_blocks = [b for b in active_blocks if b.strip()]

    # Lấy Config Lịch
    try:
        wks_sys = sh.worksheet(SHEET_SYS_CONFIG)
        df_sched = get_as_dataframe(wks_sys, evaluate_formulas=True, dtype=str)
    except: return []

    # Lấy Log chạy lần cuối
    last_run_map = {}
    try:
        wks_log = sh.worksheet(SHEET_LOG_NAME)
        logs = wks_log.get_all_values()[-200:] # Quét sâu hơn
        for row in reversed(logs):
            if len(row) > 0 and row[1] == "Auto_Runner": # Chỉ tính Auto chạy
                blk = row[-1]
                if blk not in last_run_map:
                    try:
                        # Parse time format: 02/01/2026 15:30:00
                        dt = datetime.strptime(row[0], "%d/%m/%Y %H:%M:%S")
                        last_run_map[blk] = TZ_VN.localize(dt)
                    except: pass
    except: pass

    # Duyệt từng block xem cái nào đến giờ
    jobs = []
    print(f"🕒 Time Check: {datetime.now(TZ_VN).strftime('%H:%M %d/%m')}")
    for block in active_blocks:
        last_time = last_run_map.get(block, None)
        if is_block_due(block, df_sched, last_time):
            jobs.append(block)
            
    return jobs

# --- 4. XỬ LÝ (RÚT GỌN) ---
def safe_api_call(func, *args, **kwargs):
    for i in range(3):
        try: return func(*args, **kwargs)
        except: time.sleep(2)
    return None

def extract_id(url):
    try: return url.split("/d/")[1].split("/")[0]
    except: return None

def process_row(row, bot_creds):
    try:
        sid = extract_id(row['Link dữ liệu lấy dữ liệu'])
        if not sid: return "Lỗi Link", 0
        gc = gspread.authorize(bot_creds)
        sh_src = safe_api_call(gc.open_by_key, sid)
        ws_src = sh_src.worksheet(row['Tên sheet nguồn dữ liệu gốc']) if row['Tên sheet nguồn dữ liệu gốc'] else sh_src.sheet1
        data = safe_api_call(ws_src.get_all_values)
        if not data: return "Sheet trắng", 0
        
        headers = data[0]; body = data[1:]
        df = pd.DataFrame(body, columns=headers)
        
        df['Src_Link'] = row['Link dữ liệu lấy dữ liệu']
        df['Src_Sheet'] = row['Tên sheet nguồn dữ liệu gốc']
        df['Month'] = row['Tháng']
        df['Thời điểm ghi'] = datetime.now(TZ_VN).strftime("%d/%m/%Y")
        
        tid = extract_id(row['Link dữ liệu đích'])
        sh_tgt = safe_api_call(gc.open_by_key, tid)
        t_sheet = row['Tên sheet dữ liệu đích'] or "Tong_Hop_Data"
        try: ws_tgt = sh_tgt.worksheet(t_sheet)
        except: ws_tgt = sh_tgt.add_worksheet(t_sheet, 1000, 20)
        
        existing = safe_api_call(ws_tgt.get_all_values)
        if not existing:
            ws_tgt.update([df.columns.tolist()] + df.fillna("").values.tolist())
        else:
            tgt_cols = existing[0]
            df_aligned = pd.DataFrame()
            for c in tgt_cols:
                df_aligned[c] = df[c] if c in df.columns else ""
            safe_api_call(ws_tgt.append_rows, df_aligned.fillna("").values.tolist())
            
        return "Thành công", len(df)
    except Exception as e: return f"Lỗi: {str(e)[:20]}", 0

# --- MAIN ---
if __name__ == "__main__":
    if not SHEET_ID: print("❌ Thiếu Secret"); exit(1)
    
    master_creds = get_bot_creds_by_index(0)
    gc_master = gspread.authorize(master_creds)
    
    # 1. Tìm việc
    blocks_to_run = get_jobs_to_run(gc_master)
    
    if not blocks_to_run:
        print("💤 Không có việc gì để làm lúc này.")
        exit(0)
        
    # 2. Chạy từng việc
    for target_block in blocks_to_run:
        print(f"🚀 Bắt đầu chạy: {target_block}")
        bot_email = assign_bot_to_block(target_block)
        worker_creds = get_bot_creds_by_email(bot_email)
        
        if worker_creds:
            sh = gc_master.open_by_key(SHEET_ID)
            ws_cfg = sh.worksheet(SHEET_CONFIG_NAME)
            df_cfg = get_as_dataframe(ws_cfg, evaluate_formulas=True, dtype=str)
            block_rows = df_cfg[(df_cfg['Block_Name'] == target_block) & (df_cfg['Trạng thái'].str.contains('Chưa chốt', na=False))]
            
            log_rows = []
            now_str = datetime.now(TZ_VN).strftime("%d/%m/%Y %H:%M:%S")
            
            for i, row in block_rows.iterrows():
                print(f"  > Row {i}...")
                status, count = process_row(row, worker_creds)
                log_rows.append([
                    now_str, row.get('Vùng lấy dữ liệu'), row.get('Tháng'), "Auto_Runner",
                    row.get('Link dữ liệu lấy dữ liệu'), row.get('Link dữ liệu đích'),
                    row.get('Tên sheet dữ liệu đích'), row.get('Tên sheet nguồn dữ liệu gốc'),
                    status, count, "Auto", target_block
                ])
            
            try: sh.worksheet(SHEET_LOG_NAME).append_rows(log_rows)
            except: pass
            print(f"✅ Xong {target_block}")
