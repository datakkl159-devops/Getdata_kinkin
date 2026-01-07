import pandas as pd
import gspread
import json
import os
import time
import requests 
import traceback 
from datetime import datetime
import pytz
from google.oauth2 import service_account
from gspread_dataframe import get_as_dataframe

# --- 0. LOAD MÔI TRƯỜNG ---
# Fix lỗi dotenv trên GitHub Actions: Nếu không có thư viện này thì bỏ qua
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass 

# Lấy thông tin cấu hình
SHEET_ID = os.environ.get("HISTORY_SHEET_ID")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

SHEET_CONFIG_NAME = "luu_cau_hinh"
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_SYS_CONFIG = "sys_config"

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
TZ_VN = pytz.timezone('Asia/Ho_Chi_Minh')

MY_BOT_LIST = [
    "kinkingetdulieu1@kinkin1.iam.gserviceaccount.com",
    "botnew@kinkin2.iam.gserviceaccount.com",
    "kinkingetdulieu3@kinkin3.iam.gserviceaccount.com",
    "kinkingetdulieu4@kinkin4.iam.gserviceaccount.com",
    "kinkingetdulieu5@kinkin5.iam.gserviceaccount.com"
]

# --- HÀM GỬI TELEGRAM ---
def send_telegram(msg, is_error=False):
    if not TELEGRAM_TOKEN or not CHAT_ID: return
    icon = "❌ CẢNH BÁO LỖI" if is_error else "✅ BÁO CÁO TỰ ĐỘNG"
    formatted_msg = f"<b>[{icon}]</b>\n{msg}"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": CHAT_ID, "text": formatted_msg, "parse_mode": "HTML"}, timeout=10)
    except: pass

# --- 1. XÁC THỰC (AUTH) ---
def get_bot_creds_by_index(idx):
    env_name = "GCP_SERVICE_ACCOUNT" if idx == 0 else f"GCP_SERVICE_ACCOUNT_{idx}"
    json_str = os.environ.get(env_name)
    if not json_str: return None
    try: return service_account.Credentials.from_service_account_info(json.loads(json_str), scopes=SCOPES)
    except: return None

def get_bot_creds_by_email(target_email):
    try: return get_bot_creds_by_index(MY_BOT_LIST.index(target_email))
    except: return get_bot_creds_by_index(0)

def assign_bot_to_block(block_name):
    # Chỉ chia việc nếu block_name là chuỗi hợp lệ
    if not isinstance(block_name, str) or not block_name.strip(): return None
    valid_bots = [b for b in MY_BOT_LIST if b.strip()]
    if not valid_bots: return None
    hash_val = sum(ord(c) for c in block_name)
    return valid_bots[hash_val % len(valid_bots)]

# --- 2. LOGIC LỊCH TRÌNH ---
def parse_weekday(day_str):
    map_day = {'T2':0, 'T3':1, 'T4':2, 'T5':3, 'T6':4, 'T7':5, 'CN':6}
    return map_day.get(str(day_str).upper().strip(), -1)

def is_block_due(block_name, sched_df, last_run_time):
    now = datetime.now(TZ_VN)
    row = sched_df[sched_df['Block_Name'] == block_name]
    if row.empty: return False 
    
    row = row.iloc[0]
    l_type = str(row.get('Loai_Lich', '')).strip()
    val1 = str(row.get('Thong_So_Chinh', '')).strip()
    val2 = str(row.get('Thong_So_Phu', '')).strip()
    
    if l_type == "Không chạy": return False
    
    if l_type == "Chạy theo phút":
        if not last_run_time: return True
        try:
            if ((now - last_run_time).total_seconds() / 60) >= int(val1): return True
        except: return False
        return False

    try: target_hour = int(val1.split(':')[0])
    except: return False
    if now.hour != target_hour: return False 
    if last_run_time and last_run_time.date() == now.date(): return False

    if l_type == "Hàng ngày": return True
    if l_type == "Hàng tuần":
        if now.weekday() in [parse_weekday(d) for d in val2.split(',')]: return True
    if l_type == "Hàng tháng":
        if now.day in [int(d) for d in val2.split(',') if d.strip().isdigit()]: return True
    return False

# --- 3. TÌM VIỆC (FIX LỖI FLOAT TRIỆT ĐỂ) ---
def get_jobs_to_run(gc_master):
    try:
        sh = gc_master.open_by_key(SHEET_ID)
        wks_cfg = sh.worksheet(SHEET_CONFIG_NAME)
        # Ép kiểu string toàn bộ bảng để an toàn hơn
        df_cfg = get_as_dataframe(wks_cfg, evaluate_formulas=True, dtype=str)
        
        # 1. Lọc lấy các dòng 'Chưa chốt'
        # Dùng .fillna('') để xử lý ô trống trước khi check contains
        df_active = df_cfg[df_cfg['Trạng thái'].fillna('').astype(str).str.contains('Chưa chốt', case=False, na=False)]
        
        # 2. Lấy danh sách Block_Name thô
        raw_blocks = df_active['Block_Name'].unique().tolist()
        
        # 3. LỌC SẠCH: Chỉ lấy chữ, bỏ qua số/rỗng/NaN (Theo yêu cầu của bạn)
        active_blocks = []
        for b in raw_blocks:
            # Nếu giá trị là None hoặc không phải là chữ (ví dụ là số float) -> KỆ NÓ (continue)
            if b is None or not isinstance(b, str):
                continue
            
            # Cắt khoảng trắng
            clean_b = b.strip()
            
            # Nếu cắt xong mà rỗng hoặc là chữ 'nan' -> KỆ NÓ
            if not clean_b or clean_b.lower() == 'nan':
                continue
            
            # Nếu là chữ đàng hoàng -> Thêm vào danh sách chạy
            active_blocks.append(clean_b)

        # Đọc lịch
        try:
            wks_sys = sh.worksheet(SHEET_SYS_CONFIG)
            df_sched = get_as_dataframe(wks_sys, evaluate_formulas=True, dtype=str)
        except: return []

        # Đọc log cũ
        last_run_map = {}
        try:
            wks_log = sh.worksheet(SHEET_LOG_NAME)
            logs = wks_log.get_all_values()[-300:] 
            for row in reversed(logs):
                if len(row) > 10 and row[10] == "Auto":
                    blk = row[11]
                    if blk not in last_run_map:
                        try: last_run_map[blk] = TZ_VN.localize(datetime.strptime(row[0], "%d/%m/%Y %H:%M:%S"))
                        except: pass
        except: pass

        jobs = []
        for block in active_blocks:
            last_time = last_run_map.get(block, None)
            if is_block_due(block, df_sched, last_time):
                jobs.append(block)
        return jobs
    except Exception as e:
        raise Exception(f"Lỗi đọc cấu hình: {str(e)}")

# --- 4. XỬ LÝ DATA ---
def safe_api_call(func, *args, **kwargs):
    for i in range(3):
        try: return func(*args, **kwargs)
        except: time.sleep(3)
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
        if not sh_src: return "Lỗi truy cập nguồn", 0
        ws_src = sh_src.worksheet(row['Tên sheet nguồn dữ liệu gốc']) if row['Tên sheet nguồn dữ liệu gốc'] else sh_src.sheet1
        data = safe_api_call(ws_src.get_all_values)
        if not data: return "Sheet trắng", 0
        
        df = pd.DataFrame(data[1:], columns=data[0])
        df['Src_Link'] = row['Link dữ liệu lấy dữ liệu']
        df['Src_Sheet'] = row['Tên sheet nguồn dữ liệu gốc']
        df['Month'] = row['Tháng']
        df['Thời điểm ghi'] = datetime.now(TZ_VN).strftime("%d/%m/%Y")
        
        tid = extract_id(row['Link dữ liệu đích'])
        sh_tgt = safe_api_call(gc.open_by_key, tid)
        if not sh_tgt: return "Lỗi truy cập đích", 0
        t_sheet = row['Tên sheet dữ liệu đích'] or "Tong_Hop_Data"
        try: ws_tgt = sh_tgt.worksheet(t_sheet)
        except: ws_tgt = sh_tgt.add_worksheet(t_sheet, 1000, 20)
        
        existing = safe_api_call(ws_tgt.get_all_values)
        if not existing: ws_tgt.update([df.columns.tolist()] + df.fillna("").values.tolist())
        else:
            df_aligned = pd.DataFrame()
            for c in existing[0]: df_aligned[c] = df[c] if c in df.columns else ""
            safe_api_call(ws_tgt.append_rows, df_aligned.fillna("").values.tolist())
        return "Thành công", len(df)
    except Exception as e: return f"Lỗi: {str(e)[:50]}", 0

# --- MAIN ---
if __name__ == "__main__":
    start_time = datetime.now(TZ_VN)
    try:
        if not SHEET_ID: 
            print("Chưa có HISTORY_SHEET_ID"); exit(0)
        
        master_creds = get_bot_creds_by_index(0)
        if not master_creds: 
            print("Chưa có Key Bot"); exit(0)

        blocks_to_run = get_jobs_to_run(gspread.authorize(master_creds))
        if not blocks_to_run: 
            print("Không có lịch chạy."); exit(0)
            
        success_log = []
        gc_master = gspread.authorize(master_creds)

        for target_block in blocks_to_run:
            bot_email = assign_bot_to_block(target_block)
            if not bot_email: continue # Bỏ qua nếu tên block lỗi

            worker_creds = get_bot_creds_by_email(bot_email)
            if worker_creds:
                sh = gc_master.open_by_key(SHEET_ID)
                ws_cfg = sh.worksheet(SHEET_CONFIG_NAME)
                df_cfg = get_as_dataframe(ws_cfg, evaluate_formulas=True, dtype=str)
                
                # Lọc lại lần nữa để chắc chắn dòng này có text
                block_rows = df_cfg[
                    (df_cfg['Block_Name'].astype(str) == str(target_block)) & 
                    (df_cfg['Trạng thái'].astype(str).str.contains('Chưa chốt', na=False))
                ]
                
                total_count = 0
                log_entries = []
                for i, row in block_rows.iterrows():
                    status, count = process_row(row, worker_creds)
                    total_count += count
                    log_entries.append([
                        datetime.now(TZ_VN).strftime("%d/%m/%Y %H:%M:%S"), row.get('Vùng lấy dữ liệu'), row.get('Tháng'), "Auto_Runner",
                        row.get('Link dữ liệu lấy dữ liệu'), row.get('Link dữ liệu đích'),
                        row.get('Tên sheet dữ liệu đích'), row.get('Tên sheet nguồn dữ liệu gốc'),
                        status, count, "Auto", target_block
                    ])
                
                if log_entries: safe_api_call(sh.worksheet(SHEET_LOG_NAME).append_rows, log_entries)
                success_log.append(f"• <b>{target_block}</b>: {total_count} dòng")

        msg = f"⏰ <b>Hoàn tất:</b> {datetime.now(TZ_VN).strftime('%H:%M %d/%m')}\n⏳ <b>Chạy trong:</b> {datetime.now(TZ_VN) - start_time}\n{chr(10).join(success_log)}"
        send_telegram(msg, is_error=False)

    except Exception as e:
        err_msg = f"⏰ <b>Lỗi lúc:</b> {datetime.now(TZ_VN).strftime('%H:%M')}\n<pre>{traceback.format_exc()[-1000:]}</pre>"
        send_telegram(err_msg, is_error=True)
        print(traceback.format_exc())
        exit(1)
