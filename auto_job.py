import pandas as pd
import gspread
import json
import os
import time
import requests # Thư viện gửi Telegram
import traceback # Thư viện bắt lỗi chi tiết
from datetime import datetime
import pytz
from google.oauth2 import service_account
from gspread_dataframe import get_as_dataframe
from dotenv import load_dotenv # Thư viện đọc file .env

# --- 0. LOAD MÔI TRƯỜNG & CẤU HÌNH ---
# Tự động tìm file .env cùng thư mục để lấy Key
load_dotenv()

# Lấy thông tin từ file .env (hoặc biến môi trường nếu chạy GitHub Actions)
SHEET_ID = os.environ.get("HISTORY_SHEET_ID")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

# Tên các sheet cấu hình
SHEET_CONFIG_NAME = "luu_cau_hinh"
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_SYS_CONFIG = "sys_config"

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
TZ_VN = pytz.timezone('Asia/Ho_Chi_Minh') # Múi giờ Việt Nam

# Danh sách Bot (Có thể thêm bot vào file .env nếu muốn bảo mật hơn)
MY_BOT_LIST = [
    "kinkingetdulieu1@kinkin1.iam.gserviceaccount.com",
    "botnew@kinkin2.iam.gserviceaccount.com",
    "kinkingetdulieu3@kinkin3.iam.gserviceaccount.com",
    "kinkingetdulieu4@kinkin4.iam.gserviceaccount.com",
    "kinkingetdulieu5@kinkin5.iam.gserviceaccount.com"
]

# --- HÀM GỬI TELEGRAM (QUAN TRỌNG) ---
def send_telegram(msg, is_error=False):
    """Gửi thông báo về Telegram. Nếu chưa cấu hình thì bỏ qua."""
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    
    icon = "❌ CẢNH BÁO LỖI" if is_error else "✅ BÁO CÁO TỰ ĐỘNG"
    # Format tin nhắn HTML
    formatted_msg = f"<b>[{icon}]</b>\n{msg}"
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": CHAT_ID, 
            "text": formatted_msg, 
            "parse_mode": "HTML"
        }, timeout=10)
    except Exception as e:
        print(f"Không gửi được Telegram: {e}")

# --- 1. XÁC THỰC (AUTH) ---
def get_bot_creds_by_index(idx):
    # Ưu tiên lấy từ .env (VPS), nếu không có thì thử lấy kiểu GitHub Secrets
    env_name = "GCP_SERVICE_ACCOUNT" if idx == 0 else f"GCP_SERVICE_ACCOUNT_{idx}"
    json_str = os.environ.get(env_name)
    
    if not json_str: return None
    try:
        return service_account.Credentials.from_service_account_info(json.loads(json_str), scopes=SCOPES)
    except:
        return None

def get_bot_creds_by_email(target_email):
    try:
        idx = MY_BOT_LIST.index(target_email)
        return get_bot_creds_by_index(idx)
    except: return get_bot_creds_by_index(0)

def assign_bot_to_block(block_name):
    valid_bots = [b for b in MY_BOT_LIST if b.strip()]
    if not valid_bots: return None
    # Chia việc cho bot dựa trên tên Block để cố định bot cho 1 việc
    hash_val = sum(ord(c) for c in str(block_name))
    return valid_bots[hash_val % len(valid_bots)]

# --- 2. LOGIC LỊCH TRÌNH (SMART SCHEDULE) ---
def parse_weekday(day_str):
    map_day = {'T2':0, 'T3':1, 'T4':2, 'T5':3, 'T6':4, 'T7':5, 'CN':6}
    return map_day.get(str(day_str).upper().strip(), -1)

def is_block_due(block_name, sched_df, last_run_time):
    now = datetime.now(TZ_VN)
    
    # Tìm cấu hình lịch của block này
    row = sched_df[sched_df['Block_Name'] == block_name]
    if row.empty: return False # Không có lịch -> Không chạy tự động
    
    row = row.iloc[0]
    l_type = str(row.get('Loai_Lich', '')).strip()
    val1 = str(row.get('Thong_So_Chinh', '')).strip()
    val2 = str(row.get('Thong_So_Phu', '')).strip()
    
    if l_type == "Không chạy": return False

    # A. CHẠY THEO PHÚT
    if l_type == "Chạy theo phút":
        if not last_run_time: return True
        try:
            interval_min = int(val1)
            delta = now - last_run_time
            if (delta.total_seconds() / 60) >= interval_min: return True
            return False
        except: return False

    # B. HÀNG NGÀY / TUẦN / THÁNG
    target_hour = -1
    try: target_hour = int(val1.split(':')[0])
    except: return False

    if now.hour != target_hour: return False 

    # Nếu hôm nay đã chạy rồi thì thôi (tránh chạy lặp lại trong cùng 1 giờ)
    if last_run_time and last_run_time.date() == now.date(): return False

    if l_type == "Hàng ngày": return True

    if l_type == "Hàng tuần":
        target_days = [parse_weekday(d) for d in val2.split(',')]
        if now.weekday() in target_days: return True
        
    if l_type == "Hàng tháng":
        target_dates = [int(d) for d in val2.split(',') if d.strip().isdigit()]
        if now.day in target_dates: return True
    
    return False

# --- 3. TÌM VIỆC (ĐÃ SỬA LỖI FLOAT STRIP) ---
def get_jobs_to_run(gc_master):
    try:
        sh = gc_master.open_by_key(SHEET_ID)
        
        # Đọc Sheet Cấu hình
        wks_cfg = sh.worksheet(SHEET_CONFIG_NAME)
        # Ép kiểu tất cả về str để tránh lỗi float
        df_cfg = get_as_dataframe(wks_cfg, evaluate_formulas=True, dtype=str)
        
        # Lọc các block có trạng thái 'Chưa chốt'
        # [FIX QUAN TRỌNG]: Ép kiểu str(b) trước khi .strip() để tránh lỗi AttributeError
        raw_blocks = df_cfg[df_cfg['Trạng thái'].astype(str).str.contains('Chưa chốt', na=False, case=False)]['Block_Name'].unique().tolist()
        active_blocks = [str(b).strip() for b in raw_blocks if str(b).strip().lower() not in ['nan', 'none', '', '0']]

        # Đọc Sheet Lịch (Sys_Config)
        try:
            wks_sys = sh.worksheet(SHEET_SYS_CONFIG)
            df_sched = get_as_dataframe(wks_sys, evaluate_formulas=True, dtype=str)
        except: return [] # Chưa cấu hình lịch

        # Đọc Log chạy lần cuối để tính giờ
        last_run_map = {}
        try:
            wks_log = sh.worksheet(SHEET_LOG_NAME)
            # Lấy 300 dòng cuối để check cho nhanh
            logs = wks_log.get_all_values()[-300:] 
            for row in reversed(logs):
                if len(row) > 10 and row[10] == "Auto": # Cột loại chạy
                    blk = row[11] # Cột tên Block
                    if blk not in last_run_map:
                        try:
                            dt = datetime.strptime(row[0], "%d/%m/%Y %H:%M:%S")
                            last_run_map[blk] = TZ_VN.localize(dt)
                        except: pass
        except: pass

        # Quyết định chạy cái nào
        jobs = []
        for block in active_blocks:
            last_time = last_run_map.get(block, None)
            if is_block_due(block, df_sched, last_time):
                jobs.append(block)
        return jobs
    except Exception as e:
        # Nếu lỗi ở bước tìm việc, ném lỗi ra ngoài để gửi Telegram
        raise Exception(f"Lỗi khi đọc file cấu hình/lịch: {str(e)}")

# --- 4. XỬ LÝ DATA (HELPER) ---
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
        # 1. Kết nối nguồn
        sid = extract_id(row['Link dữ liệu lấy dữ liệu'])
        if not sid: return "Lỗi Link", 0
        
        gc = gspread.authorize(bot_creds)
        sh_src = safe_api_call(gc.open_by_key, sid)
        if not sh_src: return "Không quyền truy cập nguồn", 0
        
        ws_name = row['Tên sheet nguồn dữ liệu gốc']
        ws_src = sh_src.worksheet(ws_name) if ws_name else sh_src.sheet1
        
        # Lấy data
        data = safe_api_call(ws_src.get_all_values)
        if not data: return "Sheet trắng", 0
        
        # Chuyển thành DataFrame
        headers = data[0]; body = data[1:]
        df = pd.DataFrame(body, columns=headers)
        
        # Thêm cột hệ thống
        df['Src_Link'] = row['Link dữ liệu lấy dữ liệu']
        df['Src_Sheet'] = row['Tên sheet nguồn dữ liệu gốc']
        df['Month'] = row['Tháng']
        df['Thời điểm ghi'] = datetime.now(TZ_VN).strftime("%d/%m/%Y")
        
        # 2. Kết nối đích
        tid = extract_id(row['Link dữ liệu đích'])
        sh_tgt = safe_api_call(gc.open_by_key, tid)
        if not sh_tgt: return "Không quyền truy cập đích", 0
        
        t_sheet = row['Tên sheet dữ liệu đích'] or "Tong_Hop_Data"
        try: ws_tgt = sh_tgt.worksheet(t_sheet)
        except: ws_tgt = sh_tgt.add_worksheet(t_sheet, 1000, 20)
        
        # Ghi nối tiếp
        existing = safe_api_call(ws_tgt.get_all_values)
        if not existing:
            # Nếu chưa có gì thì ghi cả header
            ws_tgt.update([df.columns.tolist()] + df.fillna("").values.tolist())
        else:
            # Nếu có rồi thì map cột cho đúng
            tgt_cols = existing[0]
            df_aligned = pd.DataFrame()
            for c in tgt_cols:
                df_aligned[c] = df[c] if c in df.columns else ""
            safe_api_call(ws_tgt.append_rows, df_aligned.fillna("").values.tolist())
            
        return "Thành công", len(df)
    except Exception as e: return f"Lỗi: {str(e)[:50]}", 0

# --- MAIN RUNNER ---
if __name__ == "__main__":
    start_time = datetime.now(TZ_VN)
    print(f"--- BẮT ĐẦU CHẠY: {start_time} ---")
    
    try:
        # Kiểm tra biến môi trường quan trọng
        if not SHEET_ID: raise Exception("Thiếu HISTORY_SHEET_ID trong file .env hoặc Secrets")
        
        # Lấy Master Creds để đọc file cấu hình
        master_creds = get_bot_creds_by_index(0)
        if not master_creds: raise Exception("Không tìm thấy Key Google Service Account (Index 0)")
        
        gc_master = gspread.authorize(master_creds)
        
        # 1. Tìm những việc cần làm ngay bây giờ
        blocks_to_run = get_jobs_to_run(gc_master)
        
        if not blocks_to_run:
            print("💤 Không có lịch chạy phù hợp lúc này.")
            # Kết thúc êm đẹp, không báo Telegram cho đỡ phiền
            exit(0)
            
        success_log = []
        
        # 2. Chạy từng Block
        for target_block in blocks_to_run:
            print(f"🚀 Đang chạy Block: {target_block}")
            
            # Lấy Bot chuyên trách cho block này
            bot_email = assign_bot_to_block(target_block)
            worker_creds = get_bot_creds_by_email(bot_email)
            
            if worker_creds:
                sh = gc_master.open_by_key(SHEET_ID)
                ws_cfg = sh.worksheet(SHEET_CONFIG_NAME)
                df_cfg = get_as_dataframe(ws_cfg, evaluate_formulas=True, dtype=str)
                
                # Lọc lấy các dòng lệnh của Block này
                block_rows = df_cfg[
                    (df_cfg['Block_Name'] == target_block) & 
                    (df_cfg['Trạng thái'].astype(str).str.contains('Chưa chốt', na=False))
                ]
                
                now_str = datetime.now(TZ_VN).strftime("%d/%m/%Y %H:%M:%S")
                total_count = 0
                log_entries = []
                
                # Chạy từng dòng lệnh (từng file nguồn)
                for i, row in block_rows.iterrows():
                    status, count = process_row(row, worker_creds)
                    total_count += count
                    
                    # Chuẩn bị log để ghi vào Sheet
                    log_entries.append([
                        now_str, row.get('Vùng lấy dữ liệu'), row.get('Tháng'), "Auto_Runner",
                        row.get('Link dữ liệu lấy dữ liệu'), row.get('Link dữ liệu đích'),
                        row.get('Tên sheet dữ liệu đích'), row.get('Tên sheet nguồn dữ liệu gốc'),
                        status, count, "Auto", target_block
                    ])
                    print(f"  -> {row.get('Tên sheet nguồn dữ liệu gốc')}: {status} ({count} dòng)")
                
                # Ghi Log vào Sheet 1 lần cho cả Block
                if log_entries:
                    try: sh.worksheet(SHEET_LOG_NAME).append_rows(log_entries)
                    except: pass
                
                success_log.append(f"• <b>{target_block}</b>: {total_count} dòng (Bot: {bot_email})")

        # 3. Gửi báo cáo thành công về Telegram
        duration = datetime.now(TZ_VN) - start_time
        msg = (
            f"⏰ <b>Hoàn tất lúc:</b> {datetime.now(TZ_VN).strftime('%H:%M %d/%m')}\n"
            f"⏳ <b>Thời gian xử lý:</b> {duration}\n"
            f"-------------------\n"
            f"{chr(10).join(success_log)}"
        )
        send_telegram(msg, is_error=False)

    except Exception as e:
        # 4. BẮT LỖI TOÀN CỤC & GỬI TELEGRAM
        err_trace = traceback.format_exc()
        # Cắt ngắn lỗi nếu quá dài (Telegram giới hạn 4096 ký tự)
        err_short = err_trace[-1000:] 
        
        print("❌ CÓ LỖI XẢY RA!")
        print(err_trace)
        
        err_msg = (
            f"⏰ <b>Lỗi lúc:</b> {datetime.now(TZ_VN).strftime('%H:%M %d/%m')}\n"
            f"-------------------\n"
            f"<pre>{err_short}</pre>"
        )
        send_telegram(err_msg, is_error=True)
        # Thoát với mã lỗi để hệ thống (Task Scheduler/GitHub) biết
        exit(1)
