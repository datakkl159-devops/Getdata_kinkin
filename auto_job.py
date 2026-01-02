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
SHEET_LOG_NAME = "log_lanthucthi"
# Lấy ID Sheet từ Secret, nếu không có thì báo lỗi ngay
SHEET_ID = os.environ.get("HISTORY_SHEET_ID")

# Danh sách Email Bot khớp với thứ tự Secret đã cài
MY_BOT_LIST = [
    "kinkingetdulieu1@kinkin1.iam.gserviceaccount.com", # Ứng với GCP_SERVICE_ACCOUNT
    "botnew@kinkin2.iam.gserviceaccount.com",          # Ứng với GCP_SERVICE_ACCOUNT_1
    "kinkingetdulieu3@kinkin3.iam.gserviceaccount.com", # Ứng với GCP_SERVICE_ACCOUNT_2
    "kinkingetdulieu4@kinkin4.iam.gserviceaccount.com", # Ứng với GCP_SERVICE_ACCOUNT_3
    "kinkingetdulieu5@kinkin5.iam.gserviceaccount.com"  # Ứng với GCP_SERVICE_ACCOUNT_4
]

# --- 1. HÀM KẾT NỐI (BẢO MẬT) ---
def get_bot_creds_by_email(target_email):
    """Lấy Key JSON từ biến môi trường dựa trên Email"""
    try:
        if target_email not in MY_BOT_LIST:
            # Fallback về Bot Master nếu không tìm thấy
            return get_bot_creds_by_index(0)
        
        idx = MY_BOT_LIST.index(target_email)
        return get_bot_creds_by_index(idx)
    except: return None

def get_bot_creds_by_index(idx):
    """Lấy Key theo số thứ tự Secret"""
    env_name = "GCP_SERVICE_ACCOUNT" if idx == 0 else f"GCP_SERVICE_ACCOUNT_{idx}"
    json_str = os.environ.get(env_name)
    if not json_str:
        print(f"❌ Lỗi: Chưa cài Secret {env_name} trên GitHub.")
        return None
    return service_account.Credentials.from_service_account_info(json.loads(json_str), scopes=SCOPES)

def assign_bot_to_block(block_name):
    """Logic hash để tìm Bot (Khớp với App.py)"""
    valid_bots = [b for b in MY_BOT_LIST if b.strip()]
    if not valid_bots: return None
    hash_val = sum(ord(c) for c in block_name)
    return valid_bots[hash_val % len(valid_bots)]

# --- 2. LOGIC TÌM VIỆC (ROUND ROBIN) ---
def get_next_block_to_run(gc_master):
    """Tìm khối tiếp theo dựa trên lịch sử"""
    try:
        sh = gc_master.open_by_key(SHEET_ID)
        
        # A. Lấy danh sách khối đang Active
        wks_cfg = sh.worksheet(SHEET_CONFIG_NAME)
        df_cfg = get_as_dataframe(wks_cfg, evaluate_formulas=True, dtype=str)
        # Chỉ lấy những khối có trạng thái "Chưa chốt..."
        active_df = df_cfg[df_cfg['Trạng thái'].str.contains('Chưa chốt', na=False, case=False)]
        active_blocks = sorted(active_df['Block_Name'].unique().tolist())
        active_blocks = [b for b in active_blocks if b.strip()]
        
        if not active_blocks:
            print("💤 Không có khối nào đang mở (Active).")
            return None

        # B. Xem lịch sử chạy gần nhất
        last_block = None
        try:
            wks_log = sh.worksheet(SHEET_LOG_NAME)
            logs = wks_log.get_all_values()[-20:] # Lấy 20 dòng cuối
            # Tìm ngược từ dưới lên log của Auto_Runner
            for row in reversed(logs):
                # Giả định cột log: [Time, ConfigInfo..., User, ..., BlockName]
                # Log của Auto_Runner thường có User="Auto_Runner"
                if "Auto_Runner" in row:
                    # Tìm tên khối trong log (thường ở cuối hoặc cột Block)
                    # Cách đơn giản: Check xem row có chứa tên block nào trong active_blocks không
                    for blk in active_blocks:
                        if blk in row:
                            last_block = blk
                            break
                    if last_block: break
        except: pass

        # C. Chọn người kế nhiệm
        print(f"📜 Danh sách Active: {active_blocks}")
        print(f"⏮️ Lần trước chạy: {last_block}")
        
        if last_block and last_block in active_blocks:
            idx = active_blocks.index(last_block)
            next_idx = (idx + 1) % len(active_blocks)
            return active_blocks[next_idx]
        else:
            return active_blocks[0] # Chạy cái đầu tiên nếu mới tinh
            
    except Exception as e:
        print(f"❌ Lỗi đọc Config: {e}")
        return None

# --- 3. LOGIC XỬ LÝ DATA (CORE ETL) ---
def safe_api_call(func, *args, **kwargs):
    for i in range(5):
        try: return func(*args, **kwargs)
        except Exception as e:
            if "429" in str(e): time.sleep((2**i)+3)
            else: time.sleep(2)
    return None

def extract_id(url):
    try: return url.split("/d/")[1].split("/")[0]
    except: return None

def apply_filter(df, filter_str):
    """Logic lọc đơn giản cho script chạy ngầm"""
    if not filter_str: return df
    try:
        # (Giản lược logic filter để script nhẹ hơn, hoặc copy full từ app.py nếu cần)
        # Ở đây dùng pandas query cơ bản
        return df # Placeholder, bạn có thể paste full logic V108 vào đây nếu muốn filter kỹ
    except: return df

def process_row(row, bot_creds):
    """Xử lý 1 dòng lệnh"""
    try:
        # Đọc Nguồn
        sid = extract_id(row['Link dữ liệu lấy dữ liệu'])
        if not sid: return "Lỗi Link", 0
        
        gc = gspread.authorize(bot_creds)
        sh_src = safe_api_call(gc.open_by_key, sid)
        ws_src = sh_src.worksheet(row['Tên sheet nguồn dữ liệu gốc']) if row['Tên sheet nguồn dữ liệu gốc'] else sh_src.sheet1
        data = safe_api_call(ws_src.get_all_values)
        if not data: return "Sheet trắng", 0
        
        headers = data[0]; body = data[1:]
        df = pd.DataFrame(body, columns=headers)
        
        # Thêm cột hệ thống
        df['Src_Link'] = row['Link dữ liệu lấy dữ liệu']
        df['Src_Sheet'] = row['Tên sheet nguồn dữ liệu gốc']
        df['Month'] = row['Tháng']
        df['Thời điểm ghi'] = datetime.now().strftime("%d/%m/%Y")
        
        # Ghi Đích
        tid = extract_id(row['Link dữ liệu đích'])
        sh_tgt = safe_api_call(gc.open_by_key, tid)
        t_sheet = row['Tên sheet dữ liệu đích'] or "Tong_Hop_Data"
        
        try: ws_tgt = sh_tgt.worksheet(t_sheet)
        except: ws_tgt = sh_tgt.add_worksheet(t_sheet, 1000, 20)
        
        # Append (An toàn nhất cho chạy tự động)
        existing = safe_api_call(ws_tgt.get_all_values)
        if not existing:
            ws_tgt.update([df.columns.tolist()] + df.fillna("").values.tolist())
        else:
            # Map cột cho đúng
            tgt_cols = existing[0]
            df_final = pd.DataFrame()
            for c in tgt_cols:
                df_final[c] = df[c] if c in df.columns else ""
            safe_api_call(ws_tgt.append_rows, df_final.fillna("").values.tolist())
            
        return "Thành công", len(df)
    except Exception as e:
        print(f"Lỗi dòng: {e}")
        return f"Lỗi: {str(e)[:20]}", 0

# --- MAIN ---
if __name__ == "__main__":
    if not SHEET_ID:
        print("❌ Lỗi: Chưa cấu hình Secret HISTORY_SHEET_ID")
        exit(1)

    print("🚀 Auto Runner bắt đầu...")
    
    # 1. Dùng Master Bot để tìm việc
    master_creds = get_bot_creds_by_index(0)
    gc_master = gspread.authorize(master_creds)
    
    target_block = get_next_block_to_run(gc_master)
    
    if target_block:
        print(f"🎯 Khối được chọn: {target_block}")
        
        # 2. Xác định Bot phụ trách khối này
        assigned_bot_email = assign_bot_to_block(target_block)
        print(f"🤖 Bot thực thi: {assigned_bot_email}")
        
        worker_creds = get_bot_creds_by_email(assigned_bot_email)
        if not worker_creds:
            print("❌ Không lấy được quyền Bot con. Dừng.")
            exit(1)
            
        # 3. Lấy chi tiết các dòng lệnh trong khối
        sh = gc_master.open_by_key(SHEET_ID)
        ws_cfg = sh.worksheet(SHEET_CONFIG_NAME)
        df_cfg = get_as_dataframe(ws_cfg, evaluate_formulas=True, dtype=str)
        
        # Lọc ra các dòng thuộc khối này VÀ đang active
        block_rows = df_cfg[
            (df_cfg['Block_Name'] == target_block) & 
            (df_cfg['Trạng thái'].str.contains('Chưa chốt', na=False))
        ]
        
        # 4. Chạy từng dòng
        total_success = 0
        log_rows = []
        now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        
        for i, row in block_rows.iterrows():
            print(f" > Xử lý dòng {i}...")
            status, count = process_row(row, worker_creds)
            
            if status == "Thành công": total_success += 1
            
            # Chuẩn bị log
            log_rows.append([
                now, row.get('Vùng lấy dữ liệu'), row.get('Tháng'), "Auto_Runner",
                row.get('Link dữ liệu lấy dữ liệu'), row.get('Link dữ liệu đích'),
                row.get('Tên sheet dữ liệu đích'), row.get('Tên sheet nguồn dữ liệu gốc'),
                status, count, "Auto", target_block
            ])
            
        # 5. Ghi Log tập trung vào Master Sheet
        try:
            ws_log = sh.worksheet(SHEET_LOG_NAME)
            ws_log.append_rows(log_rows)
            print("✅ Đã ghi log.")
        except:
            print("⚠️ Lỗi ghi log.")
            
    else:
        print("💤 Hệ thống nghỉ ngơi (Không có khối nào Active).")
