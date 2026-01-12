import pandas as pd
import gspread
import json
import os
import time
import requests 
import traceback 
import re
from datetime import datetime, timedelta
import pytz
from google.oauth2 import service_account
from gspread_dataframe import get_as_dataframe

# ==========================================
# 0. CẤU HÌNH MÔI TRƯỜNG & HẰNG SỐ
# ==========================================
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass 

SHEET_ID = os.environ.get("HISTORY_SHEET_ID")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

# Tên các Sheet (Phải khớp 100% với app.py)
SHEET_CONFIG_NAME = "luu_cau_hinh"
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_BEHAVE_NAME = "log_hanh_vi"
SHEET_SYS_CONFIG = "sys_config"

# Định nghĩa cột (Mapping với Google Sheet)
COL_BLOCK_NAME = "Block_Name"; COL_STATUS = "Trạng thái"
COL_SRC_LINK = "Link dữ liệu lấy dữ liệu"; COL_SRC_SHEET = "Tên sheet nguồn dữ liệu gốc"
COL_TGT_LINK = "Link dữ liệu đích"; COL_TGT_SHEET = "Tên sheet dữ liệu đích"
COL_DATA_RANGE = "Vùng lấy dữ liệu"; COL_MONTH = "Tháng"
COL_FILTER = "Dieu_Kien_Loc"; COL_HEADER = "Lay_Header"; COL_WRITE_MODE = "Cach_Ghi"
COL_RESULT = "Kết quả"; COL_LOG_ROW = "Dòng dữ liệu"

# Cột hệ thống thêm vào file đích
SYS_COL_LINK = "Src_Link"; SYS_COL_SHEET = "Src_Sheet"
SYS_COL_MONTH = "Month"; SYS_COL_TIME = "Thời điểm ghi"

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
TZ_VN = pytz.timezone('Asia/Ho_Chi_Minh')

MY_BOT_LIST = [
    "kinkingetdulieu1@kinkin1.iam.gserviceaccount.com",
    "botnew@kinkin2.iam.gserviceaccount.com",
    "kinkingetdulieu3@kinkin3.iam.gserviceaccount.com",
    "kinkingetdulieu4@kinkin4.iam.gserviceaccount.com",
    "kinkingetdulieu5@kinkin5.iam.gserviceaccount.com"
]

# ==========================================
# 1. CÁC HÀM TIỆN ÍCH (UTILS)
# ==========================================
def send_telegram(msg, is_error=False):
    if not TELEGRAM_TOKEN or not CHAT_ID: return
    icon = "❌ CẢNH BÁO" if is_error else "✅ BÁO CÁO"
    formatted_msg = f"<b>[{icon}]</b>\n{msg}"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": CHAT_ID, "text": formatted_msg, "parse_mode": "HTML"}, timeout=10)
    except: pass

def get_bot_creds_by_index(idx):
    env_name = "GCP_SERVICE_ACCOUNT" if idx == 0 else f"GCP_SERVICE_ACCOUNT_{idx}"
    json_str = os.environ.get(env_name)
    if not json_str: return None
    try: return service_account.Credentials.from_service_account_info(json.loads(json_str), scopes=SCOPES)
    except: return None

def get_bot_creds_by_email(target_email):
    try:
        if target_email in MY_BOT_LIST:
            idx = MY_BOT_LIST.index(target_email)
            return get_bot_creds_by_index(idx)
        return get_bot_creds_by_index(0)
    except: return get_bot_creds_by_index(0)

def assign_bot_to_block(block_name):
    valid_bots = [b for b in MY_BOT_LIST if b.strip()]
    if not valid_bots: return MY_BOT_LIST[0]
    hash_val = sum(ord(c) for c in block_name)
    return valid_bots[hash_val % len(valid_bots)]

def safe_api_call(func, *args, **kwargs):
    for i in range(3):
        try: return func(*args, **kwargs)
        except Exception as e:
            if "429" in str(e): time.sleep((i+2)*2)
            else: time.sleep(1)
    return None

def extract_id(url):
    if not isinstance(url, str): return None
    try: return url.split("/d/")[1].split("/")[0]
    except: return None

def col_name_to_index(col):
    col = col.upper().strip(); idx=0
    for c in col: idx = idx*26 + (ord(c)-ord('A'))+1
    return idx-1

def write_behavior_log(gc, action, target, detail, status="Completed"):
    try:
        sh = gc.open_by_key(SHEET_ID)
        try: wks = sh.worksheet(SHEET_BEHAVE_NAME)
        except: wks = sh.add_worksheet(SHEET_BEHAVE_NAME, 1000, 10)
        now_str = datetime.now(TZ_VN).strftime("%d/%m/%Y %H:%M:%S")
        wks.append_row([now_str, "Auto_Runner", action, target, detail, status])
    except: pass

def update_config_result(wks_config, row_idx, status_text, range_text):
    """Cập nhật trực tiếp kết quả vào sheet Config để App hiển thị"""
    try:
        # Tìm cột Kết quả và Dòng dữ liệu (giả sử header ở dòng 1)
        headers = wks_config.row_values(1)
        try:
            col_res = headers.index(COL_RESULT) + 1
            col_row = headers.index(COL_LOG_ROW) + 1
            # Row index trong dataframe bắt đầu từ 0, trong sheet là +2 (1 cho header, 1 cho index 0)
            sheet_row = row_idx + 2 
            wks_config.update_cell(sheet_row, col_res, status_text)
            wks_config.update_cell(sheet_row, col_row, range_text)
        except: pass
    except: pass

# ==========================================
# 2. XỬ LÝ NGÀY ĐỘNG & BỘ LỌC (SMART FILTER V90)
# ==========================================
def parse_dynamic_date(val_str):
    """Xử lý TODAY, TODAY-1, YESTERDAY"""
    if not isinstance(val_str, str): return val_str
    val_upper = val_str.strip().upper().replace(" ", "").replace("'", "").replace('"', "")
    now = datetime.now(TZ_VN).replace(hour=0, minute=0, second=0, microsecond=0)
    
    if "TODAY" in val_upper:
        calc_part = val_upper.replace("TODAY()", "").replace("TODAY", "")
        if not calc_part: return now
        try:
            days = int(calc_part)
            return now + timedelta(days=days)
        except: pass

    if val_upper == "YESTERDAY": return now - timedelta(days=1)
    return val_str

def apply_smart_filter_auto(df, filter_str):
    if not filter_str or str(filter_str).strip().lower() in ['nan', 'none', 'null', '']: return df
    conditions = str(filter_str).split(';')
    current_df = df.copy()
    
    for cond in conditions:
        fs = cond.strip()
        if not fs: continue 
        op_list = [" contains ", "==", "!=", ">=", "<=", ">", "<", "="]
        op = next((o for o in op_list if o in fs), None)
        if not op: continue 
        
        parts = fs.split(op, 1)
        col_raw = parts[0].strip().replace("`", "").replace("'", "").replace('"', "")
        val_raw = parts[1].strip()
        
        val_resolved = parse_dynamic_date(val_raw)
        val_clean = val_raw[1:-1] if (isinstance(val_raw, str) and (val_raw.startswith("'") or val_raw.startswith('"'))) else val_raw
        
        real_col = next((c for c in current_df.columns if str(c).lower() == col_raw.lower()), None)
        if not real_col: continue 
        
        try:
            series = current_df[real_col]
            if op == " contains ": 
                current_df = current_df[series.astype(str).str.contains(val_clean, case=False, na=False)]
            else:
                is_dt = False; v_dt = None
                if isinstance(val_resolved, datetime):
                    is_dt = True; v_dt = pd.to_datetime(val_resolved).tz_localize(None)
                else:
                    try: 
                        s_dt = pd.to_datetime(series, dayfirst=True, errors='coerce')
                        v_dt_try = pd.to_datetime(val_clean, dayfirst=True)
                        if s_dt.notna().any() and pd.notna(v_dt_try): is_dt = True; v_dt = v_dt_try
                    except: pass
                
                is_num = False
                if not is_dt:
                    try: s_num = pd.to_numeric(series, errors='coerce'); v_num = float(val_clean); is_num = True
                    except: pass
                
                if is_dt:
                    s_dt = pd.to_datetime(series, dayfirst=True, errors='coerce')
                    if op==">": current_df=current_df[s_dt>v_dt]
                    elif op=="<": current_df=current_df[s_dt<v_dt]
                    elif op==">=": current_df=current_df[s_dt>=v_dt]
                    elif op=="<=": current_df=current_df[s_dt<=v_dt]
                    elif op in ["=","=="]: current_df=current_df[s_dt==v_dt]
                    elif op=="!=": current_df=current_df[s_dt!=v_dt]
                elif is_num:
                    if op==">": current_df=current_df[s_num>v_num]
                    elif op=="<": current_df=current_df[s_num<v_num]
                    elif op==">=": current_df=current_df[s_num>=v_num]
                    elif op=="<=": current_df=current_df[s_num<=v_num]
                    elif op in ["=","=="]: current_df=current_df[s_num==v_num]
                    elif op=="!=": current_df=current_df[s_num!=v_num]
                else:
                    s_str = series.astype(str).str.strip(); val_str_cmp = str(val_clean)
                    if op==">": current_df=current_df[s_str>val_str_cmp]
                    elif op=="<": current_df=current_df[s_str<val_str_cmp]
                    elif op==">=": current_df=current_df[s_str>=val_str_cmp]
                    elif op=="<=": current_df=current_df[s_str<=val_str_cmp]
                    elif op in ["=","=="]: current_df=current_df[s_str==val_str_cmp]
                    elif op=="!=": current_df=current_df[s_str!=val_str_cmp]
        except: pass
    return current_df

# ==========================================
# 3. LOGIC GHI & XÓA (DEEP SCAN & ID MATCH)
# ==========================================
def get_rows_to_delete_dynamic(wks, keys_to_delete):
    """
    Quét toàn bộ sheet để tìm dòng cần xóa.
    So sánh bằng ID File (trích xuất từ Link) + Sheet + Tháng.
    """
    try:
        all_values = safe_api_call(wks.get_all_values)
        if not all_values or len(all_values) < 2: return []
        
        header_row_idx = -1; headers = []
        for i in range(min(10, len(all_values))):
            row_lower = [str(c).strip().lower() for c in all_values[i]]
            if SYS_COL_LINK.lower() in row_lower and SYS_COL_SHEET.lower() in row_lower:
                header_row_idx = i; headers = row_lower; break
        
        if header_row_idx == -1: return []

        try:
            idx_link = headers.index(SYS_COL_LINK.lower())
            idx_sheet = headers.index(SYS_COL_SHEET.lower())
            idx_month = headers.index(SYS_COL_MONTH.lower())
        except ValueError: return []

        rows_to_delete = []
        total_rows = len(all_values)
        
        for i in range(header_row_idx + 1, total_rows):
            row = all_values[i]
            if len(row) <= max(idx_link, idx_sheet, idx_month): continue
            
            # [FIX] Trích xuất ID từ link trong cell để so sánh chuẩn xác
            val_link_raw = str(row[idx_link]).strip()
            val_id = extract_id(val_link_raw)
            if not val_id: val_id = val_link_raw 
            
            val_sheet = str(row[idx_sheet]).strip()
            val_month = str(row[idx_month]).strip()
            
            if (val_id, val_sheet, val_month) in keys_to_delete:
                rows_to_delete.append(i + 1)

        return rows_to_delete
    except: return []

def batch_delete_rows(sh, sheet_id, row_indices):
    if not row_indices: return
    row_indices.sort(reverse=True) 
    ranges = []
    if len(row_indices) > 0:
        start = row_indices[0]; end = start
        for r in row_indices[1:]:
            if r == start - 1: start = r
            else: ranges.append((start, end)); start = r; end = r
        ranges.append((start, end))
    requests = [{"deleteDimension": {"range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": s-1, "endIndex": e}}} for s, e in ranges]
    for i in range(0, len(requests), 100):
        safe_api_call(sh.batch_update, {'requests': requests[i:i+100]})
        time.sleep(1)

# ==========================================
# 4. CORE PIPELINE (FETCH + PROCESS + WRITE)
# ==========================================
def process_single_row_automation(row, bot_creds):
    src_link = str(row.get(COL_SRC_LINK, '')).strip()
    src_sheet_name = str(row.get(COL_SRC_SHEET, '')).strip()
    month_val = str(row.get(COL_MONTH, ''))
    tgt_link = str(row.get(COL_TGT_LINK, '')).strip()
    tgt_sheet_name = str(row.get(COL_TGT_SHEET, '')).strip() or "Tong_Hop_Data"
    
    sid = extract_id(src_link); tid = extract_id(tgt_link)
    if not sid or not tid: return "Lỗi Link", 0, ""

    try:
        # 1. Fetch Data
        gc = gspread.authorize(bot_creds)
        sh_src = safe_api_call(gc.open_by_key, sid)
        ws_src = sh_src.worksheet(src_sheet_name) if src_sheet_name else sh_src.sheet1
        data = safe_api_call(ws_src.get_all_values)
        if not data: return "Sheet trắng", 0, "0 dòng"

        # 2. Xử lý Range
        data_range = str(row.get(COL_DATA_RANGE, '')).strip().upper()
        if ":" in data_range and len(data_range) < 10 and "LẤY HẾT" not in data_range:
            try:
                s_char, e_char = data_range.split(":")
                s_idx = col_name_to_index(s_char); e_idx = col_name_to_index(e_char)
                data = [r[s_idx : e_idx + 1] for r in data]
            except: pass
        
        header_row = data[0]; body_rows = data[1:]
        unique_headers = []
        seen = {}
        for col in header_row:
            col_str = str(col).strip()
            if col_str in seen: seen[col_str] += 1; unique_headers.append(f"{col_str}_{seen[col_str]}")
            else: seen[col_str] = 0; unique_headers.append(col_str)
        
        df = pd.DataFrame(body_rows, columns=unique_headers)
        
        # Convert Numeric
        for c in df.columns:
            try: df[c] = pd.to_numeric(df[c])
            except: pass

        # 3. Lọc Smart Filter
        filter_cond = str(row.get(COL_FILTER, '')).strip()
        if filter_cond and filter_cond.lower() not in ['nan', 'none']:
            df = apply_smart_filter_auto(df, filter_cond)
        
        if df.empty: return "Thành công (Lọc hết)", 0, "0 dòng"

        # 4. Xử lý Header
        h_val = str(row.get(COL_HEADER, 'FALSE')).strip().upper()
        if h_val == 'TRUE':
            header_df = pd.DataFrame([df.columns.tolist()], columns=df.columns)
            df = pd.concat([header_df, df], ignore_index=True)

        # 5. Thêm cột Hệ thống
        df[SYS_COL_LINK] = src_link
        df[SYS_COL_SHEET] = src_sheet_name
        df[SYS_COL_MONTH] = month_val
        df[SYS_COL_TIME] = datetime.now(TZ_VN).strftime("%d/%m/%Y")

        # 6. Chuẩn bị File Đích
        sh_tgt = safe_api_call(gc.open_by_key, tid)
        try: ws_tgt = sh_tgt.worksheet(tgt_sheet_name)
        except: ws_tgt = sh_tgt.add_worksheet(tgt_sheet_name, 1000, 20)

        existing_vals = safe_api_call(ws_tgt.get_all_values)
        
        # [FIX] Khởi tạo start_row_idx sớm
        start_row_idx = len(existing_vals) + 1 if existing_vals else 1

        if not existing_vals:
            # Sheet mới -> Ghi luôn
            ws_tgt.update([df.columns.tolist()] + df.fillna("").values.tolist())
            return "Thành công (New)", len(df), f"1 - {len(df)}"
        else:
            tgt_headers = existing_vals[0]
            updated_headers = tgt_headers.copy(); added = False
            for c in [SYS_COL_LINK, SYS_COL_SHEET, SYS_COL_MONTH, SYS_COL_TIME]:
                if c not in updated_headers: updated_headers.append(c); added = True
            if added:
                ws_tgt.update(range_name="A1", values=[updated_headers])
                tgt_headers = updated_headers

            # Lọc cột an toàn
            sys_cols = [SYS_COL_LINK, SYS_COL_SHEET, SYS_COL_MONTH, SYS_COL_TIME]
            cols_to_write = []
            for h in tgt_headers:
                if h in df.columns or h in sys_cols:
                    cols_to_write.append(h)
            
            df_aligned = pd.DataFrame()
            for col in cols_to_write:
                 if col in df.columns: df_aligned[col] = df[col]
                 else: df_aligned[col] = "" 

        # 7. Logic Ghi Đè / Nối Tiếp
        w_mode = str(row.get(COL_WRITE_MODE, 'Ghi Đè')).strip()
        if "đè" in w_mode.lower() or "overwrite" in w_mode.lower():
            keys_to_delete = set([(sid, src_sheet_name, month_val)])
            rows_to_del = get_rows_to_delete_dynamic(ws_tgt, keys_to_delete)
            if rows_to_del:
                batch_delete_rows(sh_tgt, ws_tgt.id, rows_to_del)
                time.sleep(3) 
                # Cập nhật lại vị trí dòng sau khi xóa
                current_vals = safe_api_call(ws_tgt.get_all_values)
                start_row_idx = len(current_vals) + 1 if current_vals else 1

        # 8. Ghi dữ liệu
        chunk_size = 5000
        new_vals = df_aligned.fillna('').values.tolist()
        
        for i in range(0, len(new_vals), chunk_size):
            safe_api_call(ws_tgt.append_rows, new_vals[i:i+chunk_size], value_input_option='USER_ENTERED')
            time.sleep(1)

        end_row_idx = start_row_idx + len(df) - 1
        rng_str = f"{start_row_idx} - {end_row_idx}"
        return f"Thành công", len(df), rng_str

    except Exception as e:
        return f"Lỗi: {str(e)[:50]}", 0, "Error"
# ==========================================
# 5. SCHEDULER & MAIN LOOP
# ==========================================
def parse_weekday(day_str):
    map_day = {'T2':0, 'T3':1, 'T4':2, 'T5':3, 'T6':4, 'T7':5, 'CN':6}
    return map_day.get(str(day_str).upper().strip(), -1)

def check_block_due(block_name, sched_df, last_run_time):
    now = datetime.now(TZ_VN)
    row = sched_df[sched_df['Block_Name'] == block_name]
    if row.empty: return False, "No Config"
    
    row = row.iloc[0]
    l_type = str(row.get('Loai_Lich', '')).strip()
    val1 = str(row.get('Thong_So_Chinh', '')).strip()
    val2 = str(row.get('Thong_So_Phu', '')).strip()
    
    if l_type == "Không chạy": return False, "Disabled"
    
    has_run_today = False
    if last_run_time and last_run_time.date() == now.date(): has_run_today = True

    if l_type == "Chạy theo phút":
        if not last_run_time: return True, "First Run"
        try:
            if ((now - last_run_time).total_seconds()/60) >= int(val1): return True, "Interval Met"
        except: pass
        return False, "Wait Interval"
        
    try: target_hour = int(val1.split(':')[0])
    except: return False, "Bad Hour"
    time_ok = now.hour >= target_hour

    if l_type == "Hàng ngày":
        return (time_ok and not has_run_today), "Daily Check"

    if l_type == "Hàng tuần":
        correct_day = now.weekday() in [parse_weekday(d) for d in val2.split(',')]
        return (correct_day and time_ok and not has_run_today), "Weekly Check"
        
    if l_type == "Hàng tháng":
        correct_day = now.day in [int(d) for d in val2.split(',') if d.strip().isdigit()]
        return (correct_day and time_ok and not has_run_today), "Monthly Check"

    return False, "No Match"

def get_jobs(gc_master):
    try:
        sh = gc_master.open_by_key(SHEET_ID)
        df_cfg = get_as_dataframe(sh.worksheet(SHEET_CONFIG_NAME), evaluate_formulas=True, dtype=str)
        df_active = df_cfg[df_cfg[COL_STATUS].astype(str).str.contains('Chưa chốt', case=False, na=False)]
        
        active_blocks = [b for b in df_active[COL_BLOCK_NAME].unique() if b and str(b).strip() != '']

        try: df_sched = get_as_dataframe(sh.worksheet(SHEET_SYS_CONFIG), evaluate_formulas=True, dtype=str)
        except: return []

        last_run_map = {}
        try:
            logs = sh.worksheet(SHEET_LOG_NAME).get_all_values()[-300:]
            for row in reversed(logs):
                if len(row) > 11 and row[10] == "Auto": # Cột 10 là Type, 11 là Block
                    try: last_run_map[row[11]] = TZ_VN.localize(datetime.strptime(row[0], "%d/%m/%Y %H:%M:%S"))
                    except: pass
        except: pass

        jobs = []
        for blk in active_blocks:
            should, r = check_block_due(blk, df_sched, last_run_map.get(blk))
            if should: jobs.append(blk)
        return jobs
    except: return []

if __name__ == "__main__":
    start_time = datetime.now(TZ_VN).strftime('%H:%M:%S %d/%m')
    print(f"🚀 START AUTO: {start_time}")
    
    # [THÔNG BÁO] Gửi tin nhắn Telegram khi bắt đầu
    send_telegram(f"🏁 <b>Kinkin Tool Bắt Đầu Chạy</b>\n🕒 Lúc: {start_time}")

    try:
        if not SHEET_ID: exit(0)
        master_creds = get_bot_creds_by_index(0)
        gc_master = gspread.authorize(master_creds)

        jobs = get_jobs(gc_master)
        
        if not jobs:
            print("💤 Không có lịch chạy lúc này.")
            exit(0)

        success_msgs = []
        
        for blk in jobs:
            print(f"▶️ Processing: {blk}")
            bot_email = assign_bot_to_block(blk)
            bot_creds = get_bot_creds_by_email(bot_email)
            if not bot_creds: continue

            sh = gc_master.open_by_key(SHEET_ID)
            wks_cfg = sh.worksheet(SHEET_CONFIG_NAME) # Mở config để ghi kết quả
            df_cfg = get_as_dataframe(wks_cfg, evaluate_formulas=True, dtype=str)
            
            rows = df_cfg[(df_cfg[COL_BLOCK_NAME] == blk) & (df_cfg[COL_STATUS].str.contains('Chưa chốt', na=False))]
            
            total_rows = 0; log_buffer = []
            
            for i, r in rows.iterrows():
                status, count, range_str = process_single_row_automation(r, bot_creds)
                print(f"  + Row {i}: {status} ({count})")
                total_rows += count
                
                # 1. Update kết quả trực tiếp lên Config (Để app hiển thị)
                update_config_result(wks_cfg, i, status, range_str)
                
                # 2. Log kỹ thuật
                log_buffer.append([
                    datetime.now(TZ_VN).strftime("%d/%m/%Y %H:%M:%S"), r.get(COL_DATA_RANGE), r.get(COL_MONTH), "Auto_Runner",
                    r.get(COL_SRC_LINK), r.get(COL_TGT_LINK), r.get(COL_TGT_SHEET), r.get(COL_SRC_SHEET),
                    status, count, "Auto", blk
                ])
            
            # 3. Ghi log kỹ thuật vào Sheet Log
            if log_buffer:
                try: sh.worksheet(SHEET_LOG_NAME).append_rows(log_buffer)
                except: pass
            
            # 4. Ghi log hành vi tổng quan
            write_behavior_log(gc_master, "Chạy Tự Động", blk, f"Xử lý xong {total_rows} dòng", "Completed")
            success_msgs.append(f"• <b>{blk}</b>: {total_rows} dòng")
            
            time.sleep(5)

        # [THÔNG BÁO] Gửi tin nhắn Telegram khi kết thúc
        if success_msgs:
            end_time = datetime.now(TZ_VN).strftime('%H:%M')
            msg = f"✅ <b>ĐÃ XONG:</b> {end_time}\n{chr(10).join(success_msgs)}"
            send_telegram(msg)

    except Exception as e:
        print(traceback.format_exc())
        send_telegram(f"❌ <b>Lỗi Fatal:</b> {str(e)}", True)
