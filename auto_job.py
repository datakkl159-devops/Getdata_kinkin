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

# --- 0. LOAD MÔI TRƯỜNG ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass 

SHEET_ID = os.environ.get("HISTORY_SHEET_ID")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

# Tên các Sheet Log
SHEET_CONFIG_NAME = "luu_cau_hinh"
SHEET_LOG_NAME = "log_lanthucthi"  # Log chi tiết kỹ thuật
SHEET_BEHAVE_NAME = "log_hanh_vi"  # Log hành vi tổng quan (MỚI)
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

# --- 1. HELPER & AUTH ---
def get_bot_creds_by_index(idx):
    env_name = "GCP_SERVICE_ACCOUNT" if idx == 0 else f"GCP_SERVICE_ACCOUNT_{idx}"
    json_str = os.environ.get(env_name)
    if not json_str: return None
    try: return service_account.Credentials.from_service_account_info(json.loads(json_str), scopes=SCOPES)
    except: return None

def col_name_to_index(col_name):
    col_name = col_name.upper().strip()
    idx = 0
    for char in col_name: idx = idx * 26 + (ord(char) - ord('A')) + 1
    return idx - 1

def deduplicate_headers(headers):
    seen = {}; new_headers = []
    for h in headers:
        h_str = str(h).strip()
        if not h_str: h_str = "Unknown"
        if h_str in seen: seen[h_str] += 1; new_headers.append(f"{h_str}_{seen[h_str]}")
        else: seen[h_str] = 0; new_headers.append(h_str)
    return new_headers

def safe_api_call(func, *args, **kwargs):
    for i in range(3):
        try: return func(*args, **kwargs)
        except: time.sleep(2)
    return None

def extract_id(url):
    try: return url.split("/d/")[1].split("/")[0]
    except: return None

# --- [NEW] HÀM GHI LOG HÀNH VI (log_hanh_vi) ---
def write_behavior_log(gc, action, target, detail, status="Completed"):
    """Ghi log tổng quan vào sheet log_hanh_vi"""
    try:
        sh = gc.open_by_key(SHEET_ID)
        try: wks = sh.worksheet(SHEET_BEHAVE_NAME)
        except: wks = sh.add_worksheet(SHEET_BEHAVE_NAME, 1000, 10)
        
        now_str = datetime.now(TZ_VN).strftime("%d/%m/%Y %H:%M:%S")
        # Cấu trúc cột: Thời gian | User | Hành động | Đối tượng | Chi tiết | Trạng thái
        row_data = [now_str, "Auto_Runner", action, target, detail, status]
        
        wks.append_row(row_data)
    except Exception as e:
        print(f"Không ghi được log hành vi: {e}")

# --- 2. XỬ LÝ NGÀY ĐỘNG (TODAY-1) ---
def parse_dynamic_date(val_str):
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

# --- 3. BỘ LỌC THÔNG MINH ---
def apply_smart_filter(df, filter_str):
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
                    is_dt = True; v_dt = pd.to_datetime(val_resolved)
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

# --- 4. XÓA DỮ LIỆU CŨ ---
def delete_old_data(wks, link_src, sheet_src, month_src):
    try:
        all_vals = safe_api_call(wks.get_all_values)
        if not all_vals or len(all_vals) < 2: return
        headers = [str(c).lower().strip() for c in all_vals[0]]
        try:
            idx_link = headers.index('src_link')
            idx_sheet = headers.index('src_sheet')
            idx_month = headers.index('month')
        except: return 

        rows_to_delete = []
        for i in range(len(all_vals) - 1, 0, -1):
            row = all_vals[i]
            if len(row) <= max(idx_link, idx_sheet, idx_month): continue
            if (str(row[idx_link]).strip() == str(link_src).strip() and 
                str(row[idx_sheet]).strip() == str(sheet_src).strip() and 
                str(row[idx_month]).strip() == str(month_src).strip()):
                rows_to_delete.append(i + 1)

        if rows_to_delete:
            ranges = []
            if len(rows_to_delete) > 0:
                rows_to_delete.sort(reverse=True)
                start = rows_to_delete[0]; end = start
                for r in rows_to_delete[1:]:
                    if r == start - 1: start = r
                    else: ranges.append((start, end)); start = r; end = r
                ranges.append((start, end))
            
            reqs = [{"deleteDimension": {"range": {"sheetId": wks.id, "dimension": "ROWS", "startIndex": s-1, "endIndex": e}}} for s, e in ranges]
            safe_api_call(wks.spreadsheet.batch_update, {'requests': reqs})
            time.sleep(2)
    except: pass

# --- 5. TÌM VIỆC & LỊCH TRÌNH ---
def parse_weekday(day_str):
    map_day = {'T2':0, 'T3':1, 'T4':2, 'T5':3, 'T6':4, 'T7':5, 'CN':6}
    return map_day.get(str(day_str).upper().strip(), -1)

def check_block_should_run(block_name, sched_df, last_run_time):
    now = datetime.now(TZ_VN)
    row = sched_df[sched_df['Block_Name'] == block_name]
    if row.empty: return False, "Không có lịch"
    
    row = row.iloc[0]
    l_type = str(row.get('Loai_Lich', '')).strip()
    val1 = str(row.get('Thong_So_Chinh', '')).strip()
    val2 = str(row.get('Thong_So_Phu', '')).strip()
    
    if l_type == "Không chạy": return False, "Đang tắt"
    
    has_run_today = False
    if last_run_time and last_run_time.date() == now.date():
        has_run_today = True

    if l_type == "Chạy theo phút":
        if not last_run_time: return True, "Lần đầu"
        try:
            if ((now - last_run_time).total_seconds()/60) >= int(val1): return True, "Đến giờ"
        except: pass
        return False, "Chưa đến giờ"
        
    try: target_hour = int(val1.split(':')[0])
    except: return False, "Lỗi giờ"
    
    time_ok = now.hour >= target_hour

    if l_type == "Hàng ngày":
        if time_ok and not has_run_today: return True, "Chạy bù/Đúng giờ"
        if has_run_today: return False, "Đã chạy hôm nay"
        return False, "Chưa đến giờ"

    if l_type == "Hàng tuần":
        correct_day = now.weekday() in [parse_weekday(d) for d in val2.split(',')]
        if not correct_day: return False, "Sai thứ"
        if time_ok and not has_run_today: return True, "Chạy bù/Đúng giờ"
        return False, "Đã chạy/Chưa đến giờ"
        
    if l_type == "Hàng tháng":
        correct_day = now.day in [int(d) for d in val2.split(',') if d.strip().isdigit()]
        if not correct_day: return False, "Sai ngày"
        if time_ok and not has_run_today: return True, "Chạy bù/Đúng giờ"
        return False, "Đã chạy/Chưa đến giờ"

    return False, "Không khớp lịch"

def get_jobs_to_run(gc_master):
    print("--- 🔍 QUÉT LỊCH ---")
    try:
        sh = gc_master.open_by_key(SHEET_ID)
        df_cfg = get_as_dataframe(sh.worksheet(SHEET_CONFIG_NAME), evaluate_formulas=True, dtype=str)
        df_active = df_cfg[df_cfg['Trạng thái'].fillna('').astype(str).str.contains('Chưa chốt', case=False, na=False)]
        
        active_blocks = [b.strip() for b in df_active['Block_Name'].unique() if b and isinstance(b, str) and b.strip() and b.lower() != 'nan']

        try: df_sched = get_as_dataframe(sh.worksheet(SHEET_SYS_CONFIG), evaluate_formulas=True, dtype=str)
        except: return []

        last_run_map = {}
        try:
            logs = sh.worksheet(SHEET_LOG_NAME).get_all_values()[-300:]
            for row in reversed(logs):
                if len(row) > 11 and row[10] == "Auto":
                    try: last_run_map[row[11]] = TZ_VN.localize(datetime.strptime(row[0], "%d/%m/%Y %H:%M:%S"))
                    except: pass
        except: pass

        jobs = []
        for block in active_blocks:
            should, reason = check_block_should_run(block, df_sched, last_run_map.get(block))
            if should: 
                print(f"✅ {block}: CHẠY ({reason})")
                jobs.append(block)
            else: 
                print(f"💤 {block}: BỎ ({reason})")
        return jobs
    except Exception as e: print(f"Lỗi tìm việc: {e}"); return []

# --- 6. XỬ LÝ CHÍNH (5 BOT + FILTER + HEADER + WRITE) ---
def process_row_multi_bot(row):
    sid = extract_id(row['Link dữ liệu lấy dữ liệu'])
    tid = extract_id(row['Link dữ liệu đích'])
    if not sid or not tid: return "Lỗi Link", 0

    sh_src = None; active_gc = None
    
    for i in range(len(MY_BOT_LIST)):
        try:
            c = get_bot_creds_by_index(i)
            if not c: continue
            gc = gspread.authorize(c)
            temp_sh = gc.open_by_key(sid)
            _ = temp_sh.title
            sh_src = temp_sh; active_gc = gc 
            break 
        except Exception: continue
    
    if not sh_src: return "Lỗi: 5 Bot đều không vào được Nguồn", 0

    try:
        ws_name = row['Tên sheet nguồn dữ liệu gốc']
        try: ws_src = sh_src.worksheet(ws_name) if ws_name else sh_src.sheet1
        except: return f"Lỗi: Không tìm thấy sheet '{ws_name}'", 0
        
        raw_data = safe_api_call(ws_src.get_all_values)
        if not raw_data: return "Sheet trắng", 0

        data_range = str(row.get('Vùng lấy dữ liệu', '')).strip().upper()
        if ":" in data_range and len(data_range) < 10:
            try:
                s_char, e_char = data_range.split(":")
                s_idx = col_name_to_index(s_char); e_idx = col_name_to_index(e_char)
                raw_data = [r[s_idx : e_idx + 1] for r in raw_data]
            except: pass
        
        if not raw_data: return "Lỗi cắt vùng", 0

        headers = deduplicate_headers(raw_data[0])
        body = []
        num_cols = len(headers)
        for r in raw_data[1:]:
            if len(r) < num_cols: r = r + [""] * (num_cols - len(r))
            body.append(r[:num_cols])
        
        df = pd.DataFrame(body, columns=headers)
        
        filter_cond = str(row.get('Dieu_Kien_Loc', '')).strip() 
        if filter_cond and filter_cond.lower() != 'nan':
            df = apply_smart_filter(df, filter_cond)
        
        if df.empty: return "Không có dữ liệu sau lọc", 0

        h_val = str(row.get('Lay_Header', 'FALSE')).strip().upper()
        if h_val == 'TRUE':
            header_df = pd.DataFrame([df.columns.tolist()], columns=df.columns)
            df = pd.concat([header_df, df], ignore_index=True)

        df['Src_Link'] = row['Link dữ liệu lấy dữ liệu']
        df['Src_Sheet'] = row['Tên sheet nguồn dữ liệu gốc']
        df['Month'] = row['Tháng']
        df['Thời điểm ghi'] = datetime.now(TZ_VN).strftime("%d/%m/%Y")

        sh_tgt = safe_api_call(active_gc.open_by_key, tid)
        if not sh_tgt: return "Lỗi: Không vào được Đích", 0

        t_sheet = row['Tên sheet dữ liệu đích'] or "Tong_Hop_Data"
        try: ws_tgt = sh_tgt.worksheet(t_sheet)
        except: ws_tgt = sh_tgt.add_worksheet(t_sheet, 1000, 20)
        
        write_mode = str(row.get('Cach_Ghi', 'Ghi Đè')).strip() 
        if write_mode == "Ghi Đè":
            delete_old_data(ws_tgt, row['Link dữ liệu lấy dữ liệu'], row['Tên sheet nguồn dữ liệu gốc'], row['Tháng'])

        existing = safe_api_call(ws_tgt.get_all_values)
        if not existing:
            ws_tgt.update([df.columns.tolist()] + df.fillna("").values.tolist())
        else:
            tgt_cols = existing[0]
            df_aligned = pd.DataFrame()
            for c in tgt_cols:
                if c in df.columns: df_aligned[c] = df[c]
                else: df_aligned[c] = ""
            safe_api_call(ws_tgt.append_rows, df_aligned.fillna("").values.tolist())
            
        return f"Thành công ({write_mode})", len(df)

    except Exception as e: return f"Lỗi: {str(e)[:100]}", 0

# --- MAIN ---
if __name__ == "__main__":
    start_time = datetime.now(TZ_VN)
    print(f"🚀 START: {start_time}")
    try:
        if not SHEET_ID: print("Thiếu Sheet ID"); exit(0)
        master_creds = get_bot_creds_by_index(0)
        if not master_creds: print("Thiếu Key Master"); exit(0)
        gc_master = gspread.authorize(master_creds)

        jobs = get_jobs_to_run(gc_master)
        if not jobs: print("💤 End."); exit(0)
        
        success_log = []
        for blk in jobs:
            print(f"▶️ Block: {blk}")
            sh = gc_master.open_by_key(SHEET_ID)
            df_cfg = get_as_dataframe(sh.worksheet(SHEET_CONFIG_NAME), evaluate_formulas=True, dtype=str)
            
            rows = df_cfg[(df_cfg['Block_Name'].astype(str) == str(blk)) & (df_cfg['Trạng thái'].astype(str).str.contains('Chưa chốt', na=False))]
            
            total = 0; log_ents = []
            for i, r in rows.iterrows():
                stt, cnt = process_row_multi_bot(r)
                print(f"  + {r.get('Link dữ liệu lấy dữ liệu')[-15:]}: {stt} ({cnt})")
                total += cnt
                log_ents.append([
                    datetime.now(TZ_VN).strftime("%d/%m/%Y %H:%M:%S"), r.get('Vùng lấy dữ liệu'), r.get('Tháng'), "Auto_Runner",
                    r.get('Link dữ liệu lấy dữ liệu'), r.get('Link dữ liệu đích'), r.get('Tên sheet dữ liệu đích'), r.get('Tên sheet nguồn dữ liệu gốc'),
                    stt, cnt, "Auto", blk
                ])
            
            # Ghi Log kỹ thuật
            if log_ents: 
                try: sh.worksheet(SHEET_LOG_NAME).append_rows(log_ents)
                except: pass
            
            # [MỚI] GHI LOG HÀNH VI (Chốt sổ Block)
            write_behavior_log(gc_master, "Chạy Tự Động", blk, f"Đã xử lý {total} dòng", "Completed")
            
            success_log.append(f"• <b>{blk}</b>: {total} dòng")

        msg = f"⏰ <b>Xong:</b> {datetime.now(TZ_VN).strftime('%H:%M')}\n{chr(10).join(success_log)}"
        send_telegram(msg)

    except Exception as e:
        print(traceback.format_exc())
        send_telegram(f"Lỗi Auto: {str(e)}", True)
        # Ghi log hành vi khi lỗi
        try:
            master_creds = get_bot_creds_by_index(0)
            if master_creds:
                gc = gspread.authorize(master_creds)
                write_behavior_log(gc, "Chạy Tự Động", "System", f"Lỗi Fatal: {str(e)[:50]}", "Error")
        except: pass
        exit(1)
