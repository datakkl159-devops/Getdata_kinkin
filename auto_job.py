import pandas as pd
import time
import gspread
import json
import re
import pytz
import os
import uuid
import numpy as np
import gc
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from datetime import datetime, timedelta
from google.oauth2 import service_account
from collections import defaultdict

# ==========================================
# 1. CẤU HÌNH & CONSTANTS
# ==========================================
SHEET_CONFIG_NAME = "luu_cau_hinh"
SHEET_SYS_CONFIG = "sys_config"
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_ACTIVITY_NAME = "log_hanh_vi"
SHEET_LOCK_NAME = "sys_lock"

COL_BLOCK_NAME = "Block_Name"
COL_STATUS = "Trạng thái"
COL_DATA_RANGE = "Vùng lấy dữ liệu"
COL_MONTH = "Tháng"
COL_SRC_LINK = "Link dữ liệu lấy dữ liệu"
COL_TGT_LINK = "Link dữ liệu đích"
COL_SRC_SHEET = "Tên sheet nguồn dữ liệu gốc"
COL_TGT_SHEET = "Tên sheet dữ liệu đích"
COL_FILTER = "Dieu_Kien_Loc"
COL_HEADER = "Lay_Header"

SCHED_COL_BLOCK = "Block_Name"
SCHED_COL_TYPE = "Loai_Lich"
SCHED_COL_VAL1 = "Thong_So_Chinh" # Giờ (08:00) hoặc Số phút (50)
SCHED_COL_VAL2 = "Thong_So_Phu"   # Ngày (4,8) hoặc Thứ (T2,T3)

SYS_COL_LINK = "Link file nguồn"
SYS_COL_SHEET = "Sheet nguồn"
SYS_COL_MONTH = "Tháng"

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# Khoảng thời gian nhìn lại (phút) để bắt dính lịch khi GitHub bị trễ
LOOKBACK_MINUTES = 18 

# ==========================================
# 2. CORE UTILS (SERVER SIDE)
# ==========================================
def get_creds():
    try:
        creds_json = os.environ.get("GCP_SERVICE_ACCOUNT")
        if creds_json:
            creds_info = json.loads(creds_json)
            if "private_key" in creds_info:
                creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
            return service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        
        if os.path.exists("secrets.json"):
            return service_account.Credentials.from_service_account_file("secrets.json", scopes=SCOPES)
            
        print("❌ Lỗi: Không tìm thấy Credentials.")
        return None
    except Exception as e:
        print(f"❌ Lỗi Auth: {e}")
        return None

def get_history_sheet_id():
    raw_id = os.environ.get("HISTORY_SHEET_ID")
    if not raw_id: return None
    extracted = extract_id(raw_id)
    if extracted: return extracted
    return raw_id

def safe_api_call(func, *args, **kwargs):
    max_retries = 5
    for i in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_str = str(e).lower()
            if "429" in error_str or "quota" in error_str:
                wait_time = (2 ** i) + 5
                print(f"⚠️ Quota exceeded. Waiting {wait_time}s...")
                time.sleep(wait_time)
            elif i == max_retries - 1: raise e
            else: time.sleep(2)
    return None

def get_sh_with_retry(creds, sheet_id):
    gc_client = gspread.authorize(creds)
    masked_id = sheet_id[:5] + "..." + sheet_id[-5:] if sheet_id and len(sheet_id) > 10 else "N/A"
    print(f"🔗 Connecting to Master Sheet ID: {masked_id}")
    return safe_api_call(gc_client.open_by_key, sheet_id)

def extract_id(url):
    if not isinstance(url, str): return None
    if "docs.google.com" in url:
        try: 
            match = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
            if match: return match.group(1)
        except: return None
    return None

def col_name_to_index(col_name):
    col_name = col_name.upper()
    index = 0
    for char in col_name: index = index * 26 + (ord(char) - ord('A')) + 1
    return index - 1

# ==========================================
# 3. LOGIC SMART FILTER & ETL
# ==========================================
def apply_smart_filter(df, filter_str):
    if not filter_str or str(filter_str).strip().lower() in ['nan', 'none', 'null', '']:
        return df, None
    fs = filter_str.strip()
    operators = [" contains ", "==", "!=", ">=", "<=", ">", "<", "="]
    selected_op = None
    for op in operators:
        if op in fs: selected_op = op; break
    if not selected_op: return None, f"Lỗi cú pháp: Không tìm thấy toán tử trong '{fs}'"

    parts = fs.split(selected_op, 1)
    user_col = parts[0].strip().replace("`", "").replace("'", "").replace('"', "")
    real_col_name = None
    if user_col in df.columns: real_col_name = user_col
    else:
        for col in df.columns:
            if str(col).strip() == user_col: real_col_name = col; break
    if not real_col_name: return None, f"Không tìm thấy cột '{user_col}'"

    user_val = parts[1].strip()
    if (user_val.startswith("'") and user_val.endswith("'")) or (user_val.startswith('"') and user_val.endswith('"')):
        clean_val = user_val[1:-1]
    else: clean_val = user_val

    try:
        col_str = df[real_col_name].astype(str)
        if selected_op == " contains ": return df[col_str.str.contains(clean_val, case=False, na=False)], None
        elif selected_op in ["=", "=="]: return df[col_str == str(clean_val)], None
        elif selected_op == "!=": return df[col_str != str(clean_val)], None
        else:
            numeric_col = pd.to_numeric(df[real_col_name], errors='coerce')
            try: numeric_val = float(clean_val)
            except: return None, f"Giá trị '{clean_val}' không phải là số"
            if selected_op == ">": return df[numeric_col > numeric_val], None
            if selected_op == "<": return df[numeric_col < numeric_val], None
            if selected_op == ">=": return df[numeric_col >= numeric_val], None
            if selected_op == "<=": return df[numeric_col <= numeric_val], None
    except Exception as e: return None, f"Lỗi thực thi lọc: {str(e)}"
    return df, None

def fetch_data(row_config, creds, target_headers=None):
    link_src = str(row_config.get(COL_SRC_LINK, '')).strip()
    source_label = str(row_config.get(COL_SRC_SHEET, '')).strip()
    month_val = str(row_config.get(COL_MONTH, ''))
    
    raw_range = str(row_config.get(COL_DATA_RANGE, '')).strip()
    data_range_str = "Lấy hết" if raw_range.lower() in ['nan', 'none', 'null', '', 'lấy hết'] else raw_range

    raw_filter = str(row_config.get(COL_FILTER, '')).strip()
    if raw_filter.lower() in ['nan', 'none', 'null']: raw_filter = ""
    
    include_header = str(row_config.get(COL_HEADER, 'FALSE')).strip().upper() == 'TRUE'
    sheet_id = extract_id(link_src)
    if not sheet_id: return None, "Link lỗi"
    
    try:
        sh_source = get_sh_with_retry(creds, sheet_id)
        wks_source = sh_source.worksheet(source_label) if source_label else sh_source.sheet1
        data = safe_api_call(wks_source.get_all_values)
        if not data: return pd.DataFrame(), "Sheet trắng"

        header_row = data[0]
        body_rows = data[1:]
        
        unique_headers = []
        seen = {}
        for col in header_row:
            if col in seen:
                seen[col] += 1
                unique_headers.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                unique_headers.append(col)
        
        df_working = pd.DataFrame(body_rows, columns=unique_headers)

        if target_headers:
            num_src = len(df_working.columns); num_tgt = len(target_headers)
            min_cols = min(num_src, num_tgt)
            old_cols = df_working.columns.tolist()
            rename_map = {old_cols[i]: target_headers[i] for i in range(min_cols)}
            df_working = df_working.rename(columns=rename_map)
            if num_src > num_tgt: df_working = df_working.iloc[:, :num_tgt]

        if data_range_str != "Lấy hết" and ":" in data_range_str:
            try:
                s_str, e_str = data_range_str.split(":")
                s_idx = col_name_to_index(s_str.strip()); e_idx = col_name_to_index(e_str.strip())
                if s_idx >= 0: df_working = df_working.iloc[:, s_idx : e_idx + 1]
            except: pass

        if raw_filter:
            df_filtered, err = apply_smart_filter(df_working, raw_filter)
            if err: return None, f"Filter Error: {err}"
            df_working = df_filtered

        if include_header:
            df_header_row = pd.DataFrame([df_working.columns.tolist()], columns=df_working.columns)
            df_final = pd.concat([df_header_row, df_working], ignore_index=True)
        else:
            df_final = df_working

        df_final = df_final.astype(str).replace(['nan', 'None', '<NA>', 'null'], '')
        df_final[SYS_COL_LINK] = link_src.strip()
        df_final[SYS_COL_SHEET] = source_label.strip()
        df_final[SYS_COL_MONTH] = month_val.strip()
        
        return df_final, "OK"

    except Exception as e: return None, f"Lỗi tải: {str(e)}"

def get_rows_to_delete_dynamic(wks, keys_to_delete):
    all_values = safe_api_call(wks.get_all_values)
    if not all_values: return []
    headers = all_values[0]
    try:
        idx_link = headers.index(SYS_COL_LINK); idx_sheet = headers.index(SYS_COL_SHEET); idx_month = headers.index(SYS_COL_MONTH)
    except ValueError: return [] 
    rows_to_delete = []
    for i, row in enumerate(all_values[1:], start=2): 
        l = row[idx_link].strip() if len(row) > idx_link else ""
        s = row[idx_sheet].strip() if len(row) > idx_sheet else ""
        m = row[idx_month].strip() if len(row) > idx_month else ""
        if (l, s, m) in keys_to_delete: rows_to_delete.append(i)
    return rows_to_delete

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
    requests = []
    for start, end in ranges:
        requests.append({"deleteDimension": {"range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": start - 1, "endIndex": end}}})
    batch_size = 100
    for i in range(0, len(requests), batch_size):
        safe_api_call(sh.batch_update, {'requests': requests[i:i+batch_size]})
        time.sleep(1)

def write_data(tasks_list, target_link, target_sheet_name, creds):
    try:
        target_id = extract_id(target_link)
        if not target_id: return False, "Link lỗi", {}
        sh = get_sh_with_retry(creds, target_id)
        real_sheet_name = str(target_sheet_name).strip() or "Tong_Hop_Data"
        
        all_titles = [s.title for s in safe_api_call(sh.worksheets)]
        if real_sheet_name in all_titles:
            wks = sh.worksheet(real_sheet_name)
        else:
            wks = sh.add_worksheet(title=real_sheet_name, rows=1000, cols=20)
            print(f"✨ Created new sheet: {real_sheet_name}")
        
        df_new_all = pd.DataFrame()
        for df, _, _ in tasks_list:
            df_new_all = pd.concat([df_new_all, df], ignore_index=True)
        
        if df_new_all.empty: return True, "No Data", {}

        existing_headers = safe_api_call(wks.row_values, 1)
        if not existing_headers:
            final_headers = df_new_all.columns.tolist()
            wks.update(range_name="A1", values=[final_headers])
            existing_headers = final_headers
        else:
            updated = existing_headers.copy(); added = False
            for col in [SYS_COL_LINK, SYS_COL_SHEET, SYS_COL_MONTH]:
                if col not in updated: updated.append(col); added = True
            if added: wks.update(range_name="A1", values=[updated]); existing_headers = updated

        df_aligned = pd.DataFrame()
        for col in existing_headers:
            if col in df_new_all.columns: df_aligned[col] = df_new_all[col]
            else: df_aligned[col] = ""
        
        keys = set()
        for idx, row in df_new_all.iterrows():
            keys.add((str(row[SYS_COL_LINK]).strip(), str(row[SYS_COL_SHEET]).strip(), str(row[SYS_COL_MONTH]).strip()))
        
        rows_to_del = get_rows_to_delete_dynamic(wks, keys)
        if rows_to_del:
            batch_delete_rows(sh, wks.id, rows_to_del)
        
        start_row = len(safe_api_call(wks.get_all_values)) + 1
        chunk_size = 5000
        new_vals = df_aligned.fillna('').values.tolist()
        for i in range(0, len(new_vals), chunk_size):
            safe_api_call(wks.append_rows, new_vals[i:i+chunk_size], value_input_option='USER_ENTERED')
            time.sleep(1)

        result_map = {}
        current_cursor = start_row
        for df, _, r_idx in tasks_list:
            count = len(df)
            end = current_cursor + count - 1
            result_map[r_idx] = ("Thành công", f"{current_cursor} - {end}", count)
            current_cursor += count
            
        return True, f"Updated {len(df_aligned)} rows", result_map

    except Exception as e: return False, f"Write Error: {str(e)}", {}

# ==========================================
# 4. SCHEDULER LOGIC (V74 - STANDARD LOGIC)
# ==========================================
def is_time_in_window(target_time_str, now_dt):
    """
    Kiểm tra xem target_time (HH:MM) có xuất hiện trong khoảng 
    [now - 18 phút, now] hay không.
    Hàm này dùng chung cho Chạy Ngày, Tuần, Tháng.
    """
    try:
        h_set, m_set = map(int, target_time_str.strip().split(":"))
        # Tạo mốc thời gian chạy của ngày hôm nay
        sched_dt = now_dt.replace(hour=h_set, minute=m_set, second=0, microsecond=0)
        
        # Tính độ lệch: (Hiện tại - Mốc cài đặt)
        diff = (now_dt - sched_dt).total_seconds() / 60 
        
        # Nếu độ lệch từ 0 đến 18 phút -> Có nghĩa là vừa mới qua giờ chạy -> Chạy
        if 0 <= diff <= LOOKBACK_MINUTES:
            return True
        return False
    except: return False

def is_time_to_run_standard(row, now_dt):
    sched_type = str(row.get(SCHED_COL_TYPE, "")).strip()
    val1 = str(row.get(SCHED_COL_VAL1, "")).strip() # Giờ (08:00) hoặc Phút (50)
    val2 = str(row.get(SCHED_COL_VAL2, "")).strip() # Ngày (4,8) hoặc Thứ (T2,T3)

    if sched_type == "Không chạy": return False

    # Mapping cho Thứ và Ngày
    week_map = {0: "T2", 1: "T3", 2: "T4", 3: "T5", 4: "T6", 5: "T7", 6: "CN"}
    current_wday_str = week_map[now_dt.weekday()]
    current_day_str = str(now_dt.day)

    # 1. Chạy theo phút
    if sched_type == "Chạy theo phút":
        try:
            interval = int(val1)
            if interval < 30: interval = 30 # Min 30p theo yêu cầu
            
            # Tính tổng số phút trong ngày hiện tại
            curr_total_min = now_dt.hour * 60 + now_dt.minute
            
            # Tính thời điểm quá khứ (lùi lại để check)
            prev_total_min = curr_total_min - LOOKBACK_MINUTES
            
            # Logic: Nếu số lần chia chẵn cho interval thay đổi -> Đã qua mốc
            # Ví dụ: Interval 50. Lúc 08:10 (490p) -> 490//50 = 9
            # Lúc 08:25 (505p) -> 505//50 = 10 -> Nhảy số -> Chạy (mốc 500 - 08:20)
            
            count_curr = curr_total_min // interval
            count_prev = prev_total_min // interval
            
            if count_curr > count_prev:
                return True
        except: pass

    # 2. Hàng ngày: Chỉ check giờ (val1)
    elif sched_type == "Hàng ngày":
        return is_time_in_window(val1, now_dt)

    # 3. Hàng tuần: Check Thứ (val2) + Giờ (val1)
    elif sched_type == "Hàng tuần":
        # Tách danh sách thứ: "T2, T3" -> ["T2", "T3"]
        target_days = [x.strip() for x in val2.split(",")]
        if current_wday_str in target_days:
            return is_time_in_window(val1, now_dt)

    # 4. Hàng tháng: Check Ngày (val2) + Giờ (val1)
    elif sched_type == "Hàng tháng":
        # Tách danh sách ngày: "4, 8" -> ["4", "8"]
        target_dates = [x.strip() for x in val2.split(",")]
        if current_day_str in target_dates:
            return is_time_in_window(val1, now_dt)

    return False

def run_auto_job():
    print("🚀 Starting Auto Job (V74 - Standard Logic)...")
    
    creds = get_creds()
    if not creds: return
    
    master_id = get_history_sheet_id()
    if not master_id: 
        print("❌ Chưa set HISTORY_SHEET_ID"); return

    sh_master = get_sh_with_retry(creds, master_id)
    if not sh_master:
        print("❌ Không thể mở Sheet Master.")
        return
    
    try:
        wks_sched = sh_master.worksheet(SHEET_SYS_CONFIG)
        df_sched = get_as_dataframe(wks_sched, evaluate_formulas=True, dtype=str)
        
        wks_config = sh_master.worksheet(SHEET_CONFIG_NAME)
        df_config = get_as_dataframe(wks_config, evaluate_formulas=True, dtype=str)
        df_config['index_map'] = df_config.index
    except Exception as e:
        print(f"❌ Lỗi đọc config: {e}"); return

    # Check Schedule
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz)
    print(f"🕒 Time Check: {now.strftime('%H:%M:%S')} (Lookback {LOOKBACK_MINUTES}m)")

    blocks_to_run = []
    if SCHED_COL_BLOCK in df_sched.columns:
        for _, row in df_sched.iterrows():
            blk = str(row.get(SCHED_COL_BLOCK, ""))
            # [V74] Sử dụng hàm check chuẩn
            if is_time_to_run_standard(row, now):
                print(f"⚡ MATCH: {blk}")
                blocks_to_run.append(blk)
    
    if not blocks_to_run:
        print("💤 Không có lịch phù hợp.")
        return

    log_buffer = []
    
    for blk in blocks_to_run:
        block_rows = df_config[
            (df_config[COL_BLOCK_NAME] == blk) & 
            (df_config[COL_STATUS] == "Chưa chốt & đang cập nhật")
        ]
        
        if block_rows.empty:
            print(f"⚠️ Block {blk} rỗng/inactive.")
            continue

        grouped = defaultdict(list)
        for _, r in block_rows.iterrows():
            tgt_key = (str(r.get(COL_TGT_LINK, '')).strip(), str(r.get(COL_TGT_SHEET, '')).strip())
            grouped[tgt_key].append(r)

        for (t_link, t_sheet), rows in grouped.items():
            tasks = []
            print(f"📂 Run: {blk} -> {t_sheet}")
            
            for r in rows:
                lnk = r.get(COL_SRC_LINK, ''); lbl = r.get(COL_SRC_SHEET, '')
                idx = r.get('index_map')
                
                df, msg = fetch_data(r, creds)
                time.sleep(1.5)
                
                if df is not None:
                    tasks.append((df, lnk, idx))
                else:
                    log_buffer.append([
                        now.strftime("%d/%m/%Y %H:%M:%S"), r.get(COL_DATA_RANGE), r.get(COL_MONTH), 
                        "AUTO_BOT", lnk, t_link, t_sheet, lbl, "Lỗi tải", "0", "", blk
                    ])

            if tasks:
                ok, msg, res_map = write_data(tasks, t_link, t_sheet, creds)
                print(f"  💾 {msg}")
                
                for df, lnk, idx in tasks:
                    status, ranges, count = res_map.get(idx, ("Lỗi Ghi", "", 0))
                    orig_r = df_config.loc[idx]
                    
                    log_buffer.append([
                        now.strftime("%d/%m/%Y %H:%M:%S"), orig_r.get(COL_DATA_RANGE), orig_r.get(COL_MONTH), 
                        "AUTO_BOT", lnk, t_link, t_sheet, orig_r.get(COL_SRC_SHEET), 
                        status, str(count), ranges, blk
                    ])

    if log_buffer:
        print(f"📝 Saving {len(log_buffer)} logs...")
        try:
            wks_log = sh_master.worksheet(SHEET_LOG_NAME)
            cleaned_logs = [[str(x) for x in row] for row in log_buffer]
            safe_api_call(wks_log.append_rows, cleaned_logs)
        except Exception as e:
            print(f"❌ Log Error: {e}")

    try:
        wks_act = sh_master.worksheet(SHEET_ACTIVITY_NAME)
        safe_api_call(wks_act.append_row, [
            now.strftime("%d/%m/%Y %H:%M:%S"), "AUTO_BOT", 
            "Scheduled Run", f"Blocks: {', '.join(blocks_to_run)}"
        ])
    except: pass

    print("🏁 Done.")

if __name__ == "__main__":
    run_auto_job()
