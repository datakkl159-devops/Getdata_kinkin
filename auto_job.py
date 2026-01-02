import os
import json
import pandas as pd
import gspread
import time
import pytz
import re
import numpy as np
from datetime import datetime, timedelta
from google.oauth2 import service_account
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from collections import defaultdict

# ==========================================
# 1. CẤU HÌNH & HẰNG SỐ
# ==========================================
SHEET_CONFIG_NAME = "luu_cau_hinh"
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_SYS_CONFIG = "sys_config"
SHEET_SYS_STATE = "sys_state"

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
VN_TZ = pytz.timezone('Asia/Ho_Chi_Minh')

# --- ĐỊNH NGHĨA CỘT (Khớp 100% với file Google Sheet của bạn) ---
COL_BLOCK_NAME = "Block_Name"
COL_STATUS = "Trạng thái"
COL_WRITE_MODE = "Cach_Ghi"
COL_DATA_RANGE = "Vùng lấy dữ liệu"
COL_MONTH = "Tháng"
COL_SRC_LINK = "Link dữ liệu lấy dữ liệu"
COL_TGT_LINK = "Link dữ liệu đích"
COL_SRC_SHEET = "Tên sheet nguồn dữ liệu gốc"
COL_TGT_SHEET = "Tên sheet dữ liệu đích"
COL_FILTER = "Dieu_Kien_Loc"
COL_HEADER = "Lay_Header"

# Cột trong sheet cấu hình lịch (sys_config)
SCH_COL_BLOCK = "Block_Name"
SCH_COL_TYPE = "Loai_Lich"
SCH_COL_VAL1 = "Thong_So_Chinh"
SCH_COL_VAL2 = "Thong_So_Phu"

# Cột hệ thống ẩn (Bot tự thêm vào file đích)
SYS_COL_LINK = "Src_Link"
SYS_COL_SHEET = "Src_Sheet"
SYS_COL_MONTH = "Month"

# ==========================================
# 2. CÁC HÀM HỖ TRỢ (UTILS)
# ==========================================
def safe_api_call(func, *args, **kwargs):
    """Cơ chế tự động thử lại khi gặp lỗi Quota (429)"""
    for i in range(5):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            err_msg = str(e).lower()
            if "429" in err_msg or "quota" in err_msg or "api" in err_msg or "500" in err_msg:
                wait_time = (2 ** i) + 5
                print(f"⚠️ API Busy (Lần {i+1}). Chờ {wait_time}s...")
                time.sleep(wait_time)
            elif i == 4:
                print(f"❌ API Error: {str(e)}")
                raise e
            else:
                time.sleep(2)
    return None

def load_bots_from_env():
    """Tự động tìm và nạp tất cả Bot từ GitHub Secrets"""
    bot_pool = []
    # Bot chính
    if "GCP_SERVICE_ACCOUNT" in os.environ:
        try:
            creds = service_account.Credentials.from_service_account_info(
                json.loads(os.environ["GCP_SERVICE_ACCOUNT"]), scopes=SCOPES)
            bot_pool.append(creds)
        except Exception as e: print(f"⚠️ Lỗi nạp Bot chính: {e}")

    # Bot phụ (1-9)
    for i in range(1, 10):
        key = f"GCP_SERVICE_ACCOUNT_{i}"
        if key in os.environ:
            try:
                creds = service_account.Credentials.from_service_account_info(
                    json.loads(os.environ[key]), scopes=SCOPES)
                bot_pool.append(creds)
            except: pass
    
    if not bot_pool:
        raise Exception("❌ Không tìm thấy bất kỳ Bot nào trong Secrets!")
    
    print(f"🤖 Đã kích hoạt {len(bot_pool)} Bots.")
    return bot_pool

def extract_id(url):
    if not isinstance(url, str): return None
    if "docs.google.com" in url:
        try: return url.split("/d/")[1].split("/")[0]
        except: return None
    return None

def col_name_to_index(col_name):
    col_name = col_name.upper()
    index = 0
    for char in col_name:
        index = index * 26 + (ord(char) - ord('A')) + 1
    return index - 1

# ==========================================
# 3. LOGIC XỬ LÝ DỮ LIỆU (ETL CORE)
# ==========================================
def apply_smart_filter(df, filter_str):
    """Bộ lọc thông minh (Phiên bản chạy ngầm)"""
    if not filter_str or str(filter_str).strip().lower() in ['nan', 'none', '', 'null']:
        return df
    
    current_df = df.copy()
    conditions = str(filter_str).split(';')
    
    for cond in conditions:
        fs = cond.strip()
        if not fs: continue
        
        # Xác định toán tử
        ops = [" contains ", "==", "!=", ">=", "<=", ">", "<", "="]
        selected_op = None
        for op in ops:
            if op in fs: selected_op = op; break
        
        if not selected_op: continue

        parts = fs.split(selected_op, 1)
        col_name = parts[0].strip().replace("`", "").replace("'", "").replace('"', "")
        val_raw = parts[1].strip()
        
        # Clean value
        if (val_raw.startswith("'") and val_raw.endswith("'")) or (val_raw.startswith('"') and val_raw.endswith('"')):
            clean_val = val_raw[1:-1]
        else:
            clean_val = val_raw
        clean_val = clean_val.strip()

        # Tìm tên cột thật
        real_col = None
        for c in current_df.columns:
            if str(c).lower() == col_name.lower(): real_col = c; break
        
        if not real_col: continue

        try:
            col_series = current_df[real_col]
            if selected_op == " contains ":
                current_df = current_df[col_series.astype(str).str.contains(clean_val, case=False, na=False)]
            else:
                # Logic so sánh (Date -> Num -> Str)
                is_processed = False
                # 1. Date
                try:
                    s_dt = pd.to_datetime(col_series, dayfirst=True, errors='coerce')
                    v_dt = pd.to_datetime(clean_val, dayfirst=True)
                    if s_dt.notna().any():
                        if selected_op == ">": current_df = current_df[s_dt > v_dt]
                        elif selected_op == "<": current_df = current_df[s_dt < v_dt]
                        elif selected_op == ">=": current_df = current_df[s_dt >= v_dt]
                        elif selected_op == "<=": current_df = current_df[s_dt <= v_dt]
                        elif selected_op in ["=", "=="]: current_df = current_df[s_dt == v_dt]
                        is_processed = True
                except: pass

                # 2. Number
                if not is_processed:
                    try:
                        s_num = pd.to_numeric(col_series, errors='coerce')
                        v_num = float(clean_val)
                        if s_num.notna().any():
                            if selected_op == ">": current_df = current_df[s_num > v_num]
                            elif selected_op == "<": current_df = current_df[s_num < v_num]
                            elif selected_op == ">=": current_df = current_df[s_num >= v_num]
                            elif selected_op == "<=": current_df = current_df[s_num <= v_num]
                            elif selected_op in ["=", "=="]: current_df = current_df[s_num == v_num]
                            is_processed = True
                    except: pass
                
                # 3. String
                if not is_processed:
                    s_str = col_series.astype(str).str.strip()
                    if selected_op == ">": current_df = current_df[s_str > str(clean_val)]
                    elif selected_op == ">=": current_df = current_df[s_str >= str(clean_val)]
                    elif selected_op in ["=", "=="]: current_df = current_df[s_str == str(clean_val)]
        except: pass
        
    return current_df

def process_single_task(row, bot_creds):
    """Xử lý 1 dòng cấu hình: Đọc -> Lọc -> Trả về DataFrame"""
    try:
        gc = gspread.authorize(bot_creds)
        src_link = row.get(COL_SRC_LINK, "")
        src_sheet = row.get(COL_SRC_SHEET, "")
        
        # 1. Mở Sheet Nguồn
        src_id = extract_id(src_link)
        if not src_id: return None, "Link lỗi"
        
        sh = safe_api_call(gc.open_by_key, src_id)
        if src_sheet:
            try: wks = sh.worksheet(src_sheet)
            except: return None, f"Không thấy sheet: {src_sheet}"
        else:
            wks = sh.sheet1
            
        # 2. Đọc dữ liệu
        data = safe_api_call(wks.get_all_values)
        if not data: return None, "Sheet rỗng"
        
        header = data[0]
        # Xử lý trùng header
        unique_header = []
        seen = {}
        for h in header:
            if h in seen:
                seen[h] += 1
                unique_header.append(f"{h}_{seen[h]}")
            else:
                seen[h] = 0
                unique_header.append(h)
        
        df = pd.DataFrame(data[1:], columns=unique_header)
        
        # 3. Cắt vùng dữ liệu (Range)
        raw_range = str(row.get(COL_DATA_RANGE, "")).strip()
        if raw_range and ":" in raw_range and raw_range.lower() != "lấy hết":
            try:
                s, e = raw_range.split(":")
                s_idx = col_name_to_index(s)
                e_idx = col_name_to_index(e)
                if s_idx >= 0: df = df.iloc[:, s_idx : e_idx + 1]
            except: pass
            
        # 4. Lọc dữ liệu
        df = apply_smart_filter(df, row.get(COL_FILTER, ""))
        
        # 5. Thêm cột hệ thống
        df[SYS_COL_LINK] = src_link
        df[SYS_COL_SHEET] = src_sheet
        df[SYS_COL_MONTH] = row.get(COL_MONTH, "")
        
        # 6. Header
        use_header = str(row.get(COL_HEADER, "FALSE")).upper() == 'TRUE'
        if use_header:
            header_row = pd.DataFrame([df.columns.tolist()], columns=df.columns)
            df = pd.concat([header_row, df], ignore_index=True)
            
        return df, "OK"
        
    except Exception as e:
        return None, str(e)

def write_to_target(target_link, target_sheet_name, data_list, bot_creds):
    """Ghi dữ liệu vào file đích (Hỗ trợ Ghi Đè & Nối Tiếp)"""
    if not data_list: return 0, "No Data"
    
    try:
        gc = gspread.authorize(bot_creds)
        tgt_id = extract_id(target_link)
        if not tgt_id: return 0, "Link đích lỗi"
        
        sh = safe_api_call(gc.open_by_key, tgt_id)
        
        # Mở hoặc tạo sheet đích
        sheet_title = target_sheet_name if target_sheet_name else "Tong_Hop_Data"
        try: 
            wks = sh.worksheet(sheet_title)
        except: 
            wks = sh.add_worksheet(sheet_title, 1000, 20)
            print(f"   ✨ Đã tạo sheet mới: {sheet_title}")

        # Gộp tất cả DataFrame
        full_df = pd.DataFrame()
        for df, mode in data_list:
            full_df = pd.concat([full_df, df], ignore_index=True)
            
        if full_df.empty: return 0, "Dữ liệu trống sau khi gộp"

        # Chuẩn hóa Header đích
        current_headers = safe_api_call(wks.row_values, 1)
        if not current_headers:
            wks.update("A1", [full_df.columns.tolist()])
            current_headers = full_df.columns.tolist()
        else:
            # Thêm cột hệ thống nếu thiếu
            added = False
            for c in [SYS_COL_LINK, SYS_COL_SHEET, SYS_COL_MONTH]:
                if c not in current_headers:
                    current_headers.append(c); added = True
            if added: wks.update("A1", [current_headers])

        # Align columns
        aligned_df = pd.DataFrame()
        for col in current_headers:
            if col in full_df.columns: aligned_df[col] = full_df[col]
            else: aligned_df[col] = ""
            
        # XỬ LÝ GHI ĐÈ (DELETE OLD ROWS)
        keys_to_delete = set()
        for df, mode in data_list:
            if mode == "Ghi Đè" and not df.empty:
                l = str(df[SYS_COL_LINK].iloc[0]).strip()
                s = str(df[SYS_COL_SHEET].iloc[0]).strip()
                m = str(df[SYS_COL_MONTH].iloc[0]).strip()
                keys_to_delete.add((l, s, m))
        
        if keys_to_delete:
            all_vals = safe_api_call(wks.get_all_values)
            if all_vals:
                h = all_vals[0]
                try:
                    idx_l = h.index(SYS_COL_LINK)
                    idx_s = h.index(SYS_COL_SHEET)
                    idx_m = h.index(SYS_COL_MONTH)
                    
                    rows_to_del = []
                    for i, r in enumerate(all_vals[1:], start=2):
                        curr_key = (
                            r[idx_l].strip() if len(r) > idx_l else "",
                            r[idx_s].strip() if len(r) > idx_s else "",
                            r[idx_m].strip() if len(r) > idx_m else ""
                        )
                        if curr_key in keys_to_delete:
                            rows_to_del.append(i)
                    
                    # Xóa batch (từ dưới lên)
                    if rows_to_del:
                        print(f"   ✂️ Đang xóa {len(rows_to_del)} dòng cũ...")
                        rows_to_del.sort(reverse=True)
                        # Gom nhóm để xóa nhanh hơn
                        ranges = []
                        if rows_to_del:
                            start = rows_to_del[0]; end = start
                            for r_idx in rows_to_del[1:]:
                                if r_idx == start - 1: start = r_idx
                                else: ranges.append((start, end)); start = r_idx; end = r_idx
                            ranges.append((start, end))
                        
                        reqs = [{"deleteDimension": {"range": {"sheetId": wks.id, "dimension": "ROWS", "startIndex": s-1, "endIndex": e}}} for s, e in ranges]
                        # Chia nhỏ request nếu quá nhiều
                        for i in range(0, len(reqs), 50):
                            safe_api_call(sh.batch_update, {'requests': reqs[i:i+50]})
                            time.sleep(1)
                except: pass # Không tìm thấy cột hệ thống -> không xóa được

        # GHI DỮ LIỆU MỚI (Luôn là Append xuống cuối)
        # Lấy dòng cuối thật
        data_check = safe_api_call(wks.get_all_values)
        start_row = len(data_check) + 1 if data_check else 1
        
        values = aligned_df.fillna("").values.tolist()
        # Chia chunk để ghi
        total_rows = len(values)
        chunk_size = 5000
        for i in range(0, total_rows, chunk_size):
            safe_api_call(wks.append_rows, values[i:i+chunk_size], value_input_option='USER_ENTERED')
            time.sleep(1)
            
        return total_rows, "Thành công"

    except Exception as e:
        return 0, f"Lỗi ghi: {str(e)}"

# ==========================================
# 4. LOGIC SCHEDULER (V98)
# ==========================================
def should_run_block(row, last_run_str, current_dt):
    """Kiểm tra xem có đến giờ chạy không"""
    sched_type = str(row.get(SCH_COL_TYPE, "")).strip()
    val1 = str(row.get(SCH_COL_VAL1, "")).strip()
    val2 = str(row.get(SCH_COL_VAL2, "")).strip()
    
    last_run_dt = None
    if last_run_str and last_run_str.lower() not in ['nan', 'none', '']:
        try: last_run_dt = datetime.strptime(last_run_str, "%d/%m/%Y %H:%M:%S").replace(tzinfo=VN_TZ)
        except: pass

    if not sched_type or sched_type == "Không chạy": return False

    # 1. Chạy theo phút
    if sched_type == "Chạy theo phút":
        try: minutes = int(val1)
        except: minutes = 60
        if not last_run_dt: return True
        diff = (current_dt - last_run_dt).total_seconds() / 60
        return diff >= minutes

    # 2. Định kỳ (Ngày/Tuần/Tháng)
    try:
        target_time = datetime.strptime(val1, "%H:%M").time()
        target_dt = current_dt.replace(hour=target_time.hour, minute=target_time.minute, second=0, microsecond=0)
    except: return False

    is_time_passed = current_dt >= target_dt
    not_run_today = (last_run_dt is None) or (last_run_dt < target_dt)

    if sched_type == "Hàng ngày":
        return is_time_passed and not_run_today
    
    if sched_type == "Hàng tuần":
        wd_map = {"T2":0, "T3":1, "T4":2, "T5":3, "T6":4, "T7":5, "CN":6}
        today_wd = current_dt.weekday()
        days = [d.strip().upper() for d in val2.split(",")]
        is_today = False
        for d in days:
            if d in wd_map and wd_map[d] == today_wd: is_today = True; break
        return is_today and is_time_passed and not_run_today
        
    if sched_type == "Hàng tháng":
        today_d = current_dt.day
        dates = [int(d) for d in val2.split(",") if d.strip().isdigit()]
        return (today_d in dates) and is_time_passed and not_run_today

    return False

# ==========================================
# 5. MAIN PROCESS
# ==========================================
def run_auto_job():
    print(f"🚀 START JOB: {datetime.now(VN_TZ).strftime('%d/%m/%Y %H:%M:%S')}")
    
    # 1. Load Bots
    try:
        bots = load_bots_from_env()
        master_bot = bots[0]
        gc_master = gspread.authorize(master_bot)
    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}"); return

    # 2. Đọc Config & State
    try:
        sh_cfg = safe_api_call(gc_master.open_by_key, os.environ["CONFIG_SHEET_ID"])
        
        # Load Data Config
        wks_data = sh_cfg.worksheet(SHEET_CONFIG_NAME)
        df_data = get_as_dataframe(wks_data, evaluate_formulas=True, dtype=str).dropna(how='all')
        
        # Load Schedule
        try: wks_sched = sh_cfg.worksheet(SHEET_SYS_CONFIG)
        except: print("⚠️ Không tìm thấy sheet sys_config"); return
        df_sched = get_as_dataframe(wks_sched, evaluate_formulas=True, dtype=str).dropna(how='all')
        
        # Load State (Bộ nhớ)
        try: wks_state = sh_cfg.worksheet(SHEET_SYS_STATE)
        except: 
            wks_state = sh_cfg.add_worksheet(SHEET_SYS_STATE, 100, 2)
            wks_state.update("A1", [["Block_Name", "Last_Run"]])
        
        df_state = get_as_dataframe(wks_state, evaluate_formulas=True, dtype=str)
        state_map = {}
        if not df_state.empty and "Block_Name" in df_state.columns:
            for _, r in df_state.iterrows():
                state_map[str(r["Block_Name"]).strip()] = str(r.get("Last_Run", "")).strip()
                
    except Exception as e:
        print(f"❌ Lỗi đọc file cấu hình: {e}"); return

    # 3. Kiểm tra Block cần chạy
    now_vn = datetime.now(VN_TZ)
    blocks_to_run = []
    
    for _, row in df_sched.iterrows():
        blk = str(row.get(SCH_COL_BLOCK, "")).strip()
        if not blk: continue
        
        last_run = state_map.get(blk, "")
        if should_run_block(row, last_run, now_vn):
            print(f"⚡ TRIGGER: {blk} (Last run: {last_run})")
            blocks_to_run.append(blk)

    if not blocks_to_run:
        print("😴 Không có tác vụ nào cần chạy lúc này.")
        return

    # 4. Thực thi (Multi-Bot Round Robin)
    df_run = df_data[df_data[COL_BLOCK_NAME].isin(blocks_to_run)]
    print(f"📋 Tìm thấy {len(df_run)} dòng lệnh cần xử lý.")
    
    # Nhóm theo file đích để tối ưu ghi
    grouped = defaultdict(list)
    for idx, r in df_run.iterrows():
        # Chỉ chạy những dòng đang Active
        if str(r.get(COL_STATUS, "")).strip() == "Chưa chốt & đang cập nhật":
            grouped[(r[COL_TGT_LINK], r[COL_TGT_SHEET])].append(r)

    bot_idx = 0
    logs = []
    success_blocks = set()

    for (tgt_link, tgt_sheet), rows in grouped.items():
        # Chọn Bot
        current_bot = bots[bot_idx % len(bots)]
        bot_idx += 1
        
        print(f"⚙️ Xử lý đích: ...{tgt_link[-10:]} | Sheet: {tgt_sheet}")
        
        # A. Tải dữ liệu nguồn
        data_to_write = [] # List of (df, write_mode)
        
        for r in rows:
            df, status = process_single_task(r, current_bot)
            if df is not None:
                mode = str(r.get(COL_WRITE_MODE, "Ghi Đè"))
                data_to_write.append((df, mode))
                print(f"   ✅ Tải OK: {r[COL_SRC_SHEET]} ({len(df)} dòng)")
            else:
                print(f"   ❌ Lỗi tải: {r[COL_SRC_SHEET]} | {status}")
                # Ghi log lỗi
                logs.append([
                    now_vn.strftime("%d/%m/%Y %H:%M:%S"), r.get(COL_DATA_RANGE), r.get(COL_MONTH),
                    "AutoBot", r.get(COL_SRC_LINK), tgt_link, tgt_sheet, r.get(COL_SRC_SHEET),
                    f"Lỗi: {status}", "0", "", r.get(COL_BLOCK_NAME)
                ])

        # B. Ghi vào đích
        if data_to_write:
            count, msg = write_to_target(tgt_link, tgt_sheet, data_to_write, current_bot)
            print(f"   💾 Kết quả ghi: {msg} ({count} dòng)")
            
            # Ghi log thành công & Đánh dấu Block
            for r in rows:
                # Tìm df tương ứng để log số dòng (ước lượng)
                logs.append([
                    now_vn.strftime("%d/%m/%Y %H:%M:%S"), r.get(COL_DATA_RANGE), r.get(COL_MONTH),
                    "AutoBot", r.get(COL_SRC_LINK), tgt_link, tgt_sheet, r.get(COL_SRC_SHEET),
                    msg, str(count) if count > 0 else "0", "", r.get(COL_BLOCK_NAME)
                ])
                success_blocks.add(r.get(COL_BLOCK_NAME))

    # 5. Cập nhật Log & State
    # A. Ghi Log
    if logs:
        try:
            wks_log = sh_cfg.worksheet(SHEET_LOG_NAME)
        except:
            wks_log = sh_cfg.add_worksheet(SHEET_LOG_NAME, 1000, 12)
            wks_log.append_row(["Thời gian","Vùng lấy","Tháng","User","Link Nguồn","Link Đích","Sheet Đích","Sheet Nguồn","Kết Quả","Số Dòng","Range","Block"])
        
        safe_api_call(wks_log.append_rows, logs)
        print("📝 Đã ghi log chi tiết.")

    # B. Cập nhật State (Last Run)
    if success_blocks:
        time_str = now_vn.strftime("%d/%m/%Y %H:%M:%S")
        df_state = get_as_dataframe(wks_state, evaluate_formulas=True, dtype=str)
        if df_state.empty: df_state = pd.DataFrame(columns=["Block_Name", "Last_Run"])
        
        for blk in success_blocks:
            if blk in df_state["Block_Name"].values:
                df_state.loc[df_state["Block_Name"] == blk, "Last_Run"] = time_str
            else:
                new_row = pd.DataFrame([{"Block_Name": blk, "Last_Run": time_str}])
                df_state = pd.concat([df_state, new_row], ignore_index=True)
        
        set_with_dataframe(wks_state, df_state, row=1, col=1)
        print(f"✅ Đã cập nhật Last Run cho {len(success_blocks)} blocks.")

    print("🏁 FINISHED.")

if __name__ == "__main__":
    run_auto_job()
