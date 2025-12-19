import pandas as pd
import time
import random
import gspread
import json
import os
import pytz
from datetime import datetime
from google.oauth2 import service_account
from gspread_dataframe import get_as_dataframe
from collections import defaultdict

# --- CẤU HÌNH (Sửa lại nếu cần, hoặc dùng biến môi trường) ---
# Cách lấy Config: Ưu tiên lấy từ Biến môi trường (cho Github Actions), 
# nếu không có thì thử lấy từ file secrets.toml hoặc điền trực tiếp vào đây (không khuyến khích điền trực tiếp).

SHEET_CONFIG_NAME = "luu_cau_hinh"
SHEET_LOG_NAME = "log_lanthucthi"
SHEET_LOCK_NAME = "sys_lock"

COL_LINK_SRC = "Link file nguồn"
COL_LABEL_SRC = "Sheet nguồn"
COL_MONTH_SRC = "Tháng"
COL_BLOCK_NAME = "Block_Name"
COL_DATA_RANGE = "Vùng lấy dữ liệu"
DEFAULT_BLOCK_NAME = "Block_Mac_Dinh"

# --- 1. HÀM HỖ TRỢ XÁC THỰC ---
def get_creds_and_id():
    """
    Lấy Credential và Sheet ID từ biến môi trường (Environment Variables).
    Setup trên Github Secrets:
    1. GCP_SERVICE_ACCOUNT: Copy toàn bộ nội dung file JSON vào.
    2. HISTORY_SHEET_ID: ID của file Google Sheet cấu hình.
    """
    try:
        # Cách 1: Lấy từ Environment Variable (Dùng cho Github Actions/Server)
        creds_json_str = os.environ.get("GCP_SERVICE_ACCOUNT")
        sheet_id = os.environ.get("HISTORY_SHEET_ID")

        # Cách 2: (Fallback) Nếu chạy Local mà chưa set Env, thử đọc từ file toml (nếu bạn muốn)
        # Hoặc bạn có thể hard-code tạm thời để test (nhưng nhớ xóa khi up lên git)
        if not creds_json_str:
            # Ví dụ đọc từ file local (bỏ comment nếu cần)
            # with open("service_account.json", "r") as f: creds_json_str = f.read()
            # sheet_id = "PASTE_YOUR_SHEET_ID_HERE"
            print("⚠️ Không tìm thấy biến môi trường GCP_SERVICE_ACCOUNT")
            return None, None

        creds_info = json.loads(creds_json_str)
        if "private_key" in creds_info:
            creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
        
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
        return creds, sheet_id
    except Exception as e:
        print(f"❌ Lỗi Authentication: {e}")
        return None, None

def get_sh_with_retry(creds, sheet_id_or_key):
    gc = gspread.authorize(creds)
    max_retries = 3
    for i in range(max_retries):
        try:
            return gc.open_by_key(sheet_id_or_key)
        except Exception as e:
            if i == max_retries - 1: raise e
            time.sleep((2 ** i) + random.random())
    return None

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

# --- 2. CÁC HÀM XỬ LÝ DATA (CORE LOGIC) ---
def fetch_data_preserve_columns(row_config, creds):
    link_src = str(row_config.get('Link dữ liệu lấy dữ liệu', '')).strip()
    source_label = str(row_config.get('Tên sheet nguồn dữ liệu gốc', '')).strip()
    month_val = str(row_config.get('Tháng', ''))
    data_range_str = str(row_config.get(COL_DATA_RANGE, 'Lấy hết')).strip()
    if not data_range_str or data_range_str.lower() == 'nan': data_range_str = "Lấy hết"
    
    sheet_id = extract_id(link_src)
    if not sheet_id: return None, sheet_id, "Link lỗi"
    
    df = None
    try:
        sh_source = get_sh_with_retry(creds, sheet_id)
        if source_label:
            try: wks_source = sh_source.worksheet(source_label)
            except: return None, sheet_id, f"❌ Không tìm thấy sheet: '{source_label}'"
        else: wks_source = sh_source.sheet1
            
        data = wks_source.get_all_values()
        if data and len(data) > 0:
            headers = data[0]
            rows = data[1:]
            if not rows:
                df = pd.DataFrame(columns=headers)
            else:
                df = pd.DataFrame(rows, columns=headers)
            
            if data_range_str != "Lấy hết" and ":" in data_range_str:
                try:
                    start_col_str, end_col_str = data_range_str.split(":")
                    start_idx = col_name_to_index(start_col_str.strip())
                    end_idx = col_name_to_index(end_col_str.strip())
                    if start_idx >= 0 and end_idx >= start_idx:
                        end_idx = min(end_idx, len(df.columns) - 1)
                        df = df.iloc[:, start_idx : end_idx + 1]
                except: pass

            df = df.astype(str).replace(['nan', 'None', '<NA>', 'null'], '')
            status_msg = "Thành công"
        else:
            status_msg = "Sheet trắng tinh"
            
    except Exception as e:
        return None, sheet_id, f"Lỗi tải data: {str(e)}"

    if df is not None:
        df[COL_LINK_SRC] = link_src
        df[COL_LABEL_SRC] = source_label
        df[COL_MONTH_SRC] = month_val
        return df, sheet_id, status_msg
    return None, sheet_id, "Không lấy được dữ liệu"

def scan_realtime_row_ranges(target_link, target_sheet_name, creds):
    results = {}
    try:
        target_id = extract_id(target_link)
        if not target_id: return {}
        sh = get_sh_with_retry(creds, target_id)
        real_sheet_name = str(target_sheet_name).strip()
        if not real_sheet_name: real_sheet_name = "Tong_Hop_Data"
        try: wks = sh.worksheet(real_sheet_name)
        except: return {}
        all_data = wks.get_all_values()
        if not all_data: return {}
        headers = all_data[0]
        try: link_col_idx = headers.index(COL_LINK_SRC)
        except ValueError: return {} 
        temp_map = {} 
        for i, row in enumerate(all_data[1:], start=2):
            if len(row) > link_col_idx:
                link_val = row[link_col_idx]
                if link_val:
                    if link_val not in temp_map: temp_map[link_val] = [i, i]
                    else: temp_map[link_val][1] = i 
        for link, (start, end) in temp_map.items():
            results[link] = f"{start} - {end}"
    except: return {}
    return results

def smart_update_safe(tasks_list, target_link, target_sheet_name, creds):
    try:
        target_id = extract_id(target_link)
        if not target_id: return False, "Link đích lỗi"
        sh = get_sh_with_retry(creds, target_id)
        real_sheet_name = str(target_sheet_name).strip()
        if not real_sheet_name: real_sheet_name = "Tong_Hop_Data"
        try: wks = sh.worksheet(real_sheet_name)
        except: wks = sh.add_worksheet(title=real_sheet_name, rows=1000, cols=20)
        
        links_to_remove = [t[1] for t in tasks_list if t[1] and len(str(t[1])) > 5]
        existing_headers = []
        try: existing_headers = wks.row_values(1)
        except: pass
        
        if existing_headers and links_to_remove:
            try: 
                link_col_idx = existing_headers.index(COL_LINK_SRC) + 1
                col_values = wks.col_values(link_col_idx)
                rows_to_delete = []
                for i, val in enumerate(col_values):
                    if i > 0 and str(val).strip() in links_to_remove: rows_to_delete.append(i + 1)
                
                if rows_to_delete:
                    rows_to_delete.sort()
                    ranges = []; start = rows_to_delete[0]; end = start
                    for r in rows_to_delete[1:]:
                        if r == end + 1: end = r
                        else: ranges.append((start, end)); start = r; end = r
                    ranges.append((start, end))
                    delete_reqs = []
                    for start, end in reversed(ranges):
                        delete_reqs.append({"deleteDimension": {"range": {"sheetId": wks.id, "dimension": "ROWS", "startIndex": start - 1, "endIndex": end}}})
                    if delete_reqs:
                        sh.batch_update({'requests': delete_reqs})
                        time.sleep(1)
            except ValueError: pass

        if not existing_headers:
            first_df = tasks_list[0][0]
            if first_df is not None and not first_df.empty:
                final_headers = first_df.columns.tolist()
                wks.append_row(final_headers)
                existing_headers = final_headers
            else: return True, "Không có dữ liệu nguồn để tạo header"
        else:
            final_headers = existing_headers
            all_new_cols = []
            for t in tasks_list:
                if t[0] is not None: all_new_cols.extend(t[0].columns.tolist())
            seen = set(existing_headers)
            cols_to_add = [x for x in all_new_cols if x not in seen and not seen.add(x)]
            if cols_to_add:
                wks.resize(cols=len(existing_headers) + len(cols_to_add))
                final_headers = existing_headers + cols_to_add
                wks.update(range_name="A1", values=[final_headers])

        data_to_append = []
        for df, src_link in tasks_list:
            if df is not None and not df.empty:
                df_aligned = df.reindex(columns=final_headers, fill_value="")
                data_to_append.extend(df_aligned.values.tolist())

        if data_to_append:
            BATCH_SIZE = 5000
            for i in range(0, len(data_to_append), BATCH_SIZE):
                chunk = data_to_append[i : i + BATCH_SIZE]
                wks.append_rows(chunk)
                time.sleep(1)
            return True, f"Thành công (+{len(data_to_append)} dòng)"
        return True, "Thành công (Không có data mới)"
    except Exception as e: return False, f"Lỗi Ghi: {str(e)}"

# --- 3. SYSTEM LOCK & LOG ---
def get_system_lock(creds, history_id):
    try:
        sh = get_sh_with_retry(creds, history_id)
        try: wks = sh.worksheet(SHEET_LOCK_NAME)
        except: 
            wks = sh.add_worksheet(SHEET_LOCK_NAME, rows=10, cols=5)
            wks.update([["is_locked", "user", "time_start"], ["FALSE", "", ""]])
            return False, "", ""
        val = wks.cell(2, 1).value
        user = wks.cell(2, 2).value
        time_str = wks.cell(2, 3).value
        if val == "TRUE":
            try:
                if (datetime.now() - datetime.strptime(time_str, "%d/%m/%Y %H:%M:%S")).total_seconds() > 1800: return False, "", ""
            except: pass
            return True, user, time_str
        return False, "", ""
    except: return False, "", ""

def set_system_lock(creds, history_id, user_id, lock=True):
    try:
        sh = get_sh_with_retry(creds, history_id)
        try: wks = sh.worksheet(SHEET_LOCK_NAME)
        except: wks = sh.add_worksheet(SHEET_LOCK_NAME, rows=10, cols=5)
        now_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        wks.update("A2:C2", [["TRUE", user_id, now_str]] if lock else [["FALSE", "", ""]])
    except: pass

def write_detailed_log(creds, history_id, log_data_list):
    if not log_data_list: return
    try:
        sh = get_sh_with_retry(creds, history_id)
        try: wks = sh.worksheet(SHEET_LOG_NAME)
        except: 
            wks = sh.add_worksheet(SHEET_LOG_NAME, rows=1000, cols=15)
            wks.append_row([
                "Ngày & giờ get dữ liệu", "Vùng lấy dữ liệu", "Tháng", "Nhân sự get", 
                "Link nguồn", "Link đích", "Sheet Đích", "Sheet nguồn lấy dữ liệu", 
                "Trạng Thái", "Số Dòng Đã Lấy", "Dòng dữ liệu cập nhật", "Chạy từ khối"
            ])
        wks.append_rows(log_data_list)
    except Exception as e: print(f"Lỗi log: {e}")

# --- 4. PIPELINE ---
def process_pipeline(rows_to_run, user_id, block_name_run, creds, history_id):
    # Lock Check
    is_locked, locking_user, lock_time = get_system_lock(creds, history_id)
    if is_locked and locking_user != user_id and "AutoAll" not in user_id:
        print(f"🔒 Hệ thống đang bận bởi {locking_user}")
        return False, {}, 0
    
    set_system_lock(creds, history_id, user_id, lock=True)
    try:
        grouped_tasks = defaultdict(list)
        total_fetched_rows = 0
        
        for row in rows_to_run:
            raw_t = row.get('Link dữ liệu đích', '')
            t_link = str(raw_t[0]).strip() if isinstance(raw_t, list) and raw_t else str(raw_t).strip()
            row['Link dữ liệu đích'] = t_link 
            raw_s = row.get('Link dữ liệu lấy dữ liệu', '')
            s_link = str(raw_s[0]).strip() if isinstance(raw_s, list) and raw_s else str(raw_s).strip()
            row['Link dữ liệu lấy dữ liệu'] = s_link 
            t_sheet = str(row.get('Tên sheet dữ liệu đích', '')).strip()
            if not t_sheet: t_sheet = "Tong_Hop_Data"
            if COL_DATA_RANGE not in row or not row[COL_DATA_RANGE]: row[COL_DATA_RANGE] = "Lấy hết"
            grouped_tasks[(t_link, t_sheet)].append(row)

        global_results_map = {} 
        log_entries = []
        tz_vn = pytz.timezone('Asia/Ho_Chi_Minh')
        time_now = datetime.now(tz_vn).strftime("%d/%m/%Y %H:%M:%S")

        for (target_link, target_sheet), group_rows in grouped_tasks.items():
            if not target_link: continue
            
            tasks_list = []
            for row in group_rows:
                print(f"📥 Đang tải: {row.get('Link dữ liệu lấy dữ liệu')}...")
                df, sid, status = fetch_data_preserve_columns(row, creds)
                src_link = row['Link dữ liệu lấy dữ liệu']
                
                if df is not None:
                    tasks_list.append((df, src_link))
                    total_fetched_rows += len(df)
                else:
                    global_results_map[src_link] = ("Lỗi tải/Quyền", "")
                    log_entries.append([
                        time_now, str(row.get(COL_DATA_RANGE, 'Lấy hết')), str(row.get('Tháng', '')), 
                        user_id, src_link, target_link, target_sheet,
                        row.get('Tên sheet nguồn dữ liệu gốc', ''), "Lỗi tải", "0", "", block_name_run
                    ])

            msg_update = ""
            success_update = True
            if tasks_list:
                print(f"💾 Đang ghi vào: {target_link}...")
                success_update, msg_update = smart_update_safe(tasks_list, target_link, target_sheet, creds)
            
            realtime_ranges = scan_realtime_row_ranges(target_link, target_sheet, creds)
            
            for link, rng in realtime_ranges.items():
                if link not in global_results_map: global_results_map[link] = ("Cập nhật lại", rng)
                else:
                    current_msg = global_results_map[link][0]
                    global_results_map[link] = (current_msg, rng)

            for row in group_rows:
                s_link = row['Link dữ liệu lấy dữ liệu']
                status_str = "Thành công" if success_update else f"Lỗi: {msg_update}"
                final_range = realtime_ranges.get(s_link, "")
                
                if any(t[1] == s_link for t in tasks_list) or (s_link in global_results_map and "Lỗi" in global_results_map[s_link][0]):
                    height = "0"
                    for df, sl in tasks_list:
                        if sl == s_link: height = str(len(df))

                    log_entries.append([
                        time_now, str(row.get(COL_DATA_RANGE, 'Lấy hết')), str(row.get('Tháng', '')),
                        user_id, s_link, target_link, target_sheet,
                        row.get('Tên sheet nguồn dữ liệu gốc', ''), 
                        status_str, height, final_range, block_name_run
                    ])
                    global_results_map[s_link] = (status_str, final_range)
        
        write_detailed_log(creds, history_id, log_entries)
        return True, global_results_map, total_fetched_rows
    finally:
        set_system_lock(creds, history_id, user_id, lock=False)

def verify_access_fast(url, creds):
    sheet_id = extract_id(url)
    if not sheet_id: return False, "Link lỗi"
    try:
        get_sh_with_retry(creds, sheet_id)
        return True, "OK"
    except Exception as e: return False, f"Lỗi: {e}"

def check_permissions_strict(rows_to_run, creds):
    checked_links = {} 
    for row in rows_to_run:
        # Nguồn
        link_src = str(row.get('Link dữ liệu lấy dữ liệu', '')).strip()
        if "docs.google.com" in link_src:
            if link_src not in checked_links: checked_links[link_src] = verify_access_fast(link_src, creds)
            if not checked_links[link_src][0]: return False
        # Đích
        link_tgt = str(row.get('Link dữ liệu đích', '')).strip()
        if "docs.google.com" in link_tgt:
            if link_tgt not in checked_links: checked_links[link_tgt] = verify_access_fast(link_tgt, creds)
            if not checked_links[link_tgt][0]: return False
    return True

# --- MAIN RUNNER ---
def main():
    print("🚀 BẮT ĐẦU CHẠY AUTO JOB...")
    creds, history_id = get_creds_and_id()
    if not creds or not history_id:
        print("❌ Thiếu Credential hoặc ID Sheet Config. Dừng.")
        return

    try:
        # 1. Tải Config
        print("📥 Đang đọc cấu hình...")
        sh_config = get_sh_with_retry(creds, history_id)
        wks_config = sh_config.worksheet(SHEET_CONFIG_NAME)
        df_full = get_as_dataframe(wks_config, evaluate_formulas=True, dtype=str)
        df_full = df_full.dropna(how='all')
        
        # Chuẩn hóa cột
        required_cols = ['Trạng thái', COL_DATA_RANGE, 'Tháng', 'Link dữ liệu lấy dữ liệu', 'Link dữ liệu đích', 'Tên sheet dữ liệu đích', 'Tên sheet nguồn dữ liệu gốc', 'Kết quả', 'Dòng dữ liệu', COL_BLOCK_NAME]
        for c in required_cols:
            if c not in df_full.columns: df_full[c] = ""
        df_full[COL_BLOCK_NAME] = df_full[COL_BLOCK_NAME].replace('', DEFAULT_BLOCK_NAME).fillna(DEFAULT_BLOCK_NAME)
        df_full[COL_DATA_RANGE] = df_full[COL_DATA_RANGE].replace('', 'Lấy hết').fillna('Lấy hết')
        if 'Trạng thái' in df_full.columns:
            df_full['Trạng thái'] = df_full['Trạng thái'].apply(lambda x: "Đã chốt" if str(x).strip() in ["Đã chốt", "Đã cập nhật", "TRUE"] else "Chưa chốt & đang cập nhật")

        all_blocks = df_full[COL_BLOCK_NAME].unique()
        total_all_rows = 0
        user_id_run = "Auto_Bot_Github"

        # 2. Duyệt từng Block
        for blk in all_blocks:
            print(f"⏳ Kiểm tra khối: {blk}...")
            block_mask = (df_full[COL_BLOCK_NAME] == blk) & (df_full['Trạng thái'] == "Chưa chốt & đang cập nhật")
            rows_blk = df_full[block_mask].to_dict('records')

            if rows_blk:
                # Check quyền
                if not check_permissions_strict(rows_blk, creds):
                    print(f"❌ Khối {blk} bị bỏ qua do lỗi quyền.")
                    continue
                
                # Chạy Pipeline
                _, results_map, rows_count = process_pipeline(rows_blk, user_id_run, blk, creds, history_id)
                total_all_rows += rows_count
                
                # Cập nhật kết quả vào DataFrame
                if results_map:
                    for idx, row in df_full[block_mask].iterrows():
                        s_link = str(row['Link dữ liệu lấy dữ liệu']).strip()
                        if s_link in results_map:
                            msg, rng = results_map[s_link]
                            df_full.at[idx, 'Kết quả'] = msg
                            df_full.at[idx, 'Dòng dữ liệu'] = rng
                print(f"✅ Xong khối {blk} (+{rows_count} dòng).")
            else:
                print(f"⚪ Khối {blk} không có dữ liệu cần chạy.")

        # 3. Lưu toàn bộ Config xuống Sheet
        print("💾 Đang lưu kết quả cập nhật...")
        target_cols = [
            COL_BLOCK_NAME, 'Trạng thái', COL_DATA_RANGE, 'Tháng', 
            'Link dữ liệu lấy dữ liệu', 'Link dữ liệu đích', 'Tên sheet dữ liệu đích', 
            'Dòng dữ liệu', 'Kết quả', 'Tên sheet nguồn dữ liệu gốc'
        ]
        df_full = df_full.astype(str).replace(['nan', 'None', '<NA>'], '')
        for c in target_cols:
            if c not in df_full.columns: df_full[c] = ""
        df_full = df_full[target_cols]
        
        wks_config.clear()
        wks_config.update([df_full.columns.tolist()] + df_full.values.tolist())
        print(f"🏁 HOÀN TẤT! Tổng {total_all_rows} dòng.")

    except Exception as e:
        print(f"❌ LỖI FATAL: {e}")

if __name__ == "__main__":
    main()
