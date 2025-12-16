import utils
import pandas as pd
from datetime import datetime

def run_auto_bot():
    print("--- AUTO BOT START ---")
    try:
        ws_config = utils.get_db_worksheet("luu_cau_hinh")
        data = ws_config.get_all_records()
        df = pd.DataFrame(data)
    except Exception as e:
        print(f"Lỗi load config: {e}")
        return

    today = datetime.now().date()

    for index, row in df.iterrows():
        # Chỉ chạy khối chưa chốt
        if row['Status'] != "Chưa chốt & đang cập nhật":
            continue

        # Logic Hẹn Giờ
        last_run_str = str(row['Last_Run'])
        freq = row['Tan_Suat_Hen_Gio']
        should_run = False
        
        if not last_run_str: 
            should_run = True
        else:
            try:
                last_run = datetime.strptime(last_run_str, "%Y-%m-%d").date()
                diff = (today - last_run).days
                if freq == "Hàng ngày" and diff >= 1: should_run = True
                elif freq == "Hàng tuần" and diff >= 7: should_run = True
                elif freq == "Hàng tháng" and diff >= 30: should_run = True
            except: should_run = True

        if should_run:
            print(f"Running: {row['Block_Name']}...")
            
            # Check Lock
            locked, user = utils.check_lock()
            if locked and user != "System_Auto":
                print(f"Skip. Locked by {user}")
                continue

            # Chạy
            ok, msg, count, rng = utils.process_single_block(row, "GITHUB_BOT", is_auto=True)
            print(f"Result: {msg} | Rows: {count} | Range: {rng}")

            if ok:
                # Update DB (index + 2)
                ws_config.update_cell(index + 2, 9, str(today))
                ws_config.update_cell(index + 2, 11, count)
                ws_config.update_cell(index + 2, 13, rng)

    print("--- AUTO BOT END ---")

if __name__ == "__main__":
    run_auto_bot()
