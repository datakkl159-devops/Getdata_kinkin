import os, json, pandas as pd, gspread, time, pytz
from datetime import datetime
from google.oauth2 import service_account
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from collections import defaultdict

# --- CONFIG ---
SHEET_CONFIG = "luu_cau_hinh"; SHEET_STATE = "sys_state"; SHEET_LOG = "log_lanthucthi"; SHEET_SCHED = "sys_config"
COL_BOT = "Bot_Phu_Trach"; COL_BLOCK = "Block_Name"; COL_STATUS = "Trạng thái"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
VN_TZ = pytz.timezone('Asia/Ho_Chi_Minh')

def load_bots():
    bots = {}
    if "GCP_SERVICE_ACCOUNT" in os.environ:
        bots["Bot 1"] = service_account.Credentials.from_service_account_info(json.loads(os.environ["GCP_SERVICE_ACCOUNT"]), scopes=SCOPES)
    for i in range(1, 10):
        k = f"GCP_SERVICE_ACCOUNT_{i}"
        if k in os.environ: bots[f"Bot {i+1}"] = service_account.Credentials.from_service_account_info(json.loads(os.environ[k]), scopes=SCOPES)
    return bots

def safe_api(func, *args, **kwargs):
    for i in range(5):
        try: return func(*args, **kwargs)
        except Exception as e:
            if "429" in str(e) or "quota" in str(e).lower(): time.sleep(2**i + 5)
            elif i==4: print(f"❌ API Err: {e}"); return None
            else: time.sleep(2)

def run_job():
    print(f"🚀 START: {datetime.now(VN_TZ)}")
    bots = load_bots()
    if not bots: print("❌ No bots!"); return
    master = bots.get("Bot 1")
    
    # 1. Read Config & Schedule
    gc = gspread.authorize(master)
    sh = safe_api(gc.open_by_key, os.environ["CONFIG_SHEET_ID"])
    
    df_data = get_as_dataframe(sh.worksheet(SHEET_CONFIG), evaluate_formulas=True, dtype=str).dropna(how='all')
    df_sched = get_as_dataframe(sh.worksheet(SHEET_SCHED), evaluate_formulas=True, dtype=str).dropna(how='all')
    
    try: wks_state = sh.worksheet(SHEET_STATE)
    except: wks_state = sh.add_worksheet(SHEET_STATE, 100, 2)
    df_state = get_as_dataframe(wks_state, evaluate_formulas=True, dtype=str)
    state_map = dict(zip(df_state["Block_Name"], df_state["Last_Run"])) if not df_state.empty else {}

    # 2. Check Schedule
    now = datetime.now(VN_TZ)
    blocks_run = []
    
    for _, r in df_sched.iterrows():
        blk = r.get("Block_Name")
        # [Simplified Logic for brevity - Use V98 logic here]
        # Giả sử logic check time ở đây trả về True
        if blk: blocks_run.append(blk) 

    if not blocks_run: print("😴 Sleep."); return

    # 3. Execute
    df_run = df_data[df_data[COL_BLOCK].isin(blocks_run)]
    
    # Group by Bot -> Target File
    tasks = defaultdict(list)
    for _, r in df_run.iterrows():
        if r.get(COL_STATUS) == "Chưa chốt & đang cập nhật":
            bot_name = r.get(COL_BOT, "Bot 1")
            if bot_name not in bots: bot_name = "Bot 1"
            tasks[(bot_name, r["Link dữ liệu đích"], r["Tên sheet dữ liệu đích"])].append(r)

    for (b_name, link, sheet), rows in tasks.items():
        worker = bots[b_name]
        print(f"🤖 {b_name} running {sheet}...")
        # ... (Gọi hàm xử lý data V95 tại đây) ...
        # ... (Ghi log & Update State) ...

if __name__ == "__main__":
    run_job()
