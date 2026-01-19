# -*- coding: utf-8 -*-
"""TXF-Continuous-Data-Pipeline.py"""

# 安裝必要套件 (Colab 環境專用)
# !pip install shioaji

import os
import sys
import json
import time
import pandas as pd
import numpy as np
import gspread
import shioaji as sj
from datetime import datetime, timezone, timedelta
from typing import Tuple, Dict, Any, Optional

# 嘗試載入 dotenv (用於本地端 .env 檔案)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # 如果在 Colab 且沒安裝 python-dotenv，就跳過

# ==========================================
# 0. 環境偵測與變數載入器 (Environment Loader)
# ==========================================

def get_env_variable(var_name: str, required: bool = True) -> Optional[str]:
    """
    通用變數讀取器：
    1. 優先讀取系統環境變數 (os.environ) -> 適用 Local/Docker/VPS
    2. 其次讀取 Google Colab Userdata -> 適用 Colab
    """
    # 1. Try System Environment Variable
    value = os.getenv(var_name)
    if value:
        return value

    # 2. Try Colab Userdata
    try:
        from google.colab import userdata
        return userdata.get(var_name)
    except (ImportError, AttributeError, KeyError):
        pass

    # 3. Handle Missing Variable
    if required:
        raise EnvironmentError(f"❌ 缺少必要環境變數: {var_name}")
    return None

# ==========================================
# 1. 全域配置與常數 (Configuration)
# ==========================================

print("=== Loading Configuration & Secrets ===")

# --- A. 功能開關 (Feature Flags) ---
FORCE_MXFR1 = True      # 強制使用近月合約代碼 (MXFR1)
TRIM_DATA = True        # 是否修剪非交易時段數據
QUERY_BACK_DAYS = 7     # 回補天數 (往前抓幾天)

# --- B. 連線重試設定 (Retry Config) ---
RETRY_MAX = 3
RETRY_DELAY_BASE = 1

# --- C. 市場時段設定 (Market Hours) ---
MARKET_HOURS = {
    "D": {"open": "08:45", "close": "13:45"},
    "N": {"open": "15:00", "close": "05:00"}
}

# --- D. Google Sheets 設定 (Tab Names) ---
TAB_NAME_SETTLE = 'TXF_settle_date_price'
TAB_NAME_5MIN   = '5mink_new'
TAB_NAME_60MIN  = '60mink_1'

# --- E. 機密資訊載入 (Secrets Loading) ---
# 在這裡一次性讀取所有 Secrets，後續程式碼只使用這些常數
try:
    # API Keys
    SHIOAJI_API_KEY = get_env_variable('SHIOAJI_API_KEY')
    SHIOAJI_SECRET_KEY = get_env_variable('SHIOAJI_SECRET_KEY')

    # Google Sheets IDs
    GSHEET_ID_DATA = get_env_variable('GSHEET_ID_DATA')
    GSHEET_ID_SETTLE = get_env_variable('GSHEET_ID_SETTLE')

    # Google Credentials (JSON String)
    GSHEET_CREDENTIALS_JSON = get_env_variable('GSHEET_CREDENTIALS')

    print("✅ All secrets loaded successfully.")

except EnvironmentError as e:
    print(e)
    print("請檢查 .env 檔案 (Local) 或 Secrets 設定 (Colab)。")
    sys.exit(1) # 缺少變數直接停止程式

# ==========================================
# 2. 認證與連線工具 (AuthManager)
# ==========================================

from google.oauth2.service_account import Credentials

class AuthManager:
    """處理 Google Sheets 與 Shioaji 的連線"""

    @staticmethod
    def get_gsheet_client():
        """建立 Google Sheets 連線 (使用全域常數)"""
        try:
            if not GSHEET_CREDENTIALS_JSON:
                raise ValueError("Credentials JSON is empty")

            creds_dict = json.loads(GSHEET_CREDENTIALS_JSON)
            # 修正 private_key 的換行符號問題
            creds_dict['private_key'] = creds_dict['private_key'].replace('\\n', '\n')

            creds = Credentials.from_service_account_info(
                creds_dict,
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            return gspread.authorize(creds)
        except Exception as e:
            raise ConnectionError(f"Google Sheet Auth Failed: {e}")

    @staticmethod
    def get_shioaji_api(max_retries=RETRY_MAX, base_delay=RETRY_DELAY_BASE):
        """建立 Shioaji API 連線 (使用全域常數)"""
        api = sj.Shioaji()

        print("[Auth] Logging into Shioaji...")
        try:
            api.login(
                api_key=SHIOAJI_API_KEY,      # 直接使用常數
                secret_key=SHIOAJI_SECRET_KEY, # 直接使用常數
                contracts_cb=lambda security_type: print(f"{repr(security_type)} fetch done.")
            )
        except Exception as e:
            raise ConnectionError(f"Shioaji Login Failed: {e}")

        # --- [Smart Retry] 檢查 API 用量 ---
        for attempt in range(1, max_retries + 1):
            try:
                usage_bytes = api.usage()['bytes']
                usage_mb = round(usage_bytes / (1024 * 1024), 2)
                print(f"[Auth] API Usage: {usage_mb} MB / 500 MB")
                break
            except Exception as e:
                if attempt < max_retries:
                    wait_time = base_delay * (2 ** (attempt - 1))
                    print(f"[Auth] 取得用量失敗 (第 {attempt} 次)，{wait_time} 秒後重試... 錯誤: {e}")
                    time.sleep(wait_time)
                else:
                    print(f"[Auth] Warning: 無法取得 API 用量 (已重試 {max_retries} 次)。")

        return api

# ==========================================
# 3. 核心邏輯：結算日計算 (SettleManager)
# ==========================================

class SettleManager:
    """處理結算日邏輯與合約代碼計算"""

    def __init__(self, gc):
        self.gc = gc
        # 直接使用全域常數 GSHEET_ID_SETTLE
        self.df_config = self._load_config()

    def _load_config(self) -> pd.DataFrame:
        """讀取結算設定表"""
        try:
            worksheet = self.gc.open_by_key(GSHEET_ID_SETTLE).worksheet(TAB_NAME_SETTLE)
            data = worksheet.get_all_values()
            df = pd.DataFrame(data[1:], columns=data[0])

            # 資料型態轉換
            cols_to_numeric = ['next_contract_diff', 'accumulated_contract_diff']
            for col in cols_to_numeric:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            cols_to_datetime = ['start_k', 'settle_k']
            for col in cols_to_datetime:
                df[col] = pd.to_datetime(df[col], errors='coerce')

            return df.dropna(subset=['contract_year_month'])
        except Exception as e:
            raise RuntimeError(f"Error loading settle config: {e}")

    def calculate_next_contract(self) -> str:
        """計算下一個合約代碼 (MXF+YM)"""
        last_row = self.df_config.iloc[-1]

        # 1. 推算下個合約月份
        last_ym_dt = datetime.strptime(str(last_row['contract_year_month']), '%Y%m')
        new_ym_dt = last_ym_dt + timedelta(days=31)
        new_contract_ym = new_ym_dt.strftime('%Y%m')

        # 2. 推算下個結算日 (該月第三個週三)
        first_day = datetime(new_ym_dt.year, new_ym_dt.month, 1)
        third_wed = first_day + pd.DateOffset(weeks=2)
        while third_wed.weekday() != 2:
            third_wed += timedelta(days=1)

        new_settle_k = third_wed + timedelta(hours=13, minutes=25)
        new_start_k = last_row['settle_k'] + timedelta(minutes=5)

        # 計算累積價差 (僅用於預測)
        new_acc_diff = last_row['accumulated_contract_diff'] + last_row['next_contract_diff']

        print(f"[Info] Current Config End: {last_row['contract_year_month']}")
        print(f"[Info] Predicted Next Contract: {new_contract_ym}, Settle: {new_settle_k}")

        # 將預測的新行加回記憶體中的 DataFrame
        new_row = pd.DataFrame([{
            'contract_year_month': new_contract_ym,
            'accumulated_contract_diff': new_acc_diff,
            'start_k': new_start_k,
            'settle_k': new_settle_k
        }])
        self.df_config = pd.concat([self.df_config, new_row], ignore_index=True)

        return 'MXF' + new_contract_ym

# ==========================================
# 4. 核心邏輯：資料處理 (DataProcessor)
# ==========================================

class DataProcessor:
    """處理 K 棒資料的清洗、重取樣、價差調整與完整性檢查"""

    @staticmethod
    def fetch_and_parse_kbars(api, contract_code: str, days_back: int) -> pd.DataFrame:
        """從 Shioaji 抓取資料"""
        now = datetime.now(timezone(timedelta(hours=+8)))
        end_date = now.strftime("%Y-%m-%d")
        start_date = (now - timedelta(days=days_back)).strftime('%Y-%m-%d')

        target_code = "MXFR1" if FORCE_MXFR1 else contract_code
        if not FORCE_MXFR1 and not api.Contracts.Futures.MXF[target_code]:
             target_code = "MXFR1"

        print(f"[Action] Fetching {target_code} from {start_date} to {end_date}...")

        contract = api.Contracts.Futures.MXF[target_code]
        kbars = api.kbars(contract, start=start_date, end=end_date)

        df = pd.DataFrame({**kbars}).drop(columns=["Amount"])
        if df.empty:
            return df, target_code

        df['ts'] = pd.to_datetime(df['ts'])
        df = df.set_index("ts").sort_index()

        return df, target_code

    @staticmethod
    def resample_and_split(df_raw: pd.DataFrame, df_settle_config: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Resample, Split D/N, Back Adjust, Add Metadata"""

        # 1. 轉 5分K
        df_5m = df_raw.resample('5min', label="left", closed='right').agg({
            'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
        }).dropna()

        # 2. 分切日盤/夜盤
        df_5m_D = df_5m.between_time(MARKET_HOURS["D"]["open"], MARKET_HOURS["D"]["close"]).copy()
        df_5m_N = df_5m.between_time(MARKET_HOURS["N"]["open"], MARKET_HOURS["N"]["close"]).copy()

        # 3. 轉 60分K
        df_60m_D = df_5m_D.resample("60min", offset="45min").agg({
            'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
        }).dropna()

        df_60m_N = df_5m_N.resample("60min").agg({
            'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
        }).dropna()

        # --- 補回 date_market_type (依據盤別開始日) ---
        def get_market_date_str(ts, is_night=False):
            target_ts = ts
            # 只有在凌晨 (00:00-05:00) 且是夜盤時，才需要減一天
            if is_night and ts.hour < 5:
                target_ts = ts - timedelta(days=1)
            suffix = "N" if is_night else "D"
            return target_ts.strftime("%y%m%d") + suffix

        for df_temp, is_night in [(df_5m_D, False), (df_5m_N, True), (df_60m_D, False), (df_60m_N, True)]:
             if not df_temp.empty:
                 df_temp['date_market_type'] = df_temp.index.to_series().apply(lambda x: get_market_date_str(x, is_night))

        # 4. 價差調整與欄位補全
        def process_final_df(df_d, df_n):
            df_all = pd.concat([df_d, df_n]).sort_index()
            if df_all.empty: return df_all

            df_all['contract_year_month'] = ""
            df_all['accumulated_contract_diff'] = 0

            def enrich_row(row):
                match = df_settle_config[
                    (row.name >= df_settle_config['start_k']) &
                    (row.name <= df_settle_config['settle_k'])
                ]
                res = row.copy()
                if not match.empty:
                    cfg = match.iloc[0]
                    diff = int(cfg['accumulated_contract_diff'])
                    res['accumulated_contract_diff'] = diff
                    res['contract_year_month'] = cfg['contract_year_month']
                    res['Open'] += diff
                    res['High'] += diff
                    res['Low'] += diff
                    res['Close'] += diff
                return res

            return df_all.apply(enrich_row, axis=1)

        df_5m_final = process_final_df(df_5m_D, df_5m_N)
        df_60m_final = process_final_df(df_60m_D, df_60m_N)

        return df_5m_final, df_60m_final

    @staticmethod
    def drop_incomplete_current_session(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """
        [過濾器] 若當下的盤別尚未收盤 (資料筆數不足)，直接捨棄該盤所有資料，不進行上傳。
        確保上傳的都是「已完結」的盤。
        """
        EXPECTED = {
            '5min':  {'D': 60, 'N': 168},
            '60min': {'D': 5,  'N': 14}
        }
        if timeframe not in EXPECTED or df.empty: return df

        # 1. 取得最後一筆資料的盤別分組
        last_ts = df.index[-1]

        # 定義分組邏輯 (Local function to avoid repetition)
        def get_group_id(ts):
            if 8 <= ts.hour <= 13: return ts.strftime('%Y-%m-%d') + '_D'
            elif ts.hour >= 15: return ts.strftime('%Y-%m-%d') + '_N'
            elif ts.hour < 5: return (ts - timedelta(days=1)).strftime('%Y-%m-%d') + '_N'
            return 'UNKNOWN'

        last_group_id = get_group_id(last_ts)

        # 2. 判斷當下時間是否就是該盤 (是否正在進行中)
        now = datetime.now(timezone(timedelta(hours=+8)))
        current_active_id = get_group_id(now)

        # 3. 檢查筆數
        # 為了效能，只抓最後 200 筆來判斷即可
        df_tail = df.iloc[-200:].copy()
        df_tail['group'] = df_tail.index.to_series().apply(get_group_id)

        last_group_count = len(df_tail[df_tail['group'] == last_group_id])
        expected_count = EXPECTED[timeframe].get(last_group_id.split('_')[-1], 0)

        # 4. 決策：如果是「正在進行中」且「筆數不足」，則丟棄
        if last_group_id == current_active_id and last_group_count < expected_count:
            print(f"[Filter] 偵測到盤中資料 {last_group_id} 尚未收盤 ({last_group_count}/{expected_count}) -> 捨棄不處理 (寧缺勿濫)。")
            return df.iloc[:-last_group_count]

        return df

    @staticmethod
    def check_completeness(df: pd.DataFrame, timeframe: str):
        """[資料完整性檢查] (嚴格版：所有資料必須完整)"""
        EXPECTED = {
            '5min':  {'D': 60, 'N': 168},
            '60min': {'D': 5,  'N': 14}
        }
        if timeframe not in EXPECTED or df.empty: return

        print(f"[Check] Verifying data completeness for {timeframe}...")
        ts_series = df.index.to_series()

        def get_group_id(ts):
            if 8 <= ts.hour <= 13: return ts.strftime('%Y-%m-%d') + '_D'
            elif ts.hour >= 15: return ts.strftime('%Y-%m-%d') + '_N'
            elif ts.hour < 5:
                prev_date = ts - timedelta(days=1)
                return prev_date.strftime('%Y-%m-%d') + '_N'
            return 'UNKNOWN'

        groups = ts_series.apply(get_group_id)
        counts = groups.value_counts()
        errors = []

        for group_id, count in counts.items():
            if 'UNKNOWN' in group_id: continue
            market_type = group_id.split('_')[-1]
            expected_count = EXPECTED[timeframe][market_type]
            if count != expected_count:
                errors.append(f"  - {group_id}: 預期 {expected_count} 筆, 實際 {count} 筆")

        if errors:
            raise ValueError(f"資料完整性檢查失敗 ({timeframe})，停止上傳！\n" + "\n".join(errors))
        print(f"[Check] {timeframe} Pass. All sessions appear complete.")

# ==========================================
# 5. 上傳工具 (SheetUploader)
# ==========================================

class SheetUploader:
    """處理 Google Sheets 的寫入與上傳邏輯"""

    @staticmethod
    def _prepare_data(df_new: pd.DataFrame, existing_data: list) -> Tuple[pd.DataFrame, bool]:
        """[純邏輯] 資料清洗與比對"""
        df_process = df_new.copy()
        df_process.index.name = 'ts'
        df_process.reset_index(inplace=True)
        df_process['ts'] = df_process['ts'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # A. 空表
        if not existing_data:
            cols = ['ts'] + [c for c in df_process.columns if c != 'ts']
            return df_process[cols], True

        # B. 只有表頭
        headers = existing_data[0]
        if len(existing_data) == 1:
            valid_cols = [c for c in headers if c in df_process.columns]
            return df_process[valid_cols], False

        # C. 正常更新
        try:
            ts_col_idx = headers.index('ts') if 'ts' in headers else 0
            last_ts_str = existing_data[-1][ts_col_idx]
            last_ts = pd.to_datetime(last_ts_str)

            current_ts_series = pd.to_datetime(df_process['ts'])
            df_to_upload = df_process[current_ts_series > last_ts].copy()

            if df_to_upload.empty:
                return pd.DataFrame(), False

            valid_cols = [c for c in headers if c in df_to_upload.columns]
            return df_to_upload[valid_cols], False
        except Exception as e:
            print(f"[Error] Data preparation failed: {e}")
            return pd.DataFrame(), False

    @staticmethod
    def append_safely(gc, tab_name: str, df_new: pd.DataFrame):
        """[I/O 操作] 連線並執行上傳 (使用全域常數 GSHEET_ID_DATA)"""
        try:
            worksheet = gc.open_by_key(GSHEET_ID_DATA).worksheet(tab_name)
            existing_data = worksheet.get_all_values()
        except gspread.WorksheetNotFound:
            print(f"[Error] Worksheet '{tab_name}' not found. Skipping.")
            return
        except Exception as e:
            print(f"[Error] Google Sheet Connection failed: {e}")
            return

        df_export, needs_header = SheetUploader._prepare_data(df_new, existing_data)

        if not df_export.empty:
            print(f"[{tab_name}] Uploading {len(df_export)} rows...")
            data_to_write = df_export.values.tolist()
            if needs_header:
                print(f"[{tab_name}] Detect empty sheet. Writing headers first.")
                data_to_write = [df_export.columns.tolist()] + data_to_write

            try:
                worksheet.append_rows(data_to_write)
                print(f"[{tab_name}] Upload Success.")
            except Exception as e:
                print(f"[{tab_name}] Upload Failed: {e}")
        else:
            print(f"[{tab_name}] No new data to upload.")

# ==========================================
# 6. 主程式 (Main Execution)
# ==========================================

if __name__ == "__main__":
    start_time = time.time()

    try:
        print("=== Automation Started ===")

        # 1. 初始化
        gc = AuthManager.get_gsheet_client()
        settle_mgr = SettleManager(gc)

        # 2. 取得合約
        target_contract = settle_mgr.calculate_next_contract()

        # 3. API 抓取
        api = AuthManager.get_shioaji_api()
        df_raw, used_code = DataProcessor.fetch_and_parse_kbars(api, target_contract, QUERY_BACK_DAYS)
        api.logout()

        if not df_raw.empty:
            # 4. 數據處理 + 過濾掉尚未收盤的當下資料 (避免盤中資料不全導致報錯)
            df_5m, df_60m = DataProcessor.resample_and_split(df_raw, settle_mgr.df_config)
            df_5m = DataProcessor.drop_incomplete_current_session(df_5m, '5min')
            df_60m = DataProcessor.drop_incomplete_current_session(df_60m, '60min')

            # 5. 完整性檢查 (此時剩下的都應該是完整的歷史盤，若缺漏則必須報錯)
            DataProcessor.check_completeness(df_5m, '5min')
            DataProcessor.check_completeness(df_60m, '60min')

            # 補上代碼
            df_5m['MXF_code'] = used_code
            df_60m['MXF_code'] = used_code

            # 6. 上傳
            SheetUploader.append_safely(gc, TAB_NAME_5MIN, df_5m)
            SheetUploader.append_safely(gc, TAB_NAME_60MIN, df_60m)
        else:
            print("[Warning] No data fetched from API.")

        print(f"=== Automation Finished in {round(time.time() - start_time, 2)}s ===")

    except ValueError as ve:
        print(f"\n[DATA INTEGRITY ERROR] -------------------------")
        print(ve)
        print(f"------------------------------------------------")
        print("上傳已終止，請檢查源頭資料狀況。")

    except Exception as e:
        print(f"[FATAL ERROR] Script crashed: {e}")

