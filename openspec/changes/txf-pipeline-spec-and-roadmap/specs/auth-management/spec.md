## ADDED Requirements

### Requirement: Google Sheets 連線建立
系統 SHALL 使用 Service Account 金鑰（JSON 格式）建立 Google Sheets OAuth 連線，金鑰從環境變數 `GSHEET_CREDENTIALS` 讀取。

#### Scenario: 正常建立連線
- **WHEN** `GSHEET_CREDENTIALS` 環境變數存在且為有效的 JSON 字串
- **THEN** 回傳已認證的 `gspread` 客戶端物件，scope 限定為 `spreadsheets`

#### Scenario: private_key 換行符號修正
- **WHEN** JSON 字串中 `private_key` 包含字面字串 `\n`（非真正換行）
- **THEN** 系統自動將 `\\n` 替換為真正的換行符號後再建立憑證

#### Scenario: 金鑰為空或無效
- **WHEN** `GSHEET_CREDENTIALS` 為空字串或無法解析為 JSON
- **THEN** 拋出 `ConnectionError: Google Sheet Auth Failed: {原始錯誤訊息}`

---

### Requirement: Shioaji API 登入
系統 SHALL 使用 `SHIOAJI_API_KEY` 與 `SHIOAJI_SECRET_KEY` 環境變數登入 Shioaji API，並在登入後驗證 API 用量。

#### Scenario: 正常登入
- **WHEN** API Key 與 Secret Key 有效
- **THEN** 成功登入並輸出 `[Auth] Logging into Shioaji...`，回傳 API 物件

#### Scenario: 登入失敗
- **WHEN** API Key 無效或網路異常
- **THEN** 拋出 `ConnectionError: Shioaji Login Failed: {原始錯誤訊息}`

---

### Requirement: API 用量查詢重試機制（指數退避）
系統 SHALL 在登入後查詢 API 用量，若查詢失敗則以指數退避方式重試，最多重試 `RETRY_MAX`（預設 3）次。

退避等待時間公式：`RETRY_DELAY_BASE * (2 ** (attempt - 1))`（秒）

#### Scenario: 第一次查詢成功
- **WHEN** API 用量查詢在第一次即成功
- **THEN** 輸出 `[Auth] API Usage: {usage_mb} MB / 500 MB`，不進行重試

#### Scenario: 查詢失敗後重試成功
- **WHEN** 前幾次查詢失敗，但在 `RETRY_MAX` 次內成功
- **THEN** 輸出每次失敗的等待訊息，最終輸出用量資訊

#### Scenario: 超過最大重試次數
- **WHEN** 連續 `RETRY_MAX` 次查詢均失敗
- **THEN** 輸出警告 `[Auth] Warning: 無法取得 API 用量 (已重試 N 次)`，繼續執行（不中止）

---

### Requirement: 環境變數讀取優先順序
系統 SHALL 依以下順序讀取環境變數，第一個有值的來源優先：
1. 系統環境變數（`os.environ`）
2. Google Colab Userdata（`google.colab.userdata`）

必要變數（`required=True`）缺失時，拋出 `EnvironmentError` 並終止程式。

#### Scenario: Local 環境讀取 .env
- **WHEN** 執行於本機且 `.env` 已由 `python-dotenv` 載入
- **THEN** 從系統環境變數成功讀取所有必要變數

#### Scenario: Colab 環境讀取 Secrets
- **WHEN** 執行於 Google Colab 且 Secrets 已設定
- **THEN** 從 `google.colab.userdata` 成功讀取變數

#### Scenario: 缺少必要變數
- **WHEN** 任一必要環境變數（如 `SHIOAJI_API_KEY`）在兩個來源均未設定
- **THEN** 拋出 `EnvironmentError: ❌ 缺少必要環境變數: {var_name}`，程式以 `sys.exit(1)` 終止
