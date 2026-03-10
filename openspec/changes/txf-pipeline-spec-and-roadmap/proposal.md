## Why

TXF-Continuous-Data-Pipeline 已完成 v1.0 的核心 ETL 功能，但缺乏正式的系統規格文件與實作路線圖。補齊規格文件能讓未來的維護、擴充（Line Notify、Docker 部署、GAS 整合）有清楚的設計依據，同時降低協作與交接成本。

## What Changes

- **新增** 系統設計規格（架構、資料流、模組職責）
- **新增** 核心功能規格：ETL 管線、資料驗證、增量更新、換月調整
- **新增** 資料結構規格：K 棒欄位定義、結算設定表格式
- **新增** 環境與部署規格：環境變數、Docker、Cron 排程
- **新增** Roadmap 任務拆解：Line Notify 推播、Docker/VPS 部署、GAS 回測整合

## Capabilities

### New Capabilities

- `etl-pipeline`: 核心 ETL 流程規格，涵蓋 Extract（Shioaji API 抓取）、Transform（Resample / 日夜盤切割 / Back Adjustment）、Load（Google Sheets 寫入）
- `data-validation`: 資料品質管控機制，包含 Gate 1（完整性檢查）與盤中過濾（drop_incomplete_current_session）
- `incremental-update`: 水位線機制（Gate 2）確保冪等性，只上傳比 Sheet 最後時間戳記更新的資料
- `settle-management`: 結算日管理，讀取設定表、換月價差計算、自動推算下一合約代碼與結算日
- `auth-management`: 連線管理，Google Sheets OAuth 與 Shioaji API 登入，含指數退避重試機制
- `notification`: Line Notify 推播執行結果（Roadmap）
- `deployment`: Docker 容器化與 VPS（GCP e2-micro）部署，含 Cron 排程（Roadmap）
- `gas-integration`: 串接 Google Apps Script 觸發回測策略信號（Roadmap）

### Modified Capabilities

（目前無既有 spec，無需記錄變更）

## Impact

- **程式碼**：`main.py`（現有實作的規格化參考）
- **設定檔**：`.env.example`、`settle_config_template.csv`
- **外部依賴**：Shioaji API、Google Sheets API（gspread）、Google OAuth
- **新增依賴**（Roadmap）：Line Notify API、Docker、GCP e2-micro、Google Apps Script
