## ADDED Requirements

### Requirement: Docker 容器化
系統 SHALL 提供 `Dockerfile`，將 ETL 腳本打包為可攜式容器映像，確保在任何支援 Docker 的環境均可執行。

#### Scenario: 建構映像
- **WHEN** 執行 `docker build -t txf-pipeline .`
- **THEN** 成功建構映像，基礎映像為 `python:3.12-slim`，已安裝 `requirements.txt` 所有依賴

#### Scenario: 容器執行
- **WHEN** 執行 `docker run --env-file .env txf-pipeline`
- **THEN** 容器讀取 `.env` 環境變數並正常執行 `python main.py`

#### Scenario: 環境變數注入
- **WHEN** 透過 `--env-file` 或 `-e` 參數傳入環境變數
- **THEN** 容器內 `os.environ` 可讀取所有必要變數

---

### Requirement: Cron Job 排程
系統 SHALL 支援透過 Cron Job 定期自動執行，每個交易日於日盤收盤後（14:00 CST）與夜盤結束後（05:30 CST）各觸發一次。

#### Scenario: 日盤收盤後執行
- **WHEN** Cron 觸發時間為交易日 14:00 CST
- **THEN** 腳本執行，抓取當日日盤資料，完整性檢查通過後上傳 Google Sheets

#### Scenario: 夜盤結束後執行
- **WHEN** Cron 觸發時間為 05:30 CST（次日凌晨）
- **THEN** 腳本執行，抓取前一日夜盤資料，完整性檢查通過後上傳

#### Scenario: 非交易日執行
- **WHEN** Cron 在週末或國定假日觸發
- **THEN** Shioaji API 回傳空資料，腳本輸出 `[Warning] No data fetched from API.` 後正常結束

---

### Requirement: VPS 部署（GCP e2-micro）
系統 SHALL 可部署於 GCP e2-micro 免費層 VM，配合 Cron Job 長期穩定運行。

#### Scenario: 初始部署
- **WHEN** 在 GCP e2-micro VM 上首次設定
- **THEN** 依序完成：安裝 Docker → 拉取映像 → 設定 `.env` → 設定系統 Cron

#### Scenario: 容器重啟策略
- **WHEN** VM 重新開機
- **THEN** Cron Job 在下次排程時間自動執行，無需手動介入

#### Scenario: 日誌保存
- **WHEN** ETL 腳本執行完畢
- **THEN** stdout/stderr 輸出重導向至日誌檔，保留最近 30 天（由 `logrotate` 管理）

---

### Requirement: 部署設定文件
系統 SHALL 提供以下部署相關檔案：

| 檔案 | 說明 |
|---|---|
| `Dockerfile` | 容器映像定義 |
| `docker-compose.yml` | 本機測試用編排設定 |
| `.env.example` | 環境變數範本（不含機密值） |
| `cron.sh` | Cron 執行腳本（含日誌輸出） |

#### Scenario: .env.example 不含機密值
- **WHEN** 查看 `.env.example`
- **THEN** 所有機密欄位的值為空或佔位符（如 `your_api_key_here`），不包含真實金鑰
