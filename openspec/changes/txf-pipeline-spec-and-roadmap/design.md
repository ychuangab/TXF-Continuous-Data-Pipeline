## Context

TXF-Continuous-Data-Pipeline 是以單一 `main.py` 實作的 ETL 腳本，採 OOP 設計（4 個 Class），透過 Cron Job 定期執行。

現況：
- 核心 ETL 功能（v1.0）已上線穩定運行
- 以 Google Sheets 作為資料庫，兼具免費與 GAS/GCP 整合優勢
- 尚無 spec 文件、無容器化、無執行通知機制

主要限制：
- Shioaji API 每月流量上限 500 MB
- Google Sheets API 每分鐘 60 次寫入限制
- 台指期交易時段固定（日盤 08:45–13:45 / 夜盤 15:00–05:00）

## Goals / Non-Goals

**Goals:**
- 為現有功能建立可追溯的規格文件
- 明確定義各模組的職責邊界與資料契約
- 為 Roadmap 功能（Line Notify、Docker、GAS）規劃清楚的實作路徑
- 讓新協作者能快速理解系統行為

**Non-Goals:**
- 不重構現有 main.py 架構（除非 Roadmap 任務需要）
- 不引入資料庫（維持 Google Sheets 作為儲存層）
- 不支援台指期以外的商品（如 TX、TE）

## Decisions

### 決策 1：維持單一檔案架構（main.py）

**決定**：規格文件階段不拆分 main.py 為多個模組。

**理由**：
- 現有程式碼邏輯清晰，4 個 Class 已具備足夠分層
- 拆分會增加部署複雜度（Cron Job 需要指定入口點）
- 只有在 Roadmap 的 Docker/VPS 部署實作時，才評估拆分必要性

**替代方案**：拆分為 `auth.py`, `processor.py`, `uploader.py` → 目前不採用

---

### 決策 2：以增量更新（水位線）取代全量比對

**決定**：每次執行先讀取 Sheet 最後一筆 `ts`，只處理更新的資料。

**理由**：
- 避免重複讀取/寫入整張 Sheet（節省 API 配額）
- 即使 Cron Job 重複觸發也不會污染資料（冪等性）
- 萬一 Sheet 為空，系統自動退化為全量寫入

**替代方案**：每次全量讀取 Sheet 再比對 → 效能差，配額消耗高

---

### 決策 3：Google Sheets 作為唯一儲存層

**決定**：維持 Google Sheets 作為 K 棒資料庫。

**理由**：
- 免費且可直接由 Google Apps Script 存取，利於後續回測整合
- 可在 GCP e2-micro 免費層內完成整個系統
- 適合個人/小規模資料量（每日約 168+60 筆 5分K）

**替代方案**：TimescaleDB / BigQuery → 成本較高，對本專案過度設計

---

### 決策 4：採用 Back Adjustment（後驗調整）而非 Panama Canal Method

**決定**：以結算日價差為基礎，對歷史資料進行固定點數平移。

**理由**：
- 計算簡單透明，每個合約的調整量固定不變
- 適合 TXF 這種月結算、價差可查的商品
- `accumulated_contract_diff` 欄位讓每筆資料的調整量可追溯

**替代方案**：Panama Canal Method（比例調整）→ 實作複雜，對指數型商品才有明顯優勢

---

### 決策 5：Roadmap 功能以獨立 spec 規劃，逐步實作

**決定**：Line Notify、Docker、GAS 各自建立 spec，在主線 ETL 穩定後依序實作。

**理由**：
- 避免 Big Bang 式大重構
- 每個 Roadmap 項目可獨立部署、驗證
- Line Notify 最輕量，優先實作以增加系統可觀測性

## Risks / Trade-offs

| 風險 | 緩解策略 |
|---|---|
| Shioaji API 用量超限（500 MB/月） | `QUERY_BACK_DAYS=7` 控制回補天數；監控 `usage_bytes` 並記錄日誌 |
| Google Sheets API 配額耗盡 | `gspread` 批次寫入（`append_rows`）而非逐列寫入；避免頻繁全量讀取 |
| 換月結算日預測錯誤 | `SettleManager.calculate_next_contract()` 只做預測，正式結算資料仍需人工補入 `TXF_settle_date_price` |
| 盤中資料誤上傳 | `drop_incomplete_current_session()` 守門；Gate 1 完整性檢查雙重保護 |
| 夜盤跨日時間歸屬錯誤 | `get_market_date_str()` 明確處理 00:00–05:00 屬前一天的夜盤 |

## Migration Plan

本 Change 為純文件性質，不涉及程式碼變更，無需遷移計畫。

Roadmap 各功能的遷移計畫將於各自的 spec 中定義：
- `notification`：新增環境變數 `LINE_NOTIFY_TOKEN`，main.py 結尾加入推播邏輯
- `deployment`：新增 `Dockerfile`、`docker-compose.yml`、Cron 腳本
- `gas-integration`：Google Apps Script 端新增觸發函式，不影響 Python 端

## Open Questions

1. **結算日自動補寫**：能否在結算後自動計算並寫入 `TXF_settle_date_price`？目前須人工操作，是否值得自動化？
2. **Line Notify 棄用**：LINE 官方將停止 Line Notify 服務（2025 年），應改用 LINE Messaging API 或 Telegram Bot？
3. **多商品支援**：未來是否需要支援小台（MTX）或電子期（TE）？目前架構假設單一商品。
4. **歷史資料回補**：若需要補抓 7 天以上的歷史資料，`QUERY_BACK_DAYS` 的上限應如何設計？
