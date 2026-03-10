## 1. 規格文件補完（已完成）

- [ ] 1.1 建立 proposal.md：說明補齊規格的動機與範圍
- [ ] 1.2 建立 design.md：架構決策與 Trade-off 分析
- [ ] 1.3 建立 specs/etl-pipeline/spec.md：ETL 核心流程規格
- [ ] 1.4 建立 specs/data-validation/spec.md：Gate 1 完整性檢查規格
- [ ] 1.5 建立 specs/incremental-update/spec.md：水位線增量更新規格
- [ ] 1.6 建立 specs/settle-management/spec.md：結算日管理與換月調整規格
- [ ] 1.7 建立 specs/auth-management/spec.md：連線管理與環境變數規格
- [ ] 1.8 建立 specs/notification/spec.md：推播通知規格（Roadmap）
- [ ] 1.9 建立 specs/deployment/spec.md：Docker 容器化與 VPS 部署規格（Roadmap）
- [ ] 1.10 建立 specs/gas-integration/spec.md：GAS 回測整合規格（Roadmap）

## 2. 通知功能實作（Roadmap Phase 1）

- [ ] 2.1 評估通知渠道：確認採用 LINE Messaging API 或 Telegram Bot（Line Notify 已停止服務）
- [ ] 2.2 新增 `NOTIFY_TOKEN` 至 `.env.example`，標記為選填
- [ ] 2.3 實作 `Notifier` 類別或函式，封裝通知 API 呼叫邏輯
- [ ] 2.4 在 `main.py` 的 `try/except/finally` 區塊中呼叫通知，確保失敗時也能推播
- [ ] 2.5 實作成功訊息格式（含上傳筆數、執行時間）
- [ ] 2.6 實作失敗訊息格式（含錯誤類型與摘要）
- [ ] 2.7 測試：Token 未設定時確認靜默略過，不影響主程式

## 3. Docker 容器化（Roadmap Phase 2）

- [ ] 3.1 建立 `Dockerfile`（基礎映像：`python:3.12-slim`，安裝 requirements.txt）
- [ ] 3.2 建立 `docker-compose.yml`（本機測試用，掛載 `.env` 檔）
- [ ] 3.3 驗證 `docker build` 與 `docker run --env-file .env` 正常執行
- [ ] 3.4 建立 `cron.sh`：封裝 `docker run` 指令並重導向日誌至 `/var/log/txf-pipeline/`
- [ ] 3.5 設定 `logrotate` 規則（保留 30 天，每日切割）
- [ ] 3.6 更新 `.gitignore` 確保日誌目錄不被提交

## 4. VPS 部署（Roadmap Phase 2）

- [ ] 4.1 建立 GCP e2-micro VM（台灣或香港區，免費層配置）
- [ ] 4.2 在 VM 上安裝 Docker 與 git
- [ ] 4.3 Clone 專案並設定 `.env` 環境變數（機密值手動填入）
- [ ] 4.4 設定 crontab：日盤 `0 14 * * 1-5` 與夜盤 `30 5 * * 2-6`（CST）
- [ ] 4.5 驗證 Cron Job 首次自動觸發正常執行
- [ ] 4.6 設定 VM 防火牆規則（只開放必要 outbound 連線）

## 5. Google Apps Script 整合（Roadmap Phase 3）

- [ ] 5.1 確認 `5mink_new` 分頁欄位順序，以欄位名稱（非索引）讀取作為設計基礎
- [ ] 5.2 在 Google Sheets 建立 Apps Script，實作 `runStrategy()` 主函式
- [ ] 5.3 實作水位線讀取邏輯（GAS 端），避免重複計算
- [ ] 5.4 實作最小可行策略（如均線交叉），輸出 `BUY`/`SELL`/`FLAT` 信號
- [ ] 5.5 建立信號輸出分頁，定義欄位結構（參考 gas-integration/spec.md）
- [ ] 5.6 設定 GAS Time-driven 觸發器（每日 14:30 與 06:00 CST）
- [ ] 5.7 端到端測試：ETL 寫入 → GAS 讀取 → 信號輸出
