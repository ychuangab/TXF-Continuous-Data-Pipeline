## ADDED Requirements

### Requirement: 執行結果推播通知
系統 SHALL 在每次 ETL 流程結束後，透過通知渠道推送執行摘要，無論執行成功或失敗均須推播。

> **注意**：Line Notify 官方服務預計於 2025 年停止，實作時應優先評估替代方案（LINE Messaging API 或 Telegram Bot）。本 spec 以行為為主，不限定具體通知渠道。

#### Scenario: 執行成功時推播
- **WHEN** ETL 流程正常完成（無例外拋出）
- **THEN** 推送訊息包含：執行狀態（成功）、5分K 與 60分K 各上傳筆數、總執行時間（秒）

#### Scenario: 資料完整性錯誤時推播
- **WHEN** Gate 1 觸發 `ValueError`（資料缺漏）
- **THEN** 推送訊息包含：執行狀態（失敗）、錯誤類型（DATA INTEGRITY ERROR）、受影響的盤別與筆數

#### Scenario: 系統錯誤時推播
- **WHEN** 非預期的 `Exception` 導致程式中止
- **THEN** 推送訊息包含：執行狀態（失敗）、錯誤訊息（前 200 字元）

#### Scenario: 推播本身失敗
- **WHEN** 通知 API 呼叫失敗（如 Token 無效、網路異常）
- **THEN** 輸出警告日誌，但不影響主程式的執行結果（不重新拋出例外）

---

### Requirement: 通知渠道設定
系統 SHALL 透過環境變數設定通知 Token，若環境變數未設定則靜默略過推播（不中止程式）。

新增環境變數：`NOTIFY_TOKEN`（選填）

#### Scenario: Token 已設定
- **WHEN** `NOTIFY_TOKEN` 環境變數存在且有效
- **THEN** 程式啟動時輸出 `[Notify] Notification enabled`，每次執行後觸發推播

#### Scenario: Token 未設定
- **WHEN** `NOTIFY_TOKEN` 環境變數不存在
- **THEN** 程式啟動時輸出 `[Notify] NOTIFY_TOKEN not set, skipping notifications`，不進行任何推播

---

### Requirement: 通知訊息格式
系統 SHALL 以固定格式生成通知訊息，確保可讀性。

成功訊息範例：
```
[TXF Pipeline] ✅ 執行成功
時間：2026-03-10 14:00:05 (CST)
5分K 上傳：60 筆
60分K 上傳：5 筆
耗時：12.34 秒
```

失敗訊息範例：
```
[TXF Pipeline] ❌ 執行失敗
時間：2026-03-10 14:00:05 (CST)
錯誤類型：DATA INTEGRITY ERROR
錯誤摘要：資料完整性檢查失敗 (5min)
  - 2026-03-09_D: 預期 60 筆, 實際 58 筆
```

#### Scenario: 訊息包含台灣時區時間
- **WHEN** 生成通知訊息
- **THEN** 時間欄位採用 CST（UTC+8）時區格式：`YYYY-MM-DD HH:MM:SS (CST)`
