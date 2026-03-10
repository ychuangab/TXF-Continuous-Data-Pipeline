## ADDED Requirements

### Requirement: 水位線讀取（Gate 2 — 前置過濾）
系統 SHALL 在完整性檢查前，先讀取目標 Google Sheet 分頁最後一筆資料的 `ts` 時間戳記，作為本次上傳的水位線（Watermark）。

#### Scenario: Sheet 有資料時讀取水位線
- **WHEN** 目標分頁存在且有至少 2 列資料（含表頭）
- **THEN** 回傳最後一列的 `ts` 欄位值，轉換為 `pd.Timestamp`

#### Scenario: Sheet 為空或僅有表頭
- **WHEN** 目標分頁不存在資料列，或僅有表頭列
- **THEN** 回傳 `None`，後續視為全量上傳

#### Scenario: Sheet 不存在
- **WHEN** 目標分頁名稱找不到（`WorksheetNotFound`）
- **THEN** 回傳 `None`，後續視為全量上傳，不拋出錯誤

---

### Requirement: 增量過濾（只保留新資料）
系統 SHALL 使用水位線過濾 DataFrame，只保留 `index > last_ts` 的資料行，並在過濾前後輸出筆數差異日誌。

#### Scenario: 有新資料時過濾舊資料
- **WHEN** 水位線存在，且 DataFrame 中有部分資料的時間戳記早於或等於水位線
- **THEN** 過濾掉舊資料，輸出 `[{tab}] Incremental Filter: {original} -> {filtered} rows (Dropped old data).`

#### Scenario: 全部資料都是新的
- **WHEN** 水位線存在，但 DataFrame 所有資料都晚於水位線
- **THEN** 回傳完整 DataFrame，不輸出過濾日誌

#### Scenario: 無水位線（全量模式）
- **WHEN** 水位線為 `None`
- **THEN** 回傳完整 DataFrame，不進行任何過濾

#### Scenario: 過濾後 DataFrame 為空（資料已是最新）
- **WHEN** 過濾後沒有任何資料晚於水位線
- **THEN** 回傳空 DataFrame，後續跳過完整性檢查與上傳，輸出 `All data is up-to-date.`

---

### Requirement: 冪等性保證
系統 SHALL 確保在相同時間範圍內重複執行，不會產生重複資料或觸發錯誤。

#### Scenario: 重複執行相同日期
- **WHEN** 同一天的 ETL 已成功執行並寫入 Sheet，再次執行時抓取相同時間範圍
- **THEN** 增量過濾後 DataFrame 為空，跳過所有上傳操作，程式正常結束

#### Scenario: Sheet 寫入失敗後重跑
- **WHEN** 前一次執行因網路錯誤等原因未完整寫入，重新執行時水位線指向部分寫入的最後一筆
- **THEN** 只補寫水位線之後的資料，不重複寫入已存在的部分
