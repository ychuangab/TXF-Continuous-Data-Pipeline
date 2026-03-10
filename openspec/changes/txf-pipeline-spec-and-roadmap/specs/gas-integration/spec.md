## ADDED Requirements

### Requirement: Google Apps Script 觸發端點
系統 SHALL 在 Google Sheets 關聯的 Apps Script 中建立一個可被呼叫的函式，在新資料寫入後觸發回測策略信號計算。

> 本 spec 描述 GAS 端的行為要求，Python ETL 端無需更動。

#### Scenario: 定時觸發器啟動
- **WHEN** GAS 的 Time-driven 觸發器在排程時間啟動
- **THEN** 讀取 `5mink_new` 分頁的最新資料，並執行策略信號計算函式

#### Scenario: 新資料存在時執行計算
- **WHEN** 偵測到 `5mink_new` 有新增資料（`ts` 晚於上次記錄的水位線）
- **THEN** 對新資料執行策略邏輯，並將信號結果寫入指定的信號分頁

#### Scenario: 無新資料時跳過計算
- **WHEN** `5mink_new` 資料無變化（`ts` 未超過水位線）
- **THEN** 函式直接結束，不進行任何寫入操作

---

### Requirement: 回測信號輸出格式
系統 SHALL 將策略信號以結構化欄位寫入 Google Sheets 的信號分頁。

信號分頁欄位定義：

| 欄位 | 型態 | 說明 |
|---|---|---|
| `ts` | datetime | K 棒時間戳記（同 ETL 輸出格式） |
| `signal` | string | 信號類型：`BUY` / `SELL` / `FLAT` |
| `price` | number | 觸發信號的收盤價（Back-adjusted） |
| `strategy` | string | 策略名稱（便於多策略共存） |
| `created_at` | datetime | 信號寫入時間（CST） |

#### Scenario: 買進信號寫入
- **WHEN** 策略邏輯判定當前 K 棒應買進
- **THEN** 寫入一列 `signal = "BUY"`，`price` 為該 K 棒的 Close 值

#### Scenario: 無信號時不寫入
- **WHEN** 策略邏輯判定無操作（`FLAT`）
- **THEN** 不寫入任何資料列（靜默略過）

---

### Requirement: ETL 與 GAS 的資料契約
Python ETL 端 SHALL 維持 `5mink_new` 分頁的欄位結構不變，以確保 GAS 端讀取邏輯的穩定性。

#### Scenario: 欄位順序不變
- **WHEN** ETL 上傳新資料至 `5mink_new`
- **THEN** 欄位順序固定為：`ts`, `Open`, `High`, `Low`, `Close`, `Volume`, `date_market_type`, `contract_year_month`, `accumulated_contract_diff`, `MXF_code`

#### Scenario: 欄位新增須向後相容
- **WHEN** 未來 ETL 需要新增欄位
- **THEN** 新欄位只能附加在最後，不得插入現有欄位之間（避免破壞 GAS 的欄位索引）
