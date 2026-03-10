## ADDED Requirements

### Requirement: 載入結算設定表
系統 SHALL 從 Google Sheets 的 `TXF_settle_date_price` 分頁讀取結算設定，並轉換為結構化 DataFrame。

欄位定義：

| 欄位 | 型態 | 說明 |
|---|---|---|
| `contract_year_month` | string | 合約月份，格式 `YYYYMM`（如 `202511`） |
| `next_contract_diff` | int | 與次月合約的結算價差（點數） |
| `accumulated_contract_diff` | int | 累積調整點數（所有歷史價差加總） |
| `start_k` | datetime | 該合約開始的 K 棒時間 |
| `settle_k` | datetime | 該合約結算的 K 棒時間（第三週三 13:25） |

#### Scenario: 正常載入
- **WHEN** Google Sheets 連線正常，分頁存在且格式正確
- **THEN** 回傳包含上述欄位的 DataFrame，數值欄位為 numeric 型態，時間欄位為 datetime 型態

#### Scenario: 格式錯誤的欄位
- **WHEN** 某欄位無法轉換為預期型態（如數字欄含有文字）
- **THEN** 以 `pd.to_numeric(errors='coerce')` 轉為 `NaN`，並以 `dropna(subset=['contract_year_month'])` 濾除無效列

#### Scenario: 連線失敗
- **WHEN** Google Sheets 連線或讀取發生例外
- **THEN** 拋出 `RuntimeError: Error loading settle config: {原始錯誤訊息}`

---

### Requirement: 換月價差調整（Back Adjustment）
系統 SHALL 依據結算設定表，對每筆 K 棒的 OHLC 四欄加上該合約的 `accumulated_contract_diff`，並記錄對應的 `contract_year_month`。

#### Scenario: K 棒在合約區間內
- **WHEN** K 棒時間戳記落在某合約的 `start_k` 到 `settle_k` 之間
- **THEN** OHLC 各欄加上該合約的 `accumulated_contract_diff`，`contract_year_month` 填入該合約月份

#### Scenario: K 棒不在任何合約區間
- **WHEN** K 棒時間戳記超出所有已知合約範圍（如預測期間）
- **THEN** `accumulated_contract_diff = 0`，`contract_year_month = ""`，OHLC 不調整

#### Scenario: 多合約不重疊
- **WHEN** 結算設定表中各合約的 `start_k` 到 `settle_k` 不重疊
- **THEN** 每筆 K 棒最多匹配一個合約，取第一個匹配結果（`iloc[0]`）

---

### Requirement: 預測下一合約代碼與結算日
系統 SHALL 在每次執行時，根據設定表最後一列自動推算下一個合約，並暫時加入記憶體中的 DataFrame（不寫入 Sheet）。

推算規則：
- 合約月份：最後合約月份 +1 個月（以 +31 天近似）
- 結算日：新合約月份的第三個週三，時間 13:25
- 開始時間：前一個合約的 `settle_k` + 5 分鐘
- 累積價差：前一個 `accumulated_contract_diff` + 前一個 `next_contract_diff`

#### Scenario: 正常推算下個月合約
- **WHEN** 設定表最後一列為 `202511`
- **THEN** 推算出合約 `202512`，結算日為 2025 年 12 月的第三個週三 13:25

#### Scenario: 跨年推算
- **WHEN** 設定表最後一列為 `202512`（12 月）
- **THEN** 正確推算出合約 `202601`（翌年 1 月）

#### Scenario: 預測合約不寫入 Sheet
- **WHEN** 推算完成後
- **THEN** 新合約列只存在於記憶體中的 `df_config`，不觸發任何 Google Sheets 寫入操作
