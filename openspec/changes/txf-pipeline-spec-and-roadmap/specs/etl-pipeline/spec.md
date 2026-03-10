## ADDED Requirements

### Requirement: Extract 1 分鐘 K 棒資料
系統 SHALL 透過 Shioaji API 抓取指定合約的 1 分鐘 K 棒，時間範圍為當下往前 `QUERY_BACK_DAYS` 天（預設 7 天）。

欄位：`ts`（開盤時間）、`Open`、`High`、`Low`、`Close`、`Volume`，排除 `Amount`。

#### Scenario: 正常抓取
- **WHEN** Shioaji API 連線正常，指定合約存在
- **THEN** 回傳包含完整欄位的 DataFrame，以 `ts` 為索引並升序排列

#### Scenario: 強制使用近月合約
- **WHEN** `FORCE_MXFR1 = True`
- **THEN** 無論 `target_contract` 為何，一律使用 `MXFR1` 合約代碼抓取

#### Scenario: API 回傳空資料
- **WHEN** 指定日期範圍內無交易資料
- **THEN** 回傳空 DataFrame，主程式跳過後續所有處理步驟

---

### Requirement: Resample 為 5 分鐘 K 棒
系統 SHALL 將 1 分鐘 K 棒以「右閉左開」方式重取樣為 5 分鐘 K 棒，並以**開盤時間（left label）**作為時間戳記。

OHLC 規則：Open=first, High=max, Low=min, Close=last, Volume=sum。

#### Scenario: 標準重取樣
- **WHEN** 輸入有效的 1 分鐘 DataFrame
- **THEN** 輸出每 5 分鐘一筆的 DataFrame，標記時間為該區間的開始時間

#### Scenario: 去除空值
- **WHEN** 重取樣後某區間無資料（如非交易時段）
- **THEN** 該筆資料以 `dropna()` 移除，不出現在輸出中

---

### Requirement: 切割日盤與夜盤
系統 SHALL 依據市場時段設定，將 5 分鐘 K 棒切割為日盤與夜盤兩個獨立 DataFrame。

| 盤別 | 開始時間 | 結束時間 |
|---|---|---|
| 日盤（D） | 08:45 | 13:45 |
| 夜盤（N） | 15:00 | 05:00（次日） |

#### Scenario: 日盤切割
- **WHEN** 輸入 5 分鐘 K 棒 DataFrame
- **THEN** `df_5m_D` 僅包含 08:45–13:45 時間範圍內的資料

#### Scenario: 夜盤切割
- **WHEN** 輸入 5 分鐘 K 棒 DataFrame
- **THEN** `df_5m_N` 包含 15:00–05:00 跨日時間範圍內的資料

#### Scenario: 夜盤凌晨時段歸屬
- **WHEN** 夜盤 K 棒時間戳記為 00:00–05:00（凌晨）
- **THEN** `date_market_type` 欄位日期回退一天（歸屬前一個交易日的夜盤），例如 `260102N`

---

### Requirement: Resample 為 60 分鐘 K 棒
系統 SHALL 分別對日盤與夜盤的 5 分鐘 K 棒重取樣為 60 分鐘 K 棒。

日盤採用 `offset="45min"`（對齊 08:45 開盤），夜盤採用預設對齊。

#### Scenario: 日盤 60 分鐘重取樣
- **WHEN** 輸入日盤 5 分鐘 DataFrame
- **THEN** 輸出以 08:45, 09:45, 10:45, 11:45, 12:45 為時間戳記的 5 筆資料

#### Scenario: 夜盤 60 分鐘重取樣
- **WHEN** 輸入夜盤 5 分鐘 DataFrame
- **THEN** 輸出以整點時間為基準的 K 棒（最多 14 筆）

---

### Requirement: date_market_type 欄位生成
系統 SHALL 為每筆 K 棒自動生成 `date_market_type` 欄位，格式為 `YYMMDD` + `D`/`N`。

#### Scenario: 日盤標記
- **WHEN** K 棒屬於日盤（08:45–13:45）
- **THEN** `date_market_type = ts.strftime("%y%m%d") + "D"`，例如 `260102D`

#### Scenario: 夜盤標記（非凌晨）
- **WHEN** K 棒屬於夜盤，時間為 15:00–23:59
- **THEN** `date_market_type = ts.strftime("%y%m%d") + "N"`，例如 `260101N`

#### Scenario: 夜盤標記（凌晨跨日）
- **WHEN** K 棒屬於夜盤，時間為 00:00–05:00
- **THEN** `date_market_type = (ts - 1day).strftime("%y%m%d") + "N"`，例如 `260101N`（而非 260102N）
