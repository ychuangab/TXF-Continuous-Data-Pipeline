## ADDED Requirements

### Requirement: Gate 1 — 完整性檢查
系統 SHALL 在上傳前驗證每個交易盤別的 K 棒筆數是否符合預期，任何盤別筆數不符即中止上傳並拋出錯誤。

預期筆數定義：

| 時框 | 日盤（D） | 夜盤（N） |
|---|---|---|
| 5 分鐘 | 60 | 168 |
| 60 分鐘 | 5 | 14 |

#### Scenario: 完整資料通過檢查
- **WHEN** 所有盤別的 K 棒筆數等於預期值
- **THEN** 輸出 `[Check] {timeframe} Pass. All sessions appear complete.` 並繼續執行

#### Scenario: 資料缺漏觸發錯誤
- **WHEN** 任一盤別的 K 棒筆數與預期不符
- **THEN** 拋出 `ValueError`，訊息列出所有問題盤別（格式：`- YYYY-MM-DD_D: 預期 60 筆, 實際 N 筆`），上傳流程中止

#### Scenario: 空 DataFrame 略過檢查
- **WHEN** 輸入 DataFrame 為空
- **THEN** 跳過檢查，不拋出錯誤

#### Scenario: UNKNOWN 盤別略過
- **WHEN** K 棒時間戳記無法歸類為日盤或夜盤（時間落在 13:45–15:00 或 05:00–08:45）
- **THEN** 該盤別標記為 `UNKNOWN` 並跳過筆數驗證

---

### Requirement: 盤中資料過濾（Gate 0）
系統 SHALL 在完整性檢查前，偵測並移除尚未收盤的當前盤別資料，避免盤中不完整資料觸發 Gate 1 錯誤。

#### Scenario: 偵測到進行中的盤別且筆數不足
- **WHEN** 最後一筆 K 棒所屬盤別與當下系統時間相同，且該盤 K 棒數量少於預期
- **THEN** 移除該盤所有 K 棒，輸出 `[Filter] 偵測到盤中資料 {group_id} 尚未收盤 ({count}/{expected}) -> 捨棄不處理`

#### Scenario: 完整的歷史盤別不受影響
- **WHEN** 最後一筆 K 棒所屬盤別與當下系統時間不同（代表已收盤的歷史盤）
- **THEN** 保留所有資料，不過濾

#### Scenario: 空 DataFrame 略過過濾
- **WHEN** 輸入 DataFrame 為空
- **THEN** 直接回傳空 DataFrame，不進行任何操作

---

### Requirement: 資料品質錯誤處理
系統 SHALL 在資料品質檢查失敗時，以非零退出碼終止程式，並輸出清楚的錯誤訊息，不進行任何 Google Sheets 寫入操作。

#### Scenario: Gate 1 失敗時的錯誤輸出
- **WHEN** `check_completeness()` 拋出 `ValueError`
- **THEN** 主程式捕捉例外，輸出 `[DATA INTEGRITY ERROR]` 區塊與詳細錯誤，並終止執行（不呼叫 `SheetUploader`）
