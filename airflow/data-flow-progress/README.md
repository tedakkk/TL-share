# Airflow 2.2.3 批次進度條

在 Graph View 選取 identify_batch，按「Data flow 進度」即可開啟獨立頁面。頁面每 60 秒查一次來源表，無需為顯示進度佔用執行中的 task。範例 DAG 供手動驗證，每次執行透過 XCom 保存自己的 batch_id。

## 安裝

1. 確認 webserver、scheduler 及執行 task 的環境已有相容於 Airflow 2.2.3 的 PostgreSQL provider。webserver 必須能連到來源庫。
2. 在 Admin → Connections 建立 Postgres 連線 df_status_db，使用有權讀取 tb_df 的帳號。Schema 填資料庫名稱。若資料表在指定 schema，修改 SQL 為 schema_name.tb_df。
3. 執行 `airflow config get-value core plugins_folder`，把 plugins/df_batch_progress.py 複製到該目錄根層。把 dags/daily_data_flow.py 複製到 dags_folder。分開部署的組件須能讀到 plugin 模組。
4. 重啟 webserver、scheduler 和獨立 worker（如適用）。執行 `airflow plugins` 確認有 df_batch_progress，再以 `airflow dags list-import-errors` 檢查 DAG 載入結果。
5. 啟用範例 DAG，以實際存在的批次代替下例的 12345。輸入值型別應配合來源欄位。

```bash
airflow dags trigger daily_data_flow --conf '{"batch_id":12345}'
```

6. 待 identify_batch 成功後點開進度頁。先用 Admin 驗證。其他角色需獲授 DataFlowProgressView 的 can show 權限，此權限可查看指定 DAG 的所有批次，只授予可查看該 DAG 的角色。

## 接入現有定時 DAG

將 plugin 的 DAG_ID、TASK_ID 改成現有 DAG 和記錄批次的 Python task。在該 task 使用 DataFlowBatchOperator，保留其排程和上下游依賴。取得來源批次後執行 `ti.xcom_push(key="df_batch_id", value=batch_id)`。若由外部系統產生批次，請依該次執行的業務鍵查出批次，避免補跑讀到其他日期的最新批次。此時毋須部署範例 DAG。

## 核對數據

預期每批 99 個 partition，成功值為 SUCCESS，可在 plugin 頂部修改。如果部分批次的總數不同，應按 batch_id 讀取該批預期數量。每個 partition 只計最新紀錄；若同一 partition 的 update_time 可能相同，請在 ORDER BY 加上來源表能決定先後的事件序號。65 個成功應顯示 65 / 99（65.7%）。未有紀錄與明確的 PENDING 分開顯示。保留歷史批次資料及對應 XCom 才能回看舊批次。

Oracle 來源需使用相容的 Oracle provider、Oracle 類型連線和 OracleHook(oracle_conn_id="df_status_db")，並將 SQL 的 %s 改為 :1。

[Airflow 2.2.3 plugin 文件](https://airflow.apache.org/docs/apache-airflow/2.2.3/plugins.html)
