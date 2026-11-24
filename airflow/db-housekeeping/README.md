# Airflow 2.2.4 Metadata DB Housekeeping & Migration

> 技術討論整理 · 2026-11-24 · 尚未在實際環境驗證，並非已批准的 Production Runbook。

## 1. 背景與目標

- Airflow 2.2.4，LocalExecutor，PostgreSQL metadata database。
- 已運行約四年，未曾進行 DB housekeeping。
- 計劃遷移及升級 Airflow，但現有 DB 過大。
- 目標：減少需遷移的有效數據量、維持調度連續性、降低切換風險，並建立長期維護策略。

Airflow 2.2.4 沒有原生 `airflow db clean`；該功能從 2.3 起提供。以下命令需在指定版本及隔離環境驗證後才可使用。

## 2. 方案比較

| 方案 | 做法 | 適用情況 |
|---|---|---|
| A | 2.2.4 兼容的 maintenance DAG / Python / 批量 SQL | DB 太大，無法直接完成中間版本 schema migration |
| B（優先 POC） | 備份及還原至 staging，升級至 2.3.4，使用原生 db clean，再 dump/restore 至目標環境 | 需要保留近期執行歷史及配置 |
| C | 新建 metadata DB，只遷移必要配置，舊 DB 作歷史檔案 | 歷史不需在線查詢，且已驗證調度及依賴可安全重置 |

注意：方案 B 仍需先完整備份及還原原 DB，因此不會減少首次 staging 遷移的數據量；其目的是減少後續正式目標 DB 的大小。若首次備份或 schema upgrade 成本不可接受，先評估 A 或 C。

## 3. PostgreSQL 空間診斷（只讀）

```sql
SELECT pg_size_pretty(pg_database_size(current_database()));

SELECT
    schemaname,
    relname AS table_name,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
    pg_size_pretty(pg_table_size(relid)) AS table_size,
    pg_size_pretty(pg_indexes_size(relid)) AS index_size,
    n_live_tup,
    n_dead_tup,
    last_autovacuum,
    last_autoanalyze
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(relid) DESC;
```

`n_live_tup`、`n_dead_tup` 為統計估算，不等於精確 bloat 測量。需要結合 table/index size、歷史分布及 autovacuum 情況評估。

| Table | 初步保留期（待批准） |
|---|---|
| task_instance / dag_run | 180 天 |
| xcom | 30–90 天 |
| log | 180–365 天 |
| job | 30–90 天 |
| task_fail / sla_miss | 180 天 |
| task_reschedule | 90–180 天 |
| rendered_task_instance_fields | 30–90 天（需確認版本支援及依賴） |

保留期需滿足審計、補數、低頻 DAG、跨 DAG 依賴等要求；不能機械套用。`log` metadata table 與 task log files 是不同的維護對象。

## 4. 方案 B：隔離環境 POC

流程：Production 2.2.4 → 完整備份 → Staging restore → 2.3.4 schema upgrade → dry run → cleanup → dump/restore → 目標版本 schema migration → 驗證及切換。

### 4.1 備份及還原

```bash
pg_dump -h old_host -U airflow -Fc -f airflow_backup.dump airflow
# 先由 DBA 建立空的 airflow_staging database，配置必要權限及 extensions。
pg_restore --exit-on-error -h staging_host -U airflow -d airflow_staging -j 4 airflow_backup.dump
```

備份檔含敏感 metadata，不應存入 GitHub。首次 POC 可以使用一致性快照；正式切換前應停止所有 metadata 寫入，完成最終備份及還原演練。保留原 Fernet key（通過 secret manager，勿提交版本庫）。

### 4.2 建立臨時 CLI 環境

```bash
python3.8 -m venv airflow_cleanup_env
source airflow_cleanup_env/bin/activate
pip install 'apache-airflow[postgres]==2.3.4' \
  --constraint 'https://raw.githubusercontent.com/apache/airflow/constraints-2.3.4/constraints-3.8.txt'
export AIRFLOW_HOME=/tmp/airflow_cleanup
# 由安全渠道注入 AIRFLOW__DATABASE__SQL_ALCHEMY_CONN，必須指向 staging。
airflow version
airflow db check
airflow db upgrade
airflow db clean --help
```

2.3.4 僅作一次性離線處理，不建議部署為長期 production service。確認 Python、PostgreSQL、providers、schema revision 及目標版本的升級路徑。不要在 staging 啟動連接生產數據源的 scheduler。

### 4.3 Dry run 及 cleanup

```bash
# 示例日期，實際以已批准的保留期計算。
airflow db clean --clean-before-timestamp '2026-06-01T00:00:00+00:00' --dry-run
# 僅在 staging、完整備份已驗證且保留策略批准後執行：
airflow db clean --clean-before-timestamp '2026-06-01T00:00:00+00:00' --skip-archive --yes
```

可先選擇低依賴的表試驗：

```bash
airflow db clean --clean-before-timestamp '2026-06-01T00:00:00+00:00' \
  --tables log,xcom,job --dry-run
```

實際可用表及參數以 2.3.4 CLI help 為準。預設 archive 表可能抵銷縮減容量的目標；只有備份與審計要求均已滿足時才使用 `--skip-archive`。Dry run 不保證列出全部 cascade 影響。

### 4.4 關鍵風險

- `dag_run`、`task_instance`、`xcom`、`task_fail`、`task_reschedule` 存在邏輯或外鍵依賴，實際 schema 及 ON DELETE 規則按版本核實。
- `catchup=True` 可能重新建立歷史 run；`depends_on_past`、`ExternalTaskSensor`、低頻 DAG、歷史 XCom 讀取及尚未完成的 run 可能受影響。
- 原生工具有保留每個 DAG 最近非手動 run 的邏輯，但不代表所有歷史依賴、active runs 或 child rows 都受保護。執行前檢查未完成 run 並針對低頻 DAG 設置保留策略。
- 記錄清理前後的 row counts、最新 DAG run、狀態分布、WAL、磁碟空間、耗時及錯誤。
- 重新 dump/restore 清理後的 staging DB 至目標環境，再按目標 Airflow 版本的官方指引進行 schema migration。

## 5. 方案 A：直接清理 2.2.4

參考：
- [Airflow Maintenance DAGs](https://github.com/teamclairvoyant/airflow-maintenance-dags)
- [Cloud Composer database cleanup](https://docs.cloud.google.com/composer/docs/composer-2/cleanup-airflow-database)

需選擇適配版本並審查 ORM、schema、刪除順序、保留邏輯。不要直接將社區腳本部署 production。

批量刪除 `log` 示例（先於 staging 驗證，需批准 audit retention）：

```sql
-- 每批獨立 transaction，示例每批最多 10000 rows。
DELETE FROM log
WHERE id IN (
    SELECT id FROM log
    WHERE dttm < NOW() - INTERVAL '365 days'
    ORDER BY id
    LIMIT 10000
);
```

建議固定 cutoff timestamp，由外部 script 控制 batch、commit、lock/statement timeout、sleep、錯誤處理和停止條件。觀察 WAL、replication lag、lock、磁碟、query plan；依測試調整 batch size。禁止直接盲目刪除 dag_run 或 task_instance。

## 6. PostgreSQL 空間回收

| 操作 | 作用 |
|---|---|
| DELETE | 邏輯刪除，不會通常立即釋放磁碟 |
| VACUUM | 回收可重用空間，通常不縮減檔案 |
| VACUUM FULL | 重寫 table、釋放磁碟，需要額外空間及 exclusive lock |
| pg_dump + pg_restore | 邏輯備份有效數據並重建 table/index，避免遷移舊 bloat |

如果最終會 dump/restore，通常無需額外執行 VACUUM FULL。假設舊 DB 100 GB、清理後有效數據約 20 GB，原 DB 仍可能接近 100 GB；新 DB 可大幅縮減，但實際取決於索引、TOAST、有效數據等。該數字僅為示意。

## 7. 方案 C：新建 Metadata DB

將 DAG code、connections、variables、pools 遷移；使用者/角色及 DAG pause state 重建或還原，task logs 另行備份。歷史 dag_run/task_instance 保留於舊 archive；serialized DAG 可重新生成。XCom 是否遷移取決於依賴。

```bash
# 舊環境（驗證 CLI help）
airflow connections export connections.json
airflow variables export variables.json
airflow pools export pools.json
# 新環境（驗證 CLI help）
airflow connections import connections.json
airflow variables import variables.json
airflow pools import pools.json
```

匯出檔可能包含明文秘密，不得存入 GitHub。須逐項驗證 DAG next run、catchup、depends_on_past、跨 DAG sensor、歷史補數及外部 ETL 重複執行風險。

## 8. 長期維護

| 維護項目 | 建議頻率 |
|---|---|
| DB / table size monitoring | 每日 |
| XCom / job / DAG run / task history cleanup | 每週（批准保留期） |
| Autovacuum | 持續 |
| Task log file cleanup | 每週 |
| Backup restore test | 每季 |
| Retention review | 每半年 |

Housekeeping 建議在受控的獨立 cron/維護環境執行，避免完全依賴 Airflow scheduler。

## 9. Migration Checklist

- [ ] 確認 PostgreSQL 版本、DB size、最大 10 張表、磁碟餘量、目標 Airflow 版本。
- [ ] 確認審計、歷史補數及依賴所需保留期。
- [ ] 完整備份、restore test，確認 RPO/RTO 及回退方案。
- [ ] 在隔離環境完成 schema upgrade、dry run、cleanup、再次 dump/restore。
- [ ] 驗證 DAG 解析、next run、狀態、connections、variables、pools、角色、Fernet key、providers、外部系統連接。
- [ ] 正式切換前 drain active tasks，停止 scheduler/webserver/其他 metadata writers，完成最終備份。
- [ ] 重複已驗證流程；逐步啟用 DAG，觀察重跑、依賴、錯誤及性能。
- [ ] 保留舊 DB，避免新舊環境同時執行造成重複處理；回退前考慮已發生的外部數據寫入。
- [ ] 建立定期 housekeeping 與容量告警。

## 10. 官方參考

- [Airflow 2.3.4 CLI reference](https://airflow.apache.org/docs/apache-airflow/2.3.4/cli-and-env-variables-ref.html)
- [Airflow 2.2.4 CLI reference](https://airflow.apache.org/docs/apache-airflow/2.2.4/cli-and-env-variables-ref.html)
- [PostgreSQL routine vacuuming](https://www.postgresql.org/docs/current/routine-vacuuming.html)
- [PostgreSQL pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html)
- [PostgreSQL pg_restore](https://www.postgresql.org/docs/current/app-pgrestore.html)

本文件為對話整理及方案設計，並非已執行的操作記錄。所有實際 DB 數值、保留期、版本兼容性及執行結果待確認。
