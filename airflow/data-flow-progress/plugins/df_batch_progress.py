from airflow.models.baseoperator import BaseOperatorLink
from airflow.models.xcom import XCom
from airflow.operators.python import PythonOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from flask import abort, render_template_string, request, url_for
from flask_appbuilder import BaseView, expose
from flask_appbuilder.security.decorators import has_access

DAG_ID = "daily_data_flow"
TASK_ID = "identify_batch"
EXPECTED = 99
SUCCESS_STATUS = "SUCCESS"

SQL = """
WITH ranked AS (
    SELECT status, update_time,
           ROW_NUMBER() OVER (
               PARTITION BY partition_id ORDER BY update_time DESC NULLS LAST
           ) AS rn
    FROM tb_df
    WHERE batch_id = %s AND partition_id IS NOT NULL
)
SELECT status, COUNT(*), MAX(update_time)
FROM ranked WHERE rn = 1
GROUP BY status ORDER BY status
"""

PAGE = """
<!doctype html><html lang="zh-Hant"><meta charset="utf-8">
<meta http-equiv="refresh" content="60"><title>Data flow 進度</title>
<h1>批次 {{ batch_id }}</h1>
{% if observed <= expected %}
<progress value="{{ done }}" max="{{ expected }}"></progress>
<strong>{{ done }} / {{ expected }}（{{ percent }}%）</strong>
{% else %}
<p>已出現 {{ observed }} 個 partition，超過預期 {{ expected }} 個。請核對批次設定。</p>
{% endif %}
<ul>{% for status, count, updated in rows %}
<li>{{ status or '未填狀態' }}：{{ count }}</li>{% endfor %}
<li>未有紀錄：{{ missing }}</li></ul>
<p>來源最後更新：{{ latest or '暫無紀錄' }}。頁面每 60 秒刷新。</p>
</html>
"""


class DataFlowBatchOperator(PythonOperator):
    """Task that records the source batch ID for this run."""


class DataFlowProgressLink(BaseOperatorLink):
    name = "Data flow 進度"
    operators = [DataFlowBatchOperator]

    def get_link(self, operator, dttm):
        return url_for("DataFlowProgressView.show", logical_date=dttm.isoformat())


class DataFlowProgressView(BaseView):
    route_base = "/df-progress"
    default_view = "show"

    @expose("/")
    @has_access
    def show(self):
        raw_date = request.args.get("logical_date")
        if not raw_date:
            abort(400)
        try:
            logical_date = timezone.parse(raw_date)
        except (TypeError, ValueError):
            abort(400)

        batch_id = XCom.get_one(
            dag_id=DAG_ID, task_id=TASK_ID, execution_date=logical_date,
            key="df_batch_id",
        )
        if batch_id is None:
            abort(404, description="這次執行尚未記錄 batch_id")

        hook = PostgresHook(postgres_conn_id="df_status_db")
        rows = hook.get_records(SQL, parameters=(batch_id,))
        observed = sum(count for status, count, updated in rows)
        done = sum(count for status, count, updated in rows if status == SUCCESS_STATUS)
        latest = max((updated for status, count, updated in rows if updated is not None),
                     default=None)
        return render_template_string(
            PAGE, batch_id=batch_id, expected=EXPECTED, observed=observed,
            rows=rows, done=done, percent=round(100 * done / EXPECTED, 1),
            missing=max(EXPECTED - observed, 0), latest=latest,
        )


class DataFlowProgressPlugin(AirflowPlugin):
    name = "df_batch_progress"
    appbuilder_views = [{"view": DataFlowProgressView()}]
    operator_extra_links = [DataFlowProgressLink()]
