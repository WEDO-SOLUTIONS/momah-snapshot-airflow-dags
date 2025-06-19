import pendulum
import logging
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Variable
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule
from dags.dag_helpers import get_pod_override_config
from etl_scripts import manage_asset, sync_data
from etl_scripts.common import config as Cfg

log = logging.getLogger(__name__)

GIT_REPO_URL = "https://github.com/WEDO-SOLUTIONS/momah-airflow.git"
GIT_BRANCH = "main"

def create_asset_management_dag(schema_name: str, mapper_module) -> DAG:
    dag_id = f"{schema_name}_01_asset_management"

    def _run_management_task(**context):
        operation = context["params"].get("operation")
        log.info(f"Executing operation: {operation} for schema: {schema_name}")
        if operation.upper() in ["CREATE", "UPDATE"]:
            manage_asset.create_or_update_asset(schema_name, mapper_module.ATTRIBUTE_MAPPER, operation)
        elif operation.upper() == "CLEAR_ALL_DATA":
            manage_asset.clear_all_asset_data(schema_name)
        else:
            raise ValueError(f"Unknown operation: {operation}")

    dag = DAG(
        dag_id=dag_id, start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Riyadh"), schedule=None, catchup=False,
        tags=["urbi-pro", "management", "factory", schema_name],
        params={"operation": Param("CREATE", type="string", enum=["CREATE", "UPDATE", "CLEAR_ALL_DATA"], title="Asset Operation")},
    )
    with dag:
        PythonOperator(
            task_id="run_asset_task", python_callable=_run_management_task,
            executor_config=get_pod_override_config(GIT_REPO_URL, GIT_BRANCH),
        )
    return dag

def create_hourly_sync_dag(schema_name: str, mapper_module) -> DAG:
    dag_id = f"{schema_name}_02_hourly_sync"
    prefix = f"{schema_name.upper()}_"

    def _fetch_and_chunk_upserts(**context):
        ti = context["ti"]
        hook = OracleHook(oracle_conn_id='oracle_momrah_db')
        api_config = Cfg.get_config(schema_name)
        view_name, chunk_size = api_config["db_view_name"], api_config["etl_chunk_size"]
        last_sync_ts = ti.xcom_pull(key="last_successful_sync_timestamp", default="1970-01-01T00:00:00.000000+00:00")
        sql = f"""SELECT * FROM "{view_name}" WHERE "last_modified_date" > TO_TIMESTAMP_TZ(:ts, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZH:TZM')"""
        
        log.info(f"Executing query for upserts with chunk size {chunk_size}...")
        results_generator = hook.get_conn().cursor().execute(sql, ts=last_sync_ts)
        column_names = [d[0].lower() for d in results_generator.description]
        chunks = []
        while True:
            rows = results_generator.fetchmany(chunk_size)
            if not rows: break
            chunks.append([dict(zip(column_names, row)) for row in rows])
            log.info(f"Fetched a chunk of {len(rows)} records.")
        return chunks if chunks else [[]]

    def _get_last_known_ids(**context):
        return context["ti"].xcom_pull(key="last_known_object_ids", default=[])

    def _push_upsert_chunk(chunk: list, last_known_ids: list, **context):
        api_hook = HttpHook(http_conn_id='urbi_pro_api')
        push_token = api_hook.get_extra().get("push_data_access_token")
        if not push_token: raise ValueError("push_data_access_token not found in connection extra")
        
        manager = sync_data.SyncManager(
            schema_name=schema_name, api_base_url=api_hook.base_url, access_token=push_token,
            x_brand_header=api_hook.get_extra().get("x_brand_header"), dag_run_id=context["ti"].run_id
        )
        manager.process_upsert_chunk(chunk, set(last_known_ids), mapper_module.ATTRIBUTE_MAPPER, Variable.get(f"{prefix}DYNAMIC_ASSET_ID"))
        return manager.stats

    def _process_deletes(**context):
        ti = context["ti"]
        hook = OracleHook(oracle_conn_id='oracle_momrah_db')
        view_name = Variable.get(f"{prefix}DB_VIEW_NAME")
        last_known_ids = set(ti.xcom_pull(task_ids='get_last_known_ids', key='return_value'))
        
        if not last_known_ids:
            log.info("No previously known IDs found, skipping delete check.")
            ti.xcom_push(key="final_set_of_ids", value=[])
            return {"deleted": 0, "delete_failed": 0}
        
        log.info(f"Fetching all current IDs from {view_name} to calculate deletes...")
        all_current_ids = set(str(r[0]) for r in hook.get_records(f'SELECT "id" FROM "{view_name}"'))
        ids_to_delete = list(last_known_ids - all_current_ids)
        log.info(f"Found {len(ids_to_delete)} records to delete.")
        
        api_hook = HttpHook(http_conn_id='urbi_pro_api')
        push_token = api_hook.get_extra().get("push_data_access_token")
        manager = sync_data.SyncManager(
            schema_name=schema_name, api_base_url=api_hook.base_url, access_token=push_token,
            x_brand_header=api_hook.get_extra().get("x_brand_header"), dag_run_id=ti.run_id
        )
        manager.push_deletes_to_api(Variable.get(f"{prefix}DYNAMIC_ASSET_ID"), ids_to_delete)
        
        ti.xcom_push(key="final_set_of_ids", value=list(all_current_ids))
        return manager.stats

    def _aggregate_and_report_stats(upsert_stats_list: list, delete_stats: dict, **context):
        ti = context["ti"]
        final_stats = {"validated": 0, "skipped": 0, "added": 0, "updated": 0, "pushed": 0, "push_failed": 0, "deleted": 0, "delete_failed": 0}
        
        for stats in upsert_stats_list:
            for key in final_stats:
                final_stats[key] += stats.get(key, 0)
        
        final_stats["deleted"] += delete_stats.get("deleted", 0)
        final_stats["delete_failed"] += delete_stats.get("delete_failed", 0)
        
        log.info(f"AGGREGATED STATS: {final_stats}")
        
        # Upload the final aggregated report
        api_hook = HttpHook(http_conn_id='urbi_pro_api') # Needed to instantiate a dummy manager
        manager = sync_data.SyncManager(
            schema_name=schema_name, api_base_url=api_hook.base_url, access_token="",
            x_brand_header="", dag_run_id=ti.run_id
        )
        manager.stats = final_stats
        manager.upload_reports_to_s3()

        if final_stats['push_failed'] > 0 or final_stats['delete_failed'] > 0:
            raise Exception("Sync finished with failed API calls.")
        
        ti.xcom_push(key="last_successful_sync_timestamp", value=context['data_interval_end'].isoformat())
        final_ids = ti.xcom_pull(task_ids='process_deletes', key='final_set_of_ids')
        if final_ids is not None:
            ti.xcom_push(key="last_known_object_ids", value=final_ids)

    dag = DAG(dag_id=dag_id, start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Riyadh"), schedule="@hourly", catchup=False, tags=["urbi-pro", "etl-factory-large-data", schema_name], max_active_runs=1)
    with dag:
        get_last_known_ids_task = PythonOperator(task_id='get_last_known_ids', python_callable=_get_last_known_ids)
        fetch_chunks_task = PythonOperator(task_id="fetch_and_chunk_upsert_data", python_callable=_fetch_and_chunk_upserts)
        
        push_chunks_task = PythonOperator.partial(
            task_id="push_upsert_chunk", python_callable=_push_upsert_chunk,
            executor_config=get_pod_override_config(GIT_REPO_URL, GIT_BRANCH)
        ).expand(chunk=fetch_chunks_task.output, op_kwargs={"last_known_ids": get_last_known_ids_task.output})
        
        process_deletes_task = PythonOperator(
            task_id="process_deletes", python_callable=_process_deletes,
            op_kwargs={"last_known_ids": get_last_known_ids_task.output},
            executor_config=get_pod_override_config(GIT_REPO_URL, GIT_BRANCH)
        )
        
        aggregate_task = PythonOperator(
            task_id="aggregate_and_report_stats", python_callable=_aggregate_and_report_stats,
            op_kwargs={"upsert_stats_list": push_chunks_task.output, "delete_stats": process_deletes_task.output},
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        get_last_known_ids_task >> [fetch_chunks_task, process_deletes_task]
        fetch_chunks_task >> push_chunks_task
        [push_chunks_task, process_deletes_task] >> aggregate_task
        
    return dag