import json
import os
import pendulum
import logging
from airflow import DAG

# Импортируем утилиты из текущей папки
try:
    from sql_builder import build_dynamic_select
except ImportError:
    from ctl.xs_export.sql_builder import build_dynamic_select

# Импортируем стандартные компоненты из xStream common
try:
    from export.export_xs.xs_common import make_xs_export_task_group, DEFAULT_ARGS
except ImportError:
    # Запасной путь, если PYTHONPATH настроен иначе
    from export_xs.xs_common import make_xs_export_task_group, DEFAULT_ARGS

logger = logging.getLogger("airflow.task")

# Путь к оптимизированным метаданным (лежит в той же папке)
META_FILE = os.path.join(os.path.dirname(__file__), "export_xs_optimized.json")

def create_dynamic_dags():
    if not os.path.exists(META_FILE):
        logger.error(f"Metadata file not found: {META_FILE}")
        return

    with open(META_FILE, 'r') as f:
        dag_configs = json.load(f)

    # Общие теги для всех дагов
    common_tags = ["DataLab", "CI02420667", "ClickHouse", "xStream"]

    for dag_id, config in dag_configs.items():
        schedule = config.get("schedule_interval", "@daily")
        desc = config.get("description", "")

        dag = DAG(
            dag_id=dag_id,
            description=desc,
            default_args=DEFAULT_ARGS,
            schedule_interval=schedule,
            start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
            catchup=False,
            tags=common_tags,
            is_paused_upon_creation=True,
            render_template_as_native_obj=True
        )

        with dag:
            for table_meta in config.get("tables", []):
                full_name = table_meta["full_table_name"]
                db_name, table_name = full_name.split(".")
                base_cols = table_meta["columns"]

                # Динамически собираем SQL запросы с помощью sql_builder
                params = {
                    # Извлекаем префикс схемы (например, hrpl_lm) из имени файла
                    "replica": table_meta["name_file"].split("__")[2],
                    "sql_stmt_update_exp": build_dynamic_select(base_cols, table_meta.get("update_exp")),
                    "sql_stmt_load_delta": build_dynamic_select(base_cols, table_meta.get("load_delta")),
                    "sql_stmt_export_delta": build_dynamic_select(base_cols, table_meta.get("export_delta")),
                }

                # Создаем группу задач для этой таблицы через стандартную фабрику
                make_xs_export_task_group(
                    dag=dag,
                    database_name=db_name,
                    table_name=table_name,
                    params=params
                )
        
        # Регистрация DAG в глобальном пространстве имен
        globals()[dag_id] = dag

# Запуск генерации
if os.environ.get("AIRFLOW_HOME"): # Проверка, что запуск в контексте Airflow
    create_dynamic_dags()
else:
    # Для отладки вне Airflow
    create_dynamic_dags()
    print(f"Generated {len(globals()) - 10} DAGs") # -10 примерно на служебные импорты
