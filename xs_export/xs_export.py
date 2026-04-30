import json
import os
import pendulum
import logging
import re
from airflow import DAG

# Импортируем стандартные компоненты из xStream common
try:
    from xs_export.xs_common import make_xs_export_task_group, DEFAULT_ARGS
except ImportError:
    try:
        from export.export_xs.xs_common import make_xs_export_task_group, DEFAULT_ARGS
    except ImportError:
        from xs_common import make_xs_export_task_group, DEFAULT_ARGS

logger = logging.getLogger("airflow.task")

# --- Утилиты для сборки SQL и обработки метаданных ---

def build_dynamic_select(base_columns, query_meta, indent="    "):
    """
    Собирает строку SQL SELECT из оптимизированных метаданных.
    """
    if not query_meta:
        return ""
    if "raw" in query_meta:
        return query_meta["raw"]

    all_columns = (query_meta.get('extra_columns', []) + base_columns)
    cols_str = f",\n{indent}".join(all_columns)
    
    sql = f"SELECT\n{indent}{cols_str}\nFROM {query_meta['from']}"
    if query_meta.get('joins'):
        sql += f"\n{query_meta['joins']}"
    if query_meta.get('where'):
        sql += f"\nWHERE {query_meta['where']}"
    if query_meta.get('tail'):
        sql += f"\n{query_meta['tail']}"
    return sql

def clean_sql_string(sql):
    if not sql: return None
    sql = sql.strip()
    if (sql.startswith('f"""') or sql.startswith('f\'\'\'')) and (sql.endswith('"""') or sql.endswith('\'\'\'')):
        sql = sql[4:-3]
    elif (sql.startswith('"""') or sql.startswith('\'\'\'')) and (sql.endswith('"""') or sql.endswith('\'\'\'')):
        sql = sql[3:-3]
    return sql.strip()

def parse_sql(sql):
    sql = clean_sql_string(sql)
    if not sql: return None
    sql_norm = re.sub(r'\s+', ' ', sql)
    select_match = re.search(r'\bSELECT\b', sql_norm, re.IGNORECASE)
    if not select_match: return {"raw": sql}
    select_idx = select_match.start()
    from_match = re.search(r'\bFROM\b', sql_norm[select_idx:], re.IGNORECASE)
    if not from_match: return {"raw": sql}
    from_idx = select_idx + from_match.start()
    columns_str = sql_norm[select_idx + 6 : from_idx].strip()
    columns = [c.strip() for c in columns_str.split(',')]
    remainder = sql_norm[from_idx + 5:].strip()
    stops = [r'\bWHERE\b', r'\bORDER BY\b', r'\bGROUP BY\b', r'\bLIMIT\b', r'\{\{']
    first_stop = len(remainder)
    for stop in stops:
        m = re.search(stop, remainder, re.IGNORECASE)
        if m and m.start() < first_stop:
            first_stop = m.start()
    from_and_joins = remainder[:first_stop].strip()
    tail = remainder[first_stop:].strip()
    join_keywords = [r'\bJOIN\b', r'\bLEFT JOIN\b', r'\bINNER JOIN\b', r'\bFULL JOIN\b', r'\bOUTER JOIN\b']
    first_join = len(from_and_joins)
    for jk in join_keywords:
        m = re.search(jk, from_and_joins, re.IGNORECASE)
        if m and m.start() < first_join:
            first_join = m.start()
    from_table = from_and_joins[:first_join].strip()
    joins = from_and_joins[first_join:].strip()
    where = ""
    if tail.upper().startswith("WHERE"):
        where_remainder = tail[5:].strip()
        next_stop = len(where_remainder)
        for stop in stops[1:]:
            m = re.search(stop, where_remainder, re.IGNORECASE)
            if m and m.start() < next_stop:
                next_stop = m.start()
        where = where_remainder[:next_stop].strip()
        tail = where_remainder[next_stop:].strip()
    return {"columns": columns, "from": from_table, "joins": joins, "where": where, "tail": tail}

def optimize_table(table):
    update_cols = table.get("update_exp", {}).get("columns") if table.get("update_exp") else None
    load_cols = table.get("load_delta", {}).get("columns") if table.get("load_delta") else None
    export_cols = table.get("export_delta", {}).get("columns") if table.get("export_delta") else None
    all_col_lists = [l for l in [update_cols, load_cols, export_cols] if l is not None]
    if not all_col_lists: return table
    base = all_col_lists[0]
    common = [col for col in base if all(col in l for l in all_col_lists)]
    optimized_table = {"full_table_name": table["full_table_name"], "name_file": table["name_file"], "columns": common}
    for key in ["update_exp", "load_delta", "export_delta"]:
        if table.get(key):
            query_meta = table[key]
            if "columns" in query_meta:
                extra = [c for c in query_meta["columns"] if c not in common]
                new_meta = {k: v for k, v in query_meta.items() if k != "columns"}
                if extra: new_meta["extra_columns"] = extra
                optimized_table[key] = new_meta
    return optimized_table

# --- Основная логика фабрики DAG-ов ---

META_FILE = os.path.join(os.path.dirname(__file__), "export_xs_optimized.json")

def create_dynamic_dags():
    if not os.path.exists(META_FILE):
        logger.error(f"Metadata file not found: {META_FILE}")
        return

    with open(META_FILE, 'r') as f:
        dag_configs = json.load(f)

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

                params = {
                    "replica": table_meta["name_file"].split("__")[2],
                    "sql_stmt_update_exp": build_dynamic_select(base_cols, table_meta.get("update_exp")),
                    "sql_stmt_load_delta": build_dynamic_select(base_cols, table_meta.get("load_delta")),
                    "sql_stmt_export_delta": build_dynamic_select(base_cols, table_meta.get("export_delta")),
                }

                make_xs_export_task_group(
                    dag=dag,
                    database_name=db_name,
                    table_name=table_name,
                    params=params
                )
        globals()[dag_id] = dag

if os.environ.get("AIRFLOW_HOME") or __name__ == "__main__":
    create_dynamic_dags()
