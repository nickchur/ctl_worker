import json

def build_dynamic_select(base_columns, query_meta, indent="    "):
    """
    Собирает строку SQL SELECT из оптимизированных метаданных.
    
    :param base_columns: Основной список колонок таблицы
    :param query_meta: Словарь с метаданными конкретного запроса (from, joins, where, extra_columns, etc.)
    :param indent: Отступ для форматирования
    :return: Строка SQL
    """
    if not query_meta:
        return ""
        
    if "raw" in query_meta:
        return query_meta["raw"]

    # Объединяем основные колонки с дополнительными для этого запроса
    all_columns = (query_meta.get('extra_columns', []) + base_columns)
    
    # Форматируем колонки
    cols_str = f",\n{indent}".join(all_columns)
    
    # Базовая часть SELECT ... FROM
    sql = f"SELECT\n{indent}{cols_str}\nFROM {query_meta['from']}"
    
    if query_meta.get('joins'):
        sql += f"\n{query_meta['joins']}"
    
    if query_meta.get('where'):
        sql += f"\nWHERE {query_meta['where']}"
        
    if query_meta.get('tail'):
        sql += f"\n{query_meta['tail']}"
        
    return sql

# Пример использования
if __name__ == "__main__":
    with open('export_xs_optimized.json', 'r') as f:
        data = json.load(f)
    
    dag_id = "export_xs_hrpl_lm_mt_user_stat_by_test"
    table = data[dag_id]["tables"][0]
    base_cols = table["columns"]
    
    print(f"--- [ DAG: {dag_id} ] ---")
    
    print("\n--- [ Динамический SELECT для load_delta ] ---")
    # Передаем общие колонки и метаданные конкретного шага
    print(build_dynamic_select(base_cols, table['load_delta']))
    
    print("\n--- [ Динамический SELECT для export_delta ] ---")
    print(build_dynamic_select(base_cols, table['export_delta']))
