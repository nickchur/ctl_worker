import json

def optimize_table(table):
    # Извлекаем все доступные списки колонок
    update_cols = table.get("update_exp", {}).get("columns") if table.get("update_exp") else None
    load_cols = table.get("load_delta", {}).get("columns") if table.get("load_delta") else None
    export_cols = table.get("export_delta", {}).get("columns") if table.get("export_delta") else None

    # Собираем все непустые списки
    all_col_lists = [l for l in [update_cols, load_cols, export_cols] if l is not None]
    
    if not all_col_lists:
        return table

    # Находим общие колонки (пересечение всех списков)
    # Используем первый список как базу и проверяем наличие в остальных
    base = all_col_lists[0]
    common = []
    for col in base:
        if all(col in l for l in all_col_lists):
            common.append(col)
    
    # Обновляем таблицу
    optimized_table = {
        "full_table_name": table["full_table_name"],
        "name_file": table["name_file"],
        "columns": common
    }
    
    for key in ["update_exp", "load_delta", "export_delta"]:
        if table.get(key):
            query_meta = table[key]
            if "columns" in query_meta:
                orig_cols = query_meta["columns"]
                # Оставляем только те, которых нет в общем списке
                extra = [c for c in orig_cols if c not in common]
                
                # Создаем новый объект без громоздкого списка columns
                new_meta = {k: v for k, v in query_meta.items() if k != "columns"}
                if extra:
                    new_meta["extra_columns"] = extra
                
                optimized_table[key] = new_meta
    
    return optimized_table

with open('export_xs_structured.json', 'r') as f:
    data = json.load(f)

optimized_data = {}

for dag_id, dag_meta in data.items():
    optimized_data[dag_id] = {
        "description": dag_meta.get("description"),
        "schedule_interval": dag_meta.get("schedule_interval"),
        "tags": dag_meta.get("tags"),
        "tables": [optimize_table(t) for t in dag_meta["tables"]]
    }

with open('export_xs_optimized.json', 'w') as f:
    json.dump(optimized_data, f, indent=2, ensure_ascii=False)

print("Optimization complete. Saved to export_xs_optimized.json")
