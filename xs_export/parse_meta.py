import json
import re

def clean_sql_string(sql):
    if not sql: return None
    sql = sql.strip()
    # Remove f""" and """ or similar
    if (sql.startswith('f"""') or sql.startswith('f\'\'\'')) and (sql.endswith('"""') or sql.endswith('\'\'\'')):
        sql = sql[4:-3]
    elif (sql.startswith('"""') or sql.startswith('\'\'\'')) and (sql.endswith('"""') or sql.endswith('\'\'\'')):
        sql = sql[3:-3]
    return sql.strip()

def parse_sql(sql):
    sql = clean_sql_string(sql)
    if not sql: return None
    
    # Normalize whitespace
    sql_norm = re.sub(r'\s+', ' ', sql)
    
    # Attempt to find the SELECT part
    # If it's an INSERT, skip to the SELECT
    select_match = re.search(r'\bSELECT\b', sql_norm, re.IGNORECASE)
    if not select_match:
        return {"raw": sql}
    
    select_idx = select_match.start()
    
    # Find FROM
    from_match = re.search(r'\bFROM\b', sql_norm[select_idx:], re.IGNORECASE)
    if not from_match:
        return {"raw": sql}
    
    from_idx = select_idx + from_match.start()
    
    columns_str = sql_norm[select_idx + 6 : from_idx].strip()
    # Split columns by comma, but be careful with functions like count(distinct x)
    # Simple split for now, or just keep as string
    columns = [c.strip() for c in columns_str.split(',')]
    
    remainder = sql_norm[from_idx + 5:].strip()
    
    # Keywords that end the FROM/JOIN section
    keywords = [r'\bWHERE\b', r'\bORDER BY\b', r'\bGROUP BY\b', r'\bLIMIT\b', r'\{\{', r'\bFINAL\b']
    
    # Wait, FINAL in ClickHouse can be part of FROM table FINAL
    
    # Let's find the first keyword that isn't FINAL (unless it's just a table modifier)
    stops = [r'\bWHERE\b', r'\bORDER BY\b', r'\bGROUP BY\b', r'\bLIMIT\b', r'\{\{']
    
    first_stop = len(remainder)
    for stop in stops:
        m = re.search(stop, remainder, re.IGNORECASE)
        if m and m.start() < first_stop:
            first_stop = m.start()
            
    from_and_joins = remainder[:first_stop].strip()
    tail = remainder[first_stop:].strip()
    
    # Split from_and_joins into FROM table and JOINS
    join_keywords = [r'\bJOIN\b', r'\bLEFT JOIN\b', r'\bINNER JOIN\b', r'\bFULL JOIN\b', r'\bOUTER JOIN\b']
    
    first_join = len(from_and_joins)
    for jk in join_keywords:
        m = re.search(jk, from_and_joins, re.IGNORECASE)
        if m and m.start() < first_join:
            first_join = m.start()
            
    from_table = from_and_joins[:first_join].strip()
    joins = from_and_joins[first_join:].strip()
    
    # Separate WHERE from tail if it exists
    where = ""
    if tail.upper().startswith("WHERE"):
        # Find next keyword in tail after WHERE
        where_remainder = tail[5:].strip()
        next_stop = len(where_remainder)
        for stop in stops[1:]: # Skip WHERE itself
            m = re.search(stop, where_remainder, re.IGNORECASE)
            if m and m.start() < next_stop:
                next_stop = m.start()
        where = where_remainder[:next_stop].strip()
        tail = where_remainder[next_stop:].strip()

    return {
        "columns": columns,
        "from": from_table,
        "joins": joins,
        "where": where,
        "tail": tail
    }

with open('export_xs_meta.json', 'r') as f:
    meta = json.load(f)

structured = {}

for dag_id, dag_data in meta.items():
    structured[dag_id] = {
        "description": dag_data.get("description"),
        "schedule_interval": dag_data.get("schedule_interval"),
        "tags": dag_data.get("tags"),
        "tables": []
    }
    
    for table in dag_data.get("tables", []):
        t_struct = {
            "full_table_name": table.get("full_table_name"),
            "name_file": table.get("name_file"),
            "update_exp": parse_sql(table.get("sql_stmt_update_exp")),
            "load_delta": parse_sql(table.get("sql_stmt_load_delta")),
            "export_delta": parse_sql(table.get("sql_stmt_export_delta"))
        }
        structured[dag_id]["tables"].append(t_struct)

with open('export_xs_structured.json', 'w') as f:
    json.dump(structured, f, indent=2, ensure_ascii=False)

print("Processing complete. Saved to export_xs_structured.json")
