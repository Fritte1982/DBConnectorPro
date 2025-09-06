"""Manuelle Test-Cases in db_übungen/WideWorldimporters_tasks/tasks_a.py"""
import re
from db_connection_manager import DB_Connection

def find_problematic_tables(connection: DB_Connection, problem_types=None) -> dict:
    if problem_types is None:
        problem_types = {"geometry", "geography", "hierarchyid", "xml", "datetimeoffset"}

    query = """
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE DATA_TYPE IN ({})
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """.format(",".join(f"'{t}'" for t in problem_types))

    results, _ = connection.daten_spalten(query)

    # Gruppiere nach Tabelle
    from collections import defaultdict
    table_problems = defaultdict(list)

    for schema, table, column, dtype in results:
        table_name = f"{schema}.{table}"
        table_problems[table_name].append((column, dtype))

    return dict(table_problems)


def make_safe_query_from_df(query: str, objct: DB_Connection) -> str:
    # 1. Tabelle extrahieren (inkl. optionalem Schema)
    table_match = re.search(r"FROM\s+([\[\]\w]+(?:\.[\[\]\w]+)?)", query, flags=re.IGNORECASE)
    if not table_match:
        raise ValueError("Keine FROM-Klausel mit Tabelle gefunden.")

    full_table = table_match.group(1).replace("[", "").replace("]", "")
    parts = full_table.split(".")
    if len(parts) == 2:
        schema_name, table_name = parts
    else:
        schema_name = 'dbo'  # fallback default schema
        table_name = parts[0]

    # 2. Schema nur für diese Tabelle abfragen
    schema_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}'
          AND TABLE_NAME = '{table_name}'
    """
    objct.daten_spalten(schema_query)
    schema_df = objct.df_return()

    # 3. Problematische Typen behandeln
    PROBLEM_TYPES = {'geometry', 'geography', 'hierarchyid', 'xml', 'datetimeoffset' }
    col_type_map = dict(zip(schema_df['COLUMN_NAME'], schema_df['DATA_TYPE'].str.lower()))

    cols = []
    for col, dtype in col_type_map.items():
        if dtype in PROBLEM_TYPES:
            cols.append(f"CAST([{col}] AS NVARCHAR(MAX)) AS [{col}]")
        else:
            cols.append(f"[{col}]")

    # 4. SELECT * ersetzen
    select_part = "SELECT " + ", ".join(cols)
    safe_query = re.sub(r"SELECT\s+(TOP\s*\(?\d+\)?\s+)?\*", select_part, query, flags=re.IGNORECASE)

    return safe_query


def make_safe_query_with_joins(query: str, objct: DB_Connection) -> str:
    import re
    PROBLEM_TYPES = {'geometry', 'geography', 'hierarchyid', 'xml', 'datetimeoffset'}

    table_pattern = re.compile(r"""
        (FROM|JOIN)\s+
        (?P<full_table>[\[\]\w]+\.[\[\]\w]+)
        (?:\s+(?:AS\s+)?(?P<alias>\w+))?
    """, re.IGNORECASE | re.VERBOSE)

    table_matches = table_pattern.findall(query)
    if not table_matches:
        raise ValueError("Keine Tabellen in FROM/JOIN-Klauseln erkannt.")

    all_columns = []

    for _, full_table, alias in table_matches:
        clean_table = full_table.replace("[", "").replace("]", "")
        schema_name, table_name = clean_table.split(".")
        alias_prefix = alias or table_name

        # Schema der aktuellen Tabelle lokal abfragen
        schema_query = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema_name}'
              AND TABLE_NAME = '{table_name}'
        """
        objct.daten_spalten(schema_query)
        schema_df = objct.df_return().copy()  # 🟡 wichtig: kopieren, nicht überschreiben

        col_type_map = dict(zip(schema_df['COLUMN_NAME'], schema_df['DATA_TYPE'].str.lower()))

        for col, dtype in col_type_map.items():
            qualified_col = f"{alias_prefix}.[{col}]"
            output_alias = f"{alias_prefix}_{col}"
            if dtype in PROBLEM_TYPES:
                all_columns.append(f"CAST({qualified_col} AS NVARCHAR(MAX)) AS [{output_alias}]")
            else:
                all_columns.append(f"{qualified_col} AS [{output_alias}]")

    # Ersetze SELECT * (oder SELECT TOP ...) durch sichere Liste
    select_part = "SELECT " + ", ".join(all_columns)
    safe_query = re.sub(r"SELECT\s+(TOP\s*\(?\d+\)?\s+)?\*", select_part, query, flags=re.IGNORECASE)

    return safe_query




# TODO für Joins und mehrere From's anpassen
'''Ansatz:
table_matches = re.findall(r"(FROM|JOIN)\s+([\[\]\w]+\.[\[\]\w]+)", query, flags=re.IGNORECASE)
if not table_matches:
    raise ValueError("Keine Tabellen gefunden.")

all_columns = []
for _, full_table in table_matches:
    full_table = full_table.replace("[", "").replace("]", "")
    schema_name, table_name = full_table.split(".")
    
    schema_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}'
          AND TABLE_NAME = '{table_name}'
    """
    objct.daten_spalten(schema_query)
    schema_df = objct.df_return()

    col_type_map = dict(zip(schema_df['COLUMN_NAME'], schema_df['DATA_TYPE'].str.lower()))

    for col, dtype in col_type_map.items():
        prefix = f"[{table_name}]."  # Optional: Alias beachten
        if dtype in PROBLEM_TYPES:
            all_columns.append(f"CAST({prefix}[{col}] AS NVARCHAR(MAX)) AS [{col}]")
        else:
            all_columns.append(f"{prefix}[{col}]")

'''
