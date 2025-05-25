import duckdb
import pyarrow as pa

def run_query(sql: str, db_path: str = ':memory:', registry_db: str = None) -> pa.Table:
    with duckdb.connect(db_path) as conn:
        if registry_db:
            conn.execute(f"ATTACH DATABASE '{registry_db}' AS registry")
        result = conn.execute(sql).fetch_arrow_table()
    return result
