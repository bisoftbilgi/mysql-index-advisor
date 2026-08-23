# connection + EXPLAIN + table stats

import json
import mysql.connector
from mysql.connector import Error


def connect(info, with_db=True):
    cfg = {
        "host": (info.get("host") or "localhost").strip(),
        "port": int(info.get("port") or 3306),
        "user": (info.get("user") or "root").strip(),
        "password": info.get("password") or "",
        "connection_timeout": 8,
        "charset": "utf8mb4",
    }
    db = (info.get("database") or "").strip()
    if with_db and db:
        cfg["database"] = db
    return mysql.connector.connect(**cfg)


def test_connection(info):
    conn = None
    try:
        conn = connect(info, with_db=True)
        cur = conn.cursor()
        cur.execute("SELECT VERSION()")
        version = cur.fetchone()[0]
        cur.close()
        db = (info.get("database") or "").strip() or "(no database selected)"
        return True, f"Connected. MySQL {version} / database: {db}"
    except Error as e:
        return False, str(e)
    finally:
        if conn is not None and conn.is_connected():
            conn.close()


def _strip_sql(sql):
    sql = (sql or "").strip()
    if sql.endswith(";"):
        sql = sql[:-1].strip()
    return sql


def looks_like_multi_statement(sql):
    # ignore a single trailing semicolon, block anything else with ;
    s = _strip_sql(sql)
    return ";" in s


def run_explain(info, sql, analyze=False):
    """
    Returns dict with rows (tabular EXPLAIN), json_text, analyze_text, error.
    EXPLAIN ANALYZE actually runs the query, so it is opt-in.
    """
    sql = _strip_sql(sql)
    out = {
        "rows": [],
        "columns": [],
        "json_text": "",
        "analyze_text": "",
        "error": None,
        "analyze_error": None,
    }
    if not sql:
        out["error"] = "No query given."
        return out
    if looks_like_multi_statement(sql):
        out["error"] = "One statement only. Remove extra semicolons."
        return out

    conn = None
    try:
        conn = connect(info)
        cur = conn.cursor(dictionary=True)
        cur.execute("EXPLAIN " + sql)
        rows = cur.fetchall() or []
        cols = [d[0] for d in cur.description] if cur.description else []
        # decimal/None -> something jinja can print easily
        clean = []
        for row in rows:
            item = {}
            for k in cols:
                v = row.get(k)
                item[k] = "" if v is None else v
            clean.append(item)
        out["rows"] = clean
        out["columns"] = cols
        cur.close()

        cur = conn.cursor()
        try:
            cur.execute("EXPLAIN FORMAT=JSON " + sql)
            jrow = cur.fetchone()
            if jrow:
                raw = jrow[0]
                try:
                    out["json_text"] = json.dumps(json.loads(raw), indent=2)
                except (TypeError, json.JSONDecodeError):
                    out["json_text"] = str(raw)
        except Error:
            out["json_text"] = ""
        cur.close()

        if analyze:
            cur = conn.cursor()
            try:
                # FORMAT=TREE is the readable one in 8.0.18+
                cur.execute("EXPLAIN ANALYZE " + sql)
                lines = []
                for r in cur.fetchall() or []:
                    lines.append(str(r[0]))
                out["analyze_text"] = "\n".join(lines)
            except Error as e:
                out["analyze_error"] = (
                    "EXPLAIN ANALYZE failed (needs MySQL 8.0.18+ and it executes the query). "
                    + str(e)
                )
            cur.close()
        return out
    except Error as e:
        out["error"] = str(e)
        return out
    finally:
        if conn is not None and conn.is_connected():
            conn.close()


def flag_plan_issues(explain_rows):
    issues = []
    for row in explain_rows:
        table = row.get("table") or "?"
        access = str(row.get("type") or "").lower()
        extra = str(row.get("Extra") or "")
        key = row.get("key")
        possible = row.get("possible_keys")
        est_rows = row.get("rows")

        if access == "all":
            issues.append({
                "level": "high",
                "text": (
                    f"Full table scan on `{table}` (type=ALL), estimated rows={est_rows}. "
                    "Usually no usable index for the WHERE/JOIN."
                ),
            })
        elif access == "index":
            issues.append({
                "level": "medium",
                "text": (
                    f"Full index scan on `{table}` (type=index). Better than ALL "
                    "but still can be expensive on a big table."
                ),
            })

        key_empty = key is None or key == ""
        poss_empty = possible is None or possible == ""
        if key_empty and not poss_empty:
            issues.append({
                "level": "medium",
                "text": (
                    f"Table `{table}` has possible indexes ({possible}) "
                    "but the optimizer did not pick one."
                ),
            })
        if key_empty and poss_empty and access in ("all", "index"):
            issues.append({
                "level": "high",
                "text": f"No index looks usable on `{table}` for this query.",
            })

        extra_l = extra.lower()
        if "using temporary" in extra_l:
            issues.append({
                "level": "medium",
                "text": f"`{table}`: Using temporary table (GROUP BY / DISTINCT / ORDER BY often causes this).",
            })
        if "using filesort" in extra_l:
            issues.append({
                "level": "medium",
                "text": f"`{table}`: Using filesort. An index matching ORDER BY can avoid this.",
            })
    return issues


def fetch_columns(cur, schema, tables):
    if not tables:
        return {}
    fmt = ",".join(["%s"] * len(tables))
    cur.execute(
        f"""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_KEY, IS_NULLABLE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME IN ({fmt})
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        """,
        [schema] + list(tables),
    )
    out = {}
    for row in cur.fetchall():
        out.setdefault(row[0], []).append({
            "name": row[1],
            "data_type": row[2],
            "column_key": row[3],
            "nullable": row[4],
        })
    return out


def fetch_indexes(cur, schema, tables):
    if not tables:
        return {}
    fmt = ",".join(["%s"] * len(tables))
    cur.execute(
        f"""
        SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME IN ({fmt})
        ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
        """,
        [schema] + list(tables),
    )
    # table -> list of {name, unique, columns: []}
    tmp = {}
    for table, idx, non_unique, seq, col in cur.fetchall():
        key = (table, idx)
        if key not in tmp:
            tmp[key] = {
                "table": table,
                "name": idx,
                "unique": non_unique == 0,
                "columns": [],
            }
        tmp[key]["columns"].append(col)
    grouped = {}
    for item in tmp.values():
        grouped.setdefault(item["table"], []).append(item)
    return grouped


def fetch_table_stats(cur, schema, tables):
    if not tables:
        return []
    fmt = ",".join(["%s"] * len(tables))
    cur.execute(
        f"""
        SELECT TABLE_NAME, ENGINE, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH,
               UPDATE_TIME, CREATE_TIME, TABLE_COLLATION
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME IN ({fmt})
        """,
        [schema] + list(tables),
    )
    rows = []
    for r in cur.fetchall():
        rows.append({
            "table": r[0],
            "engine": r[1],
            "table_rows": r[2],
            "data_length": r[3],
            "index_length": r[4],
            "update_time": r[5],
            "create_time": r[6],
            "collation": r[7],
        })

    # innodb_table_stats is more honest about last stats update, but needs privilege
    last_map = {}
    try:
        fmt2 = ",".join(["%s"] * len(tables))
        cur.execute(
            f"""
            SELECT table_name, n_rows, clustered_index_size, last_update
            FROM mysql.innodb_table_stats
            WHERE database_name = %s AND table_name IN ({fmt2})
            """,
            [schema] + list(tables),
        )
        for r in cur.fetchall():
            last_map[r[0]] = {"n_rows": r[1], "last_update": r[3]}
    except Error:
        last_map = {}

    for row in rows:
        extra = last_map.get(row["table"])
        if extra:
            row["innodb_n_rows"] = extra["n_rows"]
            row["stats_last_update"] = extra["last_update"]
        else:
            row["innodb_n_rows"] = None
            row["stats_last_update"] = None
    return rows


def stats_notes(stat_rows):
    notes = []
    for s in stat_rows:
        name = s["table"]
        if (s.get("table_rows") in (None, 0)) and (s.get("data_length") or 0) > 0:
            notes.append({
                "level": "high",
                "text": (
                    f"`{name}`: information_schema.TABLE_ROWS is 0/NULL but the table has data. "
                    "Stats look missing or stale — try ANALYZE TABLE."
                ),
                "table": name,
            })
        if s.get("engine") == "InnoDB":
            notes.append({
                "level": "info",
                "text": (
                    f"`{name}`: InnoDB TABLE_ROWS ({s.get('table_rows')}) is an estimate. "
                    "If the plan looks weird after big inserts/deletes, run ANALYZE TABLE."
                ),
                "table": name,
            })
        last = s.get("stats_last_update")
        if last is not None:
            notes.append({
                "level": "info",
                "text": f"`{name}`: innodb_table_stats last_update = {last}",
                "table": name,
            })
    return notes


def run_analyze_table(info, table):
    table = (table or "").strip()
    if not table.isidentifier():
        return False, "Invalid table name."
    conn = None
    try:
        conn = connect(info)
        cur = conn.cursor()
        cur.execute(f"ANALYZE TABLE `{table}`")
        rows = cur.fetchall()
        cur.close()
        conn.commit()
        msg = "; ".join(" | ".join(str(x) for x in r) for r in rows)
        return True, msg or f"ANALYZE TABLE `{table}` done."
    except Error as e:
        return False, str(e)
    finally:
        if conn is not None and conn.is_connected():
            conn.close()


def run_create_index(info, ddl):
    ddl = (ddl or "").strip().rstrip(";")
    upper = ddl.upper()
    ok_prefix = upper.startswith("CREATE INDEX") or upper.startswith("ALTER TABLE")
    if not ok_prefix or "INDEX" not in upper:
        return False, "Refusing to run that. Only CREATE INDEX / ALTER TABLE ... INDEX is allowed."
    # very small guard against extra statements
    if ";" in ddl:
        return False, "One statement only."
    conn = None
    try:
        conn = connect(info)
        cur = conn.cursor()
        cur.execute(ddl)
        conn.commit()
        cur.close()
        return True, "Index created. Run Analyze again to see if EXPLAIN changed."
    except Error as e:
        return False, str(e)
    finally:
        if conn is not None and conn.is_connected():
            conn.close()
