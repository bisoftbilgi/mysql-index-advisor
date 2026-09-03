# connection + EXPLAIN + table stats + profiling

import json
import re
import time
from datetime import datetime, timedelta

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
        "query_cost": None,
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
                    parsed = json.loads(raw)
                    out["json_text"] = json.dumps(parsed, indent=2)
                    out["query_cost"] = extract_query_cost(parsed)
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
                    lines.append(str(r[0]) if r else "")
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


def extract_query_cost(obj):
    found = []

    def walk(node):
        if isinstance(node, dict):
            ci = node.get("cost_info") or {}
            if "query_cost" in ci:
                found.append(str(ci["query_cost"]))
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(obj)
    return found[0] if found else None


def flag_plan_issues(explain_rows, query_cost=None):
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
        try:
            n = int(float(est_rows or 0))
        except (TypeError, ValueError):
            n = 0
        if n >= 5000 and access in ("all", "index"):
            issues.append({
                "level": "high",
                "text": f"`{table}`: optimizer expects to examine ~{n} rows. That is a lot for this access type.",
            })
    if query_cost:
        issues.append({
            "level": "info",
            "text": f"Estimated query cost from EXPLAIN JSON: {query_cost} (lower is cheaper; compare before/after an index).",
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


def fetch_histograms(cur, schema, tables):
    """columns that already have a histogram (MySQL 8 COLUMN_STATISTICS)."""
    found = []
    if not tables:
        return found
    fmt = ",".join(["%s"] * len(tables))
    try:
        cur.execute(
            f"""
            SELECT TABLE_NAME, COLUMN_NAME, HISTOGRAM
            FROM information_schema.COLUMN_STATISTICS
            WHERE SCHEMA_NAME = %s AND TABLE_NAME IN ({fmt})
            """,
            [schema] + list(tables),
        )
        for table, col, hist in cur.fetchall():
            buckets = None
            if hist:
                try:
                    if isinstance(hist, str):
                        hist = json.loads(hist)
                    buckets = hist.get("number-of-buckets-specified")
                except (TypeError, AttributeError, json.JSONDecodeError):
                    buckets = None
            found.append({
                "table": table,
                "column": col,
                "buckets": buckets,
            })
    except Error:
        found = []
    return found


def _as_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    return None


def stats_notes(stat_rows, used_by_table=None, histograms=None, columns_by_table=None):
    notes = []
    hist_set = set()
    for h in histograms or []:
        hist_set.add((h["table"].lower(), h["column"].lower()))
    pk = set()
    for t, cols in (columns_by_table or {}).items():
        for c in cols:
            if (c.get("column_key") or "").upper() == "PRI":
                pk.add((t.lower(), c["name"].lower()))

    for s in stat_rows:
        name = s["table"]
        est = s.get("table_rows")
        innodb = s.get("innodb_n_rows")
        if (est in (None, 0)) and (s.get("data_length") or 0) > 0:
            notes.append({
                "level": "high",
                "text": (
                    f"`{name}`: TABLE_ROWS is 0/NULL but the table has data. "
                    "Table stats look missing — run ANALYZE TABLE."
                ),
                "table": name,
            })
        if est not in (None, 0) and innodb not in (None, 0):
            try:
                a, b = float(est), float(innodb)
                bigger, smaller = max(a, b), min(a, b)
                if smaller > 0 and (bigger / smaller) >= 1.5:
                    notes.append({
                        "level": "high",
                        "text": (
                            f"`{name}`: TABLE_ROWS={est} vs innodb n_rows={innodb}. "
                            "Those estimates disagree a lot — stale stats can trick the optimizer. "
                            "Run ANALYZE TABLE."
                        ),
                        "table": name,
                    })
            except (TypeError, ValueError):
                pass
        if s.get("engine") == "InnoDB":
            notes.append({
                "level": "info",
                "text": (
                    f"`{name}`: InnoDB TABLE_ROWS ({est}) is an estimate, not a count(*). "
                    "Bad estimates change join order and index choice."
                ),
                "table": name,
            })
        last = _as_datetime(s.get("stats_last_update"))
        if last is not None:
            age = datetime.now() - last
            if age >= timedelta(days=7):
                notes.append({
                    "level": "high",
                    "text": (
                        f"`{name}`: innodb_table_stats last_update is {last} "
                        f"({age.days} days ago). Treat as stale; run ANALYZE TABLE."
                    ),
                    "table": name,
                })
            else:
                notes.append({
                    "level": "info",
                    "text": f"`{name}`: innodb_table_stats last_update = {last}",
                    "table": name,
                })

    for table, cols in (used_by_table or {}).items():
        for col in cols:
            if (table.lower(), col.lower()) in pk:
                continue
            if (table.lower(), col.lower()) not in hist_set:
                notes.append({
                    "level": "medium",
                    "text": (
                        f"`{table}.{col}` is used in the query but has no column histogram. "
                        "Without it MySQL guesses selectivity. "
                        f"ANALYZE TABLE `{table}` UPDATE HISTOGRAM ON `{col}` WITH 32 BUCKETS;"
                    ),
                    "table": table,
                    "column": col,
                })
    return notes


def histogram_actions(used_by_table, histograms, columns_by_table=None):
    hist_set = set()
    for h in histograms or []:
        hist_set.add((h["table"].lower(), h["column"].lower()))
    pk = set()
    for t, cols in (columns_by_table or {}).items():
        for c in cols:
            if (c.get("column_key") or "").upper() == "PRI":
                pk.add((t.lower(), c["name"].lower()))
    actions = []
    seen = set()
    for table, cols in (used_by_table or {}).items():
        for col in cols:
            key = (table.lower(), col.lower())
            if key in hist_set or key in seen or key in pk:
                continue
            seen.add(key)
            sql = (
                f"ANALYZE TABLE `{table}` UPDATE HISTOGRAM ON `{col}` WITH 32 BUCKETS"
            )
            actions.append({
                "table": table,
                "column": col,
                "sql": sql,
                "label": f"{table}.{col}",
            })
    return actions


def run_update_histogram(info, ddl):
    ddl = (ddl or "").strip().rstrip(";")
    upper = ddl.upper()
    if not upper.startswith("ANALYZE TABLE") or "HISTOGRAM" not in upper:
        return False, "Only ANALYZE TABLE ... UPDATE HISTOGRAM is allowed here."
    if ";" in ddl:
        return False, "One statement only."
    conn = None
    try:
        conn = connect(info)
        cur = conn.cursor()
        cur.execute(ddl)
        rows = cur.fetchall()
        conn.commit()
        cur.close()
        msg = "; ".join(" | ".join(str(x) for x in r) for r in rows)
        return True, msg or "Histogram updated."
    except Error as e:
        return False, str(e)
    finally:
        if conn is not None and conn.is_connected():
            conn.close()


def profile_query(info, sql):
    """
    Execute a SELECT and record wall time + SHOW PROFILE steps if the server allows it.
    """
    sql = _strip_sql(sql)
    out = {
        "ok": False,
        "error": None,
        "duration_ms": None,
        "rowcount": None,
        "steps": [],
        "profiling_note": "",
    }
    if not sql:
        out["error"] = "No query given."
        return out
    if looks_like_multi_statement(sql):
        out["error"] = "One statement only."
        return out
    if not re.match(r"\s*SELECT\b", sql, re.I):
        out["error"] = "Profiling only runs SELECT (it executes the query)."
        return out

    conn = None
    try:
        conn = connect(info)
        cur = conn.cursor()
        used_show_profile = False
        try:
            cur.execute("SET profiling_history_size = 15")
            cur.execute("SET profiling = 1")
            used_show_profile = True
        except Error:
            used_show_profile = False

        t0 = time.perf_counter()
        cur.execute(sql)
        rows = cur.fetchall()
        out["duration_ms"] = round((time.perf_counter() - t0) * 1000.0, 2)
        out["rowcount"] = len(rows)

        if used_show_profile:
            try:
                cur.execute("SHOW PROFILE")
                steps = []
                for r in cur.fetchall() or []:
                    steps.append({"status": r[0], "duration": r[1]})
                out["steps"] = steps
            except Error as e:
                out["profiling_note"] = "SHOW PROFILE not available: " + str(e)
        else:
            out["profiling_note"] = (
                "Server refused SET profiling=1 (common on MySQL 8). "
                "Wall-clock time above is still real execution time."
            )
        out["ok"] = True
        cur.close()
        return out
    except Error as e:
        out["error"] = str(e)
        return out
    finally:
        if conn is not None and conn.is_connected():
            conn.close()


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
