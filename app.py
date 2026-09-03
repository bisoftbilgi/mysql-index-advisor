# MySQL Tuning Wizard - internship project
# Connect, paste a query, look at EXPLAIN, get index/stat tips.

import os
from flask import Flask, render_template, request, session

import db_utils
import advisor

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "local-dev-not-secret")

SAMPLE_QUERIES = [
    (
        "Filter on status (no index)",
        "SELECT * FROM orders WHERE status = 'shipped';",
    ),
    (
        "Function on a column",
        "SELECT order_id, amount FROM orders WHERE YEAR(order_date) = 2024;",
    ),
    (
        "Join customers + orders",
        "SELECT c.full_name, o.amount, o.order_date FROM customers c "
        "JOIN orders o ON o.customer_id = c.customer_id "
        "WHERE c.email = 'user100@example.com';",
    ),
    (
        "Leading wildcard LIKE",
        "SELECT * FROM customers WHERE full_name LIKE '%ahmet%';",
    ),
    (
        "Sort by date",
        "SELECT order_id, amount FROM orders ORDER BY order_date DESC LIMIT 50;",
    ),
    (
        "City + join + group",
        "SELECT c.city, COUNT(*) AS cnt FROM customers c "
        "JOIN orders o ON o.customer_id = c.customer_id "
        "WHERE c.city = 'Ankara' GROUP BY c.city;",
    ),
]


def form_conn():
    src = request.form if request.method == "POST" else {}
    saved = session.get("conn") or {}
    return {
        "host": src.get("host", saved.get("host", "localhost")),
        "port": src.get("port", saved.get("port", "3306")),
        "user": src.get("user", saved.get("user", "root")),
        "password": src.get("password", saved.get("password", "")),
        "database": src.get("database", saved.get("database", "tuning_demo")),
    }


def save_conn(info):
    session["conn"] = info


def empty_state():
    return {
        "conn": form_conn(),
        "query": session.get("query", SAMPLE_QUERIES[0][1]),
        "analyze_too": False,
        "profile_too": False,
        "samples": SAMPLE_QUERIES,
        "msg": None,
        "msg_ok": False,
        "explain": None,
        "issues": [],
        "suggestions": [],
        "already": [],
        "stats": [],
        "stat_notes": [],
        "histograms": [],
        "hist_actions": [],
        "tips": [],
        "hints": [],
        "profile": None,
        "used": {},
        "existing_indexes": {},
    }


@app.route("/", methods=["GET", "POST"])
def index():
    data = empty_state()
    if request.method == "GET":
        return render_template("index.html", **data)

    action = request.form.get("action") or "analyze"
    conn = form_conn()
    save_conn(conn)
    query = request.form.get("query", "")
    session["query"] = query
    data["conn"] = conn
    data["query"] = query
    data["analyze_too"] = request.form.get("analyze_too") == "1"
    data["profile_too"] = request.form.get("profile_too") == "1"

    if action == "test":
        ok, msg = db_utils.test_connection(conn)
        data["msg"] = msg
        data["msg_ok"] = ok
        return render_template("index.html", **data)

    if action == "create_index":
        ddl = request.form.get("index_sql") or ""
        ok, msg = db_utils.run_create_index(conn, ddl)
        data["msg"] = msg
        data["msg_ok"] = ok
        # still show last analysis if we have query
        if query.strip():
            data = _fill_analysis(data, conn, query)
            data["msg"] = msg
            data["msg_ok"] = ok
        return render_template("index.html", **data)

    if action == "analyze_table":
        table = request.form.get("stat_table") or ""
        ok, msg = db_utils.run_analyze_table(conn, table)
        data["msg"] = msg
        data["msg_ok"] = ok
        if query.strip():
            data = _fill_analysis(data, conn, query)
            data["msg"] = msg
            data["msg_ok"] = ok
        return render_template("index.html", **data)

    if action == "update_histogram":
        ddl = request.form.get("hist_sql") or ""
        ok, msg = db_utils.run_update_histogram(conn, ddl)
        data["msg"] = msg
        data["msg_ok"] = ok
        if query.strip():
            data = _fill_analysis(data, conn, query)
            data["msg"] = msg
            data["msg_ok"] = ok
        return render_template("index.html", **data)

    # default: analyze query
    if not query.strip():
        data["msg"] = "Paste a query first."
        return render_template("index.html", **data)

    data = _fill_analysis(data, conn, query)
    return render_template("index.html", **data)


def _fill_analysis(data, conn, query):
    explain = db_utils.run_explain(conn, query, analyze=data.get("analyze_too"))
    data["explain"] = explain
    if explain.get("error"):
        data["msg"] = explain["error"]
        data["msg_ok"] = False
        return data

    data["issues"] = db_utils.flag_plan_issues(
        explain.get("rows") or [],
        query_cost=explain.get("query_cost"),
    )
    data["tips"] = advisor.rewrite_tips(query)

    if data.get("profile_too"):
        data["profile"] = db_utils.profile_query(conn, query)

    schema = (conn.get("database") or "").strip()
    if not schema:
        data["msg"] = "Connected, but pick a database so I can read information_schema."
        data["msg_ok"] = False
        return data

    cnx = None
    try:
        cnx = db_utils.connect(conn)
        cur = cnx.cursor()
        advice = advisor.advise_indexes(cur, schema, query)
        tables = [t for t, _a in advice["tables"]]
        # unique table names, keep original case from parser
        uniq = []
        seen = set()
        for t in tables:
            if t.lower() not in seen:
                seen.add(t.lower())
                uniq.append(t)
        data["suggestions"] = advice["suggestions"]
        data["already"] = advice["already"]
        data["used"] = advice["used"]
        data["existing_indexes"] = advice["indexes"]
        data["tips"] = advisor.rewrite_tips(
            query,
            columns_by_table=advice["columns"],
            tables=advice["tables"],
        )
        data["hints"] = advisor.optimizer_hints(
            query,
            explain.get("rows") or [],
            advice["suggestions"],
            already=advice["already"],
            tables=advice["tables"],
        )
        data["stats"] = db_utils.fetch_table_stats(cur, schema, uniq)
        data["histograms"] = db_utils.fetch_histograms(cur, schema, uniq)
        data["stat_notes"] = db_utils.stats_notes(
            data["stats"],
            used_by_table=advice["used"],
            histograms=data["histograms"],
            columns_by_table=advice["columns"],
        )
        data["hist_actions"] = db_utils.histogram_actions(
            advice["used"],
            data["histograms"],
            columns_by_table=advice["columns"],
        )
        session["last_suggestions"] = advice["suggestions"]
        cur.close()
    except Exception as e:
        data["msg"] = str(e)
        data["msg_ok"] = False
    finally:
        if cnx is not None and cnx.is_connected():
            cnx.close()
    return data


if __name__ == "__main__":
    # debug on so template edits show up while I work
    app.run(debug=True)

