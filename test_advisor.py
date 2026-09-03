import advisor

q = "SELECT * FROM orders WHERE status = 'shipped';"
assert advisor.extract_tables(q) == [("orders", "orders")]
tips = [t["title"] for t in advisor.rewrite_tips(q)]
assert "SELECT *" in tips

q2 = (
    "SELECT c.full_name, o.amount FROM customers c "
    "JOIN orders o ON o.customer_id = c.customer_id "
    "WHERE c.email = 'x';"
)
tables = advisor.extract_tables(q2)
assert ("customers", "c") in tables
assert ("orders", "o") in tables

cols2 = {
    "customers": [
        {"name": "customer_id", "column_key": "PRI"},
        {"name": "email", "column_key": ""},
        {"name": "full_name", "column_key": ""},
    ],
    "orders": [
        {"name": "order_id", "column_key": "PRI"},
        {"name": "customer_id", "column_key": ""},
        {"name": "amount", "column_key": ""},
    ],
}
used2 = advisor.collect_used_columns(q2, tables, cols2)
assert "email" in used2["customers"]
assert "customer_id" in used2["orders"]

year_tips = [t["title"] for t in advisor.rewrite_tips(
    "SELECT order_id FROM orders WHERE YEAR(order_date) = 2024"
)]
assert any("YEAR" in t for t in year_tips)

like_tips = [t["title"] for t in advisor.rewrite_tips(
    "SELECT * FROM customers WHERE full_name LIKE '%ahmet%'"
)]
assert any("LIKE" in t for t in like_tips)

cols = {
    "orders": [
        {"name": "status", "column_key": ""},
        {"name": "order_id", "column_key": "PRI"},
        {"name": "order_date", "column_key": ""},
    ]
}
used = advisor.collect_used_columns(q, [("orders", "orders")], cols)
assert "status" in used["orders"]
sug, already = advisor.build_index_suggestions(
    used,
    {"orders": [{"name": "PRIMARY", "unique": True, "columns": ["order_id"]}]},
    cols,
)
assert sug and sug[0]["columns"] == ["status"]

year_q = "SELECT order_id FROM orders WHERE YEAR(order_date) = 2024"
year_used = advisor.collect_used_columns(
    year_q,
    [("orders", "orders")],
    cols,
)
assert "order_date" in year_used.get("orders", {})
year_rw = advisor.rewrite_year_predicate(year_q)
assert year_rw and "2024-01-01" in year_rw and "2025-01-01" in year_rw
assert "YEAR" not in year_rw.upper() or "YEAR(" not in year_rw.upper()

star_rw = advisor.expand_select_star(
    q,
    [("orders", "orders")],
    cols,
)
assert star_rw and "FROM" in star_rw.upper()
assert "*" not in star_rw.split("FROM")[0]
assert "status" in star_rw


hinted = advisor.inject_index_hint(q, "orders", "idx_orders_status")
assert hinted and "FORCE INDEX" in hinted and "WHERE" in hinted.upper()

print("parser checks ok")
print("suggestion:", sug[0]["sql"])
print("year rewrite:", year_rw)
print("hint:", hinted)
