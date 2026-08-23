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
print("parser checks ok")
print("suggestion:", sug[0]["sql"])
