"""
Creates tuning_demo and fills it with enough rows

python seed_demo.py --user root --password YOURPASS
"""

import argparse
import getpass
import os
import random
from datetime import date, datetime, timedelta

import mysql.connector

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "schema.sql")

CITIES = [
    "Ankara", "Istanbul", "Izmir", "Bursa", "Antalya",
    "Adana", "Gaziantep", "Konya", "Mersin", "Samsun",
]
STATUSES = ["pending", "paid", "shipped", "cancelled"]
CATEGORIES = ["books", "electronics", "home", "office", "other"]
FIRST = ["Ahmet", "Mehmet", "Ayse", "Fatma", "Ali", "Elif", "Can", "Zeynep", "Emre", "Merve"]
LAST = ["Yilmaz", "Kaya", "Demir", "Celik", "Sahin", "Aydin", "Ozturk", "Arslan", "Dogan", "Kilic"]


def load_schema_statements():
    text = open(SCHEMA_FILE, encoding="utf-8").read()
    stmts = []
    for part in text.split(";"):
        s = part.strip()
        if not s or s.startswith("--"):

            lines = [ln for ln in s.splitlines() if ln.strip() and not ln.strip().startswith("--")]
            s = "\n".join(lines).strip()
        if s:
            stmts.append(s)
    return stmts


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=3306)
    p.add_argument("--user", default="root")
    p.add_argument("--password", default=None)
    p.add_argument("--customers", type=int, default=2500)
    p.add_argument("--orders", type=int, default=9000)
    args = p.parse_args()

    password = args.password
    if password is None:
        password = getpass.getpass("MySQL password (blank if none): ")

    conn = mysql.connector.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=password,
        connection_timeout=8,
        charset="utf8mb4",
        autocommit=False,
    )
    cur = conn.cursor()
    print("Running schema.sql ...")
    for stmt in load_schema_statements():
        cur.execute(stmt)
    conn.commit()

    cur.close()
    conn.database = "tuning_demo"
    cur = conn.cursor()

    rng = random.Random(42)
    n_prod = 80
    products = []
    for i in range(n_prod):
        products.append((
            f"SKU{i:04d}",
            f"Product {i}",
            rng.choice(CATEGORIES),
            round(rng.uniform(5, 400), 2),
        ))
    cur.executemany(
        "INSERT INTO products (sku, product_name, category, price) VALUES (%s, %s, %s, %s)",
        products,
    )

    customers = []
    start = datetime(2023, 1, 1)
    for i in range(1, args.customers + 1):
        name = rng.choice(FIRST) + " " + rng.choice(LAST)
        customers.append((
            f"user{i}@example.com",
            name,
            rng.choice(CITIES),
            start + timedelta(days=rng.randint(0, 600), hours=rng.randint(0, 23)),
        ))
    print(f"Inserting {len(customers)} customers ...")
    cur.executemany(
        "INSERT INTO customers (email, full_name, city, created_at) VALUES (%s, %s, %s, %s)",
        customers,
    )

    orders = []
    d0 = date(2023, 1, 1)
    for i in range(args.orders):
        cid = rng.randint(1, args.customers)
        orders.append((
            cid,
            rng.choice(STATUSES),
            round(rng.uniform(8, 900), 2),
            d0 + timedelta(days=rng.randint(0, 800)),
        ))
    print(f"Inserting {len(orders)} orders ...")
    batch = 1000
    for i in range(0, len(orders), batch):
        cur.executemany(
            "INSERT INTO orders (customer_id, status, amount, order_date) VALUES (%s, %s, %s, %s)",
            orders[i:i + batch],
        )

    n_items = min(args.orders * 2, 15000)
    items = []
    for i in range(n_items):
        items.append((
            rng.randint(1, args.orders),
            rng.randint(1, n_prod),
            rng.randint(1, 5),
        ))
    print(f"Inserting {len(items)} order_items ...")
    for i in range(0, len(items), batch):
        cur.executemany(
            "INSERT INTO order_items (order_id, product_id, qty) VALUES (%s, %s, %s)",
            items[i:i + batch],
        )

    conn.commit()
    cur.execute("SELECT COUNT(*) FROM customers")
    nc = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM orders")
    no = cur.fetchone()[0]
    cur.close()
    conn.close()
    print(f"Done. tuning_demo is ready ({nc} customers, {no} orders).")
    print("Run: python app.py")



if __name__ == "__main__":
    main()
