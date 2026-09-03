# MySQL Tuning Wizard App

Internship project. Small Flask app: connect to MySQL, paste a query, look at EXPLAIN, get index/statistics tips.


## Setup

1. Install **MySQL 8** if you don't have it yet (the `mysql` command is not on this machine right now):
   - https://dev.mysql.com/downloads/installer/  (choose "MySQL Installer for Windows")
   - Server + set a root password, port 3306
   - Start the MySQL Windows service
2. Python 3.11+ (already used 3.11 here).

```
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python seed_demo.py --user root --password YOURPASS
python app.py
```

Open http://127.0.0.1:5000

`seed_demo.py` creates database `tuning_demo` with customers/orders and **no extra indexes**, so full scans show up. Sakila is ok too, but it is already indexed so the advisor often has nothing to add.

## What it does right now

- Connection form + test
- Execution plan: `EXPLAIN`, estimated JSON cost, flags for ALL / filesort / temp table / unused keys; optional `EXPLAIN ANALYZE`
- Query profile: optional execute SELECT, wall time + `SHOW PROFILE` when the server allows it
- Index suggestions (hypothetical until you create one from the menu)
- Table stats + stale/missing checks; column histograms; optional `ANALYZE TABLE` / `UPDATE HISTOGRAM`
- Rewrites with copy-paste SQL (SELECT *, YEAR(col), ...) and FORCE INDEX hints


## Files

- `app.py` — Flask routes
- `db_utils.py` — connection, EXPLAIN, stats, profiling
- `advisor.py` — parse query, suggest indexes, rewrites, hints
- `schema.sql` / `seed_demo.py` — demo data
- `sample_queries.sql` — same examples as the dropdown
