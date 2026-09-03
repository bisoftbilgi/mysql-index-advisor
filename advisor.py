# query parsing + index ideas + a few rewrite checks
# parser is regex 

import re

SQL_KEYWORDS = {
    "on", "where", "set", "left", "right", "inner", "outer", "join", "group",
    "order", "limit", "having", "straight_join", "as", "select", "from",
    "update", "delete", "into", "values", "union", "and", "or", "not",
    "cross", "natural", "using", "force", "use", "ignore", "index", "key",
    "for", "by", "distinct", "all", "offset", "fetch",
}


def _strip(sql):
    sql = (sql or "").strip()
    if sql.endswith(";"):
        sql = sql[:-1].strip()
    return sql


def cut_section(sql, start_kw, end_kws):
    m = re.search(r"\b" + start_kw + r"\b", sql, re.I)
    if not m:
        return ""
    rest = sql[m.end():]
    end = re.search(r"\b(?:" + "|".join(end_kws) + r")\b", rest, re.I)
    if end:
        return rest[: end.start()]
    return rest


def extract_tables(sql):
    """list of (table, alias)"""
    sql = _strip(sql)
    found = []
    seen = set()
    pattern = re.compile(
        r"\b(?:FROM|JOIN|UPDATE|INTO)\s+`?([A-Za-z_][\w]*)`?"
        r"(?:\s+(?:AS\s+)?`?([A-Za-z_][\w]*)`?)?",
        re.I,
    )
    for m in pattern.finditer(sql):
        table = m.group(1)
        alias = m.group(2)
        if alias and alias.lower() in SQL_KEYWORDS:
            alias = None
        alias = alias or table
        key = (table.lower(), alias.lower())
        if key in seen:
            continue
        seen.add(key)
        found.append((table, alias))
    return found


def _join_on_chunks(sql):
    chunks = []
    for m in re.finditer(r"\bON\b", sql, re.I):
        rest = sql[m.end():]
        end = re.search(
            r"\b(?:INNER|LEFT|RIGHT|CROSS|JOIN|WHERE|GROUP|ORDER|LIMIT|HAVING|UNION)\b",
            rest,
            re.I,
        )
        chunks.append(rest[: end.start()] if end else rest)
    return chunks


def predicate_sections(sql):
    sql = _strip(sql)
    return {
        "where": cut_section(sql, "WHERE", ["GROUP", "ORDER", "LIMIT", "HAVING", "UNION", "FOR", "OFFSET"]),
        "order": cut_section(sql, "ORDER\\s+BY", ["LIMIT", "OFFSET", "FOR", "UNION"]),
        "group": cut_section(sql, "GROUP\\s+BY", ["HAVING", "ORDER", "LIMIT", "UNION", "FOR"]),
        "on": " ".join(_join_on_chunks(sql)),
    }


def _col_names_for_table(table, alias, columns):
    names = []
    for col in columns:
        c = col["name"]
        names.append((c, c))
        names.append((f"{table}.{c}", c))
        if alias.lower() != table.lower():
            names.append((f"{alias}.{c}", c))
    # longer first so customer_id wins over id if both existed
    names.sort(key=lambda x: len(x[0]), reverse=True)
    return names


_OP_RE = r"(=|<>|!=|<=>|<=|>=|<|>|LIKE\b|BETWEEN\b|IN\b|IS\b)"


def classify_column_use(section, col_patterns, kind_hint=None):
    """
    col_patterns: list of (text, real_col)
    returns dict real_col -> set of kinds: eq, range, sort
    """
    used = {}
    if not section:
        return used
    for text, real in col_patterns:
        pat = r"(?<![\w.])" + re.escape(text) + r"\s*" + _OP_RE
        for m in re.finditer(pat, section, re.I):
            op = m.group(1).upper()
            kind = "eq"
            if op in ("<", ">", "<=", ">=", "BETWEEN", "<>", "!="):
                kind = "range"
            elif op == "LIKE":
                after = section[m.end():].lstrip()
                # leading wildcard cannot use a normal btree well
                if after.startswith("'%") or after.startswith("\"%"):
                    kind = "skip"
                else:
                    kind = "range"
            elif op == "IN":
                kind = "eq"
            if kind == "skip":
                continue
            used.setdefault(real, set()).add(kind)
        wrap = re.search(
            r"\b(?:YEAR|MONTH|DAY|DATE|HOUR|LOWER|UPPER|TRIM|SUBSTRING|CONVERT)\s*\(\s*"
            + re.escape(text)
            + r"\s*\)",
            section,
            re.I,
        )
        if wrap:
            used.setdefault(real, set()).add("range")
        if kind_hint:
            # ORDER BY / GROUP BY: column listed without needing an operator
            bare = re.search(r"(?<![\w.])" + re.escape(text) + r"(?![\w.])", section, re.I)
            if bare:
                used.setdefault(real, set()).add(kind_hint)
    return used


def collect_used_columns(sql, table_map, columns_by_table):
    """
    table_map: list of (table, alias)
    returns {table: {col: set(kinds)}}
    """
    parts = predicate_sections(sql)
    result = {}
    for table, alias in table_map:
        cols = columns_by_table.get(table) or columns_by_table.get(table.lower()) or []
        # information_schema on windows can be lowercase depending on lower_case_table_names
        if not cols:
            for k, v in columns_by_table.items():
                if k.lower() == table.lower():
                    cols = v
                    table = k
                    break
        patterns = _col_names_for_table(table, alias, cols)
        merged = {}
        for section, hint in (
            (parts["on"], None),
            (parts["where"], None),
            (parts["order"], "sort"),
            (parts["group"], "sort"),
        ):
            piece = classify_column_use(section, patterns, kind_hint=hint)
            for col, kinds in piece.items():
                merged.setdefault(col, set()).update(kinds)
        if merged:
            result[table] = merged
    return result


def _index_covers(existing_indexes, suggested):
    for idx in existing_indexes or []:
        cols = [c.lower() for c in idx["columns"]]
        want = [c.lower() for c in suggested]
        if cols[: len(want)] == want:
            return idx["name"]
        # single-col request already leftmost of something
        if len(want) == 1 and cols and cols[0] == want[0]:
            return idx["name"]
    return None


def build_index_suggestions(used_by_table, indexes_by_table, columns_by_table):
    suggestions = []
    already = []
    for table, used in used_by_table.items():
        real_table = table
        idx_list = indexes_by_table.get(table)
        col_meta = columns_by_table.get(table)
        if idx_list is None or col_meta is None:
            for k in indexes_by_table:
                if k.lower() == table.lower():
                    real_table = k
                    idx_list = indexes_by_table[k]
                    col_meta = columns_by_table.get(k, [])
                    break
        idx_list = idx_list or []
        col_meta = col_meta or []
        pk_cols = {c["name"].lower() for c in col_meta if (c.get("column_key") or "").upper() == "PRI"}

        eq, rng, srt = [], [], []
        for col, kinds in used.items():
            if col.lower() in pk_cols and kinds <= {"eq", "sort"}:
                continue
            if "eq" in kinds:
                eq.append(col)
            elif "range" in kinds:
                rng.append(col)
            elif "sort" in kinds:
                srt.append(col)

        # keep order stable / unique
        def uniq(seq):
            out = []
            seen = set()
            for x in seq:
                xl = x.lower()
                if xl in seen:
                    continue
                seen.add(xl)
                out.append(x)
            return out

        eq, rng, srt = uniq(eq), uniq(rng), uniq(srt)

        # classic mysql: equality cols, then one range, else order by cols
        suggested_cols = list(eq)
        if rng:
            suggested_cols.extend(rng[:1])
        elif srt:
            suggested_cols.extend(srt)

        if not suggested_cols:
            continue

        cover = _index_covers(idx_list, suggested_cols)
        if cover:
            already.append({
                "table": real_table,
                "columns": suggested_cols,
                "index_name": cover,
                "note": (
                    f"`{real_table}` already has index `{cover}` that starts with "
                    f"({', '.join(suggested_cols)})."
                ),
            })
            continue

        idx_name = "idx_" + real_table + "_" + "_".join(suggested_cols)
        idx_name = re.sub(r"[^A-Za-z0-9_]", "_", idx_name)[:64]
        col_sql = ", ".join(f"`{c}`" for c in suggested_cols)
        ddl = f"CREATE INDEX `{idx_name}` ON `{real_table}` ({col_sql})"
        why = []
        if eq:
            why.append("equality: " + ", ".join(eq))
        if rng:
            why.append("range: " + ", ".join(rng[:1]))
        if srt and not rng:
            why.append("order/group: " + ", ".join(srt))
        suggestions.append({
            "table": real_table,
            "columns": suggested_cols,
            "index_name": idx_name,
            "sql": ddl,
            "label": f"{real_table} ({', '.join(suggested_cols)})",
            "why": "; ".join(why) if why else "used in the query",
            "hypothetical": True,
        })
    return suggestions, already



def _table_cols(columns_by_table, table):
    if not columns_by_table:
        return []
    if table in columns_by_table:
        return columns_by_table[table]
    for k, v in columns_by_table.items():
        if k.lower() == table.lower():
            return v
    return []


def expand_select_star(sql, tables, columns_by_table):
    sql_s = _strip(sql)
    if not re.search(r"\bSELECT\s+\*", sql_s, re.I):
        return None
    if not tables or not columns_by_table:
        return None
    pieces = []
    for table, alias in tables:
        cols = _table_cols(columns_by_table, table)
        if not cols:
            continue
        qualify = alias
        for c in cols:
            pieces.append(f"`{qualify}`.`{c['name']}`")
    if not pieces:
        return None
    return re.sub(r"\bSELECT\s+\*", "SELECT " + ", ".join(pieces), sql_s, count=1, flags=re.I)


def rewrite_year_predicate(sql):
    sql_s = _strip(sql)

    def repl(m):
        col = m.group(1)
        year = int(m.group(2))
        return f"{col} >= '{year}-01-01' AND {col} < '{year + 1}-01-01'"

    new, n = re.subn(
        r"YEAR\s*\(\s*`?([A-Za-z_][\w]*)`?\s*\)\s*=\s*(\d{4})",
        repl,
        sql_s,
        flags=re.I,
    )
    return new if n else None


def rewrite_lower_eq(sql):
    sql_s = _strip(sql)

    def repl(m):
        col = m.group(1)
        lit = m.group(2)
        return f"{col} = {lit}"

    new, n = re.subn(
        r"(?:LOWER|UPPER)\s*\(\s*`?([A-Za-z_][\w]*)`?\s*\)\s*=\s*(['\"][^'\"]*['\"])",
        repl,
        sql_s,
        flags=re.I,
    )
    return new if n else None


def inject_index_hint(sql, table, index_name):
    sql_s = _strip(sql)
    pat = (
        r"(\b(?:FROM|JOIN)\s+)`?" + re.escape(table) + r"`?"
        r"(?:\s+(?:AS\s+)?"
        r"(?!(?:WHERE|JOIN|INNER|LEFT|RIGHT|ON|GROUP|ORDER|LIMIT|HAVING|SET|UNION|FORCE|USE|IGNORE)\b)"
        r"`?([A-Za-z_][\w]*)`?)?"
    )

    def repl(m):
        alias = m.group(2)
        if alias:
            return f"{m.group(1)}`{table}` {alias} FORCE INDEX (`{index_name}`)"
        return f"{m.group(1)}`{table}` FORCE INDEX (`{index_name}`)"

    new, n = re.subn(pat, repl, sql_s, count=1, flags=re.I)
    return new if n else None


def optimizer_hints(sql, explain_rows, suggestions, already=None, tables=None):
    hints = []
    seen = set()

    def real_table(name):
        n = (name or "").strip()
        for table, alias in tables or []:
            if n.lower() == table.lower() or n.lower() == alias.lower():
                return table
        return n

    for row in explain_rows or []:
        possible = str(row.get("possible_keys") or "").strip()
        key = str(row.get("key") or "").strip()
        table = real_table(str(row.get("table") or "").strip())
        if possible and not key and table:
            first_idx = possible.split(",")[0].strip()
            hinted = inject_index_hint(sql, table, first_idx)
            sig = ("force", table, first_idx)
            if hinted and sig not in seen:
                seen.add(sig)
                hints.append({
                    "title": f"FORCE INDEX on `{table}`",
                    "detail": (
                        f"EXPLAIN has possible_keys={possible} but key is empty. "
                        f"This hint asks the optimizer to use `{first_idx}`. "
                        "Only keep it if EXPLAIN gets better; hints can also make things worse."
                    ),
                    "sql": hinted,
                })
    for a in already or []:
        name = a.get("index_name")
        table = a.get("table")
        if not name or name.upper() == "PRIMARY":
            continue
        hinted = inject_index_hint(sql, table, name)
        sig = ("use-existing", table, name)
        if hinted and sig not in seen:
            seen.add(sig)
            hints.append({
                "title": f"USE / FORCE INDEX `{name}`",
                "detail": f"`{table}` already has `{name}`. If the plan ignores it, this hint can force a lookup.",
                "sql": hinted,
            })
    for s in suggestions or []:
        hinted = inject_index_hint(sql, s["table"], s["index_name"])
        if not hinted:
            continue
        hints.append({
            "title": f"Hypothetical: after `{s['index_name']}` exists",
            "detail": (
                "MySQL cannot test an index that is not created yet. "
                "This is the same query with FORCE INDEX, for after you create it from the menu."
            ),
            "sql": hinted,
        })
    return hints


def rewrite_tips(sql, columns_by_table=None, tables=None):
    sql_s = _strip(sql)
    tips = []

    star_sql = expand_select_star(sql_s, tables or [], columns_by_table or {})
    if re.search(r"\bSELECT\s+\*", sql_s, re.I):
        tips.append({
            "title": "SELECT *",
            "detail": (
                "Pulling every column makes the row wider than you need. "
                "It can also stop covering indexes from being used. List only the columns you use."
            ),
            "rewrite": star_sql,
        })

    year_sql = rewrite_year_predicate(sql_s)
    for m in re.finditer(
        r"\b(YEAR|MONTH|DAY|DATE|HOUR|LOWER|UPPER|TRIM|SUBSTRING|CONVERT)\s*\(\s*`?([A-Za-z_][\w]*)`?",
        sql_s,
        re.I,
    ):
        fn = m.group(1).upper()
        col = m.group(2)
        extra = ""
        rewrite = None
        if fn == "YEAR":
            extra = (
                f" Rewrite YEAR({col}) = 2024 as a range so an index on {col} can be used."
            )
            rewrite = year_sql
        elif fn in ("LOWER", "UPPER"):
            rewrite = rewrite_lower_eq(sql_s)
            extra = " Compare the column directly if collation already handles case."
        tips.append({
            "title": f"Function on column ({fn}({col}))",
            "detail": (
                "Wrapping the column in a function usually blocks the index. "
                "Apply functions to the constant side if you can."
                + extra
            ),
            "rewrite": rewrite,
        })

    if re.search(r"LIKE\s+'%", sql_s, re.I) or re.search(r'LIKE\s+"%', sql_s, re.I):
        tips.append({
            "title": "LIKE with leading wildcard",
            "detail": (
                "LIKE '%text' cannot use a normal BTREE index. "
                "FULLTEXT (MATCH ... AGAINST) is the usual alternative if you really need contains-search."
            ),
            "rewrite": None,
        })

    if re.search(r"\bORDER\s+BY\s+RAND\s*\(", sql_s, re.I):
        tips.append({
            "title": "ORDER BY RAND()",
            "detail": "This sorts the whole result randomly. Expensive on large tables. Avoid it if you can.",
            "rewrite": None,
        })

    if re.search(r"\bSELECT\s+DISTINCT\b", sql_s, re.I):
        tips.append({
            "title": "DISTINCT",
            "detail": (
                "DISTINCT can force a temp table. If you only need unique ids, "
                "a GROUP BY on a keyed column or EXISTS is sometimes cheaper."
            ),
            "rewrite": None,
        })

    where = cut_section(sql_s, "WHERE", ["GROUP", "ORDER", "LIMIT", "HAVING", "UNION", "FOR"])
    if where and re.search(r"\bOR\b", where, re.I):
        tips.append({
            "title": "OR in WHERE",
            "detail": (
                "OR can stop the optimizer from using one index. "
                "Sometimes UNION of two indexed queries is faster. Depends on the columns."
            ),
            "rewrite": None,
        })

    if re.search(r"\bNOT\s+IN\s*\(", sql_s, re.I):
        tips.append({
            "title": "NOT IN (...)",
            "detail": (
                "NOT IN with a subquery is often slower than NOT EXISTS / LEFT JOIN ... IS NULL. "
                "Also watch for NULLs."
            ),
            "rewrite": None,
        })

    return tips


def load_schema_bits(cur, schema, sql):
    tables = extract_tables(sql)
    table_names = []
    seen = set()
    for t, _alias in tables:
        if t.lower() not in seen:
            seen.add(t.lower())
            table_names.append(t)
    if not table_names:
        return tables, {}, {}
    from db_utils import fetch_columns, fetch_indexes
    cols = fetch_columns(cur, schema, table_names)
    idxs = fetch_indexes(cur, schema, table_names)
    return tables, cols, idxs


def advise_indexes(cur, schema, sql):
    tables, cols, idxs = load_schema_bits(cur, schema, sql)
    used = collect_used_columns(sql, tables, cols)
    suggestions, already = build_index_suggestions(used, idxs, cols)
    return {
        "tables": tables,
        "columns": cols,
        "indexes": idxs,
        "used": {t: {c: sorted(list(k)) for c, k in u.items()} for t, u in used.items()},
        "suggestions": suggestions,
        "already": already,
    }
