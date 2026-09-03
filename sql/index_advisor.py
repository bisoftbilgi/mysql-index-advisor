import re
import mysql.connector
from mysql.connector import Error
from datetime import datetime


def fetch_sample_value(connection, table, column):
    """Fetches a sample value from a given table and column."""
    try:
        cursor = connection.cursor()
        sample_query = f"SELECT {column} FROM {table} WHERE {column} IS NOT NULL LIMIT 1"
        cursor.execute(sample_query)
        result = cursor.fetchone()
        return result[0] if result else None
    except Error as e:
        print(f"Error fetching sample value for {table}.{column}: {e}")
        return None

def get_column_data_type(connection, table, column):
    """Retrieves the data type of a given column."""
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = %s AND COLUMN_NAME = %s
            """, (table, column))
        result = cursor.fetchone()
        return result[0] if result else None
    except Error as e:
        print(f"Error fetching data type for {table}.{column}: {e}")
        return None

def replace_query_placeholders(connection, query):
    """Replaces ? placeholders with actual values fetched from the database."""
    try:
        query = query.replace("`", "").replace("'", "")
        query = re.sub(r"\s*\.\s*", ".", query)  # Remove spaces around dots

        placeholders = query.count('?')
        if placeholders == 0:
            return query

        query_parts = query.split('?')
        final_query = query_parts[0]

        for i in range(1, len(query_parts)):
            prev_text = query_parts[i - 1].strip()

            if prev_text.endswith("LIMIT"):
                replacement = "10"
            else:
                match = re.search(r"([a-zA-Z0-9_]+)\s*=\s*$", prev_text)
                if match:
                    column = match.group(1)
                    table_match = re.search(r"FROM\s+([a-zA-Z0-9_]+)", query, re.IGNORECASE)
                    if table_match:
                        table = table_match.group(1)
                        sample_value = fetch_sample_value(connection, table, column)
                        data_type = get_column_data_type(connection, table, column)

                        if sample_value is not None:
                            if data_type in ["char", "varchar", "text"]:
                                replacement = f"'{sample_value}'"
                            else:
                                replacement = str(sample_value)
                        else:
                            replacement = "IS NULL"
                    else:
                        replacement = "IS NULL"
                else:
                    replacement = "IS NULL"

            final_query += replacement + query_parts[i]

        return final_query
    except Error as e:
        print(f"Error replacing query placeholders: {e}")
        return query

def fetch_top_queries(connection, database_name):
    """Fetches the top 10 most time-consuming SELECT queries."""
    query = """
        SELECT DIGEST_TEXT 
        FROM performance_schema.events_statements_summary_by_digest
        WHERE SCHEMA_NAME = %s
        AND UPPER(DIGEST_TEXT) LIKE 'SELECT%%'
        AND DIGEST_TEXT NOT LIKE '%%performance_schema%%'
        AND DIGEST_TEXT NOT LIKE '%%INFORMATION_SCHEMA%%'
        ORDER BY SUM_TIMER_WAIT DESC
        LIMIT 10;
    """
    try:
        cursor = connection.cursor()
        cursor.execute(query, (database_name,))
        return [replace_query_placeholders(connection, row[0]) for row in cursor.fetchall()]
    except Error as e:
        print(f"Error fetching top queries: {e}")
        return []

def is_column_indexed(connection, table, column):
    """Checks if a column is already indexed in the database."""
    try:
        cursor = connection.cursor()
        query = """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_NAME = %s
              AND COLUMN_NAME = %s
        """
        cursor.execute(query, (table, column))
        return cursor.fetchone()[0] > 0
    except Error as e:
        print(f"Index check error: {e}")
        return False

def suggest_missing_indexes(connection, query):
    """Suggests indexes for missing columns in a query."""
    try:
        alias_pattern = r"(?:FROM|JOIN)\s+(\w+)\s+(?:AS\s+)?(\w+)?"
        alias_matches = re.findall(alias_pattern, query, re.IGNORECASE)
        alias_mapping = {alias: table for table, alias in alias_matches if alias}
        alias_mapping.update({table: table for table, alias in alias_matches if not alias})

        column_pattern = r"(\w+)\.(\w+)"
        column_references = re.findall(column_pattern, query)
        resolved_columns = [(alias_mapping.get(table, table), column) for table, column in column_references]

        table_column_map = {}
        for table, column in resolved_columns:
            if table not in table_column_map:
                table_column_map[table] = []
            if column not in table_column_map[table]:  # Avoid duplicate column entries
                table_column_map[table].append(column)

        for table, columns in table_column_map.items():
            missing_columns = [col for col in columns if not is_column_indexed(connection, table, col)]
            if missing_columns:
                timestamp = datetime.now().isoformat(timespec='minutes')
                index_name = f"idx_{table}_" + "_".join(missing_columns)
                print(
                    f"Suggested Index for {table}: CREATE INDEX {index_name}_{timestamp} ON {table}({', '.join(missing_columns)});\n")
    except Error as e:
        print(f"Error suggesting missing indexes: {e}")

def main():
    """database configurations"""
    db_config = {
        "host": "localhost",
        "port": "3309",
        "database": "classicmodels",
        "user": "root",
        "password": "1234",
    }
    """database connection"""
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Fetching top SELECT queries...")
            top_queries = fetch_top_queries(connection, db_config["database"])
            if not top_queries:
                print("No SELECT queries found.")
                return

            for idx, query in enumerate(top_queries, 1):
                print(f"\n=== Query {idx} ===\n{query}\n")
                suggest_missing_indexes(connection, query)

    except Error as e:
        print(f"Database error: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    main()

