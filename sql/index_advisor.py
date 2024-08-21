import mysql.connector
import re
import os

def extract_columns_from_filter(filter_text):
    column_pattern = re.compile(r'([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)')
    matches = column_pattern.findall(filter_text)
    return matches

def calculate_cardinality(cursor, table_name, column_name):
    query = "SELECT COUNT(DISTINCT {})/COUNT(*) AS cardinality FROM {}".format(column_name,table_name)
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result else 0

def index_advisor(sql_query, database_ip, database_port, database_name, username, password ):
    conn = mysql.connector.connect(
    host= database_ip,
    port= database_port,
    user= username,
    passwd= password,
    database= database_name
    )
    cursor=conn.cursor()
    
    def find_indexes(table_name):
        query = "SHOW INDEX FROM {}".format(table_name)
        cursor.execute(query)
        result=cursor.fetchall()
        indexes_list=[]
        for comp in result:
            counter=0
            for i in comp:
                counter+=1
                if counter==5:
                    indexes_list.append(i)
        set_indexes=list(set(indexes_list))
        return set_indexes
    
    filter_query = "EXPLAIN FORMAT=tree " + sql_query
    cursor.execute(filter_query)
    result = cursor.fetchall()
    for row in result:
        explain_output = row[0]
        columns = extract_columns_from_filter(explain_output)
    new_columns=[]
    for t,c in columns:
        t_name=t
        new_columns.append(c)
    index_table= find_indexes(t_name)
    new_indexes=[]
    for i in new_columns:
        if not i in index_table:
            new_indexes.append(i)
    
    indexed_columns_with_cardinality = []
    for i in new_indexes:
        cardinality = calculate_cardinality(cursor, t_name, i)
        indexed_columns_with_cardinality.append((i, cardinality))
    
    
    indexed_columns_with_cardinality.sort(key=lambda x: x[1], reverse=True)
    
    for column, cardinality in indexed_columns_with_cardinality:
        hex_str = os.urandom(4).hex()
        print("create index ix_auto_{} on {}({})".format(hex_str, t_name, column))
    

    conn.commit()
    cursor.close()
    conn.close()

sql_query= input("SQL Query:")
database_ip = input("Database ip: ")
database_port= input("Database port: ")
database_name= input("Database name: ")
username= input("Username: ")
password= input("Password: ")

index_advisor(sql_query, database_ip, database_port, database_name, username, password )




