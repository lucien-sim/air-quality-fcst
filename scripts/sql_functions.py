#!/Users/Lucien/anaconda/envs/tdi/bin/python

import sqlite3

def create_sql_connection(db_file): 
    try: 
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e: 
        print(e)
    return conn


def execute_sql_command(conn,command): 
    try: 
        c = conn.cursor()
        c.execute(command)
        if command[:6].lower() == 'select': 
            return c.fetchall()
    except sqlite3.Error as e: 
        print(e)
