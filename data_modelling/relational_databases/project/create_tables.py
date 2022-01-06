import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    
    """
    Args: None
    Returns:
        cur (psycopg2.connect.cursor): Cursor to execute queries on sparkifydb
        conn (psycopg2.connect): Connection to sparkifydb
    """
    
    # Connect to default database
    try:
        conn = psycopg2.connect("host=localhost port=5432 dbname=nano_data_engineering_db user=postgres password=feyenoord")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
    except psycopg2.Error as e:
        print(e)
    
    # Create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.Error as e:
        print(e)

    # Close connection to default database
    try:
        conn.close()
    except psycopg2.Error as e:
        print(e)  
    
    # Connect to sparkify database
    try:
        conn = psycopg2.connect("host=localhost port=5432 dbname=sparkifydb user=postgres password=feyenoord")
        cur = conn.cursor()
    except psycopg2.Error as e:
        print(e)
    
    return cur, conn


def drop_tables(cur, conn):
    
    """
    Args: 
        cur (psycopg2.connect.cursor): Cursor to execute queries on sparkifydb
        conn (psycopg2.connect): Connection to sparkifydb
    Returns:
        None: Drops each table in the database using the queries in `drop_table_queries` list
    """
    
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e)

def create_tables(cur, conn):
    
    """
    Args: 
        cur (psycopg2.connect.cursor): Cursor to execute queries on sparkifydb
        conn (psycopg2.connect): Connection to sparkifydb
    Returns:
        None: Creates each table in the database using the queries in `create_table_queries` list.
    """

    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e)

def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()