import psycopg2
from typing import List, Tuple

class PostgresCommands:
    
    def __init__(self):
        print('PostgresCommands class initiated')

    # Connect to database and create cursor to execute queries
    def connect(self, host: str, database_name: str, port: str, user: str, password: str, autocommit: bool):

        """
        Args:
            database_name (str): Name of the Postgres database
            host (str): Host or IP to use (defaults to localhost)
            port (str): Port number
            user (str): Username to sign in to database
            password (str): Password to sign in to database
            autocommit (bool): Whether autocommit should be active (True) or inactive (False)

        Returns:
            None: The class now contains the connection via PostgresCommands.conn
        """

        self.database_name = database_name
        self.host = host 
        self.port = port
        self.user = user
        self.password = password

        try: 
            self.conn = psycopg2.connect(f"host={self.host} port={self.port} dbname={self.database_name} user={self.user} password={self.password}")
            print(f'Connection established with {self.database_name}')
        except psycopg2.Error as e: 
            print("Error: Could not make connection to the Postgres database")
            print(e)

        try: 
            self.cur = self.conn.cursor()
        except psycopg2.Error as e: 
            print("Error: Could not get cursor to the Database")
            print(e)
        
        self.conn.set_session(autocommit=True)
 
    # Create table in database
    def create_table(self, table_name: str, schema: str):

        """
        Args:
            table_name (str): Name of the table to be generated
            schema (str): Name of the columns together with datatype like '(artist varchar, year int)'
        Returns:
            None: Creates a table in the Postgres database
        """

        try:
            assert type(schema) == str
        except AssertionError:
            print(f"Please use string type for schema like '(artist varchar, year int)'")

        try: 
            self.cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} {schema}")
            print(f"Table named {table_name} created")
        except psycopg2.Error as e: 
            print("Error: Issue creating table")
            print(e)
    
    # Drop table
    def drop_table(self, table_name: str):

        """
        Args:
            table_name (str): Name of the table to delete
        Returns:
            None: Deletes a table in the Postgres database
        """
        try: 
            self.cur.execute(f"DROP table {table_name}")
            print(f"Dropped table {table_name}")
        except psycopg2.Error as e: 
            print(e)

    # Insert rows to table
    def insert_rows(self, table_name: str, columns: str, rows: List[Tuple]):

        """
        Args:
            table_name (str): Name of the table for row insertion
            columns (str): Names of the columns for which to insert rows like '(artist, year)'
            rows: list[tuple]: List of values to be inserted in table like [('The Beatles', '1987'), ('The Beatles', '1654')]
        Returns:
            None: Inserts rows in the specified table
        """

        value_placeholders = ('%s,' * len(columns.split(','))).rstrip(',')

        for row in rows:
            try: 
                self.cur.execute(f"INSERT INTO {table_name} {columns}  \
                                   VALUES ({value_placeholders})",
                                  row)
            except psycopg2.Error as e: 
                print(f"Error: Issue inserting row: \n {row}")
                print(e) 

    # Print rows from table
    def print_rows(self, table_name: str):

        """
        Args:
            table_name (str): Name of the table for row insertion
        Returns:
            None: Prints the table rows
        """

        try: 
            self.cur.execute(f"SELECT * FROM {table_name};")
        except psycopg2.Error as e: 
            print("Error: SELECT *")
            print(e)

        row = self.cur.fetchone()
        while row:
            print(row)
            row = self.cur.fetchone() 
    
    # Get information on the available tables
    def get_tables(self, pattern: str):
        
        """
        Args:
            pattern (str): Name or part of the table name to search for in database
        Returns:
            list: Containing the names of all matching tables
        """

        tables = []        
        
        try:
            self.cur.execute('SELECT table_name FROM information_schema.tables')
            for table in self.cur.fetchall():
                if pattern in str(table):
                    tables.append(table)
        except psycopg2.Error as e:
            print('Error: no table information found')

        return tables   

    def custom_query(self, query: str):
        
        """
        Args:
            query (str): The query you want to execute
        Returns:
            list: Containing the output of the query
        """

        results = []

        try:
            self.cur.execute(query)
            for row in self.cur.fetchall():
                results.append(row)
        except psycopg2.Error as e:
            print(e)

        return results  
    
    def close_connection(self):


        """
        Args:
            None
        Returns:
            None: Closes the database cursor and connection
        """

        try:
            self.cur.close()
            print("Closed cursor")
            self.conn.close()
            print("Closed connection")
        except psycopg2.Error as e:
            print(e)