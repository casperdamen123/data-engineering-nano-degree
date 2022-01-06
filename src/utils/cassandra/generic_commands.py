from cassandra.cluster import Cluster
from cassandra.metadata import TableExtensionInterface
from typing import List, Tuple

class CassandraCommands:
    
    def __init__(self):
        print('CassandraCommands class initiated')
    
    # Connect to Cassandra cluster
    def connect(self, ip_address: str):

        """
        Args:
            ip_address (str): IP address to use to connect to cassandra database
        Returns:
            None: The class now contains the connection via cassandra.connect
        """

        try: 
            self.cluster = Cluster([ip_address])
            self.session = self.cluster.connect()
            print(f'Connection setup with cassandra at {ip_address}')
        except Exception as e:
            print(e)
    
    # Create keyspace
    def create_keyspace(self, name: str):
    
        """
        Args:
            name (str): Name for the workspace to be created
        Returns:
            None: Sets up a keyspace to execute your queries
        """

        try:
            self.session.execute(f"""
                                 CREATE KEYSPACE IF NOT EXISTS {name}
                                 WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}
                                 """
            )
            print(f'Keyspace named {name} created')
        except Exception as e:
            print(e)

    # Connect to keyspace
    def connect_keyspace(self, name: str):

        """
        Args:
            name (str): Name for the workspace to connect to
        Returns:
            None: Connects your cassandra session to the defined workspace
        """

        try:
            self.session.set_keyspace(name)
            print(f'Connected to keyspace: {name}')
        except Exception as e:
            print(e)

    # Create table in database
    def create_table(self, table_name: str, schema: str):

        """
        Args:
            table_name (str): Name of the table to be generated
            schema (str): Name of the columns together with datatype like '(artist varchar, year int)'
        Returns:
            None: Creates a table in Cassandra database
        """

        try:
            assert type(schema) == str
        except AssertionError:
            print(f"Please use string type for schema like '(artist varchar, year int)'")

        try: 
            self.session.execute(f"CREATE TABLE IF NOT EXISTS {table_name} {schema}")
            print(f"Table named {table_name} created")
        except Exception as e: 
            print("Error: Issue creating table")
            print(e)

    # Drop table
    def drop_table(self, table_name: str):

        """
        Args:
            table_name (str): Name of the table to delete
        Returns:
            None: Deletes a table in the Cassandra database
        """
        try: 
            self.session.execute(f"DROP table {table_name}")
            print(f"Dropped table {table_name}")
        except Exception as e: 
            print(e)

    # Insert rows to table
    def insert_rows(self, table_name: str, columns: str, rows: List[Tuple]):

        """
        Args:
            table_name (str): Name of the table for row insertion
            columns (str): Names of the columns for which to insert rows like '(artist, year)'
            rows: list[tuple]: List of values to be inserted in table like [('The Beatles', '1987'), ('The Beatles', '1654')]
        Returns:
            None: Insert rows in the specified table
        """

        value_placeholders = ('%s,' * len(columns.split(','))).rstrip(',')

        for row in rows:
            try: 
                self.session.execute(f"INSERT INTO {table_name} {columns}  \
                                       VALUES ({value_placeholders})",
                                     row
                )
            except Exception as e: 
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
            rows = self.session.execute(f"SELECT * FROM {table_name};")
        except Exception as e: 
            print("Error: SELECT *")
            print(e)

        for row in rows:
            print(row)

    # Custom query
    def custom_query(self, query: str):
        
        """
        Args:
            query (str): The query you want to execute
        Returns:
            list: Containing the output of the query
        """

        results = []

        try:
            rows = self.session.execute(query)
            for row in rows:
                results.append(row)
        except Exception as e:
            print(e)

        return results  
    
    def close_connection(self):

        """
        Args:
            None
        Returns:
            None: Closes the cluster and session 
        """

        try:
            self.session.shutdown()
            print("Closed session")
            self.cluster.shutdown()
            print("Closed cluster connection")
        except Exception as e:
            print(e)