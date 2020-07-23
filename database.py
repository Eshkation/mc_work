# pylint: disable=no-member
import os
import psycopg2
import psycopg2.extras
import re
from typing import Any


class SchemaInstance:
    def __init__(self, database_instance: classmethod, table_name: str) -> None:
        self.main_instance = database_instance    
        self.table_columns = database_instance.tables[table_name]
        self.sql_query = f'ALTER TABLE {table_name}'
        self.table_name = table_name
    
    def BuildColumn(self, column_name, column_type, default_value: Any = None):
        if (column_name not in self.table_columns):
            self.table_columns.append(column_name)
            column_constraints = ' '
            if (default_value is not None):
                column_constraints += f'DEFAULT {repr(default_value)}'

            self.main_instance.cursor.execute(
                f'{self.sql_query} ADD {column_name} {column_type} {column_constraints}')

    def bigInteger(self, column_name: str, default: int = None):
        self.BuildColumn(column_name, f'BIGINT', default)

        return self

    def binary(self, column_name: str, default: int = None):
        self.BuildColumn(column_name, f'NUMBER(1)', default)

        return self
    
    def char(self, column_name: str, size: int, default: str = None):
        self.BuildColumn(column_name, f'CHAR({size})', default)

        return self
    
    def date(self, column_name: str, default: str = None):
        self.BuildColumn(column_name, f'DATE', default)

        return self 

    def decimal(self, column_name: str, precision: int, scale: int, default: float = None):
        self.BuildColumn(column_name, f'NUMERIC({precision}, {scale})', default)

        return self

    def double(self, column_name: str, default: float = None):
        self.BuildColumn(column_name, f'DOUBLE PRECISION', default)

        return self

    def floatn(self, column_name: str, default: float = None):
        self.BuildColumn(column_name, f'FLOAT', default)

        return self

    def increments(self, column_name: str):
        self.BuildColumn(column_name, f'SERIAL')

        return self

    def integer(self, column_name: str, default: int = None):
        self.BuildColumn(column_name, f'INTEGER', default)

        return self

    def json(self, column_name: str, default: dict = None):
        self.BuildColumn(column_name, f'JSON', default)

        return self

    def jsonb(self, column_name: str, default: dict = None):
        self.BuildColumn(column_name, f'JSONB', default)

        return self

    def smallInteger(self, column_name: str, default: int = None):
        self.BuildColumn(column_name, f'SMALLINT', default)

        return self

    def string(self, column_name: str, size: int = None, default: str = None):
        if (size is None):
            self.BuildColumn(column_name, f'VARCHAR', default)
        else:
            self.BuildColumn(column_name, f'VARCHAR({size})', default)

        return self

    def text(self, column_name: str, default: str = None):
        self.BuildColumn(column_name, f'TEXT', default)

        return self

    def time(self, column_name: str, default: str = None):
        self.BuildColumn(column_name, f'TIME', default)

        return self

    def timestamp(self, column_name: str, default: str = None):
        self.BuildColumn(column_name, f'TIMESTAMP', default)

        return self
    
    def nullable(self, column_name: str):
        self.main_instance.cursor.execute(
            f'{self.sql_query} ALTER COLUMN {column_name} DROP NOT NULL;')

        return self
    
    def primary(self, column_name: str):
        constraint = f'{self.table_name}_pk'

        if (type(column_name) is list):
            column_name = ', '.join(column_name)
        
        self.main_instance.cursor.execute(
            f'{self.sql_query} DROP CONSTRAINT IF EXISTS {constraint}')
        self.main_instance.cursor.execute(
            f'{self.sql_query} ADD CONSTRAINT {constraint} PRIMARY KEY ({column_name})')  

        return self  

    def save(self):
        self.main_instance.connection.commit() 


class TableInstance:
    def __init__(self, database_instance: classmethod, table_name: str) -> None:
        self.main_instance = database_instance
        self.where_clause = ''
        self.select_clause = f'SELECT * FROM {table_name}'
        self.table_name = table_name
    
    def get(self):
        sql_query = ' '.join([self.select_clause, self.where_clause])
        self.main_instance.cursor.execute(sql_query)
        return self.main_instance.cursor.fetchone()
    
    def getall(self):
        sql_query = ' '.join([self.select_clause, self.where_clause])
        self.main_instance.cursor.execute(sql_query)
        return self.main_instance.cursor.fetchall()
    
    def insert(self, array: dict) -> None:
        """
        Inserts records into the selected database table. You may insert several records into 
        the table with a single call to insert by passing an list of dict values.
        """
        sql_query = f'INSERT INTO {self.table_name}'
        if (type(array) is list):
            for record in array:
                columns = ', '.join(record.keys())
                values = ', '.join(map(lambda fs: '%s', record.values()))
                self.main_instance.cursor.execute(
                    f'{sql_query} ({columns}) VALUES ({values})', list(record.values()))
        else:
            columns = ', '.join(array.keys())
            values = ', '.join(map(lambda fs: '%s', array.values()))
            self.main_instance.cursor.execute(
                f'{sql_query} ({columns}) VALUES ({values})', list(array.values()))

        self.main_instance.connection.commit()
    
    def updateOrInsert(self, array: dict, conflict: str) -> None:
        """
        Inserts records into the selected database table. You may insert several records into 
        the table with a single call to insert by passing an list of dict values.
        """
        sql_query = f'INSERT INTO {self.table_name}'
        if (type(array) is list):
            for record in array:
                columns = ', '.join(record.keys())
                values = ', '.join(map(lambda fs: '%s', record.values()))
                self.main_instance.cursor.execute(
                    f"""
                        {sql_query} ({columns}) VALUES ({values}) 
                        ON CONFLICT ({conflict}) DO UPDATE
                        SET ({columns}) = ({values}) 
                    """, list(record.values()))
        else:
            columns = ', '.join(array.keys())
            values = ', '.join(map(lambda fs: '%s', array.values()))
            self.main_instance.cursor.execute(
                f"""
                    {sql_query} ({columns}) VALUES ({values}) 
                    ON CONFLICT ({conflict}) DO UPDATE
                    SET ({columns}) = ({values}) 
                """, list(array.values()))

        self.main_instance.connection.commit()
    
    def delete(self):
        """
        Delete records from the selected table. 
        """
        self.main_instance.cursor.execute(f'DELETE FROM {self.table_name} {self.where_clause}')
        self.main_instance.connection.commit()
    
    def drop(self):
        del self.main_instance.tables[self.table_name]

        self.main_instance.cursor.execute(f'DROP TABLE {self.table_name}')
        self.main_instance.connection.commit()    

    def select(self, column: str):
        """
        Adds a select clause to the sql operation.
        """
        if (type(column) is list):
            self.select_clause = f'SELECT ({", ".join(column)}) FROM {self.table_name}'

        else:
            self.select_clause = f'SELECT {column} FROM {self.table_name}'

        return self
    
    def truncate(self):
        """
        Truncates the selected table.
        """
        self.main_instance.cursor.execute(f'TRUNCATE {self.table_name}')
        self.main_instance.connection.commit()
    
    def update(self, array: dict):
        update_query = []
        update_values = []
        for column, value in array.items():
            update_query.append(f'{column} = %s')
            update_values.append(value)

        update_query = ', '.join(update_query)

        self.main_instance.cursor.execute(
            f'UPDATE {self.table_name} SET {update_query} {self.where_clause}', update_values)

        self.main_instance.connection.commit()

    def where(self, column: str, operator: str = '=', value: str = ''):
        """
        Applies a where clause to the SQL operation. 
        """

        if (type(column) is list):
            # more than one where clauses were served
            top_clause = column.pop(0)
            self.where_clause = f'WHERE {top_clause[0]} {top_clause[1]} {repr(top_clause[2])}'
            for clause in column:
                self.where_clause += f' AND {clause[0]} {clause[1]} {repr(clause[2])} '

        else:
            self.where_clause = f'WHERE {column} {operator} {repr(value)}'
        
        return self


class Client:
    def __init__(self, database_url: str, ssl_mode: str = 'require') -> None:
        self.connection = psycopg2.connect(database_url, sslmode=ssl_mode)
        self.cursor = self.connection.cursor(
            cursor_factory = psycopg2.extras.NamedTupleCursor
        )

        self.tables = {}
        self.LoadDatabaseTables()
    
    def LoadDatabaseTables(self) -> None:
        """
        Loads and stores all tables schemas in the database.
        """
        self.cursor.execute("""
            SELECT 
                table_name 
            FROM 
                INFORMATION_SCHEMA.TABLES 
            WHERE
                table_schema = 'public' """)

        for table in self.cursor.fetchall():
            self.tables[table.table_name] = []
            self.cursor.execute(f"""
                SELECT
                    column_name
                FROM
                    INFORMATION_SCHEMA.COLUMNS
                WHERE
                    table_name = '{table.table_name}' """)
            for column in self.cursor.fetchall():
                self.tables[table.table_name].append(column.column_name)
    
    def MakeTable(self, table_name):
        self.cursor.execute(f"""
                CREATE TABLE 
                    {table_name} (dummy INTEGER NOT NULL PRIMARY KEY);

                ALTER TABLE 
                    {table_name} 

                DROP COLUMN
                    dummy;
            """)
        self.connection.commit()
        self.LoadDatabaseTables()

    def schema(self, table_name: str):
        """
        The Schema function provides a database agnostic way of manipulating tables.
        Parameters
        ----------
        table_name: str
            The name of the table to be worked on.
        """

        if (table_name not in self.tables):
            # table does not exist, a new one that is blank will be generated
            self.MakeTable(table_name)
        
        return SchemaInstance(self, table_name)

    def table(self, table_name: str):
        """
        Selects a database table. If a table with the given name is not found, a new blank table will be generated.
        Parameters
        ----------
        table_name: str
            The name of the table to be returned.
        """

        if (table_name not in self.tables):
            # table does not exist, a new one that is blank will be generated
            self.MakeTable(table_name)
        
        return TableInstance(self, table_name)
