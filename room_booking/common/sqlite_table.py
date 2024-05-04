from abc import ABC, abstractmethod
from typing import Dict, Callable, List
import os
import sqlite3
import pandas as pd
from datetime import datetime
import logging
from room_booking import DATABASE_PATH

log = logging.getLogger(__name__)


def sqlite3_process(func: Callable) -> Callable:
    """
    Decorator to manage SQLite database connection.
    """
    def func_wrapper(self, *args, **kwargs):
        # Connect to the SQLite database
        conn = sqlite3.connect(f"{DATABASE_PATH}/{self.DEFAULT_PATH}.db")
        cursor = conn.cursor()

        # Execute the wrapped function
        output = func(self, cursor, *args, **kwargs)

        # Commit changes and close the connection
        conn.commit()
        conn.close()
        return output
    return func_wrapper


class BuildDatabase(ABC):
    """
    Abstract base class for building a SQLite database.
    """

    DEFAULT_PATH: str = None
    DEFAULT_SCHEMA: Dict[str, str] = None

    def __init__(self):
        """
        Initialize the BuildDatabase class.
        """
        if not self.DEFAULT_PATH or not self.DEFAULT_SCHEMA:
            raise ValueError("Both DEFAULT_PATH and DEFAULT_SCHEMA must be implemented in the inheriting child class!")
        self.db_name = self.DEFAULT_PATH

    @staticmethod
    def _validate_headers(headers: List[str], schema: Dict[str, str]) -> None:
        """
        Validate column headers against the schema.
        """
        mismatched_headers = [
            header for header in headers if header.lower() not in map(
                str.lower, schema.keys())]
        if mismatched_headers:
            raise ValueError(
                f"The following column(s) in the imported file do not match the DEFAULT_SCHEMA: {', '.join(mismatched_headers)}")

    @property
    def load_date(self) -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @abstractmethod
    def format_ingested_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Abstract method to format the ingested DataFrame.

        Parameters:
            df (pd.DataFrame): The DataFrame to be formatted.

        Returns:
            pd.DataFrame: The formatted DataFrame.
        """
        pass

    @sqlite3_process
    def create_database_from_file(self, cursor, file_path: str) -> None:
        """
        Create a database from a CSV or Excel file.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError("File does not exist.")

        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        else:
            raise TypeError("Unsupported file format.")

        df['LOAD_DATE'] = self.load_date
        headers = df.columns.tolist()
        self._validate_headers(headers, self.DEFAULT_SCHEMA)

        # Format date columns
        for col in df.columns:
            if self.DEFAULT_SCHEMA.get(col.upper()) == 'DATE':
                df[col] = pd.to_datetime(
                    df[col], format='mixed', dayfirst=True).dt.strftime(
                        '%Y-%m-%d %H:%M')

        df = self.format_ingested_dataframe(df=df)
        for index, row in df.iterrows():
            insert_query = f"INSERT INTO {self.db_name} ({', '.join(headers)}) VALUES ({', '.join(['?' for _ in range(len(headers))])})"
            cursor.execute(insert_query, tuple(row))


class SqliteDatabase(BuildDatabase, ABC):
    """
    Class for managing SQLite database operations.
    """

    DEFAULT_PATH: str = None
    DEFAULT_SCHEMA: Dict[str, str] = None

    def __init__(self):
        """
        Initialize the SqliteDatabase class.
        """
        if not self.DEFAULT_PATH or not self.DEFAULT_SCHEMA:
            raise ValueError("Both DEFAULT_PATH and DEFAULT_SCHEMA must be implemented in the inheriting child class!")

        self.db_name = self.DEFAULT_PATH

    @property
    def database(self) -> str:
        """
        Get the full path of the database file.
        """
        db_path = f"{DATABASE_PATH}/{self.DEFAULT_PATH}.db"
        if not os.path.exists(db_path):
            raise FileNotFoundError(
                f"Database file '{db_path}' does not exist, please create first!")
        return db_path

    @property
    def conn(self) -> sqlite3.Connection:
        """
        Establish a connection to the SQLite database.
        """
        return sqlite3.connect(self.database)

    @sqlite3_process
    def create_table(self, cursor: sqlite3.Cursor) -> None:
        """
        Create a table in the database.
        """
        columns = ', '.join(
            [f'{column_name} {column_type}' for column_name, column_type in self.DEFAULT_SCHEMA.items()])
        create_table_query = f'CREATE TABLE IF NOT EXISTS {self.db_name} ({columns})'
        cursor.execute(create_table_query)
        log.info(f"Table {self.db_name} created successfully!")

    @sqlite3_process
    def get_columns(self, cursor: sqlite3.Cursor) -> List[str]:
        """
        Retrieve column names from the database.
        """
        cursor.execute(f"PRAGMA table_info({self.db_name})")
        columns_info = cursor.fetchall()
        return [column_info[1] for column_info in columns_info]

    @sqlite3_process
    def execute_query(
        self, cursor: sqlite3.Cursor, query: str
    ) -> pd.DataFrame:
        """
        Execute a query and return results as a DataFrame.
        """
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        return pd.DataFrame(data, columns=columns)

    def format_ingested_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Abstract method to format the ingested DataFrame.

        Parameters:
            df (pd.DataFrame): The DataFrame to be formatted.

        Returns:
            pd.DataFrame: The formatted DataFrame.
        """
        log.debug("No format processes were defined.")
        return df
