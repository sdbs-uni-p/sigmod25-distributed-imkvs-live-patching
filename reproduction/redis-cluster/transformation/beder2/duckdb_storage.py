from __future__ import annotations

import datetime
import gc
import multiprocessing as mp
import os
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Optional

import duckdb
import pandas as pd
import sqlparse

from future_collector import FutureCollector

STAGING_PREFIX = "tmp_staging_"


class Storage:
    def insert(self, table: str, df_input_data: pd.DataFrame):
        pass


class DuckDBStorage(Storage):
    def __init__(self, database_file: str):
        super().__init__()
        self._database_file = database_file
        self._con: Optional[duckdb.DuckDBPyConnection] = None
        self._tables_creation_order: List[str] = []

    def connect(self, read_only: bool = False) -> DuckDBStorage:
        if self._con is not None:
            raise ValueError(
                "Connection already established. Close current connection before opening another one."
            )
        self._con = duckdb.connect(database=self._database_file, read_only=read_only)
        return self

    def close(self):
        self._con.close()
        self._con = None

    def create_tables(self):
        with open(
            os.path.join(
                os.path.realpath(os.path.dirname(__file__)), "queries/create.sql"
            )
        ) as f:
            sql = f.read()

        # Block to get creation order
        statements = sqlparse.parse(sql)
        for stmt in statements:
            # First identifier is talbe name
            if stmt.get_type() != "CREATE":
                continue
            table_name = [
                token
                for token in stmt.tokens
                if isinstance(token, sqlparse.sql.Identifier)
            ][0]
            self._tables_creation_order.append(table_name.get_name())

        self._con.execute(sql)

    def _get_tables(self) -> List[str]:
        return [tbl[0] for tbl in self._con.execute("SHOW TABLES;").fetchall()]

    def _get_tables_creation_order(self) -> List[str]:
        # Copy list
        return list(self._tables_creation_order)

    def insert(self, table: str, df_input_data: pd.DataFrame):
        query = f"INSERT INTO {table}({', '.join(df_input_data.columns)}) SELECT * FROM df_input_data;"
        print(query)
        self._con.execute(query)


class StorageProcessCollector(Storage):
    def __init__(self, queue: mp.JoinableQueue):
        self._queue: mp.JoinableQueue = queue

    def insert(self, table: str, df_input_data: pd.DataFrame):
        self._queue.put((table, df_input_data))


class DuckDBStorageThread(DuckDBStorage, threading.Thread):
    def __init__(self, database_file: str, queue: mp.JoinableQueue):
        super().__init__(database_file)
        self._queue: mp.JoinableQueue = queue

    def insert(self, table: str, df_input_data: pd.DataFrame):
        self._queue.put((table, df_input_data))

    def create_tables(self):
        super().create_tables()
        # Tables are created. Data can be inserted, so we start processing
        self.start()

    def close(self):
        self._queue.join()
        self._queue.put((None, None))
        self._queue.join()
        super().close()

    def run(self):
        while True:
            table, df_input_data = self._queue.get()

            # Abort gracefully
            if table is None and df_input_data is None:
                self._queue.task_done()
                break
            print(f"[START] {datetime.datetime.now()} Insert {table}")
            self._con.append(table, df_input_data, by_name=True)
            print(f"[END] {datetime.datetime.now()} Insert {table}")
            self._queue.task_done()
