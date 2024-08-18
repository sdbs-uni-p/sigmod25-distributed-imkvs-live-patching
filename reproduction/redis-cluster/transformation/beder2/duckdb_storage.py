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
        # This is important! We have to finally fill all tables by the creation order because of references!
        run_insert_lock = threading.Lock()

        insert_pool = FutureCollector(ThreadPoolExecutor(max_workers=10))

        staging_workers = {
            tbl: self._StagingWorker(self._con.cursor(), tbl)
            for tbl in self._get_tables_creation_order()
        }
        insert_con = self._con.cursor()

        def append(table, df, wait_for_lock: bool = False):
            # Abort
            if df.empty:
                self._queue.task_done()
                return

            if wait_for_lock:
                # We just wait until the lock is free, but we don't need it ;-)
                run_insert_lock.acquire()
                run_insert_lock.release()
            try:
                print(f"[START] {datetime.datetime.now()} Insert {table}")
                insert_con.append(table, df, by_name=True)
                print(f"[END] {datetime.datetime.now()} Insert {table}")
            except Exception as e:
                print("ERROR")
                print(e)
                print(table)
                print(df.columns)
                print(df.dtypes)
                print(df, flush=True)
                raise e
            self._queue.task_done()

        while True:
            table, df_input_data = self._queue.get()

            # Abort gracefully
            if table is None and df_input_data is None:
                # Wait for pool to finish
                insert_pool.shutdown()
                insert_con.close()

                for worker in staging_workers.values():
                    worker.noblocking_close()
                for worker in staging_workers.values():
                    worker.wait()

                self._queue.task_done()
                break

            # We insert run immediately!

            if table in ["run", "latencies", "rdb"]:
                # In order to insert run data, we have to lock the table!
                run_insert_lock.acquire()
                future = insert_pool.submit(append, table, df_input_data)
                future.add_done_callback(lambda x: run_insert_lock.release())
            # elif table in ["latencies", "rdb"]:
            #     # If our run insert already has the lock, we wait until all run data is inserted!
            #     insert_pool.submit(append, table, df_input_data, wait_for_lock=True)
            else:
                staging_workers[table].put(df_input_data)
                self._queue.task_done()

    class _StagingWorker(threading.Thread):
        def __init__(self, con: duckdb.DuckDBPyConnection, table: str):
            super().__init__()
            self._con = con
            # self._database_file = database_file
            self._table: str = table
            self._staging_table = f"{STAGING_PREFIX}{self._table}"
            self._queue = mp.JoinableQueue()
            self.start()

        def run(self):
            # con: duckdb.DuckDBPyConnection = duckdb.connect(database=self._database_file, read_only=False)

            self._con.execute(
                f"CREATE TEMP TABLE {self._staging_table} AS SELECT * FROM {self._table} WHERE 0 = 1"
            )

            while True:
                df_input_data = self._queue.get()
                if df_input_data is None:
                    # Dump data
                    print(f"[START] {datetime.datetime.now()} Flushing {self._table}")
                    self._con.execute(
                        f"INSERT INTO {self._table} SELECT * FROM {self._staging_table}"
                    )
                    print(f"[END] {datetime.datetime.now()} Flushing {self._table}")
                    self._queue.task_done()
                    break

                print(f"[START] {datetime.datetime.now()} Append {self._staging_table}")
                self._con.append(self._staging_table, df_input_data, by_name=True)
                print(
                    f"[END] {datetime.datetime.now()} Append {self._staging_table}",
                    flush=True,
                )

                self._queue.task_done()

            # We are done!
            # super().close()

        def put(self, val):
            self._queue.put(val)

        def close(self):
            self._queue.join()
            self._queue.put(None)
            self._queue.join()
            self._con.close()

        def noblocking_close(self):
            self._queue.put(None)

        def wait(self):
            self._queue.join()
