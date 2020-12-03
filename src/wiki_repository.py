from collections import namedtuple
from datetime import datetime
from typing import Generator, Optional

from cachetools import LRUCache, cached
import mysql.connector

Redirect = namedtuple(
    "redirect", ["from_id", "from_title", "target_title", "target_namespace"]
)
Page = namedtuple("page", ["page_id", "namespace", "title", "is_redirect"])


class WikiPageRepository:
    def __init__(self, host: str, database: str, user: str, password: str):
        self._host = host
        self._database = database
        self._user = user
        self._password = password
        self._short_connection = self._get_connection()
        self._short_connection.autocommit = False
        # For use in the generator which yields the new redirects to process
        self._long_connection = self._get_connection()
        self._get_redirect_prepared_statement_cursor = None
        self._get_page_prepared_statement_cursor = None
        self._get_page_by_id_prepared_statement_cursor = None

    def get_redirects(self) -> Generator[Redirect, None, None]:
        "A generator for yielding the redirects from the new data dump."
        connection = self._long_connection
        cursor = connection.cursor()
        cursor.execute(
            # Mixing concerns here, but saves a lot of processing to filter in query rather than in python
            # Given more time I would allow the option of passing in some filters, rather than them needint to be defined here
            f"""SELECT rd_from, rd_namespace, rd_title FROM {self._database}.redirect WHERE rd_namespace=0 AND LENGTH(rd_title)<=5;"""
        )
        for redirect in cursor:
            yield Redirect(
                from_id=redirect[0],
                from_title=None,
                target_namespace=redirect[1],
                target_title=redirect[2],
            )

    def get_redirect(
        self, redirect: Redirect, timestamp: datetime
    ) -> Optional[Redirect]:
        "Retrieve the specified redirect from the persistent data store, which was current at the time of the given timestamp"
        if not self._get_redirect_prepared_statement_cursor:
            self._get_redirect_prepared_statement_cursor = (
                self._short_connection.cursor(prepared=True)
            )
        cursor = self._get_redirect_prepared_statement_cursor
        cursor.execute(
            f"""SELECT page_id, page_title, root_title, root_namespace FROM {self._database}.redirect 
                    WHERE page_id=%s AND (effective_from < %s AND (effective_to IS NULL OR effective_to > %s ));""",
            (redirect.from_id, timestamp, timestamp),
        )
        try:
            redirect = next(cursor)
            return Redirect(
                from_id=redirect[0],
                from_title=redirect[1],
                target_title=redirect[2],
                target_namespace=redirect[3],
            )
        except StopIteration:
            return None

    @cached(LRUCache(maxsize=100))
    def get_page(self, namespace: str, title: str) -> Optional[Page]:
        "Get the page from the new data dump with the given title and namespace"
        if not self._get_page_prepared_statement_cursor:
            self._get_page_prepared_statement_cursor = self._short_connection.cursor(
                prepared=True
            )
        cursor = self._get_page_prepared_statement_cursor
        cursor.execute(
            f"""SELECT page_id, page_title, page_namespace, page_is_redirect FROM {self._database}.page
                WHERE page_namespace=%s AND page_title=%s;""",
            (namespace, title),
        )
        try:
            page = next(cursor)
            return Page(
                page_id=page[0],
                title=page[1],
                namespace=page[2],
                is_redirect=bool(page[3]),
            )
        except StopIteration:
            return None

    @cached(LRUCache(maxsize=100))
    def get_page_by_id(self, page_id: int) -> Optional[Page]:
        "Get the page from the new data dump with the given page id"
        if not self._get_page_by_id_prepared_statement_cursor:
            self._get_page_by_id_prepared_statement_cursor = (
                self._short_connection.cursor(prepared=True)
            )
        cursor = self._get_page_by_id_prepared_statement_cursor
        cursor.execute(
            f"""SELECT page_id, page_title, page_namespace, page_is_redirect FROM {self._database}.page
                        WHERE page_id=%s;""",
            (page_id,),
        )
        try:
            page = next(cursor)
            return Page(
                page_id=page[0],
                title=page[1],
                namespace=page[2],
                is_redirect=bool(page[3]),
            )
        except StopIteration:
            return None

    def add_redirect(self, redirect: Redirect, target: Page, timestamp: datetime):
        "Insert the provided redirect to the persistent data store"
        connection = self._short_connection
        cursor = connection.cursor()
        cursor.execute(f"""USE {self._database};""")
        cursor.execute(
            """INSERT INTO redirect (page_id, page_title, page_type, root_title, root_namespace, root_page_id, effective_from, batch_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                redirect.from_id,
                redirect.from_title,
                "redirect",
                target.title,
                target.namespace,
                target.page_id,
                timestamp,
                timestamp,
            ),
        )
        self._short_connection.commit()

    def replace_redirect(self, redirect: Redirect, target: Page, timestamp: datetime):
        "Replace and expire an existing redirect, with the new redirect information"
        connection = self._short_connection
        cursor = connection.cursor()
        cursor.execute(f"""USE {self._database};""")
        cursor.execute(
            """INSERT INTO redirect (page_id, page_title, page_type, root_title, root_namespace, root_page_id, effective_from, batch_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                redirect.from_id,
                redirect.from_title,
                "redirect",
                target.title,
                target.namespace,
                target.page_id,
                timestamp,
                timestamp,
            ),
        )
        cursor.execute(
            f"""UPDATE persistent.redirect SET effective_to=%s, batch_timestamp=%s 
                    WHERE page_id=%s AND batch_timestamp IS NULL;""",
            (timestamp, timestamp, redirect.from_id),
        )
        self._short_connection.commit()

    def update_batch_timestamp(self, redirect: Redirect, timestamp: datetime):
        "Update the batch_timestamp on a pre-existing redirect, to show that it still exists in the new data dump"
        connection = self._short_connection
        cursor = connection.cursor()
        cursor.execute(f"""USE {self._database};""")
        cursor.execute(
            f"""UPDATE redirect SET batch_timestamp=%s 
                    WHERE page_id=%s AND batch_timestamp IS NOT NULL;""",
            (timestamp, redirect.from_id),
        )
        self._short_connection.commit()

    def expire_old_redirects(self, timestamp: datetime):
        "Expire any redirects in the persistent data store which do not exist in the new data dump"
        connection = self._short_connection
        cursor = connection.cursor()
        cursor.execute(f"""USE {self._database};""")
        cursor.execute(
            f"""UPDATE redirect SET effective_to=%s, batch_timestamp=%s 
                    WHERE batch_timestamp < %s;""",
            (timestamp, timestamp, timestamp),
        )
        connection.commit()

    def create_redirect_table(self):
        connection = self._short_connection
        cursor = connection.cursor()
        cursor.execute(f"""CREATE DATABASE  IF NOT EXISTS {self._database}""")
        cursor.execute(f"""USE {self._database}""")
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS redirect (
                page_id         INT(8) unsigned NOT NULL,
                page_title      VARBINARY(255) NOT NULL,
                page_type       ENUM('root', 'redirect') NOT NULL,       
                root_title      VARBINARY(255),
                root_namespace  INT(11),
                root_page_id    INT(8) unsigned,
                effective_from  TIMESTAMP NOT NULL,
                effective_to    TIMESTAMP DEFAULT NULL,
                batch_timestamp TIMESTAMP NOT NULL
            )"""
        )
        connection.commit()

    def _get_connection(self):
        connection = mysql.connector.connect(
            host=self._host, user=self._user, password=self._password
        )
        return connection

    def __del__(self):
        self._short_connection.close()
        self._long_connection.close()

