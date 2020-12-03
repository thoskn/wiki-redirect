from collections import namedtuple
from datetime import datetime
from typing import Generator, Optional

from cachetools import LRUCache, cached


import mysql.connector


def get_connection(host: str):
    connection = mysql.connector.connect(host=host, user="root", password="pword")
    return connection


Redirect = namedtuple(
    "redirect", ["from_id", "from_title", "target_title", "target_namespace"]
)
Page = namedtuple("page", ["page_id", "namespace", "title", "is_redirect"])

# TODO rollback if errors. Commit at end of all????
# TODO logging
# TODO close connection and cursors
# TODO check ordering of functions on classes
# TODO check that all gets from the persistent db use a timestamp filter


class WikiPageRepository:
    def __init__(self, host: str, database: str):
        # TODO dependency inject the get_connection - but not in its current form
        self._connection = get_connection(host)
        self._connection_2 = get_connection(host)
        self._connection.autocommit = False
        self._database = database

    def get_redirects(self) -> Generator[Redirect, None, None]:
        connection = self._connection_2
        cursor = connection.cursor()
        cursor.execute(
            # Probably mixing concerns here, but saves a lot of processing to filter in query rather than in python
            # TODO Pass in filters as arguments, so that processor controls it not the repository
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
        connection = self._connection
        cursor = connection.cursor(prepared=True)
        cursor.execute(
            f"""SELECT page_id, page_title, root_title, root_namespace FROM {self._database}.redirect 
                    WHERE page_id=%s AND (effective_from < %s AND (effective_to IS NULL OR effective_to > %s ));""",
            (redirect.from_id, timestamp, timestamp),
        )
        # TODO maybe need separate repos because get_redirect and get_redirects are using different table structures
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
        connection = self._connection
        cursor = connection.cursor(prepared=True)
        # TODO check whether using the parameters retrieved
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
        connection = self._connection
        cursor = connection.cursor(prepared=True)
        # TODO check whether using the parameters retrieved
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
        connection = self._connection
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
        connection.commit()
        print("NEW REDIRECT")
        print(redirect)

    def replace_redirect(self, redirect: Redirect, timestamp: datetime):
        connection = self._connection
        cursor = connection.cursor()
        # TODO
        # cursor.execute("""INSERT INTO redirect .....""")
        # cursor.execute(f"""UPDATE redirect SET effective_to='{timestamp}' WHERE .....""")
        # connection.commit()
        print("REPLACE REDIRECT")
        # print(redirect, timestamp)

    def update_batch_timestamp(self, redirect: Redirect, timestamp: datetime):
        connection = self._connection
        cursor = connection.cursor()
        # TODO
        # cursor.execute(f"""UPDATE redirect SET effective_to='{timestamp}' WHERE .....""")
        # connection.commit()
        print("UPDATE TIMESTAMP")
        print(redirect, timestamp)

    def expire_old_redirects(self, timestamp: datetime):
        connection = self._connection
        cursor = connection.cursor()
        # TODO
        # cursor.execute(
        #     f"""UPDATE redirect SET effective_to='{timestamp}' WHERE batch_timestamp != '{timestamp}'"""
        # )
        # connection.commit()
        print("EXPIRE")
        print(timestamp)

    def create_redirect_table(self):
        connection = self._connection
        cursor = connection.cursor()
        cursor.execute(f"""CREATE DATABASE  IF NOT EXISTS {self._database}""")
        cursor.execute(f"""USE {self._database}""")
        # cursor.execute("DROP TABLE redirect;")
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


class WikiRedirectProcessor:
    def __init__(
        self,
        staging_wiki_repository: WikiPageRepository,
        persistent_wiki_repository: WikiPageRepository,
    ):
        self._staging_wiki_repository = staging_wiki_repository
        self._persistent_wiki_repository = persistent_wiki_repository
        self._batch_timestamp = datetime.now()

    def process(self):
        self._persistent_wiki_repository.create_redirect_table()
        redirects = self._staging_wiki_repository.get_redirects()
        for redirect in redirects:
            if redirect.target_namespace == 0 and len(redirect.target_title) <= 5:
                target = self._staging_wiki_repository.get_page(
                    redirect.target_namespace, redirect.target_title
                )
                if target and not target.is_redirect:
                    self._ingest_redirect(redirect, target)
        self._persistent_wiki_repository.expire_old_redirects(self._batch_timestamp)

    def _ingest_redirect(self, redirect: Redirect, target: Page):
        current_record = self._persistent_wiki_repository.get_redirect(
            redirect, self._batch_timestamp
        )
        redirect_page = self._staging_wiki_repository.get_page_by_id(redirect.from_id)
        redirect = Redirect(
            from_id=redirect.from_id,
            from_title=redirect_page.title,
            target_title=redirect.target_title,
            target_namespace=redirect.target_namespace,
        )
        if current_record:
            if current_record == redirect:
                self._persistent_wiki_repository.update_batch_timestamp(
                    redirect, self._batch_timestamp
                )
            else:
                self._persistent_wiki_repository.replace_redirect(
                    redirect, self._batch_timestamp
                )
        else:
            self._persistent_wiki_repository.add_redirect(
                redirect, target, self._batch_timestamp
            )

staging_repo = WikiPageRepository("localhost", "staging")
persistent_repo = WikiPageRepository("localhost", "persistent")
processor = WikiRedirectProcessor(
    staging_wiki_repository=staging_repo, persistent_wiki_repository=persistent_repo
)

processor.process()
