from collections import namedtuple
from datetime import datetime
from typing import Generator, Optional


import mysql.connector


def get_connection(host: str):
    connection = mysql.connector.connect(host=host, user="root", password="pword")
    return connection


Redirect = namedtuple("redirect", ["from_id", "target_title", "target_namespace"])
Page = namedtuple("page", ["page_id", "namespace", "title", "is_redirect"])

# TODO rollback if errors. Commit at end of all????
# TODO check ordering of functions on classes
# TODO logging
# TODO close connection and cursors


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
                target_namespace=redirect[1],
                target_title=redirect[2],
            )

    # TODO cache???
    def get_redirect(self, redirect: Redirect) -> Optional[Redirect]:
        connection = self._connection
        cursor = connection.cursor(prepared=True)
        cursor.execute(
            f"""SELECT page_id, root_title FROM {self._database}.redirect WHERE page_title=%s;""", (redirect.from_id,)
        )
        # TODO maybe need separate repos because get_redirect and get_redirects are using different table structures
        try:
            redirect = next(cursor)
            return Redirect(
                from_id=redirect[0],
                # TODO insert target namespace to data so that can retrieve here
                target_namespace=0,
                target_title=redirect[1],
            )
        except StopIteration:
            return None

    # TODO cache
    def get_page(self, namespace: str, title: str) -> Optional[Page]:
        connection = self._connection
        cursor = connection.cursor(prepared=True)
        cursor.execute(
            f"""SELECT page_id, page_title, page_namespace, page_is_redirect FROM {self._database}.page
                WHERE page_namespace=%s AND page_title=%s;""", (namespace, title)
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

    def add_redirect(self, redirect: Redirect):
        connection = self._connection
        cursor = connection.cursor()
        cursor.execute(f"""USE {self._database};""")
        # TODO implement properly
        #
        # cursor.execute("""INSERT INTO redirect """)
        # connection.commit()
        print("NEW REDIRECT")
        print(redirect)

    def replace_redirect(self, redirect: Redirect, timestamp: datetime):
        connection = self._connection
        cursor = connection.cursor()
        # cursor.execute("""INSERT INTO redirect .....""")
        # cursor.execute(f"""UPDATE redirect SET effective_to='{timestamp}' WHERE .....""")
        # connection.commit()
        print("REPLACE REDIRECT")
        print(redirect, timestamp)

    def update_batch_timestamp(self, redirect: Redirect, timestamp: datetime):
        connection = self._connection
        cursor = connection.cursor()
        # cursor.execute(f"""UPDATE redirect SET effective_to='{timestamp}' WHERE .....""")
        # connection.commit()
        print("UPDATE TIMESTAMP")
        print(redirect, timestamp)

    def expire_old_redirects(self, timestamp: datetime):
        connection = self._connection
        cursor = connection.cursor()
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
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS redirect (
                page_id         INT(8) unsigned NOT NULL,
                page_title      VARBINARY(255) NOT NULL,
                page_type       ENUM('root', 'redirect') ,       
                root_title      VARBINARY(255),
                root_page_id    INT(8) unsigned,
                effective_from  TIMESTAMP NOT NULL,
                effective_to    TIMESTAMP NOT NULL,
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
            # TODO check where the filtering is happening and that all required filtering is happening
            target = self._staging_wiki_repository.get_page(
                redirect.target_namespace, redirect.target_title
            )
            if (
                target
                and not target.is_redirect
                and target.namespace == 0
                and len(target.title) <= 5
            ):
                self._ingest_redirect(redirect)
        self._persistent_wiki_repository.expire_old_redirects(self._batch_timestamp)

    def _ingest_redirect(self, redirect: Redirect):
        current_record = self._persistent_wiki_repository.get_redirect(redirect)
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
            self._persistent_wiki_repository.add_redirect(redirect)


# Iterate through redirects
#     get page redirects to
#     if redirect, ignore
#     else
#         if already exists:
#             if no changes do nothing
#             elif changes
#                     add new row, set effictive_to on old row to current timestamp
#     search for redirects not touched in this pass
#           set effective_to to current timestamp

staging_repo = WikiPageRepository("localhost", "staging")
persistent_repo = WikiPageRepository("localhost", "persistent")
processor = WikiRedirectProcessor(
    staging_wiki_repository=staging_repo, persistent_wiki_repository=persistent_repo
)

processor.process()
