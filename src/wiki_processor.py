from datetime import datetime

from wiki_repository import Page, Redirect, WikiPageRepository


class WikiRedirectProcessor:
    def __init__(
        self,
        staging_wiki_repository: WikiPageRepository,
        persistent_wiki_repository: WikiPageRepository,
    ):
        # For accessing the data from the current data dump to be processed
        self._staging_wiki_repository = staging_wiki_repository
        # For accessing the data in the persistent data store
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
                    redirect, target, self._batch_timestamp
                )
        else:
            self._persistent_wiki_repository.add_redirect(
                redirect, target, self._batch_timestamp
            )

