from wiki_processor import WikiRedirectProcessor
from wiki_repository import WikiPageRepository

staging_repo = WikiPageRepository(
    host="localhost", database="staging", user="root", password="pword"
)
persistent_repo = WikiPageRepository(
    host="localhost", database="persistent", user="root", password="pword"
)
processor = WikiRedirectProcessor(
    staging_wiki_repository=staging_repo, persistent_wiki_repository=persistent_repo
)

processor.process()
