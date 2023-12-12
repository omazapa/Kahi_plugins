from kahi.KahiBase import KahiBase
from pymongo import MongoClient

from mohan.Similarity import Similarity


class Kahi_elasticsearch_works(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["works"]

        self.index = config["elasticsearch_works"]["index_name"] if "index_name" in config["elasticsearch_works"].keys(
        ) else "kahi_index"

        self.es_url = config["elasticsearch_works"]["es_url"] if "es_url" in config["elasticsearch_works"].keys(
        ) else "http://localhost:9200"

        self.es_client = Similarity(
            es_index=self.index,
            es_uri=self.es_url,
            es_auth=(
                config["elasticsearch_works"]["es_user"],
                config["elasticsearch_works"]["es_password"]
            ),
        )

        self.task = config["elasticsearch_works"]["task"] if "task" in config["elasticsearch_works"].keys(
        ) else None

        self.verbose = config["elasticsearch_works"]["verbose"] if "verbose" in config["elasticsearch_works"].keys(
        ) else 0

        self.inserted_ids = []

    def bulk_insert(self):
        bulk_size = 100
        es_entries = []
        count = 0
        paper_list = self.collection.find(
            {}, {"titles": 1, "source": 1, "year_published": 1, "bibliographic_info": 1, "authors.full_name": 1})
        for reg in paper_list:
            work = {}
            if "titles" not in reg.keys():
                continue
            if len(reg["titles"]) < 1:
                continue
            work["title"] = reg["titles"][0]["title"]
            work["source"] = reg["source"]["name"] if "name" in reg["source"].keys(
            ) else ""
            work["year"] = reg["year_published"]
            work["volume"] = reg["bibliographic_info"]["volume"] if "volume" in reg["bibliographic_info"].keys(
            ) else ""
            work["issue"] = reg["bibliographic_info"]["issue"] if "issue" in reg["bibliographic_info"].keys(
            ) else ""
            work["start_page"] = reg["bibliographic_info"]["start_page"] if "start_page" in reg["bibliographic_info"].keys(
            ) else ""
            work["end_page"] = reg["bibliographic_info"]["end_page"] if "end_page" in reg["bibliographic_info"].keys(
            ) else ""
            authors = []
            for author in reg["authors"]:
                authors.append(author["full_name"])
            work["authors"] = authors
            entry = {
                "_index": self.index,
                "_id": str(reg["_id"]),
                "_source": work
            }
            es_entries.append(entry)
            if len(es_entries) == bulk_size:
                self.es_client.insert_bulk(es_entries)
                count += bulk_size
                es_entries = []
                if self.verbose > 4:
                    print(f"""{count} entries inserted""")

    def delete(self):
        self.es_client.delete_index(self.index)

    def run(self):
        if self.task == "bulk_insert":
            if self.verbose > 0:
                print(f"""Bulk inserting index {self.index}""")
            self.bulk_insert()
        elif self.task == "delete":
            if self.verbose > 0:
                print(f"""Deleting index {self.index}""")
            self.delete()
        else:
            raise Exception("Please specify a task to execute")
