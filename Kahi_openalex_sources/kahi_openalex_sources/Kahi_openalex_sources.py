from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from time import time
from joblib import Parallel, delayed


def process_one(source, url, db_name, empty_source):
    client = MongoClient(url)
    db = client[db_name]
    collection = db["sources"]

    source_db = None
    if "issn" in source.keys():
        source_db = collection.find_one(
            {"external_ids.id": source["issn"]})
    if not source_db:
        if "issn_l" in source.keys():
            source_db = collection.find_one(
                {"external_ids.id": source["issn_l"]})
    if source_db:
        oa_found = False
        for up in source_db["updated"]:
            if up["source"] == "openalex":
                oa_found = True
                break
        if oa_found:
            client.close()
            return

        source_db["updated"].append(
            {"source": "openalex", "time": int(time())})
        source_db["external_ids"].append(
            {"source": "openalex", "id": source["id"]})
        source_db["types"].append(
            {"source": "openalex", "type": source["type"]})
        source_db["names"].append(
            {"name": source["display_name"], "lang": "en", "source": "openalex"})

        collection.update_one({"_id": source_db["_id"]}, {"$set": {
            "updated": source_db["updated"],
            "names": source_db["names"],
            "external_ids": source_db["external_ids"],
            "types": source_db["types"],
            "subjects": source_db["subjects"]
        }})
    else:
        entry = empty_source.copy()
        entry["updated"] = [
            {"source": "openalex", "time": int(time())}]
        entry["names"].append(
            {"name": source["display_name"], "lang": "en", "source": "openalex"})
        entry["external_ids"].append(
            {"source": "openalex", "id": source["id"]})
        if "issn" in source.keys():
            entry["external_ids"].append(
                {"source": "issn", "id": source["issn"]})
        if "issn_l" in source.keys():
            entry["external_ids"].append(
                {"source": "issn_l", "id": source["issn_l"]})
        entry["types"].append(
            {"source": "openalex", "type": source["type"]})
        if "publisher" in source.keys():
            if source["publisher"]:
                entry["publisher"] = {
                    "name": source["publisher"], "country_code": ""}
        if "apc_usd" in source.keys():
            if source["apc_usd"]:
                entry["apc"] = {"currency": "USD",
                                "charges": source["apc_usd"]}
        if "abbreviated_title" in source.keys():
            if source["abbreviated_title"]:
                entry["abbreviations"].append(
                    source["abbreviated_title"])
        for name in source["alternate_titles"]:
            entry["abbreviations"].append(name)
        if source["homepage_url"]:
            entry["external_urls"].append(
                {"source": "site", "url": source["homepage_url"]})
        if source["societies"]:
            for soc in source["societies"]:
                entry["external_urls"].append(
                    {"source": soc["organization"], "url": soc["url"]})

        collection.insert_one(entry)
    client.close()


class Kahi_openalex_sources(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["sources"]

        self.collection.create_index("external_ids.id")

        self.collection.create_index("external_ids.id")

        self.openalex_client = MongoClient(
            config["openalex_sources"]["database_url"])
        self.openalex_db = self.openalex_client[config["openalex_sources"]
                                                ["database_name"]]
        self.openalex_collection = self.openalex_db[config["openalex_sources"]
                                                    ["collection_name"]]

        self.n_jobs = config["openalex_sources"]["num_jobs"]

        self.already_processed = []

    def process_openalex(self):
        source_list = list(self.openalex_collection.find(
            {"id": {"$nin": self.already_processed}}))
        Parallel(
            n_jobs=self.n_jobs,
            verbose=10,
            backend="threading")(
            delayed(process_one)(
                source,
                self.mongodb_url,
                self.config["database_name"],
                self.empty_source()
            ) for source in source_list
        )

    def run(self):
        self.process_openalex()
        return 0
