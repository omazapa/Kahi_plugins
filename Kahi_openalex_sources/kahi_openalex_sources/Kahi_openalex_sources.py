from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from datetime import datetime as dt
from time import time


class Kahi_openalex_sources(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["sources"]

        self.openalex_client = MongoClient(
            config["openalex_sources"]["database_url"])
        self.openalex_db = self.openalex_client[config["openalex_sources"]
                                                ["database_name"]]
        self.openalex_collection = self.openalex_db[config["openalex_sources"]
                                                    ["collection_name"]]

        self.already_processed = []

    def process_openalex(self):
        with self.openalex_client.start_session() as session:
            self.openalex_db = self.openalex_client[self.config["openalex_sources"]
                                                    ["database_name"]]
            self.openalex_collection = self.openalex_db[self.config["openalex_sources"]
                                                        ["collection_name"]]
            old = dt.now()
            for source in self.openalex_collection.find({"id": {"$nin": self.already_processed}}):
                if source["id"] in self.already_processed:
                    continue
                source_db = None
                if "issn" in source.keys():
                    source_db = self.collection.find_one(
                        {"external_ids.id": source["issn"]})
                if not source_db:
                    if "issn_l" in source.keys():
                        source_db = self.collection.find_one(
                            {"external_ids.id": source["issn_l"]})
                if source_db:
                    oa_found = False
                    for up in source_db["updated"]:
                        if up["source"] == "openalex":
                            oa_found = True
                            break
                    if oa_found:
                        continue

                    source_db["updated"].append(
                        {"source": "openalex", "time": int(time())})
                    source_db["external_ids"].append(
                        {"source": "openalex", "id": source["id"]})
                    source_db["types"].append(
                        {"source": "openalex", "type": source["type"]})
                    source_db["names"].append(
                        {"name": source["display_name"], "lang": "en", "source": "openalex"})

                    self.collection.update_one({"_id": source_db["_id"]}, {"$set": {
                        "updated": source_db["updated"],
                        "names": source_db["names"],
                        "external_ids": source_db["external_ids"],
                        "types": source_db["types"],
                        "subjects": source_db["subjects"]
                    }})
                else:
                    entry = self.empty_source()
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

                    self.collection.insert_one(entry)
                    self.already_processed.append(source["id"])
                delta = dt.now() - old
                if delta.seconds > 240:
                    self.openalex_client.admin.command(
                        'refreshSessions', [session.session_id], session=session)
                    old = dt.now()

    def run(self):
        self.process_openalex()
        return 0
