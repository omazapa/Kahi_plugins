from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from time import time
import datetime as dt
from joblib import Parallel, delayed


class Kahi_openalex_subjects(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["subjects"]

        self.openalex_client = MongoClient(
            config["openalex_subjects"]["database_url"])
        self.openalex_db = self.openalex_client[config["openalex_subjects"]
                                                ["database_name"]]
        self.openalex_collection = self.openalex_db[config["openalex_subjects"]
                                                    ["collection_name"]]

        self.n_jobs = config["openalex_subjects"]["num_jobs"]

        self.inserted_concepts = []
        self.inserted_concepts_ids_tuples = []

        self.relations_inserted_ids = []
        for reg in self.collection["subjects"].find():
            oa_id = ""
            for ext in reg["external_ids"]:
                if ext["sources"] == "openalex":
                    oa_id = ext["id"]
                    break
            if oa_id != "":
                self.inserted_concepts.append(oa_id)
                self.inserted_concepts_ids_tuples.append((reg["_id"], oa_id))
                if reg["relations"] != []:
                    self.relations_inserted_ids.append(oa_id)

    def process_openalex(self):
        with self.openalex_client.start_session() as session:
            self.openalex_db = self.openalex_client[self.config["openalex_subjects"]
                                                    ["database_name"]]
            self.openaelex_collection = self.openalex_db[self.config["openalex_subjects"]
                                                         ["collection_name"]]
            old = dt.datetime.now()
            for sub in self.openalex_collection.find({"id": {"$nin": self.inserted_concepts}}):
                if sub["id"] in self.inserted_concepts:
                    continue
                db_reg = self.collection.find_one(
                    {"external_ids.id": sub["id"]})
                if db_reg:
                    self.inserted_concepts.append(sub["id"])
                    self.inserted_concepts_ids_tuples.append(
                        (db_reg["_id"], sub["id"]))
                    continue
                entry = self.empty_subjects()
                entry["updated"] = [
                    {"source": "openalex", "time": int(time())}]
                sources_inserted_ids = []
                entry["external_ids"].append(
                    {"source": "openalex", "id": sub["id"]})
                sources_inserted_ids.append("openalex")
                for source, idx in sub["ids"].items():
                    if source in sources_inserted_ids:
                        continue
                    entry["external_ids"].append({"source": source, "id": idx})
                    sources_inserted_ids.append(source)
                entry["level"] = sub["level"]
                entry["names"].append(
                    {"name": sub["display_name"], "lang": "en"})
                inserted_lang_names = ["en"]
                if sub["international"]:
                    if sub["international"]["display_name"]:
                        for lang, name in sub["international"]["display_name"].items():
                            if lang in inserted_lang_names:
                                continue
                            entry["names"].append({"name": name, "lang": lang})
                            inserted_lang_names.append(lang)
                if sub["description"]:
                    entry["descriptions"].append(
                        {"description": sub["description"], "lang": "en"})
                if sub["wikidata"]:
                    entry["external_urls"].append(
                        {"source": "wikidata", "url": sub["wikidata"]})
                if sub["image_url"]:
                    entry["external_urls"].append(
                        {"source": "image", "url": sub["image_url"]})

                response = self.collection.insert_one(entry)
                self.inserted_concepts.append(sub["id"])
                self.inserted_concepts_ids_tuples.append(
                    (response.inserted_id, sub["id"]))

                delta = dt.datetime.now() - old
                if delta.seconds > 240:
                    self.openalex_client.admin.command(
                        'refreshSessions', [session.session_id], session=session)
                    old = dt.datetime.now()

    def process_relation(self, sub, url, db_name):
        client = MongoClient(url)

        db = client[db_name]
        collection = db["subjects"]
        relations = []
        for rel in sub["related_concepts"]:
            sub_db = self.collection.find_one(
                {"external_ids.id": rel["id"]})
            if sub_db:
                name = sub_db["names"][0]["name"]
                for n in sub_db["names"]:
                    if n["lang"] == "en":
                        name = n["name"]
                        break
                rel_entry = {
                    "id": sub_db["_id"],
                    "name": name,
                    "level": sub_db["level"]
                }
                relations.append(rel_entry)
            else:
                print("Could not find related concept in colombia db")
        for rel in sub["ancestors"]:
            sub_db = collection.find_one(
                {"external_ids.id": rel["id"]})
            if sub_db:
                name = sub_db["names"][0]["name"]
                for n in sub_db["names"]:
                    if n["lang"] == "en":
                        name = n["name"]
                        break
                rel_entry = {
                    "id": sub_db["_id"],
                    "name": name,
                    "level": sub_db["level"]
                }
                relations.append(rel_entry)
            else:
                print("Could not find related concept in colombia db")
        if len(relations) > 0:
            collection.update_one({"external_ids.id": sub["id"]}, {
                "$set": {"relations": relations}})
            # self.relations_inserted_ids.append(sub["id"])

    def process_relations(self):
        Parallel(
            n_jobs=self.n_jobs,
            backend="multiprocessing",
            verbose=10
        )(delayed(self.process_relation)(sub, self.config["database_url"], self.config["database_name"]) for sub in self.openalex_collection.find({"id": {"$nin": self.relations_inserted_ids}}))

    def run(self):
        # print("Inserting the subjects")
        # self.process_openalex()
        print("Creating relations")
        self.process_relations()
        return 0
