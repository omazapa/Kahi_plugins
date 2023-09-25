from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from datetime import datetime as dt
from time import time
from langid import classify


class Kahi_scienti_sources(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["sources"]

        self.collection.create_index("external_ids.id")

        self.already_in_db = []

    def update_scienti(self, reg, entry, issn):
        updated_scienti = False
        for upd in entry["updated"]:
            if upd["source"] == "scienti":
                updated_scienti = True
                entry["updated"].remove(upd)
                entry["updated"].append(
                    {"source": "scienti", "time": int(time())})
                break
        if not updated_scienti:
            entry["updated"].append({"source": "scienti", "time": int(time())})
        journal = None
        for detail in reg["details"]:
            if "article" in detail.keys():
                paper = detail["article"][0]
                if "journal" in paper.keys():
                    journal = paper["journal"][0]
                    break
        if not journal:
            return
        if "TPO_REVISTA" in journal.keys():
            entry["types"].append(
                {"source": "scienti", "type": journal["TPO_REVISTA"]})
        entry["external_ids"].append(
            {"source": "scienti", "id": journal["COD_REVISTA"]})

        rankings_list = []
        ranks = []
        dates = [(rank["from_date"], rank["to_date"])
                 for rank in entry["ranking"] if rank["source"] == "scienti"]
        for reg_scienti in self.scienti_collection["products"].find({"details.article.journal.TXT_ISSN_SEP": issn}):
            paper = None
            journal = None
            for detail in reg_scienti["details"]:
                if "article" in detail.keys():
                    paper = detail["article"][0]
                    if "journal" in paper.keys():
                        journal = paper["journal"][0]
                        break

            if journal == None:
                continue
            if "TPO_CLASIFICACION" not in journal.keys():
                continue
            if not journal["TPO_CLASIFICACION"] in ranks:
                ranking = {
                    "from_date": int(dt.strptime(paper["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp()),
                    "to_date": int(dt.strptime(paper["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp()),
                    "rank": journal["TPO_CLASIFICACION"],
                    "issn": issn,
                    "order": None,
                    "source": "scienti"
                }
                rankings_list.append(ranking)
                ranks.append(journal["TPO_CLASIFICACION"])
                dates_tuple = (
                    int(dt.strptime(
                        paper["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp()),
                    int(dt.strptime(
                        paper["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp())
                )

                dates.append(dates_tuple)
            else:
                idx = ranks.index(journal["TPO_CLASIFICACION"])
                date1, date2 = dates[idx]

                if date1 > int(dt.strptime(paper["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp()):
                    date1 = int(dt.strptime(
                        paper["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp())
                if date2 < int(dt.strptime(paper["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp()):
                    date2 = int(dt.strptime(
                        paper["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp())
                dates[idx] = (date1, date2)

        self.collection.update_one({"_id": entry["_id"]}, {"$set": {
            "types": entry["types"],
            "external_ids": entry["external_ids"],
            "updated": entry["updated"],
            "ranking": entry["ranking"] + rankings_list
        }})

    def process_scienti(self, config, verbose=0):
        self.scienti_client = MongoClient(config["database_url"])

        if config["database_name"] not in self.scienti_client.list_database_names():
            raise Exception("Database {} not found".format(
                config["database_name"]))

        self.scienti_db = self.scienti_client[config["database_name"]]

        if config["collection_name"] not in self.scienti_db.list_collection_names():
            raise Exception("Collection {} not found".format(
                config["collection_name"]))

        self.scienti_collection = self.scienti_db[config["collection_name"]]
        for issn in self.scienti_collection.distinct("details.article.journal.TXT_ISSN_SEP"):
            reg_db = self.collection.find_one({"external_ids.id": issn})
            if reg_db:
                reg_scienti = self.scienti_collection.find_one(
                    {"details.article.journal.TXT_ISSN_SEP": issn})
                if reg_scienti:
                    self.update_scienti(reg_scienti, reg_db, issn)
            else:
                reg_scienti = self.scienti_collection.find_one(
                    {"details.article.journal.TXT_ISSN_SEP": issn})
                if reg_scienti:
                    journal = None
                    for detail in reg_scienti["details"]:
                        if "article" in detail.keys():
                            paper = detail["article"][0]
                            if "journal" in paper.keys():
                                journal = paper["journal"][0]
                                break
                    if not journal:
                        continue
                    entry = self.empty_source()
                    entry["updated"] = [
                        {"source": "scienti", "time": int(time())}]
                    lang = classify(journal["TXT_NME_REVISTA"])[0]
                    entry["names"] = [
                        {"lang": lang, "name": journal["TXT_NME_REVISTA"], "source": "scienti"}]
                    entry["external_ids"].append(
                        {"source": "issn", "id": journal["TXT_ISSN_SEP"]})
                    entry["external_ids"].append(
                        {"source": "scienti", "id": journal["COD_REVISTA"]})
                    if "TPO_REVISTA" in journal.keys():
                        entry["types"].append(
                            {"source": "scienti", "type": journal["TPO_REVISTA"]})
                    if "editorial" in journal.keys():
                        entry["publisher"] = {
                            "country_code": "", "name": journal["editorial"][0]["TXT_NME_EDITORIAL"]}
                    rankings_list = []
                    ranks = []
                    dates = []
                    for reg_scienti in self.scienti_collection.find({"details.article.journal.TXT_ISSN_SEP": issn}):
                        paper = None
                        journal = None
                        for detail in reg_scienti["details"]:
                            if "article" in detail.keys():
                                paper = detail["article"][0]
                                if "journal" in paper.keys():
                                    journal = paper["journal"][0]
                                    break
                        if journal:
                            if "TPO_CLASIFICACION" not in journal.keys():
                                continue
                            if not journal["TPO_CLASIFICACION"] in ranks:
                                try:
                                    from_date = int(dt.strptime(
                                        paper["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp())
                                    to_date = int(dt.strptime(
                                        paper["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp())
                                except Exception as e:
                                    print(e)
                                    try:
                                        from_date = int(dt.strptime(
                                            paper["DTA_CREACION"], "%Y-%m-%d %H:%M:%S").timestamp())
                                        to_date = int(dt.strptime(
                                            paper["DTA_CREACION"], "%Y-%m-%d %H:%M:%S").timestamp())
                                    except Exception as e:
                                        print(e)
                                        from_date = None
                                        to_date = None
                                ranking = {
                                    "from_date": from_date,
                                    "to_date": to_date,
                                    "rank": journal["TPO_CLASIFICACION"],
                                    "issn": issn,
                                    "order": None,
                                    "source": "scienti"
                                }
                                rankings_list.append(ranking)
                                ranks.append(journal["TPO_CLASIFICACION"])
                                try:
                                    dates_tuple = (
                                        int(dt.strptime(
                                            paper["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp()),
                                        int(dt.strptime(
                                            paper["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp())
                                    )
                                except Exception as e:
                                    print(e)
                                    try:
                                        dates_tuple = (
                                            int(dt.strptime(
                                                paper["DTA_CREACION"], "%Y-%m-%d %H:%M:%S").timestamp()),
                                            int(dt.strptime(
                                                paper["DTA_CREACION"], "%Y-%m-%d %H:%M:%S").timestamp())
                                        )
                                    except Exception as e:
                                        print(e)
                                        dates_tuple = (
                                            None,
                                            None
                                        )

                                dates.append(dates_tuple)
                            else:
                                # if is already ranked but dates changed
                                idx = ranks.index(journal["TPO_CLASIFICACION"])
                                date1, date2 = dates[idx]
                                try:
                                    if date1 > int(dt.strptime(paper["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp()):
                                        date1 = int(dt.strptime(
                                            paper["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp())
                                    if date2 < int(dt.strptime(paper["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp()):
                                        date2 = int(dt.strptime(
                                            paper["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp())
                                except Exception as e:
                                    print(e)
                                    try:
                                        if date1 > int(dt.strptime(paper["DTA_CREACION"], "%Y-%m-%d %H:%M:%S").timestamp()):
                                            date1 = int(dt.strptime(
                                                paper["DTA_CREACION"], "%Y-%m-%d %H:%M:%S").timestamp())
                                        if date2 < int(dt.strptime(paper["DTA_CREACION"], "%Y-%m-%d %H:%M:%S").timestamp()):
                                            date2 = int(dt.strptime(
                                                paper["DTA_CREACION"], "%Y-%m-%d %H:%M:%S").timestamp())
                                    except Exception as e:
                                        print(e)
                                dates[idx] = (date1, date2)
                    entry["ranking"] = rankings_list
                    self.collection.insert_one(entry)

    def run(self):
        for config in self.config["scienti_sources"]:
            print("Processing {} database".format(config["database_name"]))
            self.process_scienti(config, verbose=5)
        return 0
