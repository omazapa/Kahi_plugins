from kahi.KahiBase import KahiBase
from pymongo import MongoClient, ASCENDING
from time import time
from langid import classify
from kahi_impactu_utils.Utils import check_date_format


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

        self.create_source_indexes()

    def create_source_indexes(self):
        for db_info in self.config["scienti_sources"]:
            database_url = db_info.get('database_url', '')
            database_name = db_info.get('database_name', '')
            collection_name = db_info.get('collection_name', '')

            if database_url and database_name and collection_name:
                client = MongoClient(database_url)
                db = client[database_name]
                collection = db[collection_name]

                collection.create_index([("details.article.journal_others.TXT_ISSN", ASCENDING)])
                collection.create_index([("details.article.journal.TXT_ISSN_SEP", ASCENDING)])
                client.close()

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
            if "details" not in reg_scienti.keys():
                continue
            for detail in reg_scienti["details"]:
                if "article" in detail.keys():
                    paper = detail["article"][0]
                    if "journal" in paper.keys():
                        journal = paper["journal"][0]
                        break

            if journal is None:
                continue
            if "TPO_CLASIFICACION" not in journal.keys():
                continue
            DTA_CREACION = check_date_format(paper["DTA_CREACION"])
            if not journal["TPO_CLASIFICACION"] in ranks:
                ranking = {
                    "from_date": DTA_CREACION,
                    "to_date": DTA_CREACION,
                    "rank": journal["TPO_CLASIFICACION"],
                    "issn": issn,
                    "order": None,
                    "source": "scienti"
                }
                rankings_list.append(ranking)
                ranks.append(journal["TPO_CLASIFICACION"])
                dates_tuple = (DTA_CREACION, DTA_CREACION)

                dates.append(dates_tuple)
            else:
                idx = ranks.index(journal["TPO_CLASIFICACION"])
                date1, date2 = dates[idx]

                try:
                    if date1 > DTA_CREACION:
                        date1 = DTA_CREACION
                    if date2 < DTA_CREACION:
                        date2 = DTA_CREACION
                    dates[idx] = (date1, date2)
                except Exception as e:
                    print(e)
                    date1 = ''
                    date2 = ''

                dates[idx] = ('', '')

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
        issn_list = list(self.scienti_collection.distinct(
            "details.article.journal.TXT_ISSN_SEP"))
        issn_list.extend(self.scienti_collection.distinct(
            "details.article.journal_others.TXT_ISSN"))
        for issn in set(issn_list):
            reg_db = self.collection.find_one({"external_ids.id": issn})
            if reg_db:
                reg_scienti = self.scienti_collection.find_one(
                    {"details.article.journal.TXT_ISSN_SEP": issn})
                if reg_scienti:
                    self.update_scienti(reg_scienti, reg_db, issn)
                else:
                    reg_scienti = self.scienti_collection.find_one(
                        {"details.article.journal_others.TXT_ISSN": issn})
                    if reg_scienti:
                        self.update_scienti(reg_scienti, reg_db, issn)
            else:
                reg_scienti = self.scienti_collection.find_one(
                    {"details.article.journal.TXT_ISSN_SEP": issn})
                if not reg_scienti:
                    reg_scienti = self.scienti_collection.find_one(
                        {"details.article.journal_others.TXT_ISSN": issn})
                if reg_scienti:
                    journal = None
                    if "details" not in reg_scienti.keys():
                        continue
                    for detail in reg_scienti["details"]:
                        if "article" in detail.keys():
                            paper = detail["article"][0]
                            if "journal" in paper.keys():
                                journal = paper["journal"][0]
                                break
                            elif "journal_others" in paper.keys():
                                journal = paper["journal_others"][0]
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
                        {"source": "issn", "id": journal["TXT_ISSN_SEP"] if "TXT_ISSN_SEP" in journal.keys() else journal["TXT_ISSN"]})
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
                        if "details" not in reg_scienti.keys():
                            continue
                        for detail in reg_scienti["details"]:
                            if "article" in detail.keys():
                                paper = detail["article"][0]
                                if "journal" in paper.keys():
                                    journal = paper["journal"][0]
                                    break
                        if journal:
                            if "TPO_CLASIFICACION" not in journal.keys():
                                continue
                            DTA_CREACION = check_date_format(paper["DTA_CREACION"])

                            if not journal["TPO_CLASIFICACION"] in ranks:
                                from_date = DTA_CREACION
                                to_date = DTA_CREACION
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
                                dates_tuple = (from_date, to_date)
                                dates.append(dates_tuple)
                            else:
                                # if is already ranked but dates changed
                                idx = ranks.index(journal["TPO_CLASIFICACION"])
                                date1, date2 = dates[idx]
                                try:
                                    if date1 > DTA_CREACION:
                                        date1 = DTA_CREACION
                                    if date2 < DTA_CREACION:
                                        date2 = DTA_CREACION
                                except Exception as e:
                                    print(e)
                                    date1 = ''
                                    date2 = ''

                                dates[idx] = (date1, date2)
                    entry["ranking"] = rankings_list
                    self.collection.insert_one(entry)

    def run(self):
        start_time = time()
        for config in self.config["scienti_sources"]:
            print("Processing {} database".format(config["database_name"]))
            self.process_scienti(config, verbose=5)
        print("Execution time: {} minutes".format(round((time() - start_time) / 60, 2)))
        return 0
