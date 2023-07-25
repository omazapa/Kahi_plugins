from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from time import time
from pandas import read_csv
import iso3166


class Kahi_scimago_sources(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["sources"]

        self.scimago_file_path = self.config["scimago_sources"]["file_path"]
        self.scimago = read_csv(self.scimago_file_path,
                                sep=";", dtype={"Sourceid": str})

        self.already_in_db = []

    def update_scimago(self, sjr, entry):
        _id = entry["_id"]
        del (entry["_id"])
        ids = [extid["id"] for extid in entry["external_ids"]]
        entry["updated"].append({"source": "scimago", "time": int(time())})
        for name in entry["names"]:
            if name == reg["title"]:
                entry["names"].append(
                    {"lang": "en", "name": reg["title"], "source": "scimago"})
        entry["external_ids"].append(
            {"source": "scimago", "id": str(sjr["Sourceid"])})
        entry["types"].append({"source": "scimago", "type": sjr["Type"]})
        for extid in sjr["Issn"].split(","):
            extid = extid.strip()
            extid = extid[:4] + "-" + extid[4:]
            if extid not in ids:
                entry["external_ids"].append({"source": "issn", "id": extid})
        entry["ranking"].append({
            "to_date": 1640995199,
            "from_date": 1640995199,
            "rank": sjr["SJR Best Quartile"],
            "order": int(sjr["Rank"]) if sjr["Rank"] else None,
            "source": "scimago Best Quartile"
        })
        entry["ranking"].append({
            "to_date": 1640995199,
            "from_date": 1640995199,
            "rank": int(sjr["H index"]),
            "order": int(sjr["Rank"]) if sjr["Rank"] else None,
            "source": "scimago hindex"
        })
        if sjr["SJR"]:
            rank = ""
            if isinstance(sjr["SJR"], str):
                rank = float(sjr["SJR"].replace(",", "."))
            else:
                rank = sjr["SJR"]
            entry["ranking"].append({
                "to_date": 1640995199,
                "from_date": 1640995199,
                "rank": rank,
                "order": int(sjr["Rank"]) if sjr["Rank"] else None,
                "source": "scimago"
            })
        scimago_subjects = []
        for cat in sjr["Categories"].split(";"):
            scimago_subjects.append({
                "id": "",
                "name": cat.split(" (")[0],
                "level": None,
                "external_ids": []
            })
        entry["subjects"].append({
            "source": "scimago",
            "subjects": scimago_subjects
        })
        self.collection.update_one({"_id": _id}, {"$set": entry})

    def process_scimago(self):
        for issn_list in self.scimago["Issn"].unique():
            db_found = False
            db_reg = None
            ext_ids = []
            found_issn = None
            for issn in issn_list.split(","):
                issn = issn.strip()
                extid = issn[:4] + "-" + issn[4:]
                db_reg = self.collection.find_one({"external_ids.id": extid})
                if db_reg:
                    found_issn = issn
                    db_found = True
                    break
                ext_ids.append({"source": "issn", "id": extid})
            if db_found:
                self.already_in_db.append(found_issn)
                sjr = self.scimago[self.scimago["Issn"] == issn_list]
                sjr = sjr.iloc[0]
                self.update_scimago(sjr, db_reg)
            else:
                entry = self.empty_source()
                entry["updated"] = [{"source": "scimago", "time": int(time())}]
                sjr = self.scimago[self.scimago["Issn"] == issn_list]
                sjr = sjr.iloc[0]
                entry["types"].append(
                    {"source": "scimago", "type": sjr["Type"]})
                entry["external_ids"] = ext_ids
                entry["external_ids"].append(
                    {"source": "scimago", "id": int(sjr["Sourceid"])})
                entry["names"] = [{"lang": "en", "name": sjr["Title"],"source":"scimago"}]
                country = None
                try:
                    if sjr["Country"] == "United States":
                        sjr["Country"] = "United States of America"
                    elif sjr["Country"] == "United Kingdom":
                        sjr["Country"] = "United Kingdom of Great Britain and Northern Ireland"
                    elif sjr["Country"] == "South Korea":
                        sjr["Country"] = "Korea, Republic of"
                    elif sjr["Country"] == "Czech Republic":
                        sjr["Country"] = "Czechia"
                    elif sjr["Country"] == "Taiwan":
                        sjr["Country"] = "Taiwan, Province of China"
                    elif sjr["Country"] == "Iran":
                        sjr["Country"] = "Iran, Islamic Republic of"
                    elif sjr["Country"] == "Moldova":
                        sjr["Country"] = "Moldova, Republic of"
                    elif sjr["Country"] == "Venezuela":
                        sjr["Country"] = "Venezuela, Bolivarian Republic of"
                    elif sjr["Country"] == "Macedonia":
                        sjr["Country"] = "North Macedonia"
                    elif sjr["Country"] == "Palestine":
                        sjr["Country"] = "Palestine, State of"
                    elif sjr["Country"] == "Tanzania":
                        sjr["Country"] = "Tanzania, United Republic of"

                    country = iso3166.countries_by_name.get(
                        sjr["Country"].upper()).alpha2
                except Exception as e:
                    print(e)
                    print(sjr["Country"])
                if country:
                    entry["publisher"] = {
                        "country_code": country, "name": sjr["Publisher"]}
                entry["ranking"].append({
                    "to_date": 1640995199,
                    "from_date": 1640995199,
                    "rank": sjr["SJR Best Quartile"],
                    "order": int(sjr["Rank"]) if sjr["Rank"] else None,
                    "source": "Scimago Best Quartile"
                })
                entry["ranking"].append({
                    "to_date": 1640995199,
                    "from_date": 1640995199,
                    "rank": int(sjr["H index"]),
                    "order": int(sjr["Rank"]) if sjr["Rank"] else None,
                    "source": "Scimago hindex"
                })
                if sjr["SJR"]:
                    rank = ""
                    if isinstance(sjr["SJR"], str):
                        rank = float(sjr["SJR"].replace(",", "."))
                    else:
                        rank = sjr["SJR"]
                    entry["ranking"].append({
                        "to_date": 1640995199,
                        "from_date": 1640995199,
                        "rank": rank,
                        "order": int(sjr["Rank"]) if sjr["Rank"] else None,
                        "source": "Scimago"
                    })
                scimago_subjects = []
                for cat in sjr["Categories"].split(";"):
                    scimago_subjects.append({
                        "id": "",
                        "name": cat.split(" (")[0],
                        "level": None,
                        "external_ids": []
                    })
                entry["subjects"].append({
                    "source": "Scimago",
                    "subjects": scimago_subjects
                })
                self.collection.insert_one(entry)

    def run(self):
        self.process_scimago()
        return 0
