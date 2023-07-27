from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from datetime import datetime as dt
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

        self.scimago_file_paths = self.config["scimago_sources"]["file_path"]

        self.already_in_db = []

    def update_scimago(self, sjr, entry):
        _id = entry["_id"]
        del (entry["_id"])

        for upd in entry["updated"]:
            if upd["source"] == "scimago":
                entry["updated"].remove(upd)
                entry["updated"].append(
                    {"source": "scimago", "time": int(time())})
                break
        for name in entry["names"]:
            if name == sjr["Title"]:
                if name["source"] != "scimago":
                    entry["names"].append(
                        {"lang": "en", "name": sjr["Title"], "source": "scimago"})
        found_scimagoid = False
        for ext in entry["external_ids"]:
            if ext["source"] == "scimago":
                found_scimagoid = True
                break
        if not found_scimagoid:
            entry["external_ids"].append(
                {"source": "scimago", "id": str(sjr["Sourceid"])})
        found_scimago_type = False
        for typ in entry["types"]:
            if typ["source"] == "scimago":
                found_scimago_type = True
                break
        if not found_scimago_type:
            entry["types"].append({"source": "scimago", "type": sjr["Type"]})

        ids = [extid["id"] for extid in entry["external_ids"]]
        for extid in sjr["Issn"].split(","):
            extid = extid.strip()
            extid = extid[:4] + "-" + extid[4:]
            if extid not in ids:
                entry["external_ids"].append({"source": "issn", "id": extid})

        rankings = [(rank["source"], rank["from_date"], rank["to_date"])
                    for rank in entry["ranking"]]

        if ("scimago Best Quartile", self.scimago_start_ts, self.scimago_end_ts) not in rankings:
            entry["ranking"].append({
                "to_date": self.scimago_end_ts,
                "from_date": self.scimago_start_ts,
                "rank": sjr["SJR Best Quartile"],
                "order": int(sjr["Rank"]) if sjr["Rank"] else None,
                "source": "scimago Best Quartile"
            })
        if ("scimago hindex", self.scimago_start_ts, self.scimago_end_ts) not in rankings:
            entry["ranking"].append({
                "to_date": self.scimago_end_ts,
                "from_date": self.scimago_start_ts,
                "rank": int(sjr["H index"]),
                "order": int(sjr["Rank"]) if sjr["Rank"] else None,
                "source": "scimago hindex"
            })
        if ("scimago", self.scimago_start_ts, self.scimago_end_ts) not in rankings:
            if sjr["SJR"]:
                rank = ""
                if isinstance(sjr["SJR"], str):
                    rank = float(sjr["SJR"].replace(",", "."))
                else:
                    rank = sjr["SJR"]
                entry["ranking"].append({
                    "to_date": self.scimago_end_ts,
                    "from_date": self.scimago_start_ts,
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
        found_scimago_subjects = False
        scimago_subjetcs_index = -1
        for sub in entry["subjects"]:
            if sub["source"] == "scimago":
                found_scimago_subjects = True
                break
        if found_scimago_subjects:
            for sub in scimago_subjects:
                if sub not in entry["subjects"][scimago_subjetcs_index]["subjects"]:
                    entry["subjects"][scimago_subjetcs_index]["subjects"].append(
                        sub)
        else:
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
                entry["names"] = [
                    {"lang": "en", "name": sjr["Title"], "source":"scimago"}]
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
                    elif sjr["Country"] == "Vatican City State":
                        sjr["Country"] = "Holy See"
                    elif sjr["Country"] == "Tanzania":
                        sjr["Country"] = "Tanzania, United Republic of"
                    elif sjr["Country"] == "Bolivia":
                        sjr["Country"] = "Bolivia, Plurinational State of"

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
        for filename in self.scimago_file_paths:
            self.scimago_year = int(
                filename.replace(".csv", "").split(" ")[-1])
            self.scimago_start_ts = dt.strptime(
                "01 01 " + str(self.scimago_year), "%d %m %Y").timestamp()
            self.scimago_end_ts = dt.strptime(
                "31 12 " + str(self.scimago_year), "%d %m %Y").timestamp()
            self.scimago = read_csv(filename,
                                    sep=";", dtype={"Sourceid": str})
            self.process_scimago()
        return 0
