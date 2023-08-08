from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from time import time


class Kahi_ror_affiliations(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config
        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)
        self.db = self.client[config["database_name"]]
        self.collection = self.db["affiliations"]

        self.ror_client = MongoClient(config["ror_affiliations"]["database_url"])
        self.ror_db = self.ror_client[config["ror_affiliations"]["database_name"]]
        self.ror_collection = self.ror_db[config["ror_affiliations"]["collection_name"]]

        # logs for higher verbosity
        self.already_in_db = []
        self.inserted = []

    def process_ror(self, verbose=0):
        for inst in self.ror_collection.find():
            found_entry = self.collection.find_one(
                {"external_ids.id": inst["id"]})
            if found_entry:
                self.already_in_db.append(inst["id"])
                if verbose > 4:
                    print("Already in db: ",inst["id"])
                if verbose > 0:
                    print("Total already found: ",len(self.already_in_db))
                continue
                # may be updatable, check accordingly
            else:
                entry = self.empty_affiliations()
                entry["updated"].append({"time": int(time()), "source": "ror"})
                entry["names"].append({"name": inst["name"], "lang": "en"})
                entry["aliases"].extend(inst["aliases"])
                entry["abbreviations"].extend(inst["acronyms"])
                entry["year_established"] = int(
                    inst["established"]) if inst["established"] else -1
                entry["status"] = [inst["status"]]

                # types
                for typ in inst["types"]:
                    entry["types"].append({"source": "ror", "type": typ})

                # addresses
                for add in inst["addresses"]:
                    add_entry = {
                        "lat": add["lat"],
                        "lng": add["lng"],
                        "postcode": add["postcode"] if add["postcode"] else "",
                        "state": add["state"],
                        "city": add["city"],
                        "country": "",
                        "country_code": "",
                    }
                    entry["addresses"].append(add_entry)
                entry["addresses"][0]["country"] = inst["country"]["country_name"]
                entry["addresses"][0]["country_code"] = inst["country"]["country_code"]

                # external_urls
                if inst["links"]:
                    for link in inst["links"]:
                        url_entry = {"source": "site", "url": inst["links"][0]}
                        if url_entry not in entry["external_urls"]:
                            entry["external_urls"].append(url_entry)
                if inst["wikipedia_url"]:
                    entry["external_urls"].append(
                        {"source": "wikipedia", "url": inst["wikipedia_url"]})

                # external_ids
                if inst["external_ids"]:
                    for key, ext in inst["external_ids"].items():
                        if isinstance(ext["all"], list):
                            alll = ext["all"][0] if len(
                                ext["all"]) > 0 else ext["all"]
                            ext_entry = {"source": key.lower(), "id": alll}
                            if ext_entry not in entry["external_ids"]:
                                entry["external_ids"].append(ext_entry)
                entry["external_ids"].append(
                    {"source": "ror", "id": inst["id"]})
            response = self.collection.insert_one(entry)
            self.inserted.append(response.inserted_id)
            if verbose > 4:
                print("Inserted: ",response.inserted_id)
            if verbose > 0:
                print("Total inserted: ",len(self.inserted))

    def run(self):
        self.process_ror(verbose=5)
        return 0
