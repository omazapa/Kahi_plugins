from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from time import time
from joblib import Parallel, delayed


def process_one(kahi_col, wikid_col, inst, verbose):
    """

    """
    for name in inst["names"]:
        if name["source"] == "wikidata":
            return
    wikidata_id = None
    for j in inst["external_ids"]:
        if j["source"] == "wikidata":
            wikidata_id = j["id"]
            break
    rec = wikid_col.find_one({"id": wikidata_id}, {
                             "labels": 1, "claims.P18.mainsnak.datavalue.value": 1})

    if not rec:
        if verbose > 4:
            print(f"WARNING: record with id {wikidata_id} not found")
        return
    for lang in rec["labels"].keys():
        name = {"name": rec["labels"][lang]["value"], "lang": lang,
                "source": "wikidata", "provenance": "wikidata"}
        inst["names"].append(name)
    if "P18" in rec["claims"].keys() and len(rec["claims"]["P18"]) > 0:
        img = rec["claims"]["P18"][0]["mainsnak"]["datavalue"]["value"]
        url_img = f"https://commons.wikimedia.org/w/index.php?title=Special:Redirect/file/{img}&width=300"
        inst["external_urls"].append(
            {"provenance": "wikidata", "source": "logo", "url": url_img})
    inst["updated"].append({"source": "wikidata", "time": int(time())})
    kahi_col.update_one({"_id": inst["_id"]}, {
        "$set": {"names": inst["names"], "external_urls": inst["external_urls"], "updated": inst["updated"]}})


class Kahi_wikidata_affiliations(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["affiliations"]

        self.wikidata_db = self.client[config["wikidata_affiliations"]
                                       ["database_name"]]
        self.wikidata_col = self.wikidata_db[config["wikidata_affiliations"]
                                             ["collection_name"]]

        self.n_jobs = config["wikidata_affiliations"]["num_jobs"]

        self.verbose = config["wikidata_affiliations"]["verbose"] if "verbose" in config["wikidata_affiliations"].keys(
        ) else 0

        self.wikidata_col.create_index("id")
        self.wikidata_col.create_index(
            "claims.P31.mainsnak.datavalue.value.id")

    def process_wikidata(self):
        institutions = list(self.collection.find(
            {"external_ids.source": "wikidata"}))

        Parallel(
            n_jobs=self.n_jobs,
            backend="threading",
            verbose=10
        )(delayed(process_one)(
            self.collection, self.wikidata_col,
            inst, self.verbose) for inst in institutions)

    def run(self):
        self.process_wikidata()
        return 0
