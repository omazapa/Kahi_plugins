from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed
from kahi_impactu_utils.Utils import get_id_from_url


def process_one(oa_author, client, db_name, empty_person, related_works, max_tries=10):
    db = client[db_name]
    collection = db["person"]

    entry = empty_person.copy()
    entry["updated"].append({"source": "openalex", "time": int(time())})

    entry["full_name"] = oa_author["display_name"]

    for name in oa_author["display_name_alternatives"]:
        if not name.lower() in entry["aliases"]:
            entry["aliases"].append(name.lower())
    for source, idx in oa_author["ids"].items():
        idx = get_id_from_url(idx)
        if idx:
            entry["external_ids"].append(
                {"provenance": "openalex", "source": source, "id": idx})

    if "last_known_institution" in oa_author.keys():
        if oa_author["last_known_institution"]:
            aff_reg = None
            for source, idx in oa_author["last_known_institution"].items():
                aff_reg = db["affiliations"].find_one(
                    {"external_ids.id": idx})
                if aff_reg:
                    break
            if aff_reg:
                name = aff_reg["names"][0]["name"]
                for n in aff_reg["names"]:
                    if n["lang"] == "es":
                        name = n["name"]
                        break
                    elif n["lang"] == "en":
                        name = n["name"]
                entry["affiliations"].append({
                    "id": aff_reg["_id"],
                    "name": name,
                    "types": aff_reg["types"],
                    "start_date": -1,
                    "end_date": -1
                })
    for rwork in related_works:
        for key in rwork["ids"].keys():
            rec = {"provenance": "openalex",
                   "source": key, "id": rwork["ids"][key], "affiliations": rwork["authorships"]["institutions"], "year": rwork["publication_year"]}
            if rec not in entry["related_works"]:
                entry["related_works"].append(rec)
    collection.insert_one(entry)


class Kahi_openalex_person(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])

        self.openalex_client = MongoClient(
            config["openalex_person"]["database_url"])
        if config["openalex_person"]["database_name"] not in self.openalex_client.list_database_names():
            raise Exception("Database {} not found in {}".format(
                config["openalex_person"]['database_name'], config["openalex_person"]["database_url"]))
        self.openalex_db = self.openalex_client[config["openalex_person"]
                                                ["database_name"]]
        if config["openalex_person"]["collection_name"] not in self.openalex_db.list_collection_names():
            raise Exception("Collection {}.{} not found in {}".format(config["openalex_person"]['database_name'],
                                                                      config["openalex_person"]['collection_name'], config["openalex_person"]["database_url"]))
        self.openalex_collection = self.openalex_db[config["openalex_person"]
                                                    ["collection_name"]]
        self.openalex_collection_works = self.openalex_db[config["openalex_person"]
                                                          ["collection_name_works"]]
        self.openalex_collection_works.create_index("authorships.author.id")

        self.n_jobs = config["openalex_person"]["num_jobs"] if "num_jobs" in config["openalex_person"].keys(
        ) else 1
        self.verbose = config["openalex_person"]["verbose"] if "verbose" in config["openalex_person"].keys(
        ) else 0

    def related_works(self, author_id):
        return list(self.openalex_collection_works.aggregate([{"$project": {"authorships.author.id": 1, "ids": 1, "authorships.institutions.id": 1, "publication_year": 1}},
                                                              {"$match": {
                                                                  "authorships.author.id": author_id}},
                                                              {"$unwind": "$authorships"},
                                                              {"$match": {
                                                                  "authorships.author.id": author_id}},
                                                              ]))

    def process_openalex(self):
        author_cursor = self.openalex_collection.find(no_cursor_timeout=True)
        Parallel(
            n_jobs=self.n_jobs,
            verbose=self.verbose,
            backend="threading")(
            delayed(process_one)(
                author,
                self.client,
                self.config["database_name"],
                self.empty_person(),
                self.related_works(author["id"])
            ) for author in author_cursor
        )

    def run(self):
        self.process_openalex()
        self.client.close()
        return 0
