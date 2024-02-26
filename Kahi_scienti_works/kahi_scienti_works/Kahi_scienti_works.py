from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from joblib import Parallel, delayed
from kahi_scienti_works.process_one import process_one
from mohan.Similarity import Similarity


class Kahi_scienti_works(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["works"]

        self.collection.create_index("year_published")
        self.collection.create_index("authors.affiliations.id")
        self.collection.create_index("authors.id")
        self.collection.create_index([("titles.title", TEXT)])
        self.collection.create_index("external_ids.id")
        if "es_index" in config["scienti_works"].keys() and "es_url" in config["scienti_works"].keys() and "es_user" in config["scienti_works"].keys() and "es_password" in config["scienti_works"].keys():
            es_index = config["scienti_works"]["es_index"]
            es_url = config["scienti_works"]["es_url"]
            if config["scienti_works"]["es_user"] and config["scienti_works"]["es_password"]:
                es_auth = (config["scienti_works"]["es_user"],
                           config["scienti_works"]["es_password"])
            else:
                es_auth = None
            self.es_handler = Similarity(
                es_index, es_uri=es_url, es_auth=es_auth)
        else:
            self.es_handler = None
            print("WARNING: No elasticsearch configuration provided")

        self.task = config["scienti_works"]["task"]

        self.n_jobs = config["scienti_works"]["num_jobs"] if "num_jobs" in config["scienti_works"].keys(
        ) else 1
        self.verbose = config["scienti_works"]["verbose"] if "verbose" in config["scienti_works"].keys(
        ) else 0

        # checking if the databases and collections are available
        self.check_databases_and_collections()

    def check_databases_and_collections(self):
        for db_info in self.config["scienti_works"]["databases"]:
            client = MongoClient(db_info["database_url"])
            if db_info['database_name'] not in client.list_database_names():
                raise Exception("Database {} not found".format(
                    db_info['database_name']))
            if db_info['collection_name'] not in client[db_info['database_name']].list_collection_names():
                raise Exception("Collection {}.{} not found".format(db_info['database_name'],
                                                                    db_info['collection_name']))
            client.close()

    def process_doi_group(self, group, db, collection, empty_work, es_handler, verbose=0):
        for i in group["ids"]:
            reg = collection.find_one({"_id": i})
            process_one(reg, db, collection, empty_work, es_handler, verbose=0)

    def process_scienti(self, db, collection, config):
        client = MongoClient(config["database_url"])
        scienti = client[config["database_name"]][config["collection_name"]]
        if self.task == "doi":
            pipeline = [
                {"$match": {"TXT_DOI": {"$ne": None}}},
                {"$project": {"doi": {"$trim": {"input": "$TXT_DOI"}}}},
                {"$project": {"doi": {"$toLower": "$doi"}}},
                {"$group": {"_id": "$doi", "ids": {"$push": "$_id"}}}
            ]
            paper_group_doi_cursor = scienti.aggregate(
                pipeline)  # update for doi and not doi
            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading")(
                delayed(self.process_doi_group)(
                    doi_group,
                    db,
                    collection,
                    self.empty_work(),
                    self.es_handler,
                    verbose=self.verbose
                ) for doi_group in paper_group_doi_cursor
            )
        else:
            paper_cursor = scienti.find(
                {"$or": [{"doi": {"$eq": ""}}, {"doi": {"$eq": None}}]})
            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading")(
                delayed(self.process_doi_group)(
                    [doi_group],  # trick to make the function signature compatible
                    db,
                    collection,
                    self.empty_work(),
                    self.es_handler,
                    verbose=self.verbose
                ) for doi_group in paper_cursor
            )
        client.close()

    def run(self):
        for config in self.config["scienti_works"]["databases"]:
            if self.verbose > 0:
                print("Processing {}.{} database".format(
                    config["database_name"], config["collection_name"]))
            if self.verbose > 4:
                print("Updating already inserted entries")
            self.process_scienti(self.db, self.collection, config)
        return 0
