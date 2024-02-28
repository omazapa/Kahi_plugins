from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from joblib import Parallel, delayed
from kahi_scienti_works.process_one import process_one
from mohan.Similarity import Similarity


class Kahi_scienti_works(KahiBase):

    config = {}

    def __init__(self, config):
        """
        Constructor for the Kahi_scienti_works class.

        Several indices are created in the MongoDB collection to speed up the queries.
        We also handle the error to check db and collection existence.

        Parameters
        ----------
        config : dict
            The configuration dictionary. It should contain the following keys:
            - scienti_works: a dictionary with the following keys:
                - task: the task to be performed. It can be "doi" or "all"
                - num_jobs: the number of jobs to be used in parallel processing
                - verbose: the verbosity level
                - databases: a list of dictionaries with the following keys:
                    - database_url: the URL for the MongoDB database
                    - database_name: the name of the database
                    - collection_name: the name of the collection
                    - es_index: the name of the Elasticsearch index
                    - es_url: the URL for the Elasticsearch server
                    - es_user: the username for the Elasticsearch server
                    - es_password: the password for the Elasticsearch server
        """
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
        if "es_index" in config["scienti_works"].keys() and "es_url" in config["scienti_works"].keys() and "es_user" in config["scienti_works"].keys() and "es_password" in config["scienti_works"].keys():  # noqa: E501
            es_index = config["scienti_works"]["es_index"]
            es_url = config["scienti_works"]["es_url"]
            if config["scienti_works"]["es_user"] and config["scienti_works"]["es_password"]:
                es_auth = (config["scienti_works"]["es_user"],
                           config["scienti_works"]["es_password"])
            else:
                es_auth = None
            self.es_handler = Similarity(
                es_index, es_uri=es_url, es_auth=es_auth)
            print("INFO: ES handler created successfully")
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
        """
        Method to check if the databases and collections are available.
        """
        for db_info in self.config["scienti_works"]["databases"]:
            client = MongoClient(db_info["database_url"])
            if db_info['database_name'] not in client.list_database_names():
                raise Exception("Database {} not found".format(
                    db_info['database_name']))
            if db_info['collection_name'] not in client[db_info['database_name']].list_collection_names():
                raise Exception("Collection {}.{} not found".format(db_info['database_name'],
                                                                    db_info['collection_name']))
            client.close()

    def process_doi_group(self, group, db, collection, collection_scienti, empty_work, es_handler, similarity, verbose=0):
        """
        This method processes a group of documents with the same DOI.
        This allows to process the documents in parallel without having to worry about the DOI being processed more than once.

        Parameters
        ----------
        group : dict
            A dictionary with the group of documents to be processed. It should have the following keys:
            - _id: the DOI
            - ids: a list with the IDs of the documents
        db : Database
            The MongoDB database to be used. (colav database genrated by the kahi)
        collection : Collection
            The MongoDB collection to be used. (works collection genrated by the kahi)
        collection_scienti : Collection
            The MongoDB collection with the scienti data.
        empty_work : dict
            A template for the work entry. Structure is defined in the schema.
        es_handler : Similarity
            The Elasticsearch handler to be used for similarity checks. Take a look in Mohan package.
        similarity : bool
            A flag to indicate if similarity checks should be performed if doi is not available.
        verbose : int
            The verbosity level. Default is 0.
        """
        for i in group["ids"]:
            reg = collection_scienti.find_one({"_id": i})
            process_one(reg, db, collection, empty_work,
                        es_handler, similarity, verbose)

    def process_scienti(self, db, collection, config):
        """
        Method to process the scienti database.
        Checks if the task is "doi" or not and processes the documents accordingly.

        Parameters:
        -----------
        db : Database
            The MongoDB database to be used. (colav database genrated by the kahi)
        collection : Collection
            The MongoDB collection to be used. (works collection genrated by the kahi)
        config : dict
            A dictionary with the configuration for the scienti database. It should have the following keys:
            - database_url: the URL for the MongoDB database
            - database_name: the name of the database
            - collection_name: the name of the collection
            - es_index: the name of the Elasticsearch index
            - es_url: the URL for the Elasticsearch server
            - es_user: the username for the Elasticsearch server
            - es_password: the password for the Elasticsearch server
        """
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
                    scienti,
                    self.empty_work(),
                    self.es_handler,
                    similarity=False,
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
                delayed(process_one)(
                    work,
                    db,
                    collection,
                    self.empty_work(),
                    self.es_handler,
                    similarity=True,
                    verbose=self.verbose
                ) for work in paper_cursor
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
