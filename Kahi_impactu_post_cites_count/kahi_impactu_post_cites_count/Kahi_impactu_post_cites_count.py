from kahi.KahiBase import KahiBase
from pymongo import MongoClient
import time
from joblib import Parallel, delayed


class Kahi_impactu_post_cites_count(KahiBase):
    """
    This class is a plugin for Kahi that calculates the cites count for each person, institution, faculty, department and group.
    This plugin is intended to be used after ETL calculation of the impactu plugins.
    """
    config = {}

    def __init__(self, config):
        """
        Constructor for the class
        :param config: Configuration dictionary

        Example of configuration:
        ```
        config:
            database_url: localhost:27017
            database_name: kahi
            log_database: kahi
            log_collection: log
        workflow:
            impactu_post_cites_count:
                database_url: localhost:27017
                database_name: kahi_calculations
                verbose: 5
        ```
        """
        self.config = config
        self.mongodb_url = config["database_url"]
        self.database_name = config["database_name"]

        self.verbose = self.config["impactu_post_cites_count"]["verbose"]

        self.client = MongoClient(self.mongodb_url)
        self.db = self.client[self.database_name]
        self.works_collection = self.db["works"]
        self.person_collection = self.db["person"]
        self.affiliations_collection = self.db["affiliations"]

    def count_cites_person(self):
        """
        Method to calculate the cites count for each person.
        """

        person_ids = self.person_collection.find({}, {"_id"})
        if self.verbose > 0:
            print("Calculating cites count for person")
        for pid in person_ids:
            pipeline = [
                {
                    "$match": {
                        "authors.id": pid["_id"],
                    },
                },
                {"$project": {"citations_count": 1}},
                {"$unwind": "$citations_count"},
                {
                    "$group": {
                        "_id": "$citations_count.source",
                        "count": {"$sum": "$citations_count.count"},
                    },
                },
            ]
            ret = list(self.works_collection.aggregate(pipeline))
            rec = {"citations_count": []}
            for cites in ret:
                rec["citations_count"] += [{"source": cites["_id"],
                                            "count": cites["count"]}]
            self.person_collection.update_one(
                {"_id": pid["_id"]}, {"$set": rec}, upsert=True)

    def count_cites_institutions(self):
        """
        Method to calculate the cites count for each institution.
        """
        aff_ids = self.affiliations_collection.find(
            {"types.type": {"$nin": ["department", "faculty", "group"]}}, {"_id"})
        if self.verbose > 0:
            print("Calculating cites count for institutions")
        for pid in aff_ids:
            pipeline = [
                {
                    "$match": {
                        "authors.affiliations.id": pid["_id"],
                    },
                },
                {"$project": {"citations_count": 1}},
                {"$unwind": "$citations_count"},
                {
                    "$group": {
                        "_id": "$citations_count.source",
                        "count": {"$sum": "$citations_count.count"},
                    },
                },
            ]
            ret = list(self.works_collection.aggregate(pipeline))
            rec = {"citations_count": []}
            for cites in ret:
                rec["citations_count"] += [{"source": cites["_id"],
                                            "count": cites["count"]}]
            self.affiliations_collection.update_one(
                {"_id": pid["_id"]}, {"$set": rec}, upsert=True)

    def count_cites_faculty_department_group(self):
        """
        Method to calculate the cites count for each faculty, department and group.
        """
        aff_ids = self.affiliations_collection.find(
            {"types.type": {"$in": ["department", "faculty", "group"]}}, {"_id"})
        if self.verbose > 0:
            print("Calculating cites count for faculty, department and group")
        for pid in aff_ids:
            pipeline = [{"$match": {"affiliations.id": pid["_id"]}},
                        {"$project": {"_id": 1}},
                        {
                "$lookup": {
                    "from": "works",
                            "localField": "_id",
                            "foreignField": "authors.id",
                            "as": "works",
                }
            },
                {"$unwind": "$works"},
                {"$group": {"_id": "$works._id", "works": {"$first": "$works"}}},
                {"$project": {"works.citations_count": 1}},
                {"$unwind": "$works.citations_count"},
                {
                "$group": {
                    "_id": "$works.citations_count.source",
                    "count": {"$sum": "$works.citations_count.count"},
                },
            },
            ]
            ret = list(self.person_collection.aggregate(pipeline))
            rec = {"citations_count": []}
            for cites in ret:
                rec["citations_count"] += [{"source": cites["_id"],
                                            "count": cites["count"]}]
            self.affiliations_collection.update_one(
                {"_id": pid["_id"]}, {"$set": rec}, upsert=True)

    def run_cites_count(self):
        """
        Method to run the cites count calculation for each person, institution, faculty, department and group.
        """
        person_ids = self.person_collection.find({}, {"_id"})
        with MongoClient(self.mongodb_url) as client:
            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading")(
                delayed(self.count_cites_person)(
                    reg,
                    self.verbose
                ) for reg in person_ids
            )
            client.close()
    
    def run(self):
        start_time = time.time()
        
        self.count_cites_person()
        end_time = time.time()
        duration = end_time - start_time
        if self.verbose > 0:
            print(f"Cites count calculation completed in {duration:.2f} seconds.")

        start_time = time.time()
        self.count_cites_institutions()
        end_time = time.time()
        duration = end_time - start_time
        if self.verbose > 0:
            print(f"Cites count calculation completed in {duration:.2f} seconds.")

        start_time = time.time()
        self.count_cites_faculty_department_group()
        end_time = time.time()
        duration = end_time - start_time
        if self.verbose > 0:
            print(f"Cites count calculation completed in {duration:.2f} seconds.")
        
