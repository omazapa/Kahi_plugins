from kahi.KahiBase import KahiBase
from pymongo import MongoClient


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

        self.impactu_database_url = config["impactu_post_cites_count"]["database_url"]
        self.impactu_database_name = config["impactu_post_cites_count"]["database_name"]
        self.verbose = self.config["impactu_post_cites_count"]["verbose"]

        self.client = MongoClient(self.mongodb_url)
        self.db = self.client[self.database_name]

        self.impactu_client = MongoClient(self.impactu_database_url)
        self.impactu_db = self.impactu_client[self.impactu_database_name]

    def count_cites_person(self):
        """
        Method to calculate the cites count for each person.
        """

        person_ids = self.db.person.find({}, {"_id"})
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
            ret = list(self.db.kahi.works.aggregate(pipeline))
            rec = {"citations_count": []}
            for cites in ret:
                rec["citations_count"] += [{"source": cites["_id"],
                                            "count": cites["count"]}]
            self.impactu_db.person.update_one(
                {"_id": pid["_id"]}, {"$set": rec}, upsert=True)

    def count_cites_institutions(self,):
        """
        Method to calculate the cites count for each institution.
        """
        aff_ids = self.db.affiliations.find(
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
            ret = list(self.db.works.aggregate(pipeline))
            rec = {"citations_count": []}
            for cites in ret:
                rec["citations_count"] += [{"source": cites["_id"],
                                            "count": cites["count"]}]
            self.impactu_db.affiliations.update_one(
                {"_id": pid["_id"]}, {"$set": rec}, upsert=True)

    def count_cites_faculty_department_group(self):
        """
        Method to calculate the cites count for each faculty, department and group.
        """
        aff_ids = self.db.affiliations.find(
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
            ret = list(self.db.person.aggregate(pipeline))
            rec = {"citations_count": []}
            for cites in ret:
                rec["citations_count"] += [{"source": cites["_id"],
                                            "count": cites["count"]}]
            self.impactu_db.affiliations.update_one(
                {"_id": pid["_id"]}, {"$set": rec}, upsert=True)

    def run(self):
        self.count_cites_person()
        self.count_cites_institutions()
        self.count_cites_faculty_department_group()
