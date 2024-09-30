from kahi.KahiBase import KahiBase
from pymongo import MongoClient
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
                num_jobs: 20
                verbose: 5
        ```
        """
        self.config = config
        self.mongodb_url = config["database_url"]
        self.database_name = config["database_name"]

        self.n_jobs = self.config["impactu_post_cites_count"]["num_jobs"]
        self.verbose = self.config["impactu_post_cites_count"]["verbose"]

        self.client = MongoClient(self.mongodb_url)
        self.db = self.client[self.database_name]
        self.works_collection = self.db["works"]
        self.person_collection = self.db["person"]
        self.affiliations_collection = self.db["affiliations"]

    def count_cites_products_person(self, pid):
        """
        Method to calculate the citation and product count for each author.
        """
        # Count cites for each author
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

        # Count products for each author
        count = self.works_collection.count_documents({"authors.id": pid["_id"]})
        rec["products_count"] = count

        # Update the person collection
        self.person_collection.update_one(
            {"_id": pid["_id"]}, {"$set": rec}, upsert=True)

    def count_cites_products_institutions(self, pid):
        """
        Method to calculate the citation and product count for each institution.
        """
        # Count cites for each institution
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
        # Count products for each institution
        count = self.works_collection.count_documents(
            {"authors.affiliations.id": pid["_id"]})
        rec["products_count"] = count

        # Update the institution collection
        self.affiliations_collection.update_one(
            {"_id": pid["_id"]}, {"$set": rec}, upsert=True)

    def count_cites_products_faculty_department_group(self, pid):
        """
        Method to calculate the citation and product count for each faculty, department and group.
        """
        # Count cites for each faculty, department and group
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
        # Count products for each faculty, department and group
        count = self.works_collection.count_documents(
            {"authors.affiliations.id": pid["_id"]})
        rec["products_count"] = count

        # Update the faculty, department and group collection
        self.affiliations_collection.update_one(
            {"_id": pid["_id"]}, {"$set": rec}, upsert=True)

    def run_cites_count(self):
        """
        Method to run the cites and products count calculation for each person, institution, faculty, department and group.
        """

        # Count cites for each author
        person_ids = list(self.person_collection.find({}, {"_id"}))
        if self.verbose > 0:
            print("Calculating cites and products count for {} authors".format(
                len(person_ids)))
        with MongoClient(self.mongodb_url) as client:
            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading")(
                delayed(self.count_cites_products_person)(
                    reg,
                ) for reg in person_ids
            )
            client.close()

        # Count cites for each institution
        aff_ids = list(self.affiliations_collection.find(
            {"types.type": {"$nin": ["department", "faculty", "group"]}}, {"_id"}))
        if self.verbose > 0:
            print("Calculating cites count and products for {} institutions".format(
                len(aff_ids)))
        with MongoClient(self.mongodb_url) as client:
            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading")(
                delayed(self.count_cites_products_institutions)(
                    reg,
                ) for reg in aff_ids
            )
            client.close()

        # Count cites for each faculty, department and group
        aff_ids = list(self.affiliations_collection.find(
            {"types.type": {"$in": ["department", "faculty", "group"]}}, {"_id"}))
        if self.verbose > 0:
            print("Calculating cites and products count for {} faculties, departments and groups".format(
                len(aff_ids)))
        with MongoClient(self.mongodb_url) as client:
            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading")(
                delayed(self.count_cites_products_faculty_department_group)(
                    reg,
                ) for reg in aff_ids
            )
            client.close()

    def run(self):
        self.run_cites_count()
        self.client.close()
