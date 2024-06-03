from kahi_impactu_utils.Utils import compare_author
from pymongo import MongoClient, TEXT
from joblib import Parallel, delayed
from kahi.KahiBase import KahiBase
from bson import ObjectId
from time import time
import copy


class Kahi_authors_unicity(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]
        self.client = MongoClient(config["database_url"])
        self.db = self.client[config["database_name"]]

        if config["authors_unicity"]["collection_name"] not in self.db.list_collection_names():
            raise Exception("Collection {} not found in {}".format(
                config["authors_unicity"]['collection_name'], config["authors_unicity"]["database_url"]))
        self.collection = self.db[config["authors_unicity"]["collection_name"]]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])

        self.authors_threshold = config["authors_unicity"]["max_authors_threshold"] if "max_authors_threshold" in config["authors_unicity"].keys(
        ) else 0

        self.n_jobs = config["authors_unicity"]["num_jobs"] if "num_jobs" in config["authors_unicity"].keys(
        ) else 1

        self.verbose = config["authors_unicity"][
            "verbose"] if "verbose" in config["authors_unicity"].keys() else 0

    # Function to merge affiliations

    def merge_affiliations(self, target_doc, doc):
        if 'affiliations' in doc:
            if 'affiliations' not in target_doc:
                target_doc['affiliations'] = []
            target_affiliation_ids = {aff['id']
                                      for aff in target_doc['affiliations']}
            for aff in doc['affiliations']:
                if aff['id'] not in target_affiliation_ids:
                    target_doc['affiliations'].append(copy.deepcopy(aff))

    # Function to merge list fields

    def merge_lists(self, target_ids, source_ids):
        for item in source_ids:
            if item not in target_ids:
                target_ids.append(item)

    # Function to merge other fields

    def merge_fields(self, target, source, fields):
        for field in fields:
            if not target[field]:
                target[field] = source[field]

    # Function to merge, store and delete documents

    def merge_documents(self, authors_docs, target_doc, collection):
        target_id = target_doc["_id"]

        for doc in authors_docs:
            if doc['_id'] != target_id:
                # updated
                target_update_sources = {profile["source"]
                                         for profile in target_doc["updated"]}
                for source in doc["updated"]:
                    if source["source"] not in target_update_sources:
                        target_doc["updated"].append(
                            {"source": source["source"], "time": int(time())})

                # full_name
                if len(doc["full_name"]) > len(target_doc["full_name"]):
                    target_doc["full_name"] = doc["full_name"]

                # first_names, last_names, initials, sex, marital_status, birthplace, birthdate
                self.merge_fields(target_doc, doc, [
                                  "first_names", "last_names", "initials", "keywords", "sex", "marital_status", "birthplace", "birthdate"])

                # aliases, external_ids, ranking, degrees, subjects, related_works
                fields = ["aliases", "external_ids", "ranking",
                          "degrees", "subjects", "related_works"]
                for field in fields:
                    self.merge_lists(target_doc[field], doc[field])
                # affiliations
                self.merge_affiliations(target_doc, doc)

        # Update the target document with new external ids
        collection.update_one({"_id": target_id}, {"$set": target_doc})

        # Delete all other documents that are not the target_id
        other_ids = [doc['_id']
                     for doc in authors_docs if doc['_id'] != target_id]
        collection.delete_many({"_id": {"$in": other_ids}})

        return

    # Find the target document based on the 'provenance' of 'external_ids'
    def find_target_doc(self, author_docs, _id):
        target_doc = None

        # Define the priority order of provenance to search for
        priority_order = ['staff', 'scienti', 'minciencias']
        for provenance in priority_order:
            for doc in author_docs:
                for ext_id in doc.get('external_ids', []):
                    if ext_id['provenance'] == provenance:
                        target_doc = doc
                        break
                if target_doc:
                    break
            if target_doc:
                break

        # Handle the case where _id is 'orcid'
        if not target_doc and _id == 'orcid':
            target_doc = author_docs[0]
        # Handle the case where _id is 'doi'
        elif not target_doc and _id == 'doi':
            target_doc = max(author_docs, key=lambda doc: len(
                doc.get('full_name', '')))

        return target_doc

    # Function to process authors unicity based on ORCID

    def orcid_unicity(self, reg, collection, verbose=0):
        # Fetch all author documents by given IDs
        author_ids = reg["document_ids"]
        author_docs = list(collection.find(
            {"_id": {"$in": [ObjectId(aid) for aid in author_ids]}}))
        if not author_docs:
            print("No authors found with the provided IDs.")
            return

        target_doc = self.find_target_doc(author_docs, "orcid")
        if target_doc:
            self.merge_documents(author_docs, target_doc, collection)

    # Function to compare authors based on DOI

    def doi_unicity(self, reg, collection, verbose=0):
        # Fetch author documents from the database
        author_ids = reg["authors"]
        author_docs = list(collection.find({"_id": {"$in": author_ids}}))

        if not author_docs:
            return

        author_found = None
        # Compare each author with others
        for author in author_docs:
            for other_author in author_docs:
                if author["_id"] == other_author["_id"]:
                    continue
                # Perform the author comparison
                author_match = compare_author(author, other_author)
                if author_match:
                    author_found = [author["_id"], other_author["_id"]]
                    break
            if author_found:
                break
        if not author_found:
            return
        author_docs_ = list(collection.find({"_id": {"$in": author_found}}))

        if not author_docs_:
            # print("No authors found with the provided IDs.")
            return

        target_doc = self.find_target_doc(author_docs_, "doi")
        if target_doc:
            self.merge_documents(author_docs_, target_doc, collection)

    def process_authors(self):
        # ORCID unicity
        pipeline = [
            {"$unwind": "$external_ids"},
            {"$match": {"external_ids.source": "orcid"}},
            {"$group": {"_id": "$external_ids.id", "document_ids": {
                "$addToSet": "$_id"}, "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}}
        ]
        authors_cursor = list(self.collection.aggregate(
            pipeline, allowDiskUse=True))

        with MongoClient(self.mongodb_url) as client:
            Parallel(
                n_jobs=self.n_jobs,
                verbose=1,
                backend="threading")(
                delayed(self.orcid_unicity)(
                    reg,
                    self.collection,
                    self.verbose
                ) for reg in authors_cursor
            )
            client.close()
        if self.verbose > 1:
            print("ORCID unicity for {} groups of authors is done!".format(
                len(authors_cursor)))

        # DOI unicity
        if self.authors_threshold == 0:
            pepeline_count = {"$gt": 1}
        else:
            pepeline_count = {"$gt": 1, "$lte": self.authors_threshold}
        pipeline = [
            {"$unwind": "$related_works"},
            {"$match": {"related_works.source": "doi"}},
            {"$group": {"_id": "$related_works.id", "authors": {
                "$addToSet": "$_id"}, "count": {"$sum": 1}}},
            {"$match": {"count": pepeline_count}}
        ]
        authors_cursor = list(self.collection.aggregate(
            pipeline, allowDiskUse=True))

        with MongoClient(self.mongodb_url) as client:
            Parallel(
                n_jobs=self.n_jobs,
                verbose=1,
                backend="threading")(
                delayed(self.doi_unicity)(
                    reg,
                    self.collection,
                    self.verbose
                ) for reg in authors_cursor
            )
            client.close()
        if self.verbose > 1:
            print("DOI unicity for {} groups of authors is done!".format(
                len(authors_cursor)))

    def run(self):
        self.process_authors()
        return
