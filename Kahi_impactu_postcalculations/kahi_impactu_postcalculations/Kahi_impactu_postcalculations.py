from joblib import Parallel, delayed
from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from math import log, exp
import datetime as dt
import subprocess
import spacy


class Kahi_impactu_postcalculations(KahiBase):
    """
    Plugin for performing post calculations for Impactu.

    This class extends KahiBase and implements functions for creating co-authorship networks and
    extracting top words

    Args:
        config (dict): YAML workflow file

    Attributes:
        config (dict): Plugin configuration.
        mongodb_url (str): MongoDB database URL.
        client (MongoClient): MongoDB client for the main database.
        db (MongoDatabase): Main database.
        affiliations (MongoCollection): Affiliations collection.
        impactu_client (MongoClient): MongoDB client for the Impactu database.
        impactu_db (MongoDatabase): Impactu database.
        verbose (bool): Indicates whether verbose output is enabled.
        n_jobs (int): Number of parallel jobs.

    """

    config = {}

    def _check_and_install_spacy_models(self):
        """
        Check if the spaCy models are installed and install them if needed.
        """
        if not self.are_spacy_models_installed():
            self.install_spacy_models()

    def are_spacy_models_installed(self):
        """
        Check if the required spaCy models are installed.

        Returns:
            bool: True if models are installed, False otherwise.
        """
        return "en_core_web_sm" in  spacy.cli.info()["pipelines"].keys() and "es_core_news_sm" in  spacy.cli.info()["pipelines"].keys()

    def install_spacy_models(self):
        """
        Install the required spaCy models.
        """
        subprocess.run(["python3", "-m", "spacy", "download", "en_core_web_sm"])
        subprocess.run(["python3", "-m", "spacy", "download", "es_core_news_sm"])

    def __init__(self, config):
        """
        Initialize the Kahi_impactu_postcalculations plugin.
        """
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)
        self.db = self.client[config["database_name"]]
        self.affiliations = self.db["affiliations"]

        self.impactu_client = MongoClient(
            config["impactu_postcalculations"]["database_url"])
        self.impactu_db = self.impactu_client[
            config["impactu_postcalculations"]["database_name"]]
        self.verbose = self.config["impactu_postcalculations"]["verbose"]
        self.n_jobs = self.config["impactu_postcalculations"]["n_jobs"]

        self._check_and_install_spacy_models()

        self.en_model = spacy.load('en_core_web_sm')
        self.es_model = spacy.load('es_core_news_sm')
        self.stopwords = self.en_model.Defaults.stop_words.union(self.es_model.Defaults.stop_words)

    def network_creation(self, idx, collection_type):
        """
        Create a co-authorship network for an affiliation or person.

        Args:
            idx (str): Identifier of the affiliation or person.
            collection_type (str): Type of the collection ("affiliations" or "person").
        """
        already = self.impactu_db[collection_type].find_one({"_id": idx})
        if already:
            return None

        if collection_type == "affiliations":
            aff_info = self.db["affiliations"].find_one({"_id": idx})
            name = aff_info["names"][0]["name"]
            for n in aff_info["names"]:
                if n["lang"] in ["es", "en"]:
                    name = n["name"]
                    break
            authors_key = "authors.affiliations.id"
        elif collection_type == "person":
            aff_info = self.db["person"].find_one({"_id": idx})
            name = aff_info["full_name"]
            authors_key = "authors.id"

        nodes = [idx]
        nodes_labels = [name]
        edges = []
        edges_coauthorships = {}
        works_count = 0

        for work in self.db["works"].find({authors_key: idx, "author_count": {"$lte": 10}}):
            works_count += 1
            work_nodes = [idx]
            work_edges = []

            if collection_type == "affiliations":
                for author in work["authors"]:
                    for aff in author["affiliations"]:
                        if not aff["id"]:
                            continue
                        if aff["id"] == "":
                            continue
                        if aff["id"] == idx:
                            continue
                        if not aff["id"] in nodes:
                            nodes.append(aff["id"])
                            name = aff["name"]
                            nodes_labels.append(name)
                        if not aff["id"] in work_nodes:
                            for node in work_nodes:
                                edge_found = False
                                if (idx, aff["id"]) in work_edges:
                                    edge_found = True
                                elif (aff["id"], idx) in edges:
                                    edge_found = True
                                if edge_found is False:
                                    work_edges.append((idx, aff["id"]))
                            work_nodes.append(aff["id"])

            elif collection_type == "person":
                for author in work["authors"]:
                    if not author["id"]:
                        continue
                    if author["id"] == "":
                        continue
                    if author["id"] == idx:
                        continue
                    if not author["id"] in nodes:
                        nodes.append(author["id"])
                        name = author["full_name"]
                        nodes_labels.append(name)
                    if not author["id"] in work_nodes:
                        for node in work_nodes:
                            edge_found = False
                            if (idx, author["id"]) in work_edges:
                                edge_found = True
                            elif (author["id"], idx) in edges:
                                edge_found = True
                            if edge_found is False:
                                work_edges.append((idx, author["id"]))
                        work_nodes.append(author["id"])

            # Connecting all the nodes in the work among them
            for node in work_nodes:
                if node not in nodes:
                    nodes.append(node)
            for node_a, node_b in work_edges:
                edge_found = (node_a, node_b) in edges or (node_b, node_a) in edges
                if not edge_found:
                    edges_coauthorships[str(node_a) + str(node_b)] = edges_coauthorships.get(str(node_a) + str(node_b), 0) + 1
                    edges.append((node_a, node_b))

        # Adding the connections between the coauthoring institutions
        for node in nodes:
            if node == idx:
                continue
            for work in self.db["works"].find({"$and": [{authors_key: node}, {authors_key: {"$ne": idx}}], "author_count": {"$lte": 10}}):
                if collection_type == "affiliations":
                    for author in work["authors"]:
                        for aff in author["affiliations"]:
                            if aff["id"] == idx:
                                print("Problem found")
                                continue
                            if not aff["id"] in nodes:
                                continue
                            if node == aff["id"]:
                                continue
                            if (node, aff["id"]) in edges:
                                edges_coauthorships[str(node) + str(aff["id"])] += 1
                            elif (aff["id"], node) in edges:
                                edges_coauthorships[str(aff["id"]) + str(node)] += 1
                            else:
                                edges_coauthorships[str(node) + str(aff["id"])] = 1
                                edges.append((node, aff["id"]))

                elif collection_type == "person":
                    for author in work["authors"]:
                        if author["id"] == idx:
                            print("Problem found")
                            continue
                        if not author["id"] in nodes:
                            continue
                        if node == author["id"]:
                            continue
                        if (node, author["id"]) in edges:
                            edges_coauthorships[str(node) + str(author["id"])] += 1
                        elif (author["id"], node) in edges:
                            edges_coauthorships[str(author["id"]) + str(node)] += 1
                        else:
                            edges_coauthorships[str(node) + str(author["id"])] = 1
                            edges.append((node, author["id"]))

        # Constructing the actual format to insert in the database
        num_nodes = len(nodes)
        nodes_db = [
            {
                "id": str(node),
                "label": nodes_labels[nodes.index(node)],
                "degree": len([(i, j) for i, j in edges if i == node or j == node]),
                "size": 50 * log(1 + len([(i, j) for i, j in edges if i == node or j == node]) / (num_nodes - 1), 2) if num_nodes > 1 else 1
            } for node in nodes]
        edges_db = [
            {
                "source": str(node_a),
                "sourceName": nodes_labels[nodes.index(node_a)],
                "target": str(node_b),
                "targetName": nodes_labels[nodes.index(node_b)],
                "coauthorships": edges_coauthorships.get(str(node_a) + str(node_b), 0),
                "size": edges_coauthorships.get(str(node_a) + str(node_b), 0)
            } for node_a, node_b in edges
        ]

        top = max([e["coauthorships"] for e in edges_db]) if edges_db else 1
        bot = min([e["coauthorships"] for e in edges_db]) if edges_db else 1
        for edge in edges_db:
            if abs(top - edge["coauthorships"]) < 0.01:
                edge["size"] = 10
            elif abs(bot - edge["coauthorships"]) < 0.01:
                edge["size"] = 1
            else:
                size = 10 / (1 + exp(6 - 10 * edge["coauthorships"] / top))
                edge["size"] = size if size >= 1 else 1

        self.impactu_db[collection_type].insert_one({
            "_id": idx,
            "coauthorship_network": {
                "nodes": nodes_db,
                "edges": edges_db
            }
        })

    def top_words(self, collection):
        """
        Extract the top words for a given collection (affiliations or person).

        Args:
            collection (str): Type of the collection ("affiliations" or "person").
        """
        words_inserted_ids = []  # Asegúrate de definir esta lista en el ámbito adecuado

        with self.client.start_session() as session:
            old = dt.datetime.now()

            if collection == "person":
                documents = self.db[collection].find({"_id": {"$nin": words_inserted_ids}})
                authors_key = "authors.id"
            else:
                documents = self.db[collection].find()
                authors_key = "authors.affiliations.id"

            for aff in documents:
                aff_db = self.impactu[collection].find_one({"_id": aff["_id"], "top_words": {"$exists": 1}})
                if aff_db:
                    if collection == "person":
                        words_inserted_ids.append(aff["_id"])
                    continue
                results = {}

                for work in self.db["works"].find({authors_key: aff["_id"], "titles.title": {"$exists": 1}}, {"titles": 1}):
                    title = work["titles"][0]["title"].lower()
                    lang = work["titles"][0]["lang"]

                    if lang == "es":
                        model = self.es
                    else:
                        model = self.en

                    title = model(title)

                    for token in title:
                        if token.lemma_.isnumeric():
                            continue
                        if token.lemma_ in self.stopwords:
                            continue
                        if len(token.lemma_) < 4:
                            continue
                        if token.lemma_ in results.keys():
                            results[token.lemma_] += 1
                        else:
                            results[token.lemma_] = 1

                topN = sorted(results.items(), key=lambda x: x[1], reverse=True)[:20]
                results = [{"name": top[0], "value": top[1]} for top in topN]
                aff_db = self.impactu[collection].find_one({"_id": aff["_id"]})
                if aff_db:
                    self.impactu[collection].update_one({"_id": aff["_id"]}, {"$set": {"top_words": results}})
                else:
                    self.impactu[collection].insert_one({"_id": aff["_id"], "top_words": results})
                delta = dt.datetime.now() - old
                if delta.seconds > 240:
                    self.client.admin.command('refreshSessions', [session.session_id], session=session)
                    old = dt.datetime.now()

    def run(self):
        """
        Execute the plugin to create co-authorship networks and extract top words.
        """
        # Getting the list of institutions ids with works
        institutions_ids = []
        for aff in self.affiliations.find({"types.type": {"$nin": ["faculty", "department", "group"]}}):
            count = self.db["works"].count_documents({"authors.affiliations.id": aff["_id"]})
            if count != 0:
                institutions_ids.append(aff["_id"])

        # Creating the networks of coauthorship for each affiliation
        if institutions_ids:
            Parallel(
                n_jobs=self.n_jobs,
                verbose=10,
                backend="threading")(
                    delayed(self.network_creation)(
                        oaid,
                        "affiliations"
                    ) for oaid in institutions_ids)

        # Getting the list of institutions ids with works
        authors_ids = []
        for author in self.db["person"].find():
            count = self.db["works"].count_documents({"authors.id": author["_id"]})
            if count != 0:
                authors_ids.append(author["_id"])

        # Creating the networks of coauthorship for each author
        if authors_ids:
            Parallel(
                n_jobs=self.n_jobs,
                verbose=10,
                backend="threading")(
                    delayed(self.network_creation)(
                        oaid,
                        "person"
                    ) for oaid in authors_ids)

        self.client.close()
        self.impactu_client.close()

        # Getting the top words for each institution and author
        top_words_collections = ["affiliations", "person"]
        if top_words_collections:
            Parallel(
                n_jobs=self.n_jobs,
                verbose=10,
                backend="threading")(
                    delayed(self.top_words)(
                        collection
                    ) for collection in top_words_collections)
