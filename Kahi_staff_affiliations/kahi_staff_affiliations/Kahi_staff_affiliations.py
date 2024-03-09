from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from bson.objectid import ObjectId
from pandas import read_excel
from time import time


class Kahi_staff_affiliations(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.client = MongoClient(config["database_url"])

        self.db = self.client[config["database_name"]]
        self.collection = self.db["affiliations"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("names.name")
        self.collection.create_index("types.type")
        self.collection.create_index([("names.name", TEXT)])

        self.verbose = config["verbose"] if "verbose" in config else 0

    def staff_affiliation(self, data, institution_name, staff_reg):
        # inserting faculties and departments
        for idx, reg in data.iterrows():
            name = reg["Nombre fac"]
            if name not in self.facs_inserted.keys():
                is_in_db = self.collection.find_one(
                    {"names.name": name, "relations.id": staff_reg["_id"]})
                if is_in_db:
                    if name not in self.facs_inserted.keys():
                        self.facs_inserted[name] = is_in_db["_id"]
                        print(name, " already in db")
                    # continue
                    # may be updatable, check accordingly
                else:
                    entry = self.empty_affiliation()
                    entry["updated"].append(
                        {"time": int(time()), "source": "staff"})
                    entry["names"].append(
                        {"name": name, "lang": "es", "source": "staff"})
                    entry["types"].append(
                        {"source": "staff", "type": "faculty"})
                    entry["relations"].append(
                        {"id": staff_reg["_id"], "name": institution_name, "types": staff_reg["types"]})

                    fac = self.collection.insert_one(entry)
                    self.facs_inserted[name] = fac.inserted_id

            if reg["Nombre cencos"] not in self.deps_inserted.keys():
                is_in_db = self.collection.find_one(
                    {"names.name": reg["Nombre cencos"], "relations.id": staff_reg["_id"]})
                if is_in_db:
                    if reg["Nombre cencos"] not in self.deps_inserted.keys():
                        self.deps_inserted[reg["Nombre cencos"]
                                           ] = is_in_db["_id"]
                        print(reg["Nombre cencos"], " already in db")
                    # continue
                    # may be updatable, check accordingly
                else:
                    entry = self.empty_affiliation()
                    entry["updated"].append(
                        {"time": int(time()), "source": "staff"})
                    entry["names"].append(
                        {"name": reg["Nombre cencos"], "lang": "es", "source": "staff"})
                    entry["types"].append(
                        {"source": "staff", "type": "department"})
                    entry["relations"].append(
                        {"id": staff_reg["_id"], "name": institution_name, "types": staff_reg["types"]})

                    dep = self.collection.insert_one(entry)
                    self.deps_inserted[reg["Nombre cencos"]] = dep.inserted_id

            if (name, reg["Nombre cencos"]) not in self.fac_dep:
                self.fac_dep.append((name, reg["Nombre cencos"]))

        # Creating relations between faculties and departments
        for fac, dep in self.fac_dep:
            fac_id = self.facs_inserted[fac]
            dep_id = self.deps_inserted[dep]
            dep_reg = self.collection.find_one({"_id": ObjectId(dep_id)})
            fac_reg = self.collection.find_one({"_id": ObjectId(fac_id)})
            self.collection.update_one({"_id": fac_reg["_id"]},
                                       {"$push": {
                                           "relations": {
                                               "id": dep_reg["_id"],
                                               "name": dep_reg["names"][0]["name"], "types": dep_reg["types"]}}})
            self.collection.update_one({"_id": dep_reg["_id"]},
                                       {"$push": {
                                           "relations": {
                                               "id": fac_reg["_id"],
                                               "name": fac_reg["names"][0]["name"], "types": fac_reg["types"]}}})
        return 0

    def run(self):
        if self.verbose > 4:
            start_time = time()

        for config in self.config["staff_affiliations"]["databases"]:
            if self.verbose > 0:
                print("Processing {} database".format(
                    config["institution_name"]))

            institution_name = config["institution_name"]

            staff_reg = self.collection.find_one(
                {"names.name": institution_name})
            if not staff_reg:
                print("Institution not found in database")
                raise ValueError(
                    f"Institution {institution_name} not found in database")

            file_path = config["file_path"]
            data = read_excel(file_path)

            self.facs_inserted = {}
            self.deps_inserted = {}
            self.fac_dep = []

            self.staff_affiliation(data, institution_name, staff_reg)

        if self.verbose > 4:
            print("Execution time: {} minutes".format(
                round((time() - start_time) / 60, 2)))
        return 0
