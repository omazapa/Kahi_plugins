from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from datetime import datetime as dt
from time import time
from thefuzz import fuzz


class Kahi_scienti_affiliations(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["affiliations"]

        self.verbose = config["scienti_affiliations"]["verbose"] if "verbose" in config["scienti_affiliations"].keys(
        ) else 0

        name_index = False
        for key, val in self.collection.index_information().items():
            if key == "names.name_text":
                name_index = True
                break
        if not name_index:
            self.collection.create_index([("names.name", TEXT)])
            print("Text index created on names.name field")

    def process_scienti_institutions(self, config, verbose=0):
        client = MongoClient(config["database_url"])
        db = client[config["database_name"]]
        scienti = db[config["collection_name"]]
        for cod_inst in scienti.distinct("group.institution.TXT_NIT"):
            reg_scienti = scienti.find_one(
                {"group.institution.TXT_NIT": cod_inst})
            for inst in reg_scienti["group"][0]["institution"]:
                token = inst["NME_INST"]
                stopwords = ["y", "and", "de", "la", "los", "las", "el", "o", "or", "un", "una", "uno", "en", "por", "para", "según", "a", "ante",
                             "con", "de", "sin", "so", "tras", "e", "u", "del", "and", "or", "from", "to", "after", "about", "by", "in", "out", "next",
                             "under", "our", "your", "yours", "them", "their", "my", "it", "we", "have", "had", "be", "do", "are", "him", "her", "hers", "his",
                             "then", "where", "why", "how", "what", "which", "who", "whom", "all", "any", "both", "each", "few", "at", "this", "these", "those",
                             "that", "if", "as", "with", "while", "against", "about", "here", "there", "off", "of", "-"]
                inst_name = " ".join(
                    [w for w in token.lower().split() if w not in stopwords])
                inst_name = inst_name.replace("universidad", "").replace(
                    "institución universitaria", "").replace("industrial", "")
                inst_name = inst_name.replace("corporación", "").replace(
                    "fundación", "").replace("instituto", "").strip()
                col_list = self.collection.find(
                    {"$text": {"$search": inst_name}}).limit(30)
                reg_col = None
                name = None
                highest_score = 0
                highest_name = None
                if "colciencias" in inst_name:
                    reg_col = self.collection.find_one(
                        {"names.name": "Colciencias"})
                    name = reg_col["names"][0]["name"]
                if inst["NME_INST"] == "UNIVERSIDAD CATOLICA DE ORIENTE":
                    reg_col = self.collection.find_one(
                        {"names.name": "Universidad Católica de Oriente"})
                    name = reg_col["names"][0]["name"]
                if inst["NME_INST"] == "UNIVERSIDAD ":
                    reg_col = self.collection.find_one(
                        {"names.name": "Universidad Católica de Oriente"})
                    name = reg_col["names"][0]["name"]
                if not reg_col:
                    for reg in col_list:
                        for name in reg["names"]:
                            if inst["NME_INST"].lower() == name["name"].lower():
                                name = name["name"]
                                reg_col = reg
                                break
                        if reg_col:
                            break
                        for name in reg["names"]:
                            score = fuzz.ratio(
                                inst["NME_INST"].lower(), name["name"].lower())
                            if score > 90:
                                name = name["name"]
                                reg_col = reg
                                break
                            elif score > 70:
                                score = fuzz.partial_ratio(
                                    inst["NME_INST"].lower(), name["name"].lower())
                                if score > 93:
                                    reg_col = reg
                                    name = name["name"]
                                    break
                                else:
                                    if score > highest_score:
                                        highest_score = score
                                        highest_name = name["name"]
                            else:
                                if score > highest_score:
                                    highest_score = score
                                    highest_name = name["name"]
                        if reg_col:
                            break
                if reg_col:
                    reg_col["updated"].append(
                        {"source": "scienti", "time": int(time())})
                    reg_col["external_ids"].append(
                        {"source": "minciencias", "id": inst["COD_INST"]})
                    reg_col["external_ids"].append(
                        {"source": "nit", "id": inst["TXT_NIT"] + "-" + inst["TXT_DIGITO_VERIFICADOR"]})
                    if inst["SGL_INST"] not in reg_col["abbreviations"]:
                        reg_col["abbreviations"].append(inst["SGL_INST"])
                    if "URL_HOME_PAGE" in inst.keys():
                        if {"source": "site", "url": inst["URL_HOME_PAGE"]} not in reg_col["external_urls"]:
                            reg_col["external_urls"].append(
                                {"source": "site", "url": inst["URL_HOME_PAGE"]})
                    self.collection.update_one({"_id": reg_col["_id"]},
                                               {"$set": {
                                                   "updated": reg_col["updated"],
                                                   "external_ids": reg_col["external_ids"],
                                                   "abbreviations": reg_col["abbreviations"],
                                                   "external_urls": reg_col["external_urls"]
                                               }})
                else:
                    if self.verbose == 4:
                        print(inst_name)
                        print("Almost similar (", highest_score, "): ",
                              inst["NME_INST"], " - ", highest_name)

    def extract_subject(self, subjects, data):
        subjects.append({
            "id": "",
            "name": data["TXT_NME_AREA"],
            "level": data["NRO_NIVEL"],
            "external_ids": [{"source": "OCDE", "id": data["COD_AREA_CONOCIMIENTO"]}]
        })
        if "knowledge_area" in data.keys():
            self.extract_subject(subjects, data["knowledge_area"][0])
        return subjects

    def process_scienti_groups(self, config, verbose=0):
        client = MongoClient(config["database_url"])
        db = client[config["database_name"]]
        scienti = db[config["collection_name"]]
        for group_id in scienti.distinct("group.COD_ID_GRUPO"):
            db_reg = self.collection.find_one({"external_ids.id": group_id})
            if db_reg:
                continue
            entry = self.empty_affiliation()
            entry["updated"].append({"time": int(time()), "source": "scienti"})
            entry["external_ids"].append(
                {"source": "minciencias", "id": group_id})
            entry["types"].append({"source": "scienti", "type": "group"})

            group = scienti.find_one({"group.COD_ID_GRUPO": group_id})
            group = group["group"][0]

            if group:
                entry["external_ids"].append(
                    {"source": "scienti", "id": group["NRO_ID_GRUPO"]})
                entry["names"].append(
                    {"name": group["NME_GRUPO"], "lang": "es"})
                entry["birthdate"] = int(dt.strptime(
                    str(group["ANO_FORMACAO"]) + "-" + str(group["MES_FORMACAO"]), "%Y-%m").timestamp())
                if group["STA_ELIMINADO"] == "F":
                    entry["status"].append(
                        {"source": "minciencias", "status": "activo"})
                if group["STA_ELIMINADO"] == "T" or group["STA_ELIMINADO"] == "V":
                    entry["status"].append(
                        {"source": "minciencias", "status": "eliminado"})

                entry["description"].append({
                    "source": "scienti",
                    "description": {
                        "TXT_PLAN_TRABAJO": group["TXT_PLAN_TRABAJO"] if "TXT_PLAN_TRABAJO" in group.keys() else "",
                        "TXT_ESTADO_ARTE": group["TXT_ESTADO_ARTE"] if "TXT_ESTADO_ARTE" in group.keys() else "",
                        "TXT_OBJETIVOS": group["TXT_OBJETIVOS"]if "TXT_OBJETIVOS" in group.keys() else "",
                        "TXT_PROD_DESTACADA": group["TXT_PROD_DESTACADA"]if "TXT_PROD_DESTACADA" in group.keys() else "",
                        "TXT_RETOS": group["TXT_RETOS"]if "TXT_RETOS" in group.keys() else "",
                        "TXT_VISION": group["TXT_VISION"] if "TXT_VISION" in group.keys() else ""
                    }
                })

                if "TXT_CLASIF" in group.keys() and "DTA_CLASIF" in group.keys():
                    entry["ranking"].append({
                        "source": "scienti",
                        "rank": group["TXT_CLASIF"],
                        "from_date": int(dt.strptime(group["DTA_CLASIF"].split(", ")[-1].replace(" GMT", ""), "%d %b %Y %H:%M:%S").timestamp()),
                        "to_date": int(dt.strptime(group["DTA_FIN_CLASIF"].split(", ")[-1].replace(" GMT", ""), "%d %b %Y %H:%M:%S").timestamp())
                    })
                subjects = self.extract_subject([], group["knowledge_area"][0])
                if len(subjects) > 0:
                    entry["subjects"].append({
                        "source": "OCDE",
                        "subjects": subjects
                    })

                for reg in scienti.find({"group.COD_ID_GRUPO": group_id}):
                    for inst in reg["group"][0]["institution"]:
                        db_inst = self.collection.find_one(
                            {"external_ids.id": inst["TXT_NIT"] + "-" + inst["TXT_DIGITO_VERIFICADOR"]})
                        if db_inst:
                            rel_entry = {
                                "names": db_inst["names"], "id": db_inst["_id"], "types": db_inst["types"]}
                            if rel_entry not in entry["relations"]:
                                entry["relations"].append(rel_entry)

                self.collection.insert_one(entry)

    def run(self):
        for config in self.config["scienti_affiliations"]["databases"]:
            if self.verbose > 0:
                print("Processing {} database".format(config["database_name"]))
            self.process_scienti_institutions(config, verbose=self.verbose)
            self.process_scienti_groups(config, verbose=self.verbose)
        return 0
