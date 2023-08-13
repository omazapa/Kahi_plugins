from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from pandas import read_csv
from time import time
import unidecode
from thefuzz import fuzz
from datetime import datetime as dt


class Kahi_minciencias_opendata_affiliations(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.client = MongoClient(config["database_url"])

        self.db = self.client[config["database_name"]]
        self.collection = self.db["affiliations"]

        self.file_path = config["minciencias_opendata_affiliations"]["file_path"]
        self.grupos_minciencias = read_csv(self.file_path)

        self.verbose = config["minciencias_opendata_affiliations"][
            "verbose"] if "verbose" in config["minciencias_opendata_affiliations"].keys() else 0

        self.inserted_cod_grupo = []

        for reg in self.collection.find({"types.type": "group"}):
            for ext in reg["external_ids"]:
                if ext["source"] == "minciencias":
                    self.inserted_cod_grupo.append(ext["id"])

        name_index = False
        for key, val in self.collection.index_information().items():
            if key == "names.name_text":
                name_index = True
                break
        if not name_index:
            self.collection.create_index([("names.name", TEXT)])
            print("Text index created on names.name field")

    def rename_institution(self, name):
        if name == "Colegio Mayor Nuestra Señora del Rosario".lower() or name == "Colegio Mayor de Nuestra Señora del Rosario".lower():
            return "universidad del rosario"
        elif name == "universidad de la guajira":
            return "guajira"
        elif "minuto" in name and "dios" in name:
            return "minuto dios"
        elif "salle" in name:
            return "universidad salle"
        elif "icesi" in name:
            return "icesi"
        elif "sede" in name:
            return name.split("sede")[0].strip()
        elif name == "universidad militar nueva granada":
            return "nueva granada"
        elif "pamplona" in name:
            return "pamplona"
        elif "sucre" in name:
            return "sucre"
        elif "santo tomás" in name or "santo tomas" in name:
            return "santo tomas"
        elif name == "universidad simón bolívar":
            return "simon bolivar"
        elif "unidades" in name and "santander" in name:
            return "unidades tecnológicas santander"
        elif "popayán" in name:
            return "popayán"
        elif "tecnológico metropolitano" in name:
            return "tecnológico metropolitano"
        elif "cesmag" in name:
            return "estudios superiores maría goretti"
        elif "distrital francisco" in name:
            return "distrital francisco josé"
        elif "santander" in name and "industrial" in name:
            return "industrial santander"
        elif "santander" in name and "industrial" not in name:
            return "universidad santander"
        elif "francisco" in name and "paula" in name and "santander" in name:
            return "francisco paula"
        elif "magdalena" in name:
            return "magdalena"
        elif "corporacion universitaria iberoamericana" == name:
            return "iberoamericana"
        else:
            return name

    def process_openadata(self):
        for idgr in self.grupos_minciencias["COD_GRUPO_GR"].unique():
            db_reg = self.collection.find_one({"external_ids.id": idgr})
            if db_reg:
                if idgr not in self.inserted_cod_grupo:
                    self.inserted_cod_grupo.append(idgr)
                continue
            self.inserted_cod_grupo.append(idgr)
            subset = self.grupos_minciencias[self.grupos_minciencias["COD_GRUPO_GR"] == idgr]
            reg = subset.iloc[-1]
            entry = self.empty_affiliation()
            entry["updated"].append(
                {"source": "minciencias", "time": int(time())})
            entry["names"].append({"lang": "es", "name": reg["NME_GRUPO_GR"]})
            entry["types"].append({"source": "minciencias", "type": "group"})
            entry["year_established"] = int(reg["FCREACION_GR"].split("/")[-1])
            entry["external_ids"].append(
                {"source": "minciencias", "id": reg["COD_GRUPO_GR"]})

            entry["subjects"].append({
                "source": "OECD",
                "subjects": [
                    {
                        "level": 0,
                        "name": reg["NME_GRAN_AREA_GR"],
                        "id":"",
                        "external_ids":[{"source": "OECD", "id": reg["ID_AREA_CON_GR"][0]}]
                    },
                    {
                        "level": 1,
                        "name": reg["NME_AREA_GR"],
                        "id":"",
                        "external_ids":[{"source": "OECD", "id": reg["ID_AREA_CON_GR"][1]}]
                    },
                ]
            })

            # START AVAL INSTITUTION SECTION
            for inst_aval in reg["INST_AVAL"].split("|"):
                inst_aval = inst_aval.split("-")[0]
                inst_aval = inst_aval.lower().strip()

                inst_aval = self.rename_institution(inst_aval)

                inst_aval = unidecode.unidecode(inst_aval)
                institutions = self.collection.find(
                    {"$text": {"$search": inst_aval}, "addresses.country": "Colombia"}).limit(50)
                institution = ""
                score = 10
                for inst in institutions:
                    method = ""
                    name = ""
                    for n in inst["names"]:
                        if n["lang"] == "es":
                            name = n["name"]
                            break
                        elif n["lang"] == "en":
                            name = n["name"]
                    name_mod = name.lower().replace("(colombia)", "").replace(
                        "(", "").replace(")", "").replace("bogotá", "")
                    # name_mod=name_mod.replace("universidad","").replace("de","").replace("del","").replace("los","").strip()
                    name_mod = unidecode.unidecode(name_mod)

                    if "santander" in name_mod and "industrial" in name_mod:
                        name_mod = "industrial santander"
                    if "santander" in name_mod and "industrial" not in name_mod:
                        name_mod = "universidad santander"
                    if "francisco" in name_mod and "paula" in name_mod and "santander" in name_mod:
                        inst_aval = "francisco paula"
                    score = fuzz.ratio(name_mod, inst_aval)
                    if score > 90:
                        method = "ratio"
                        institution = inst
                        break
                    elif score > 39:
                        score = fuzz.partial_ratio(name_mod, inst_aval)
                        # print("Partial ratio score: {}. {} -against- {}".format(score,name,reg["INST_AVAL"]))
                        if score > 93:
                            method = "partial ratio"
                            institution = inst
                            break
                        elif score > 55:
                            score = fuzz.token_set_ratio(name_mod, inst_aval)
                            # print("Token set ratio score: {}. {} -against- {}".format(score,name,reg["INST_AVAL"]))
                            if score > 98:
                                method = "token set ratio"
                                # print("Token set ratio score: {}. {} -against- {}".format(score,name,inst_aval))
                                institution = inst
                                break
                if institution != "":
                    entry["relations"].append(
                        {"types": institution["types"], "id": institution["_id"], "names": institution["names"]})
                    entry["addresses"].append({
                        "lat": institution["addresses"][0]["lat"],
                        "lng": institution["addresses"][0]["lng"],
                        "postcode": institution["addresses"][0]["postcode"],
                        "state": institution["addresses"][0]["state"],
                        "city": institution["addresses"][0]["city"],
                        "country": institution["addresses"][0]["country"],
                        "country_code": institution["addresses"][0]["country_code"]
                    })
                else:
                    if score == 98 and method == "token set ratio":
                        print(
                            "(LAST) {} score: {}. {} -against- {}".format(method, score, name, inst_aval))
                    entry["addresses"].append({
                        "lat": "",
                        "lng": "",
                        "postcode": "",
                        "state": reg["NME_DEPARTAMENTO_GR"],
                        "city": reg["NME_MUNICIPIO_GR"],
                        "country": "Colombia",
                        "country_code": "CO"
                    })
            # END AVAL INSTITUTION

            # START LOOP OVER DIFFERENT CLASSIFICATIONS IN TIME
            for idx, reg in subset.iterrows():
                entry_rank = {
                    "source": "minciencias",
                    "rank": reg["NME_CLASIFICACION_GR"],
                    "order": reg["ORDEN_CLAS_GR"],
                    "date": int(dt.strptime(reg["ANO_CONVO"], "%d/%m/%Y").timestamp())
                }
                entry["ranking"].append(entry_rank)
            # END CLASSIFICATION SECTION
            self.collection.insert_one(entry)

    def run(self):
        self.process_openadata()
        return 0
