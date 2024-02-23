from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from pandas import read_csv, isna
from time import time
from datetime import datetime as dt
from joblib import Parallel, delayed
import re
from urllib.parse import unquote


def get_id_type(url):
    """
    This function returns the type of the id based on the url

    Parameters:
    ----------
    url: str
        The url of the id
    Returns:
    --------
    str
        The type of the id
    """
    if "orcid" in url:
        return "orcid"
    if "researchgate" in url:
        return "researchgate"
    if "linkedin" in url:
        return "linkedin"
    if "scholar.google" in url:
        return "scholar"
    if "scopus" in url:
        return "scopus"
    return None


def parse_scholar_id(value):
    """
    Parse the google scholar id from the url, 
    the id is the value of the user parameter.

    Parameters:
    ----------
    value: str
        The url of the google scholar profile

    Returns:
    --------
    str
        The google scholar id
    """
    value = value.replace("authuser", "")
    value = re.findall(r"user=([^&]{1,12})", value)
    if value:
        value = value[-1]
        if len(value) == 12:
            return value
    return None


def parse_researchgate_id(value):
    """
    Function to parse the researchgate id from the url,
    it is the value of the profile path in the url

    Parameters:
    ----------
    value: str
        The url of the researchgate profile

    Returns:
    --------
    str
        The researchgate id
    """
    value = re.search(
        r"https://www\.researchgate\.net/profile/([^\s/?&]+)", value)
    if value:
        return value.group(1)
    return None


def parse_linkedin_id(value):
    """
    Function to parse the linkedin id from the url,
    it is the value of the "in" parameter in the url.

    Parameters:
    ----------
    value: str
        The url of the linkedin profile

    Returns:
    --------
    str
        The linkedin id
    """
    value = re.search(r"linkedin\.com/in/([^/?&]+)", value)
    if value:
        return value.group(1)
    return None


def parse_orcid_id(value):
    """
    Function to parse the orcid id from the url,
    it is the value of the orcid parameter in the url.
    It is four groups of four characters separated by dashes.

    Parameters:
    ----------
    value: str
        The url of the orcid profile

    Returns:
    --------
    str
        The orcid id
    """
    value = value.replace("-", "")
    value = value.replace("_", "")
    value = re.search(
        r"(?:ORCID\s?)?([a-zA-Z0-9]{4})-?([a-zA-Z0-9]{4})-?([a-zA-Z0-9]{4})-?([a-zA-Z0-9]{4})", value)
    if value:
        return "-".join(value.groups())
    return None


def parse_scopus_id(value):
    """
    Function to parse the scopus id from the url,
    it is the value of the authorID or authorId parameter in the url.
    **The number of characters have to be 10 or 11 but not more or less.**

    Parameters:
    ----------
    value: str
        The url of the scopus profile

    Returns:
    --------
    str
        The scopus id
    """

    ##
    value = re.search(r"(?:authorId=|authorID=)(\d{10,11})", value)
    if value:
        return value.group(1)
    return None


def get_id(value):
    """
    Function to get the id from the url, it uses the get_id_type function to get the type of the id
    and then uses the corresponding function to parse the id from the url.
    Returns the ids without url encoding, without spaces and without url path.

    Parameters:
    ----------
    value: str
        The url of the profile

    Returns:
    --------
    str
        The id of the profile
    """
    value = unquote(value)
    value = value.replace(" ", "")
    if get_id_type(value) == "scholar":
        return parse_scholar_id(value)
    if get_id_type(value) == "researchgate":
        return parse_researchgate_id(value)
    if get_id_type(value) == "linkedin":
        return parse_linkedin_id(value)
    if get_id_type(value) == "orcid":
        return parse_orcid_id(value)
    if get_id_type(value) == "scopus":
        return parse_scopus_id(value)

    return None


def process_one(client, db_name, empty_person, auid, cv, articulos_, subset, verbose):
    db = client[db_name]
    collection = db["person"]

    reg_db = collection.find_one({"external_ids.id": auid})
    if reg_db:
        return
    if len(cv) < 1:
        return
    cv = cv.iloc[0]

    if "765376" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Femenino"
    if "1385093" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Femenino"
    if "37351" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Femenino"
    if "0000000082" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Femenino"
    if "1393305" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "1426093" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "1455061" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "37796" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "702404" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "896519" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "287938" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "1317792" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "1506130" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "34201" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"

    if "1340197" in cv["ID_PERSONA_PD"]:
        cv["INICIALES"] = "G"
        cv["NOMBRES"] = "Gabriel"
        cv["PRIMER APELLIDO"] = "de la Luz"
        cv["SEGUNDO APELLIDO"] = "Rodríguez"
        cv["Nombre"] = "Gabriel de la Luz Rodríguez"

    articulos = articulos_.copy()
    years = []
    for idx, reg in articulos.iterrows():
        years.append(int(reg["FCREACION_PD"].split("/")[-1]))
    articulos.loc[:, "year"] = years
    articulos.sort_values("year", inplace=True)

    reg = subset.iloc[-1]

    entry = empty_person.copy()
    entry["updated"].append(
        {"source": "minciencias", "time": int(time())})
    entry["full_name"] = cv["Nombre"]
    entry["first_names"] = cv["NOMBRES"].strip().split()
    entry["last_names"].extend(cv["PRIMER APELLIDO"].split("-"))
    if isinstance(cv["SEGUNDO APELLIDO"], str):
        entry["last_names"].append(cv["SEGUNDO APELLIDO"])
    entry["initials"] = cv["INICIALES"].replace(
        ".", "").replace(" ", "").strip(),
    if isinstance(cv["Nombre en citaciones"], str):
        entry["aliases"].append(cv["Nombre en citaciones"].lower())
    entry["sex"] = cv["Sexo"][0].lower()

    entry["external_ids"].append({
        "source": "minciencias",
        "id": cv["ID_PERSONA_PD"]
    })

    # all the ids are mixed, so we need to check each one in the next columns
    ids = []
    if not isna(cv["Google Scholar"]):
        ids.extend(cv["Google Scholar"])
    if not isna(cv["ResearchGate"]):
        ids.extend(cv["ResearchGate"])
    if not isna(cv['Linkedln']):
        ids.extend(cv['Linkedln'])
    if not isna(cv['ORCID']):
        ids.extend(cv['ORCID'])
    if not isna(cv['Scopus']):
        ids.extend(cv['Scopus'])
    if not isna(cv['Academia.edu']):  # TODO:implement those other ids
        ids.extend(cv['Academia.edu'])
    if not isna(cv['Mendeley']):
        ids.extend(cv['Mendeley'])
    if not isna(cv['Network']):
        ids.extend(cv['Network'])
    if not isna(cv['Social Sciences Research']):
        ids.extend(cv["Social Sciences Research"])
    # NOTA: remover la URL y dejar solo el ID
    for _id in ids:
        if isinstance(_id, str):
            value = get_id(_id)
            if value:
                entry["external_ids"].append({
                    "source": get_id_type(_id),
                    "id": value
                })

    entry["subjects"].append({
        "source": "OECD",
        "subjects": [
            {
                "level": 0,
                "name": reg["NME_GRAN_AREA_GR"],
                "id": "",
                "external_ids": [{"source": "OECD", "id": reg["ID_AREA_CON_GR"][0]}]
            },
            {
                "level": 1,
                "name": reg["NME_AREA_GR"],
                "id": "",
                "external_ids": [{"source": "OECD", "id": reg["ID_AREA_CON_GR"][1]}]
            },
        ]
    })
    groups_cod = []
    inst_cod = []
    # print("Adding affiliations to ",auid)
    for idx, reg in articulos.iterrows():
        if reg["COD_GRUPO_GR"] in groups_cod:
            continue
        groups_cod.append(reg["COD_GRUPO_GR"])
        group_db = db["affiliations"].find_one(
            {"external_ids.id": reg["COD_GRUPO_GR"]})
        if group_db:
            name = group_db["names"][0]["name"]
            for n in group_db["names"]:
                if n["lang"] == "es":
                    name = n["name"]
                    break
                elif n["lang"] == "en":
                    name = n["name"]
            entry["affiliations"].append({
                "name": name,
                "id": group_db["_id"],
                "types": group_db["types"],
                "start_date": reg["FCREACION_PD"].split("/")[-1],
                "end_date": "",
                "position": ""
            })
            if "relations" in group_db.keys():
                if group_db["relations"]:
                    for rel in group_db["relations"]:
                        if rel["id"] in inst_cod:
                            continue
                        inst_cod.append(rel["id"])
                        if "names" in rel.keys():
                            name = rel["names"][0]["name"]
                            for n in rel["names"]:
                                if n["lang"] == "es":
                                    name = n["name"]
                                    break
                                elif n["lang"] == "en":
                                    name = n["name"]
                        else:
                            name = rel["name"]
                        entry["affiliations"].append({
                            "name": name,
                            "id": rel["id"],
                            "types": rel["types"] if "types" in rel.keys() else [],
                            "start_date": reg["FCREACION_PD"].split("/")[-1],
                            "end_date": "",
                            "position": ""
                        })
    # print("Adding ranks to ", auid)
    for idx, reg in subset.iterrows():
        try:
            entry_rank = {
                "source": "minciencias",
                "rank": reg["NME_CLAS_PR"],
                "id": reg["ID_CLAS_PR"],
                "order": reg["ORDEN_CLAS_PR"],
                "date": int(dt.strptime(reg["ANO_CONVO"], "%d/%m/%Y").timestamp())
            }
            entry["ranking"].append(entry_rank)
        except Exception as e:
            if verbose > 4:
                print(e)

    collection.insert_one(entry)


class Kahi_minciencias_opendata_person(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(config["database_url"])

        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])

        self.researchers_file = config["minciencias_opendata_person"]["researchers"]
        self.cvlac_file = config["minciencias_opendata_person"]["cvlac"]
        self.groups_file = config["minciencias_opendata_person"]["groups_production"]

        self.investigadores_minciencias = read_csv(
            self.researchers_file, dtype={"ID_PERSONA_PR": str})
        self.cvlac = read_csv(self.cvlac_file, dtype={"ID_PERSONA_PD": str})
        self.articulos_grupos = read_csv(
            self.groups_file, dtype={"ID_PERSONA_PD": str})

        self.n_jobs = config["minciencias_opendata_person"]["num_jobs"] if "num_jobs" in config["minciencias_opendata_person"].keys(
        ) else 1

        self.verbose = config["minciencias_opendata_person"][
            "verbose"] if "verbose" in config["minciencias_opendata_person"].keys() else 0

    def process_openadata(self):
        client = MongoClient(self.mongodb_url)
        Parallel(
            n_jobs=self.n_jobs,
            verbose=10,
            backend="threading")(
            delayed(process_one)(
                client,
                self.config["database_name"],
                self.empty_person(),
                auid,
                self.cvlac[self.cvlac["ID_PERSONA_PD"] == auid],
                self.articulos_grupos[self.articulos_grupos["ID_PERSONA_PD"] == auid],
                self.investigadores_minciencias[self.investigadores_minciencias["ID_PERSONA_PR"] == auid],
                self.verbose
            ) for auid in self.investigadores_minciencias["ID_PERSONA_PR"].unique()
        )
        client.close()

    def run(self):
        self.process_openadata()
        return 0
