from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed
from re import sub, split, UNICODE
import unidecode
from pandas import read_excel

from langid import classify
import pycld2 as cld2
from langdetect import DetectorFactory, PROFILES_DIRECTORY
from fastspell import FastSpell
from lingua import LanguageDetectorBuilder
import iso639

fast_spell = FastSpell("en", mode="cons")


def lang_poll(text, verbose=0):
    text = text.lower()
    text = text.replace("\n", "")
    lang_list = []

    lang_list.append(classify(text)[0].lower())

    detected_language = None
    try:
        _, _, _, detected_language = cld2.detect(text, returnVectors=True)
    except Exception as e:
        if verbose > 4:
            print("Language detection error using cld2, trying without ascii")
            print(e)
        try:
            text = str(unidecode.unidecode(text).encode("ascii", "ignore"))
            _, _, _, detected_language = cld2.detect(text, returnVectors=True)
        except Exception as e:
            if verbose > 4:
                print("Language detection error using cld2")
                print(e)

    if detected_language:
        lang_list.append(detected_language[0][-1].lower())

    try:
        _factory = DetectorFactory()
        _factory.load_profile(PROFILES_DIRECTORY)
        detector = _factory.create()
        detector.append(text)
        lang_list.append(detector.detect().lower())
    except Exception as e:
        if verbose > 4:
            print("Language detection error using langdetect")
            print(e)

    try:
        result = fast_spell.getlang(text)  # low_memory breaks the function
        lang_list.append(result.lower())
    except Exception as e:
        if verbose > 4:
            print("Language detection error using fastSpell")
            print(e)

    detector = LanguageDetectorBuilder.from_all_languages().build()
    res = detector.detect_language_of(text)
    if res:
        if res.name.capitalize() == "Malay":
            la = "ms"
        elif res.name.capitalize() == "Sotho":
            la = "st"
        elif res.name.capitalize() == "Bokmal":
            la = "no"
        elif res.name.capitalize() == "Swahili":
            la = "sw"
        elif res.name.capitalize() == "Nynorsk":
            la = "is"
        elif res.name.capitalize() == "Slovene":
            la = "sl"
        else:
            la = iso639.find(
                res.name.capitalize())["iso639_1"].lower()
        lang_list.append(la)

    lang = None
    for prospect in set(lang_list):
        votes = lang_list.count(prospect)
        if votes > len(lang_list) / 2:
            lang = prospect
            break
    return lang


def split_names(s, exceptions=['GIL', 'LEW', 'LIZ', 'PAZ', 'REY', 'RIO', 'ROA', 'RUA', 'SUS', 'ZEA']):
    """
    Extract the parts of the full name `s` in the format ([] → optional):

    [SMALL_CONECTORS] FIRST_LAST_NAME [SMALL_CONECTORS] [SECOND_LAST_NAME] NAMES

    * If len(s) == 2 → Foreign name assumed with single last name on it
    * If len(s) == 3 → Colombian name assumed two last mames and one first name

    Add short last names to `exceptions` list if necessary

    Works with:
    ----
        s='LA ROTTA FORERO DANIEL ANDRES'
        s='MONTES RAMIREZ MARIA DEL CONSUELO'
        s='CALLEJAS POSADA RICARDO DE LA MERCED'
        s='DE LA CUESTA BENJUMEA MARIA DEL CARMEN'
        s='JARAMILLO OCAMPO NICOLAS CARLOS MARTI'
        s='RESTREPO QUINTERO DIEGO ALEJANDRO'
        s='RESTREPO ZEA JAIRO HUMBERTO'
        s='JIMENEZ DEL RIO MARLEN'
        s='RESTREPO FERNÁNDEZ SARA' # Colombian: two LAST_NAMES NAME
        s='NARDI ENRICO' # Foreing
    Fails:
    ----
        s='RANGEL MARTINEZ VILLAL ANDRES MAURICIO' # more than 2 last names
        s='ROMANO ANTONIO ENEA' # Foreing → LAST_NAME NAMES
    """
    s = s.title()
    exceptions = [e.title() for e in exceptions]
    sl = sub('(\s\w{1,3})\s', r'\1-', s, UNICODE)  # noqa: W605
    sl = sub('(\s\w{1,3}\-\w{1,3})\s', r'\1-', sl, UNICODE)  # noqa: W605
    sl = sub('^(\w{1,3})\s', r'\1-', sl, UNICODE)  # noqa: W605
    # Clean exceptions
    # Extract short names list
    lst = [s for s in split(
        '(\w{1,3})\-', sl) if len(s) >= 1 and len(s) <= 3]  # noqa: W605
    # intersection with exceptions list
    exc = [value for value in exceptions if value in lst]
    if exc:
        for e in exc:
            sl = sl.replace('{}-'.format(e), '{} '.format(e))

    # if sl.find('-')>-1:
    # print(sl)
    sll = [s.replace('-', ' ') for s in sl.split()]
    if len(s.split()) == 2:
        sll = [s.split()[0]] + [''] + [s.split()[1]]
    #
    d = {'NOMBRE COMPLETO': ' '.join(sll[2:] + sll[:2]),
         'PRIMER APELLIDO': sll[0],
         'SEGUNDO APELLIDO': sll[1],
         'NOMBRES': ' '.join(sll[2:]),
         'INICIALES': ' '.join([i[0] + '.' for i in ' '.join(sll[2:]).split()])
         }
    return d


def parse_ranking_udea(reg, affiliation, empty_work):
    entry = empty_work.copy()
    entry["updated"] = [{"source": "ranking_udea", "time": int(time())}]
    lang = lang_poll(reg["titulo"])
    entry["titles"].append(
        {"title": reg["titulo"], "lang": lang, "source": "ranking_udea"})
    if reg["DOI"]:
        entry["external_ids"].append(
            {"source": "doi", "id": reg["DOI"].lower()})
    if reg["issn"]:
        if isinstance(reg["issn"], str):
            for issn in reg["issn"].strip().split():
                if "-" not in issn:
                    continue
                issn = issn.strip()
                entry["source"] = {"name": reg["nombre rev o premio"],
                                   "external_ids": [{"source": "issn", "id": issn}]}
    if not entry["source"]:
        entry["source"] = {
            "name": reg["nombre rev o premio"], "external_ids": []}
    entry["year_published"] = int(reg["año realiz"])
    name = split_names(reg["nombre"])
    aff = {
        "id": affiliation["_id"],
        "name": affiliation["names"][0]["name"],
        "types": affiliation["types"]
    }
    for affname in affiliation["names"]:
        if affname["lang"] == "es":
            aff["name"] = affname["name"]
            break
        elif affname["lang"] == "en":
            aff["name"] = affname["name"]
        elif affname["source"] == "ror":
            aff["name"] = affname["name"]
    entry["authors"].append({
        "external_ids": [{"source": "Cédula de Ciudadanía", "id": reg["cedula"]}],
        "full_name": name["NOMBRE COMPLETO"],
        "types": [],
        "affiliations": [aff]
    })
    return entry


def process_one(ranking_udea_reg, db, collection, affiliation, empty_work, verbose=0):
    # client = MongoClient(url)
    # db = client[db_name]
    # collection = db["works"]
    doi = None
    # register has doi
    if ranking_udea_reg["DOI"]:
        if isinstance(ranking_udea_reg["DOI"], str):
            doi = ranking_udea_reg["DOI"].lower().strip()
    if doi:
        # is the doi in colavdb?
        colav_reg = collection.find_one({"external_ids.id": doi})
        if colav_reg:  # update the register
            # updated
            for upd in colav_reg["updated"]:
                if upd["source"] == "ranking_udea":
                    # client.close()
                    return None  # Register already on db
                    # Could be updated with new information when ranking file updates
            entry = parse_ranking_udea(
                ranking_udea_reg, affiliation, empty_work.copy())
            colav_reg["updated"].append(
                {"source": "ranking_udea", "time": int(time())})
            # titles
            colav_reg["titles"].extend(entry["titles"])
            # external_ids
            ext_ids = [ext["id"] for ext in colav_reg["external_ids"]]
            for ext in entry["external_ids"]:
                if ext["id"] not in ext_ids:
                    colav_reg["external_ids"].append(ext)
                    ext_ids.append(ext["id"])

            collection.update_one(
                {"_id": colav_reg["_id"]},
                {"$set": {
                    "updated": colav_reg["updated"],
                    "titles": colav_reg["titles"],
                    "external_ids": colav_reg["external_ids"]
                }}
            )
        else:  # insert a new register
            # parse
            entry = parse_ranking_udea(
                ranking_udea_reg, affiliation, empty_work.copy())
            # link
            source_db = None
            if "external_ids" in entry["source"].keys():
                for ext in entry["source"]["external_ids"]:
                    source_db = db["sources"].find_one(
                        {"external_ids.id": ext["id"]})
                    if source_db:
                        break
            if source_db:
                name = source_db["names"][0]["name"]
                for n in source_db["names"]:
                    if n["lang"] == "es":
                        name = n["name"]
                        break
                    if n["lang"] == "en":
                        name = n["name"]
                entry["source"] = {
                    "id": source_db["_id"],
                    "name": name
                }
            else:
                if len(entry["source"]["external_ids"]) == 0:
                    if verbose > 4:
                        print(
                            f'Register with doi: {ranking_udea_reg["DOI"]} does not provide a source')
                else:
                    if verbose > 4:
                        print("No source found for\n\t",
                              entry["source"]["external_ids"])
                entry["source"] = {
                    "id": "",
                    "name": entry["source"]["name"]
                }

            # search authors and affiliations in db
            for i, author in enumerate(entry["authors"]):
                author_db = None
                for ext in author["external_ids"]:
                    author_db = db["person"].find_one(
                        {"external_ids.id": ext["id"]})
                    if author_db:
                        break
                if author_db:
                    sources = [ext["source"]
                               for ext in author_db["external_ids"]]
                    ids = [ext["id"] for ext in author_db["external_ids"]]
                    for ext in author["external_ids"]:
                        if ext["id"] not in ids:
                            author_db["external_ids"].append(ext)
                            sources.append(ext["source"])
                            ids.append(ext["id"])
                    entry["authors"][i] = {
                        "id": author_db["_id"],
                        "full_name": author_db["full_name"],
                        "affiliations": author["affiliations"]
                    }
                    if "external_ids" in author.keys():
                        del (author["external_ids"])
                else:
                    author_db = db["person"].find_one(
                        {"full_name": author["full_name"]})
                    if author_db:
                        sources = [ext["source"]
                                   for ext in author_db["external_ids"]]
                        ids = [ext["id"] for ext in author_db["external_ids"]]
                        for ext in author["external_ids"]:
                            if ext["id"] not in ids:
                                author_db["external_ids"].append(ext)
                                sources.append(ext["source"])
                                ids.append(ext["id"])
                        entry["authors"][i] = {
                            "id": author_db["_id"],
                            "full_name": author_db["full_name"],
                            "affiliations": author["affiliations"]
                        }
                    else:
                        entry["authors"][i] = {
                            "id": "",
                            "full_name": author["full_name"],
                            "affiliations": author["affiliations"]
                        }
                for j, aff in enumerate(author["affiliations"]):
                    aff_db = None
                    if "external_ids" in aff.keys():
                        for ext in aff["external_ids"]:
                            aff_db = db["affiliations"].find_one(
                                {"external_ids.id": ext["id"]})
                            if aff_db:
                                break
                    if aff_db:
                        name = aff_db["names"][0]["name"]
                        for n in aff_db["names"]:
                            if n["source"] == "ror":
                                name = n["name"]
                                break
                            if n["lang"] == "en":
                                name = n["name"]
                            if n["lang"] == "es":
                                name = n["name"]
                        entry["authors"][i]["affiliations"][j] = {
                            "id": aff_db["_id"],
                            "name": name,
                            "types": aff_db["types"]
                        }
                    else:
                        aff_db = db["affiliations"].find_one(
                            {"names.name": aff["name"]})
                        if aff_db:
                            name = aff_db["names"][0]["name"]
                            for n in aff_db["names"]:
                                if n["source"] == "ror":
                                    name = n["name"]
                                    break
                                if n["lang"] == "en":
                                    name = n["name"]
                                if n["lang"] == "es":
                                    name = n["name"]
                            entry["authors"][i]["affiliations"][j] = {
                                "id": aff_db["_id"],
                                "name": name,
                                "types": aff_db["types"]
                            }
                        else:
                            entry["authors"][i]["affiliations"][j] = {
                                "id": "",
                                "name": aff["name"],
                                "types": []
                            }

            entry["author_count"] = len(entry["authors"])
            # insert in mongo
            collection.insert_one(entry)
            # insert in elasticsearch
    else:  # does not have a doi identifier
        # elasticsearch section
        pass
    # client.close()


class Kahi_ranking_udea_works(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["works"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("year_published")
        self.collection.create_index("authors.affiliations.id")
        self.collection.create_index("authors.id")
        self.collection.create_index([("titles.title", TEXT)])

        self.ranking = read_excel(config["ranking_udea_works"]["file_path"], dtype={
                                  "cedula": str, "DOI": str}).to_dict(orient="records")

        self.n_jobs = config["ranking_udea_works"]["num_jobs"] if "num_jobs" in config["ranking_udea_works"].keys(
        ) else 1
        self.verbose = config["ranking_udea_works"]["verbose"] if "verbose" in config["ranking_udea_works"].keys(
        ) else 0

        self.udea_reg = self.db["affiliations"].find_one(
            {"names.name": "University of Antioquia"})
        if not self.udea_reg:
            self.udea_reg = self.db["affiliations"].find_one(
                {"names.name": "Universidad de Antioquia"})
        if not self.udea_reg:
            print(
                "University of Antioquia not found in database. Creating it with minimal information...")
            udea_reg = self.empty_affiliation()
            udea_reg["updated"].append(
                {"time": int(time()), "source": "manual"})
            udea_reg["names"] = [
                {"name": 'Universidad de Antioquia',
                    "lang": 'es', "source": "staff_udea"}
            ]
            udea_reg["abbreviations"] = ['UdeA']
            udea_reg["year_established"] = 1803
            udea_reg["addresses"] = [
                {
                    "lat": 6.267417,
                    "lng": -75.568389,
                    "postcode": '',
                    "state": "Antioquia",
                    "city": 'Medellín',
                    "country": 'Colombia',
                    "country_code": 'CO'
                }
            ]
            udea_reg["external_ids"] = [
                {"source": 'isni', "id": '0000 0000 8882 5269'},
                {"source": 'fundref', "id": '501100005278'},
                {"source": 'orgref', "id": '2696975'},
                {"source": 'wikidata', "id": 'Q1258413'},
                {"source": 'ror', "id": 'https://ror.org/03bp5hc83'},
                {"source": 'minciencias', "id": '007300000887'},
                {"source": 'nit', "id": '890980040-8'}
            ]
            self.db["affiliations"].insert_one(udea_reg)
            self.udea_reg = self.db["affiliations"].find_one(
                {"names.name": "Universidad de Antioquia"})

    def process_ranking_udea(self):
        with MongoClient(self.mongodb_url) as client:
            db = client[self.config["database_name"]]
            collection = db["works"]

            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading")(
                delayed(process_one)(
                    paper,
                    db,
                    collection,
                    self.udea_reg,
                    self.empty_work(),
                    verbose=self.verbose
                ) for paper in self.ranking
            )

    def run(self):
        self.process_ranking_udea()
        return 0
