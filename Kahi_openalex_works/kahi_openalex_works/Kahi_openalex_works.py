from kahi.KahiBase import KahiBase
from pymongo import MongoClient, collation, TEXT
from time import time
from datetime import datetime as dt
from joblib import Parallel, delayed
from pandas import read_excel
import traceback
from thefuzz import fuzz, process
from re import sub, split, UNICODE
import unidecode

from langid import classify
import pycld2 as cld2
from langdetect import detect
import ftlangdetect as fd
from lingua import LanguageDetectorBuilder
import iso639


def lang_poll(text):
    text = text.lower()
    text = text.replace("\n", "")
    lang_list = []

    lang_list.append(classify(text)[0].lower())

    detected_language = None
    try:
        _, _, _, detected_language = cld2.detect(text, returnVectors=True)
    except Exception as e:
        print(e)
        text = str(unidecode.unidecode(text).encode("ascii", "ignore"))
        _, _, _, detected_language = cld2.detect(text, returnVectors=True)

    if detected_language:
        lang_list.append(detected_language[0][-1].lower())

    try:
        lang_list.append(detect(text).lower())
    except Exception as e:
        print(e)

    result = fd.detect(text=text, low_memory=True)
    lang_list.append(result["lang"].lower())

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
            la = iso639.languages.get(
                name=res.name.capitalize()).alpha2.lower()
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


def parse_openalex(reg, empty_work):
    entry = empty_work.copy()
    entry["updated"] = [{"source": "openalex", "time": int(time())}]
    if "http" in reg["title"]:
        reg["title"] = reg["title"].split("//")[-1]
    lang = lang_poll(reg["title"])
    entry["titles"].append(
        {"title": reg["title"], "lang": lang, "source": "openalex"})
    for source, idx in reg["ids"].items():
        if "doi" in source:
            idx = idx.replace("https://doi.org/", "").lower()
        entry["external_ids"].append({"source": source, "id": idx})
    entry["year_published"] = reg["publication_year"]
    entry["date_published"] = int(dt.strptime(
        reg["publication_date"], "%Y-%m-%d").timestamp())
    entry["types"].append({"source": "openalex", "type": reg["type"]})
    entry["citations_by_year"] = reg["counts_by_year"]

    entry["source"] = {
        "name": reg["host_venue"]["display_name"],
        "external_ids": [{"source": "openalex", "id": reg["host_venue"]["id"]}]
    }

    if "issn_l" in reg["host_venue"].keys():
        if reg["host_venue"]["issn_l"]:
            entry["source"]["external_ids"].append(
                {"source": "issn_l", "id": reg["host_venue"]["issn_l"]})
    if "issn" in reg["host_venue"].keys():
        if reg["host_venue"]["issn"]:
            entry["source"]["external_ids"].append(
                {"source": "issn", "id": reg["host_venue"]["issn"][0]})

    entry["citations_count"].append(
        {"source": "openalex", "count": reg["cited_by_count"]})

    if "volume" in reg["biblio"]:
        if reg["biblio"]["volume"]:
            entry["bibliographic_info"]["volume"] = reg["biblio"]["volume"]
    if "issue" in reg["biblio"]:
        if reg["biblio"]["issue"]:
            entry["bibliographic_info"]["issue"] = reg["biblio"]["issue"]
    if "first_page" in reg["biblio"]:
        if reg["biblio"]["first_page"]:
            entry["bibliographic_info"]["start_page"] = reg["biblio"]["first_page"]
    if "last_page" in reg["biblio"]:
        if reg["biblio"]["last_page"]:
            entry["bibliographic_info"]["end_page"] = reg["biblio"]["last_page"]
    if "open_access" in reg.keys():
        if "is_oa" in reg["open_access"].keys():
            entry["bibliographic_info"]["is_open_acess"] = reg["open_access"]["is_oa"]
        if "oa_status" in reg["open_access"].keys():
            entry["bibliographic_info"]["open_access_status"] = reg["open_access"]["oa_status"]
        if "oa_url" in reg["open_access"].keys():
            if reg["open_access"]["oa_url"]:
                entry["external_urls"].append(
                    {"source": "oa", "url": reg["open_access"]["oa_url"]})

    # authors section
    for author in reg["authorships"]:
        if not author["author"]:
            continue
        affs = []
        for inst in author["institutions"]:
            if inst:
                aff_entry = {
                    "external_ids": [{"source": "openalex", "id": inst["id"]}],
                    "name": inst["display_name"]
                }
                if "ror" in inst.keys():
                    aff_entry["external_ids"].append(
                        {"source": "ror", "id": inst["ror"]})
                affs.append(aff_entry)
        author = author["author"]
        author_entry = {
            "external_ids": [{"source": "openalex", "id": author["id"]}],
            "full_name": author["display_name"],
            "types": [],
            "affiliations": affs
        }
        if author["orcid"]:
            author_entry["external_ids"].append(
                {"source": "orcid", "id": author["orcid"].replace("https://orcid.org/", "")})
        entry["authors"].append(author_entry)
    # concepts section
    subjects = []
    for concept in reg["concepts"]:
        sub_entry = {
            "external_ids": [{"source": "openalex", "id": concept["id"]}],
            "name": concept["display_name"],
            "level": concept["level"]
        }
        subjects.append(sub_entry)
    entry["subjects"].append({"source": "openalex", "subjects": subjects})

    return entry

def process_one(oa_reg,url,db_name,empty_work):
    client = MongoClient(url)
    db = client[db_name]
    collection = db["works"]
    doi = None
    #register has doi
    if "doi" in oa_reg.keys():
        if oa_reg["doi"]:
            doi = reg["doi"].split(".org/")[-1].lower()
    if doi:
        #is the doi in colavdb?
        colav_reg = collection.find_one({"external_ids.id":doi})
        if colav_reg: #update the register
            pass
        else: #insert a new register
            #parse
            parsed = parse_openalex(oa_reg)
            #link
            #insert
    else:
        #elasticsearch section
        pass

class Kahi_openalex_works(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["works"]

        self.collection.create_index("year_published")
        self.collection.create_index("authors.affiliations.id")
        self.collection.create_index("authors.id")
        self.collection.create_index([("titles.title", TEXT)])

        self.openalex_client = MongoClient(
            config["openalex_works"]["database_url"])
        self.openalex_db = self.openalex_client[config["openalex_works"]
                                                ["database_name"]]
        self.openalex_collection = self.openalex_db[config["openalex_works"]
                                                    ["collection_name"]]

        self.n_jobs = config["works"]["num_jobs"] if "num_jobs" in config["works"].keys(
        ) else 1
        self.verbose = config["works"]["verbose"] if "verbose" in config["works"].keys(
        ) else 0

    def process_openalex(self):
        paper_list = list(self.openalex_collection.find())
        Parallel(
            n_jobs=self.n_jobs,
            verbose=self.verbose,
            backend="threading")(
            delayed(process_one)(
                paper,
                self.mongodb_url,
                self.config["database_name"],
                self.empty_work()
            ) for paper in self.paper_list
        )

    def run(self):
        self.process_openalex()
        return 0
