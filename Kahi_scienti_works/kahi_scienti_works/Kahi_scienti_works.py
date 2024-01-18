from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed
from re import sub, split, UNICODE
import unidecode

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


def parse_scienti(reg, empty_work, verbose=0):
    entry = empty_work.copy()
    entry["updated"] = [{"source": "scienti", "time": int(time())}]
    lang = lang_poll(reg["TXT_NME_PROD"], verbose=verbose)
    entry["titles"].append(
        {"title": reg["TXT_NME_PROD"], "lang": lang, "source": "scienti"})
    entry["external_ids"].append({"source": "COD_RH", "id": reg["COD_RH"]})
    entry["external_ids"].append(
        {"source": "COD_PRODUCTO", "id": reg["COD_PRODUCTO"]})
    if "TXT_DOI" in reg.keys():
        entry["external_ids"].append(
            {"source": "doi", "id": reg["TXT_DOI"].lower()})
    if "TXT_WEB_PRODUCTO" in reg.keys():
        entry["external_urls"].append(
            {"source": "scienti", "url": reg["TXT_WEB_PRODUCTO"]})
    if "SGL_CATEGORIA" in reg.keys():
        entry["ranking"].append(
            {"date": "", "rank": reg["SGL_CATEGORIA"], "source": "scienti"})
    entry["types"].append(
        {"source": "scienti", "type": reg["product_type"][0]["TXT_NME_TIPO_PRODUCTO"]})
    if "product_type" in reg["product_type"][0].keys():
        typ = reg["product_type"][0]["product_type"][0]["TXT_NME_TIPO_PRODUCTO"]
        entry["types"].append({"source": "scienti", "type": typ})

    # details only for articles
    if "details" in reg.keys() and len(reg["details"]) > 0 and "article" in reg["details"][0].keys():
        details = reg["details"][0]["article"][0]
        try:
            if "TXT_PAGINA_INICIAL" in details.keys():
                entry["bibliographic_info"]["start_page"] = details["TXT_PAGINA_INICIAL"]
        except Exception as e:
            if verbose > 4:
                print(
                    f'Error parsing start page on RH:{reg["COD_RH"]} and COD_PROD:{reg["COD_PRODUCTO"]}')
                print(e)
        try:
            if "TXT_PAGINA_FINAL" in details.keys():
                entry["bibliographic_info"]["end_page"] = details["TXT_PAGINA_FINAL"]
        except Exception as e:
            if verbose > 4:
                print(
                    f'Error parsing end page on RH:{reg["COD_RH"]} and COD_PROD:{reg["COD_PRODUCTO"]}')
                print(e)
        try:
            if "TXT_VOLUMEN_REVISTA" in details.keys():
                entry["bibliographic_info"]["volume"] = details["TXT_VOLUMEN_REVISTA"]
        except Exception as e:
            if verbose > 4:
                print(
                    f'Error parsing volume on RH:{reg["COD_RH"]} and COD_PROD:{reg["COD_PRODUCTO"]}')
                print(e)
        try:
            if "TXT_FASCICULO_REVISTA" in details.keys():
                entry["bibliographic_info"]["issue"] = details["TXT_FASCICULO_REVISTA"]
        except Exception as e:
            if verbose > 4:
                print(
                    f'Error parsing issue on RH:{reg["COD_RH"]} and COD_PROD:{reg["COD_PRODUCTO"]}')
                print(e)

        # source section
        source = {"external_ids": [], "title": ""}
        if "journal" in details.keys():
            journal = details["journal"][0]
            source["title"] = journal["TXT_NME_REVISTA"]
            if "TXT_ISSN_REF_SEP" in journal.keys():
                source["external_ids"].append(
                    {"source": "issn", "id": journal["TXT_ISSN_REF_SEP"]})
            if "COD_REVISTA" in journal.keys():
                source["external_ids"].append(
                    {"source": "scienti", "id": journal["COD_REVISTA"]})
        elif "journal_others" in details.keys():
            journal = details["journal_others"][0]
            source["title"] = journal["TXT_NME_REVISTA"]
            if "TXT_ISSN_REF_SEP" in journal.keys():
                source["external_ids"].append(
                    {"source": "issn", "id": journal["TXT_ISSN_REF_SEP"]})
            if "COD_REVISTA" in journal.keys():
                source["external_ids"].append(
                    {"source": "scienti", "id": journal["COD_REVISTA"]})

        entry["source"] = source

    # authors section
    affiliations = []
    if "group" in reg.keys():
        group = reg["group"][0]
        affiliations.append({
            "external_ids": [{"source": "scienti", "id": group["COD_ID_GRUPO"]}],
            "name": group["NME_GRUPO"]
        })
        if "institution" in group.keys():
            inst = group["institution"][0]
            affiliations.append({
                "external_ids": [{"source": "scienti", "id": inst["COD_INST"]}],
                "name": inst["NME_INST"]
            })
    author = reg["author"][0]
    author_entry = {
        "full_name": author["TXT_TOTAL_NAMES"],
        "types": [],
        "affiliations": affiliations,
        "external_ids": [{"source": "scienti", "id": author["COD_RH"]}]
    }
    if author["TPO_DOCUMENTO_IDENT"] == "P":
        author_entry["external_ids"].append(
            {"source": "Passport", "id": author["NRO_DOCUMENTO_IDENT"]})
    if author["TPO_DOCUMENTO_IDENT"] == "C":
        author_entry["external_ids"].append(
            {"source": "Cédula de Ciudadanía", "id": author["NRO_DOCUMENTO_IDENT"]})
    if author["TPO_DOCUMENTO_IDENT"] == "E":
        author_entry["external_ids"].append(
            {"source": "Cédula de Extranjería", "id": author["NRO_DOCUMENTO_IDENT"]})
    entry["authors"] = [author_entry]

    return entry


def process_one(scienti_reg, client, url, db_name, empty_work, verbose=0, multiprocessing=False):
    if multiprocessing:
        # TODO: fix multiprocessing support if possible
        client = MongoClient(url)
    db = client[db_name]
    collection = db["works"]
    doi = None
    # register has doi
    if "TXT_DOI" in scienti_reg.keys():
        if scienti_reg["TXT_DOI"]:
            doi = sub(r'https*\:\/\/[\w\.]+\/',
                      '', scienti_reg["TXT_DOI"]).lower()
    if doi:
        # is the doi in colavdb?
        colav_reg = collection.find_one({"external_ids.id": doi})
        if colav_reg:  # update the register
            entry = parse_scienti(
                scienti_reg, empty_work.copy(), verbose=verbose)
            # updated
            for upd in colav_reg["updated"]:
                if upd["source"] == "scienti":
                    if multiprocessing:
                        client.close()
                    return None  # Register already on db
                    # Could be updated with new information when scienti database changes
            colav_reg["updated"].append(
                {"source": "scienti", "time": int(time())})
            # titles
            lang = lang_poll(entry["titles"][0]["title"])
            entry["titles"].append(
                {"title": entry["titles"][0]["title"], "lang": lang, "source": "scienti"})
            # external_ids
            ext_ids = [ext["id"] for ext in colav_reg["external_ids"]]
            for ext in entry["external_ids"]:
                if ext["id"] not in ext_ids:
                    colav_reg["external_ids"].append(ext)
                    ext_ids.append(ext["id"])
            # types
            types = [ext["source"] for ext in colav_reg["types"]]
            for typ in entry["types"]:
                if typ["source"] not in types:
                    colav_reg["types"].append(typ)

            # external urls
            url_sources = [url["source"]
                           for url in colav_reg["external_urls"]]
            for ext in entry["external_urls"]:
                if ext["source"] not in url_sources:
                    colav_reg["external_urls"].append(ext)
                    url_sources.append(ext["source"])

            collection.update_one(
                {"_id": colav_reg["_id"]},
                {"$set": {
                    "updated": colav_reg["updated"],
                    "titles": colav_reg["titles"],
                    "external_ids": colav_reg["external_ids"],
                    "types": colav_reg["types"],
                    "bibliographic_info": colav_reg["bibliographic_info"],
                    "external_urls": colav_reg["external_urls"],
                    "subjects": colav_reg["subjects"],
                }}
            )
        else:  # insert a new register
            # parse
            entry = parse_scienti(scienti_reg, empty_work.copy())
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
                if "external_ids" in entry["source"].keys():
                    if len(entry["source"]["external_ids"]) == 0:
                        if verbose > 4:
                            if "title" in entry["source"].keys():
                                print(
                                    f'Register with RH: {scienti_reg["COD_RH"]} and COD_PROD: {scienti_reg["COD_PRODUCTO"]} could not be linked to a source with name: {entry["source"]["title"]}')
                            else:
                                print(
                                    f'Register with RH: {scienti_reg["COD_RH"]} and COD_PROD: {scienti_reg["COD_PRODUCTO"]} does not provide a source')
                    else:
                        if verbose > 4:
                            print(
                                f'Register with RH: {scienti_reg["COD_RH"]} and COD_PROD: {scienti_reg["COD_PRODUCTO"]} could not be linked to a source with {entry["source"]["external_ids"][0]["source"]}: {entry["source"]["external_ids"][0]["id"]}')
                else:
                    if "title" in entry["source"].keys():
                        if entry["source"]["title"] == "":
                            if verbose > 4:
                                print(
                                    f'Register with RH: {scienti_reg["COD_RH"]} and COD_PROD: {scienti_reg["COD_PRODUCTO"]} does not provide a source')
                        else:
                            if verbose > 4:
                                print(
                                    f'Register with RH: {scienti_reg["COD_RH"]} and COD_PROD: {scienti_reg["COD_PRODUCTO"]} could not be linked to a source with name: {entry["source"]["title"]}')
                    else:
                        if verbose > 4:
                            print(
                                f'Register with RH: {scienti_reg["COD_RH"]} and COD_PROD: {scienti_reg["COD_PRODUCTO"]} could not be linked to a source (no ids and no name)')

                entry["source"] = {
                    "id": "",
                    "name": entry["source"]["title"] if "title" in entry["source"].keys() else ""
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
    if multiprocessing:
        client.close()


class Kahi_scienti_works(KahiBase):

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
        self.collection.create_index("external_ids.id")

        self.n_jobs = config["scienti_works"]["num_jobs"] if "num_jobs" in config["scienti_works"].keys(
        ) else 1
        self.verbose = config["scienti_works"]["verbose"] if "verbose" in config["scienti_works"].keys(
        ) else 0

    def process_scienti(self, config):
        client = MongoClient(config["database_url"])
        db = client[config["database_name"]]
        scienti = db[config["collection_name"]]
        paper_list = list(scienti.find())
        client.close()
        if self.verbose > 0:
            print("Processing {} papers".format(len(paper_list)))
        Parallel(
            n_jobs=self.n_jobs,
            verbose=self.verbose,
            backend="threading")(
            delayed(process_one)(
                paper,
                self.client,
                self.mongodb_url,
                self.config["database_name"],
                self.empty_work(),
                verbose=self.verbose
            ) for paper in paper_list
        )

    def run(self):
        for config in self.config["scienti_works"]["databases"]:
            if self.verbose > 0:
                print("Processing {} database".format(config["database_name"]))
            if self.verbose > 4:
                print("Updating already inserted entries")
            print(config)
            print(type(config))
            self.process_scienti(config)
        self.client.close()
        return 0
