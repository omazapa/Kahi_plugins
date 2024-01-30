from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed
from re import sub, split, UNICODE
import unidecode
from thefuzz import fuzz, process

from langid import classify
import pycld2 as cld2
from langdetect import DetectorFactory, PROFILES_DIRECTORY
from fastspell import FastSpell
from lingua import LanguageDetectorBuilder
import iso639

from mohan.Similarity import Similarity

fast_spell = FastSpell("en", mode="cons")


def lang_poll(text, verbose=0):
    """"
    Detects the language of a text using several methods and returns the most voted language

    Parameters
    ----------
    text : str
        Text to detect language
    verbose : int
        Verbosity level
    Returns
    -------
    str
        Most voted language
    """
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


def parse_scholar(reg, empty_work, verbose=0):
    entry = empty_work.copy()
    entry["updated"] = [{"source": "scholar", "time": int(time())}]
    lang = lang_poll(reg["title"])
    entry["titles"].append(
        {"title": reg["title"], "lang": lang, "source": "scholar"})
    if "year" in reg.keys():
        year = ""
        try:
            if reg["year"][-1] == "\n":
                reg["year"] = reg["year"][:-1]
            year = int(reg["year"])
        except Exception as e:
            if verbose > 4:
                print(f"""Could not convert year to int in {reg["doi"]}""")
                print(e)
        entry["year_published"] = year
    if "doi" in reg.keys():
        entry["external_ids"].append(
            {"source": "doi", "id": reg["doi"].lower()})
    if "cid" in reg.keys():
        entry["external_ids"] = [{"source": "scholar", "id": reg["cid"]}]
    if "abstract" in reg.keys():
        entry["abstract"] = reg["abstract"]
    if "volume" in reg.keys():
        if reg["volume"]:
            if reg["volume"][-1] == "\n":
                reg["volume"] = reg["volume"][:-1]
            entry["bibliographic_info"]["volume"] = reg["volume"]
    if "issue" in reg.keys():
        if reg["issue"]:
            if reg["issue"][-1] == "\n":
                reg["issue"] = reg["issue"][:-1]
            entry["bibliographic_info"]["issue"] = reg["issue"]
    if "pages" in reg.keys():
        pages = ""
        if reg["pages"]:
            if reg["pages"][-1] == "\n":
                reg["pages"] = reg["pages"][:-1]
            if "--" in reg["pages"]:
                reg["pages"].replace("\n", "")
                pages = reg["pages"].split("--")
                entry["bibliographic_info"]["start_page"] = pages[0]
                entry["bibliographic_info"]["end_page"] = pages[1]
                try:
                    entry["bibliographic_info"]["pages"] = str(int(
                        entry["bibliographic_info"]["end_page"]) - int(entry["bibliographic_info"]["start_page"]))
                except Exception as e:
                    if verbose > 4:
                        print(
                            f"""Could not cast pages to substract in {reg["doi"]}""")
                        print(e)
            else:
                if verbose > 4:
                    print(
                        f"""Malformed pages in source database for {reg["doi"]}. Inserting anyway""")
                entry["bibliographic_info"]["pages"] = reg["pages"]
                entry["bibliographic_info"]["start_page"] = reg["pages"]
    if "bibtex" in reg.keys():
        entry["bibliographic_info"]["bibtex"] = reg["bibtex"]
        typ = reg["bibtex"].split("{")[0].replace("@", "")
        entry["types"].append({"source": "scholar", "type": typ})
    if "cites" in reg.keys():
        entry["citations_count"].append(
            {"source": "scholar", "count": int(reg["cites"])})
    if "cites_link" in reg.keys():
        entry["external_urls"].append(
            {"source": "scholar citations", "url": reg["cites_link"]})
    if "pdf" in reg.keys():
        entry["external_urls"].append({"source": "pdf", "url": reg["pdf"]})

    if "journal" in reg.keys():
        entry["source"] = {"name": reg["journal"], "external_ids": []}

    # authors section
    full_name_list = []
    if "author" in reg.keys():
        for author in reg["author"].strip().split(" and "):
            if "others" in author:
                continue
            author_entry = {}
            names_list = author.split(", ")
            last_names = ""
            first_names = ""
            if len(names_list) > 0:
                last_names = names_list[0].strip()
            if len(names_list) > 1:
                first_names = names_list[1].strip()
            full_name = first_names + " " + last_names
            author_entry["full_name"] = full_name
            author_entry["affiliations"] = []
            author_entry["external_ids"] = []
            entry["authors"].append(author_entry)
            full_name_list.append(full_name)
    if "profiles" in reg.keys():
        if reg["profiles"]:
            for name in reg["profiles"].keys():
                for i, author in enumerate(full_name_list):
                    score = fuzz.ratio(name, author)
                    if score >= 80:
                        entry["authors"][i]["external_ids"] = [
                            {"source": "scholar", "id": reg["profiles"][name]}]
                        break
                    elif score > 70:
                        score = fuzz.partial_ratio(name, author)
                        if score >= 90:
                            entry["authors"][i]["external_ids"] = [
                                {"source": "scholar", "id": reg["profiles"][name]}]
                            break

    return entry


def update_register(scholar_reg, colav_reg, url, db_name, empty_work, verbose=0):
    client = MongoClient(url)
    db = client[db_name]
    collection = db["works"]

    # updated
    for upd in colav_reg["updated"]:
        if upd["source"] == "scholar":
            client.close()
            return None  # Register already on db
            # Could be updated with new information when scholar database changes
    entry = parse_scholar(
        scholar_reg, empty_work.copy(), verbose=verbose)
    colav_reg["updated"].append(
        {"source": "scholar", "time": int(time())})
    # titles
    colav_reg["titles"].extend(entry["titles"])
    # external_ids
    ext_ids = [ext["id"] for ext in colav_reg["external_ids"]]
    for ext in entry["external_ids"]:
        if ext["id"] not in ext_ids:
            colav_reg["external_ids"].append(ext)
            ext_ids.append(ext["id"])
    # types
    colav_reg["types"].extend(entry["types"])
    # bibliographic info
    if "start_page" not in colav_reg["bibliographic_info"].keys():
        if "start_page" in entry["bibliographic_info"].keys():
            colav_reg["bibliographic_info"]["start_page"] = entry["bibliographic_info"]["start_page"]
    if "end_page" not in colav_reg["bibliographic_info"].keys():
        if "end_page" in entry["bibliographic_info"].keys():
            colav_reg["bibliographic_info"]["end_page"] = entry["bibliographic_info"]["end_page"]
    if "volume" not in colav_reg["bibliographic_info"].keys():
        if "volume" in entry["bibliographic_info"].keys():
            colav_reg["bibliographic_info"]["volume"] = entry["bibliographic_info"]["volume"]
    if "issue" not in colav_reg["bibliographic_info"].keys():
        if "issue" in entry["bibliographic_info"].keys():
            colav_reg["bibliographic_info"]["issue"] = entry["bibliographic_info"]["issue"]

    # external urls
    urls_sources = [url["source"]
                    for url in colav_reg["external_urls"]]
    for ext in entry["external_urls"]:
        if ext["url"] not in urls_sources:
            colav_reg["external_urls"].append(ext)
            urls_sources.append(ext["url"])

    # citations count
    if entry["citations_count"]:
        colav_reg["citations_count"].extend(entry["citations_count"])

    # authors
    authors_list = [au["full_name"]
                    for au in colav_reg["authors"] if au["full_name"]]  # list of names from authors in db register
    # loop over authors in scholar register (already parsed)
    for author in entry["authors"]:
        idx = None
        match, score = process.extractOne(
            author["full_name"], authors_list, scorer=fuzz.ratio)
        # print("Ratio: ",score,author["full_name"],match)
        if score >= 70:
            idx = authors_list.index(match)
        elif score > 50:
            match, score = process.extractOne(
                author["full_name"], authors_list, scorer=fuzz.partial_ratio)
            # print("Partial ratio: ",score,author["full_name"],match)
            if score >= 80:
                idx = authors_list.index(match)
            elif score > 60:
                match, score = process.extractOne(
                    author["full_name"], authors_list, scorer=fuzz.token_sort_ratio)
                # print("Token sort ratio: ",score,author["full_name"],match)
                if score >= 99:
                    idx = authors_list.index(match)
        if idx:  # if author already on db
            # Get the sources and ids of the external ids of the author
            sources = [ext["source"]
                       for ext in colav_reg["authors"][idx]["external_ids"]]
            ids = [ext["id"]
                   for ext in colav_reg["authors"][idx]["external_ids"]]
            # Add the new external ids to the author
            for ext in author["external_ids"]:
                if ext["id"] not in ids:
                    colav_reg["authors"][idx]["external_ids"].append(ext)
                    sources.append(ext["source"])
                    ids.append(ext["id"])
            # Create the same loop as above to improve affiliations
            aff_name_list = [aff["name"] for aff in colav_reg["authors"][idx]
                             ["affiliations"] if "affiliations" in colav_reg["authors"][idx].keys()]
            if "affiliations" in author.keys():
                for aff in author["affiliations"]:
                    jdx = None
                    match, score = process.extractOne(
                        aff["name"], aff_name_list, scorer=fuzz.ratio)
                    # print("Ratio: ",score,author["full_name"],match)
                    if score >= 70:
                        jdx = aff_name_list.index(match)
                    elif score > 50:
                        match, score = process.extractOne(
                            aff["name"], aff_name_list, scorer=fuzz.partial_ratio)
                        # print("Partial ratio: ",score,author["full_name"],match)
                        if score >= 80:
                            jdx = aff_name_list.index(match)
                        elif score > 60:
                            match, score = process.extractOne(
                                aff["full_name"], aff_name_list, scorer=fuzz.token_sort_ratio)
                            # print("Token sort ratio: ",score,author["full_name"],match)
                            if score >= 99:
                                jdx = aff_name_list.index(match)
                    if jdx:
                        sources = [ext["source"] for ext in colav_reg["authors"]
                                   [idx]["affiliations"][jdx]["external_ids"]]
                        ids = [ext["id"] for ext in colav_reg["authors"]
                               [idx]["affiliations"][jdx]["external_ids"]]
                        for ext in author["affiliations"][jdx]["external_ids"]:
                            if ext["id"] not in ids:
                                colav_reg["authors"][idx]["affiliations"][jdx]["external_ids"].append(
                                    ext)
                                sources.append(ext["source"])
                                ids.append(ext["id"])

    collection.update_one(
        {"_id": colav_reg["_id"]},
        {"$set": {
            "updated": colav_reg["updated"],
            "titles": colav_reg["titles"],
            "external_ids": colav_reg["external_ids"],
            "types": colav_reg["types"],
            "bibliographic_info": colav_reg["bibliographic_info"],
            "external_urls": colav_reg["external_urls"],
            "citations_count": colav_reg["citations_count"],
            "authors": colav_reg["authors"]
        }}
    )
    client.close()


def insert_new_register(scholar_reg, url, db_name, empty_work, es_handler=None, verbose=0):
    client = MongoClient(url)
    db = client[db_name]
    collection = db["works"]
    # parse
    entry = parse_scholar(
        scholar_reg, empty_work.copy(), verbose=verbose)
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
                    f'Register with doi: {scholar_reg["doi"]} does not provide a source')
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
    response = collection.insert_one(entry)
    client.close()

    # insert in elasticsearch
    if es_handler:
        work = {}
        work["title"] = entry["titles"][0]["title"]
        work["source"] = entry["source"]["name"]
        work["year"] = entry["year_published"]
        work["volume"] = entry["bibliographic_info"]["volume"] if "volume" in entry["bibliographic_info"].keys() else ""
        work["issue"] = entry["bibliographic_info"]["issue"] if "issue" in entry["bibliographic_info"].keys() else ""
        work["first_page"] = entry["bibliographic_info"]["first_page"] if "first_page" in entry["bibliographic_info"].keys() else ""
        work["last_page"] = entry["bibliographic_info"]["last_page"] if "last_page" in entry["bibliographic_info"].keys() else ""
        authors = []
        for author in entry['authors']:
            if len(authors) >= 5:
                break
            if "full_name" in author.keys():
                authors.append(author["full_name"])
        work["authors"] = authors
        es_handler.insert_work(_id=str(response.inserted_id), work=work)
    else:
        if verbose > 4:
            print("No elasticsearch index provided")


def process_one_with_doi(scholar_reg, url, db_name, empty_work, es_handler=None, verbose=0):
    client = MongoClient(url)
    db = client[db_name]
    collection = db["works"]
    doi = None
    # register has doi
    if scholar_reg["doi"]:
        if isinstance(scholar_reg["doi"], str):
            doi = scholar_reg["doi"].lower().strip()

    # is the doi in colavdb?
    colav_reg = collection.find_one({"external_ids.id": doi})
    client.close()
    if colav_reg:  # update the register
        update_register(scholar_reg, colav_reg, url,
                        db_name, empty_work, verbose)
    else:  # insert a new register
        insert_new_register(scholar_reg, url, db_name,
                            empty_work, es_handler=es_handler, verbose=verbose)


def process_one_without_doi(scholar_reg, url, db_name, empty_work, es_handler=None, verbose=0):
    client = MongoClient(url)
    db = client[db_name]
    collection = db["works"]

    if es_handler:
        # Search in elasticsearch

        response = es_handler.search_work(
            title=scholar_reg["title"],
            source=scholar_reg["journal"],
            year=str(scholar_reg["year"]),
            authors=[auth.split(", ")[-1] + " " + auth.split(", ")[0]
                     for auth in scholar_reg["author"].split(" and ")],
            volume=scholar_reg["volume"] if "volume" in scholar_reg.keys(
            ) else "",
            issue=scholar_reg["issue"] if "issue" in scholar_reg.keys(
            ) else "",
            page_start=scholar_reg["pages"].split(
                "--")[0] if "pages" in scholar_reg.keys() else "",
            page_end=scholar_reg["pages"].split(
                "--")[-1] if "pages" in scholar_reg.keys() else "",
        )

        if response:  # register already on db... update accordingly
            colav_reg = collection.find_one({"_id": response["_id"]})
            client.close()
            if colav_reg:
                update_register(scholar_reg, colav_reg, url,
                                db_name, empty_work, verbose)
            else:
                if verbose > 4:
                    print("Register with {} not found in mongodb".format(
                        response["_id"]))
                    print(response)
        else:  # insert new register
            insert_new_register(scholar_reg, url, db_name,
                                empty_work, es_handler, verbose)
    else:
        if verbose > 4:
            print("No elasticsearch index provided")


class Kahi_scholar_works(KahiBase):

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

        self.scholar_client = MongoClient(
            config["scholar_works"]["database_url"])
        self.scholar_db = self.scholar_client[config["scholar_works"]
                                              ["database_name"]]
        self.scholar_collection = self.scholar_db[config["scholar_works"]
                                                  ["collection_name"]]

        if "es_index" in config["scholar_works"].keys() and "es_url" in config["scholar_works"].keys() and "es_user" in config["scholar_works"].keys() and "es_password" in config["scholar_works"].keys():
            es_index = config["scholar_works"]["es_index"]
            es_url = config["scholar_works"]["es_url"]
            es_auth = (config["scholar_works"]["es_user"], config["scholar_works"]["es_password"])
            self.es_handler = Similarity(
                es_index, es_uri=es_url, es_auth=es_auth)
        else:
            self.es_handler = None
            print("WARNING: No elasticsearch configuration provided")

        self.task = config["scholar_works"]["task"]

        self.n_jobs = config["scholar_works"]["num_jobs"] if "num_jobs" in config["scholar_works"].keys(
        ) else 1
        self.verbose = config["scholar_works"]["verbose"] if "verbose" in config["scholar_works"].keys(
        ) else 0

    def process_scholar(self):
        # selects papers with doi according to task variable
        if self.task == "doi":
            paper_list = list(
                self.scholar_collection.find({"doi": {"$ne": ""}}))
            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading")(
                delayed(process_one_with_doi)(
                    paper,
                    self.mongodb_url,
                    self.config["database_name"],
                    self.empty_work(),
                    es_handler=self.es_handler,
                    verbose=self.verbose
                ) for paper in paper_list
            )
        else:  # By default the task processes papers without doi
            paper_list = list(self.scholar_collection.find({"doi": ""}))
            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading")(
                delayed(process_one_without_doi)(
                    paper,
                    self.mongodb_url,
                    self.config["database_name"],
                    self.empty_work(),
                    es_handler=self.es_handler,
                    verbose=self.verbose
                ) for paper in paper_list
            )

    def run(self):
        self.process_scholar()
        return 0
