from kahi.KahiBase import KahiBase
from pymongo import MongoClient, collation
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
        _, _, _, detected_language = cld2.detect(text,  returnVectors=True)
    except Exception as e:
        print(e)
        text = str(unidecode.unidecode(text).encode("ascii", "ignore"))
        _, _, _, detected_language = cld2.detect(text,  returnVectors=True)

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
        if votes > len(lang_list)/2:
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


def parse_puntaje(regs, empty_work, affiliation):
    entry = empty_work.copy()
    reg = regs[0]
    entry["updated"] = [{"source": affiliation, "time": int(time())}]
    lang = lang_poll(reg["titulo"])
    entry["titles"].append(
        {"title": reg["titulo"], "lang": lang, "source": "puntaje"})
    if reg["DOI"]:
        entry["external_ids"].append(
            {"source": "doi", "id": reg["DOI"].lower()})
    if reg["issn"]:
        entry["source"] = {"name": reg["nombre rev o premio"],
                           "external_ids": [{"source": "issn", "id": reg["issn"]}]}
    else:
        entry["source"] = {
            "name": reg["nombre rev o premio"], "external_ids": []}
    entry["year_published"] = int(reg["año realiz"])
    for reg in regs:
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
        entry["authors"].append({
            "external_ids": [{"source": "Cédula de Ciudadanía", "id": reg["cedula"]}],
            "full_name": name["NOMBRE COMPLETO"],
            "types": [],
            "affiliations": [aff]
        })
    return entry


def parse_wos(reg, empty_work):
    entry = empty_work.copy()
    entry["updated"] = [{"source": "wos", "time": int(time())}]
    if "TI" in reg.keys():
        lang = lang_poll(reg["TI"])
        entry["titles"].append(
            {"title": reg["TI"], "lang": lang, "source": "wos"})
    if "AB" in reg.keys():
        if reg["AB"] and reg["AB"] == reg["AB"]:
            entry["abstract"] = reg["AB"].strip()
    if "DT" in reg.keys():
        if reg["DT"] and reg["DT"] == reg["DT"]:
            entry["types"].append(
                {"source": "wos", "type": reg["DT"].strip().lower()})
    if "PY" in reg.keys():
        if reg["PY"] and reg["PY"] == reg["PY"]:
            entry["year_published"] = int(reg["PY"].strip())
    if "BP" in reg.keys():
        if reg["BP"] and reg["BP"] == reg["BP"]:
            entry["bibliographic_info"]["start_page"] = reg["BP"]
    if "EP" in reg.keys():
        if reg["EP"] and reg["EP"] == reg["EP"]:
            entry["bibliographic_info"]["end_page"] = reg["EP"]
    if "VL" in reg.keys():
        if reg["VL"] and reg["VL"] == reg["VL"]:
            entry["bibliographic_info"]["volume"] = reg["VL"].strip()
    if "IS" in reg.keys():
        if reg["IS"] and reg["IS"] == reg["IS"]:
            entry["bibliographic_info"]["issue"] = reg["IS"].strip()

    count = None
    if "Z9" in reg.keys():
        if reg["Z9"] and reg["Z9"] == reg["Z9"]:
            try:
                count = int(reg["Z9"].replace("\n", ""))
            except Exception as e:
                print(e)
                count = None
            entry["citations_count"].append({"source": "wos", "count": count})

    if "DI" in reg.keys():
        if reg["DI"]:
            ext = {"source": "doi", "id": reg["DI"].lower()}
            entry["external_ids"].append(ext)
    if "UT" in reg.keys():
        if reg["UT"]:
            ext = {"source": "wos", "id": reg["UT"].strip().split(":")[1]}
            entry["external_ids"].append(ext)

    source = {"external_ids": []}
    if "SO" in reg.keys():
        if reg["SO"]:
            source["name"] = reg["SO"].rstrip()
    if "SN" in reg.keys():
        if reg["SN"]:
            source["external_ids"].append(
                {"source": "pissn", "id": reg["SN"].rstrip()})
    if "EI" in reg.keys():
        if reg["EI"]:
            source["external_ids"].append(
                {"source": "eissn", "id": reg["EI"].rstrip()})
    if "BN" in reg.keys():
        if reg["BN"]:
            source["external_ids"].append(
                {"source": "isbn", "id": reg["BN"].rstrip()})
    entry["source"] = source

    # authors_section
    if "C1" in reg.keys():
        if reg["C1"]:
            orcid_list = []
            researcherid_list = []
            if "RI" in reg.keys():
                if reg["RI"] and reg["RI"] == reg["RI"]:
                    reg["RI"] = reg["RI"].strip()
                    if reg["RI"][0] == ";":
                        reg["RI"] = reg["RI"][1:]
                    researcherid_list = reg["RI"].replace(
                        "\n", "").rstrip().replace("; ", ";").split(";")
            if "OI" in reg.keys():
                if reg["OI"] and reg["OI"] == reg["OI"]:
                    orcid_list = reg["OI"].rstrip().replace(
                        "; ", ";").split(";")
            for auwaf in reg["C1"].strip().replace(".", "").split("\n"):
                aulen = len(auwaf.split(";"))
                if aulen == 1:
                    auaff = auwaf.split("] ")
                    if len(auaff) == 1:
                        aff = auwaf
                        authors = [""]
                        if "AF" in reg.keys():
                            if len(reg["AF"].rstrip().split("\n")) == 1:
                                authors = reg["AF"].rstrip()
                    else:
                        aff = auaff[1]
                        authors = [auaff[0][1:]]
                else:
                    aff = auwaf.split("] ")[1]
                    authors = auwaf.split("] ")[0][1:].split("; ")
                try:
                    instname = "".join(aff.split(", ")[0])
                except Exception as e:
                    print(e)
                    instname = ""
                for author in authors:
                    entry_ext = []
                    for res in researcherid_list:
                        try:
                            name, rid = res.split("/")[-2:]
                        except Exception as e:
                            print(e)
                        ratio = fuzz.partial_ratio(name, author)
                        if ratio > 90:
                            entry_ext.append(
                                {"source": "researcherid", "id": rid})
                            break
                        elif ratio > 50:
                            ratio = fuzz.token_set_ratio(name, author)
                            if ratio > 90:
                                entry_ext.append(
                                    {"source": "researcherid", "id": rid})
                                break
                            elif ratio > 50:
                                ratio = fuzz.partial_token_set_ratio(
                                    name, author)
                                if ratio > 95:
                                    entry_ext.append(
                                        {"source": "researcherid", "id": rid})
                                    break
                    for res in orcid_list:
                        try:
                            name, rid = res.split("/")[-2:]
                        except Exception as e:
                            print(e)
                        ratio = fuzz.partial_ratio(name, author)
                        if ratio > 90:
                            entry_ext.append({"source": "orcid", "id": rid})
                            break
                        elif ratio > 50:
                            ratio = fuzz.token_set_ratio(name, author)
                            if ratio > 90:
                                entry_ext.append(
                                    {"source": "orcid", "id": rid})
                                break
                            elif ratio > 50:
                                ratio = fuzz.partial_token_set_ratio(
                                    name, author)
                                if ratio > 95:
                                    entry_ext.append(
                                        {"source": "orcid", "id": rid})
                                    break
                    author_entry = {
                        "full_name": author,
                        "types": [],
                        "external_ids": entry_ext,
                        "affiliations": [{
                            "name": instname
                        }]
                    }
                    entry["authors"].append(author_entry)

    return entry


def parse_scopus(reg, empty_work):
    entry = empty_work.copy()
    entry["updated"] = [{"source": "scopus", "time": int(time())}]
    lang = lang_poll(reg["Title"])
    entry["titles"].append(
        {"title": reg["Title"], "lang": lang, "source": "scopus"})
    if "Abstract" in reg.keys():
        if reg["Abstract"] and reg["Abstract"] == reg["Abstract"]:
            entry["abstract"] = reg["Abstract"]
    if "Year" in reg.keys():
        entry["year_published"] = reg["Year"]

    if "Document Type" in reg.keys():
        entry["types"].append(
            {"source": "scopus", "type": reg["Document Type"]})
    if "Index Keywords" in reg.keys():
        if reg["Index Keywords"] and reg["Index Keywords"] == reg["Index Keywords"]:
            entry["keywords"].extend(reg["Index Keywords"].lower().split("; "))
    if "Author Keywords" in reg.keys():
        if reg["Author Keywords"] and reg["Author Keywords"] == reg["Author Keywords"]:
            entry["keywords"].extend(
                reg["Author Keywords"].lower().split("; "))

    if "DOI" in reg.keys():
        entry["external_ids"].append(
            {"source": "doi", "id": reg["DOI"].lower()})
    if "EID" in reg.keys():
        entry["external_ids"].append({"source": "scopus", "id": reg["EID"]})
    if "Pubmed ID" in reg.keys():
        entry["external_ids"].append(
            {"source": "pubmed", "id": reg["Pubmed ID"]})

    if "Link" in reg.keys():
        entry["external_urls"].append({"source": "scopus", "url": reg["Link"]})

    if "Volume" in reg.keys():
        if reg["Volume"] and reg["Volume"] == reg["Volume"]:
            entry["bibliographic_info"]["volume"] = reg["Volume"]
    if "Issue" in reg.keys():
        if reg["Issue"] and reg["Issue"] == reg["Issue"]:
            entry["bibliographic_info"]["issue"] = reg["Issue"]
    if "Page start" in reg.keys():
        # checking for NaN in the second criteria
        if reg["Page start"] and reg["Page start"] == reg["Page start"]:
            entry["bibliographic_info"]["start_page"] = reg["Page start"]
    if "Page end" in reg.keys():
        if reg["Page end"] and reg["Page end"] == reg["Page end"]:
            entry["bibliographic_info"]["end_page"] = reg["Page end"]

    if "Cited by" in reg.keys():
        if reg["Cited by"]:
            try:
                entry["citations_count"].append(
                    {"source": "scopus", "count": int(reg["Cited by"])})
            except Exception as e:
                print(e)

    source = {"external_ids": []}
    if "Source title" in reg.keys():
        if reg["Source title"] and reg["Source title"] == reg["Source title"]:
            source["title"] = reg["Source title"]
    if "ISSN" in reg.keys():
        if reg["ISSN"] and reg["ISSN"] == reg["ISSN"] and isinstance(reg["ISSN"], str):
            source["external_ids"].append(
                {"source": "issn", "id": reg["ISSN"][:4]+"-"+reg["ISSN"][4:]})
        if reg["ISBN"] and reg["ISBN"] == reg["ISBN"] and isinstance(reg["ISBN"], str):
            source["external_ids"].append(
                {"source": "isbn", "id": reg["ISBN"]})
        if reg["CODEN"] and reg["CODEN"] == reg["CODEN"] and isinstance(reg["CODEN"], str):
            source["external_ids"].append(
                {"source": "coden", "id": reg["CODEN"]})
    entry["source"] = source

    # authors section
    ids = None

    if "Authors with affiliations" in reg.keys():
        if reg["Authors with affiliations"] and reg["Authors with affiliations"] == reg["Authors with affiliations"]:
            if "Author(s) ID" in reg.keys():
                ids = reg["Author(s) ID"].split(";")
            auwaf_list = reg["Authors with affiliations"].split("; ")
            for i in range(len(auwaf_list)):
                auaf = split('(^[\w\-\s\.]+,\s+[\w\s\.\-]+,\s)',  # noqa: W605
                             auwaf_list[i], UNICODE)
                if len(auaf) == 1:
                    author = auaf[0]
                    aff_name = ""
                else:
                    author = auaf[1]
                    affiliations = auaf[-1]
                    aff_name = affiliations.split(
                        ",")[-3].strip() if len(affiliations.split(",")) > 2 else affiliations.strip()
                author_entry = {
                    "full_name": author.replace("-", " ").strip(),
                    "types": [],
                    "affiliations": [{"name": aff_name}],
                    "external_ids": []
                }
                if ids:
                    try:
                        author_entry["external_ids"] = [
                            {"source": "scopus", "id": ids[i]}]
                    except Exception as e:
                        print(e)
                entry["authors"].append(author_entry)

    return entry


def parse_scholar(reg, empty_work):
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
        volume = ""
        try:
            if reg["volume"][-1] == "\n":
                reg["volume"] = reg["volume"][:-1]
            volume = int(reg["volume"])
            entry["bibliographic_info"]["volume"] = volume
        except Exception as e:
            print(e)
    if "issue" in reg.keys():
        issue = ""
        try:
            if reg["issue"][-1] == "\n":
                reg["issue"] = reg["issue"][:-1]
            issue = int(reg["issue"])
            entry["bibliographic_info"]["issue"] = issue
        except Exception as e:
            print(e)
    if "pages" in reg.keys():
        pages = ""
        try:
            if reg["pages"][-1] == "\n":
                reg["pages"] = reg["pages"][:-1]
            pages = int(reg["pages"])
            entry["bibliographic_info"]["pages"] = pages
        except Exception as e:
            print(e)
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
            full_name = first_names+" "+last_names
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


def parse_scienti(reg, empty_work):
    entry = empty_work.copy()
    entry["updated"] = [{"source": "scienti", "time": int(time())}]
    lang = lang_poll(reg["TXT_NME_PROD"])
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

    details = reg["details"][0]["article"][0]
    try:
        entry["bibliographic_info"]["start_page"] = details["TXT_PAGINA_INICIAL"]
    except Exception as e:
        print(e)
    try:
        entry["bibliographic_info"]["end_page"] = details["TXT_PAGINA_FINAL"]
    except Exception as e:
        print(e)
    try:
        entry["bibliographic_info"]["volume"] = details["TXT_VOLUMEN_REVISTA"]
    except Exception as e:
        print(e)
    try:
        entry["bibliographic_info"]["issue"] = details["TXT_FASCICULO_REVISTA"]
    except Exception as e:
        print(e)

    # source section
    source = {"external_ids": []}
    if "journal" in details.keys():
        journal = details["journal"][0]
        source["title"] = journal["TXT_NME_REVISTA"]
        if "TXT_ISSN_REF_SEP" in journal.keys():
            source["external_ids"].append(
                {"source": "issn", "id": journal["TXT_ISSN_REF_SEP"]})
        if "COD_REVISTA" in journal.keys():
            source["external_ids"].append(
                {"source": "scienti", "id": journal["COD_REVISTA"]})

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
        "affiliations": [{"name": affiliations}],
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

    return entry


def find_doi(doi, dbs, verbose=0):
    client = MongoClient(dbs["openalex"]["client"])
    db = client[dbs["openalex"]["db"]]
    openalexco = db[dbs["openalex"]["collection"]]
    client = MongoClient(dbs["wos"]["client"])
    db = client[dbs["wos"]["db"]]
    wos = db[dbs["wos"]["collection"]]
    client = MongoClient(dbs["scopus"]["client"])
    db = client[dbs["scopus"]["db"]]
    scopus = db[dbs["scopus"]["collection"]]
    client = MongoClient(dbs["scholar"]["client"])
    db = client[dbs["scholar"]["db"]]
    scholar = db[dbs["scholar"]["collection"]]
    puntaje = read_excel(dbs["puntaje"], dtype={"cedula": str, "DOI": str})
    client = MongoClient(dbs["scienti"]["client"])
    db = client[dbs["scienti"]["db"]]
    scienti = db[dbs["scienti"]["collection"]]
    data = {}
    if verbose > 4:
        print("Searching OpenAlex")
    openalex_reg = openalexco.find({"doi": "https://doi.org/"+doi}).collation(
        collation.Collation(locale="en", strength=2)).limit(1)
    for reg in openalex_reg:
        if reg:
            data["openalex"] = reg
    if verbose > 4:
        print("Searching WoS")
    wos_reg = wos.find({"DI": doi}).collation(
        collation.Collation(locale="en", strength=2)).limit(1)
    for reg in wos_reg:
        if reg:
            data["wos"] = reg
    # if "wos" not in data.keys():
    #    wos_reg=client["wos_colombia_update"]["stage"].find({"DI":doi}).collation(collation.Collation(locale="en",strength=2)).limit(1)
    #    for reg in wos_reg:
    #        if reg:
    #            data["wos"]=reg
    if verbose > 4:
        print("Searching Scopus")
    scopus_reg = scopus.find({"DOI": doi}).collation(
        collation.Collation(locale="en", strength=2)).limit(1)
    for reg in scopus_reg:
        if reg:
            data["scopus"] = reg
    # if scopus not in data.keys():
    #    scopus_reg=client["scopus_colombia_update"]["stage"].find({"DOI":doi}).collation(collation.Collation(locale="en",strength=2)).limit(1)
    #    for reg in scopus_reg:
    #        if reg:
    #            data["scopus"]=reg
    if verbose > 4:
        print("Searching Scholar")
    scholar_reg = scholar.find({"doi": doi}).collation(
        collation.Collation(locale="en", strength=2)).limit(1)
    for reg in scholar_reg:
        if reg:
            data["scholar"] = reg
    # if "scholar" not in data.keys():
    #    scholar_reg=client["scholar_colombia_update"]["stage"].find({"doi":doi}).collation(collation.Collation(locale="en",strength=2)).limit(1)
    #    for reg in scholar_reg:
    #        if reg:
    #            data["scholar"]=reg
    if verbose > 4:
        print("Searching Puntaje")
    puntaje_reg = puntaje[puntaje["DOI"] == doi].to_dict(orient="records")
    if puntaje_reg:
        data["puntaje"] = puntaje_reg
    if verbose > 4:
        print("Searching Scienti")
    scienti_reg = scienti.find({"TXT_DOI": doi}).collation(
        collation.Collation(locale="en", strength=2)).limit(1)
    for reg in scienti_reg:
        if reg:
            data["scienti"] = reg
    return data


def add_source(data, data_source, entry, sources_lists, ids_lists, authors_list):
    entry["updated"].extend(data["updated"])
    titles_sources = [t["source"] for t in entry["titles"]]
    if data_source not in ["wos", "scopus"]:
        if entry["abstract"] == "":
            entry["abstract"] = data["abstract"]
        for title in data["titles"]:
            if title["source"] not in titles_sources:
                entry["titles"].append(title)

    for keyword in data["keywords"]:
        if keyword not in entry["keywords"]:
            entry["keywords"].append(keyword.lower())
    if data["citations_count"]:
        entry["citations_count"].extend(data["citations_count"])
    for ext in data["external_ids"]:
        if ext["id"]:
            if ext["id"] not in ids_lists["ids"] and ext["source"] not in sources_lists["ids"]:
                entry["external_ids"].append(ext)
                sources_lists["ids"].append(ext["source"])
                ids_lists["ids"].append(ext["id"])
    for ext in data["external_urls"]:
        if ext["url"]:
            if ext["url"] not in ids_lists["urls"] and ext["source"] not in sources_lists["urls"]:
                entry["external_urls"].append(ext)
                sources_lists["urls"].append(ext["source"])
                ids_lists["urls"].append(ext["url"])
    for ext in data["types"]:
        if ext["type"]:
            if ext["source"] not in sources_lists["types"]:
                entry["types"].append(ext)
                sources_lists["types"].append(ext["source"])
                ids_lists["types"].append(ext["type"])
            elif ext["source"] == "scienti":
                entry["types"].append(ext)
                sources_lists["types"].append(ext["source"])
                ids_lists["types"].append(ext["type"])
    if "source" in data.keys():
        if data["source"]:
            for ext in data["source"]["external_ids"]:
                if ext["id"] not in ids_lists["sources"] and ext["source"] not in sources_lists["sources"]:
                    entry["source"]["external_ids"].append(ext)
                    sources_lists["sources"].append(ext["source"])
                    ids_lists["sources"].append(ext["id"])
    for key, val in data["bibliographic_info"].items():
        if key not in entry["bibliographic_info"].keys():
            entry["bibliographic_info"][key] = val
        elif not entry["bibliographic_info"][key]:
            entry["bibliographic_info"][key] = val

    for author in data["authors"]:
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
        if idx:
            sources = [ext["source"]
                       for ext in entry["authors"][idx]["external_ids"]]
            ids = [ext["id"] for ext in entry["authors"][idx]["external_ids"]]
            for ext in author["external_ids"]:
                if ext["id"] not in ids:
                    entry["authors"][idx]["external_ids"].append(ext)
                    sources.append(ext["source"])
                    ids.append(ext["id"])
        # Create the same loop as above to improve affiliations
        if "affiliations" in author.keys():
            aff_name_list = [aff["name"] for aff in author["affiliations"]]
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
                    sources = [ext["source"] for ext in entry["authors"]
                               [idx]["affiliations"][jdx]["external_ids"]]
                    ids = [ext["id"] for ext in entry["authors"]
                           [idx]["affiliations"][jdx]["external_ids"]]
                    for ext in author["affiliations"][jdx]["external_ids"]:
                        if ext["id"] not in ids:
                            entry["authors"][idx]["affiliations"][jdx]["external_ids"].append(
                                ext)
                            sources.append(ext["source"])
                            ids.append(ext["id"])
        else:
            author["affiliations"] = []
    return entry, sources_lists, ids_lists, authors_list


# making sure openalex exists to create a copy
def process_doi(data, empty_work, ranking_aff):
    entry = empty_work.copy()
    if "openalex" in data.keys():
        data["openalex"] = parse_openalex(data["openalex"], empty_work.copy())
        entry = data["openalex"].copy()

    sources_lists = {}
    ids_lists = {}
    sources_lists["ids"] = [ext["source"] for ext in entry["external_ids"]]
    ids_lists["ids"] = [ext["id"] for ext in entry["external_ids"]]
    sources_lists["urls"] = [ext["source"] for ext in entry["external_urls"]]
    ids_lists["urls"] = [ext["url"] for ext in entry["external_urls"]]
    sources_lists["types"] = [ext["source"] for ext in entry["types"]]
    ids_lists["types"] = [ext["type"] for ext in entry["types"]]
    sources_lists["sources"] = [ext["source"]
                                for ext in entry["source"]["external_ids"]]
    ids_lists["sources"] = [ext["id"]
                            for ext in entry["source"]["external_ids"]]
    if "external_ids" not in entry["source"]:
        entry["source"]["external_ids"] = []

    au_name_list = [au["full_name"]
                    for au in entry["authors"] if au["full_name"]]
    # print(au_name_list)

    if "wos" in data.keys():
        data["wos"] = parse_wos(data["wos"], empty_work.copy())
        entry, sources_lists, ids_lists, au_name_list = add_source(data["wos"],
                                                                   "wos",
                                                                   entry,
                                                                   sources_lists,
                                                                   ids_lists,
                                                                   au_name_list
                                                                   )

    if "puntaje" in data.keys():
        data["puntaje"] = parse_puntaje(
            data["puntaje"], empty_work.copy(), ranking_aff)
        entry, sources_lists, ids_lists, au_name_list = add_source(data["puntaje"],
                                                                   "puntaje",
                                                                   entry,
                                                                   sources_lists,
                                                                   ids_lists,
                                                                   au_name_list
                                                                   )

    if "scienti" in data.keys():
        data["scienti"] = parse_scienti(data["scienti"], empty_work.copy())
        entry, sources_lists, ids_lists, au_name_list = add_source(data["scienti"],
                                                                   "scienti",
                                                                   entry,
                                                                   sources_lists,
                                                                   ids_lists,
                                                                   au_name_list
                                                                   )
        entry["ranking"] = data["scienti"]["ranking"]
    if "scholar" in data.keys():
        data["scholar"] = parse_scholar(data["scholar"], empty_work.copy())
        entry, sources_lists, ids_lists, au_name_list = add_source(data["scholar"],
                                                                   "scholar",
                                                                   entry,
                                                                   sources_lists,
                                                                   ids_lists,
                                                                   au_name_list
                                                                   )

    if "scopus" in data.keys():
        data["scopus"] = parse_scopus(data["scopus"], empty_work.copy())
        entry, sources_lists, ids_lists, au_name_list = add_source(data["scopus"],
                                                                   "scopus",
                                                                   entry,
                                                                   sources_lists,
                                                                   ids_lists,
                                                                   au_name_list
                                                                   )

    return entry


def linking_doi(entry, url, db_name):
    client = MongoClient(url)
    db = client[db_name]
    # source searching
    source_db = None
    for ext in entry["source"]["external_ids"]:
        source_db = db["sources"].find_one({"external_ids.id": ext["id"]})
        if source_db:
            break
    if source_db:
        entry["source"] = {
            "id": source_db["_id"],
            "names": source_db["names"]
        }
    else:
        print("No source found for\n\t", entry["source"]["external_ids"])

    # subjects searching
    for subjects in entry["subjects"]:
        for i, subj in enumerate(subjects["subjects"]):
            for ext in subj["external_ids"]:
                sub_db = db["subjects"].find_one(
                    {"external_ids.id": ext["id"]})
                if sub_db:
                    name = sub_db["names"][0]["name"]
                    for n in sub_db["names"]:
                        if n["lang"] == "en":
                            name = n["name"]
                            break
                        elif n["lang"] == "es":
                            name = n["name"]
                    entry["subjects"][0]["subjects"][i] = {
                        "id": sub_db["_id"],
                        "name": name,
                        "level": sub_db["level"]
                    }
                    break

    # search authors and affiliations in db
    for i, author in enumerate(entry["authors"]):
        author_db = None
        for ext in author["external_ids"]:
            author_db = db["person"].find_one({"external_ids.id": ext["id"]})
            if author_db:
                break
        if author_db:
            sources = [ext["source"] for ext in author_db["external_ids"]]
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
            author_db = db["person"].find_one(
                {"full_name": author["full_name"]})
            if author_db:
                sources = [ext["source"] for ext in author_db["external_ids"]]
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
                entry["authors"][i]["affiliations"][j] = {
                    "id": aff_db["_id"],
                    "names": aff_db["names"],
                    "types": aff_db["types"]
                }
            else:
                aff_db = db["affiliations"].find_one(
                    {"names.name": aff["name"]})
                if aff_db:
                    entry["authors"][i]["affiliations"][j] = {
                        "id": aff_db["_id"],
                        "names": aff_db["names"],
                        "types": aff_db["types"]
                    }
                else:
                    entry["authors"][i]["affiliations"][j] = {
                        "id": "",
                        "names": [{"name": aff["name"]}],
                        "types": []
                    }

    entry["author_count"] = len(entry["authors"])
    return entry


def insert_doi(entry, url, db_name):
    # check if there is an affiliation of udea when puntaje file is present
    client = MongoClient(url)
    db = client[db_name]
    for author in entry["authors"]:
        if "external_ids" in author.keys():
            del (author["external_ids"])
        for affiliation in author["affiliations"]:
            if "external_ids" in affiliation.keys():
                del (affiliation["external_ids"])
    db["works"].insert_one(entry)


def process_one(doi, url, db_name, dbs, empty_work):
    client = MongoClient(url)
    db = client[db_name]
    ranking_aff = db["affiliaitons"].find_one(
        {"names.name": "University of Antioquia"}, {"_id": 1, "names": 1, "types": 1})
    try:
        entry = find_doi(doi, dbs)
        entry = process_doi(entry, empty_work, ranking_aff=ranking_aff)
        entry = linking_doi(entry, url, db_name)
        insert_doi(entry, url, db_name)
    except Exception:
        print("Error in doi: ", doi)
        print(traceback.format_exc())


class Kahi_works(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["works"]

        self.n_jobs = config["works"]["num_jobs"] if "num_jobs" in config["works"].keys(
        ) else 1
        self.verbose = config["works"]["verbose"] if "verbose" in config["works"].keys(
        ) else 0

        self.dbs = {}

        for key, val in config["works"].items():
            if key == "verbose" or key == "num_jobs":
                continue
            if key == "puntaje":
                self.dbs[key] = val["file_path"]
                continue
            client = val["database_url"]
            db = val["database_name"]
            collection = val["collection_name"]
            self.dbs[key] = {"client": client,
                             "db": db, "collection": collection}

        client = MongoClient(self.dbs["openalex"]["client"])
        db = client[self.dbs["openalex"]["db"]]
        collection = db[self.dbs["openalex"]["collection"]]
        self.openalex_list = list(collection.find(
            {"doi": {"$ne": None}}, {"doi": 1}))
        self.doi_list = []
        for reg in self.openalex_list:
            if "doi" not in reg.keys():
                continue
            if not reg["doi"]:
                continue
            doi = reg["doi"].split(".org/")[-1].lower()
            if doi not in self.doi_list:
                self.doi_list.append(doi)
        del (self.openalex_list)
        if self.verbose > 4:
            print("Processing ", len(self.doi_list), " dois")

    def process_dois(self):
        Parallel(
            n_jobs=self.n_jobs,
            verbose=self.verbose,
            backend="threading")(
            delayed(process_one)(
                doi,
                self.mongodb_url,
                self.config["database_name"],
                self.dbs,
                self.empty_work()
            ) for doi in self.doi_list
        )

    def run(self):
        self.process_dois()
        return 0
