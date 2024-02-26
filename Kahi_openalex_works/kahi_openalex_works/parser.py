from kahi_impactu_utils.Utils import lang_poll
from time import time
from datetime import datetime as dt


def parse_openalex(reg, empty_work, verbose=0):
    entry = empty_work.copy()
    entry["updated"] = [{"source": "openalex", "time": int(time())}]
    if reg["title"]:
        if "http" in reg["title"]:
            reg["title"] = reg["title"].split("//")[-1]
        lang = lang_poll(reg["title"], verbose=verbose)
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

    if reg["primary_location"]['source']:
        entry["source"] = {
            "name": reg["primary_location"]['source']["display_name"],
            "external_ids": [{"source": "openalex", "id": reg["primary_location"]['source']["id"]}]
        }

        if "issn_l" in reg["primary_location"]['source'].keys():
            if reg["primary_location"]['source']["issn_l"]:
                entry["source"]["external_ids"].append(
                    {"source": "issn_l", "id": reg["primary_location"]['source']["issn_l"]})

        if "issn" in reg["primary_location"]['source'].keys():
            if reg["primary_location"]['source']["issn"]:
                entry["source"]["external_ids"].append(
                    {"source": "issn", "id": reg["primary_location"]['source']["issn"][0]})

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
            entry["bibliographic_info"]["is_open_access"] = reg["open_access"]["is_oa"]
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
