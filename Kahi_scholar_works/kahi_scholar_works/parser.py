from thefuzz import fuzz
from kahi_impactu_utils.Utils import lang_poll, doi_processor
from time import time


def parse_scholar(reg, empty_work, verbose=0):
    """
    Parse a record from the scholar database into a work entry, using the empty_work as template.

    Parameters
    ----------
    reg : dict
        The record to be parsed from scholar.
    empty_work : dict
        A template for the work entry. Structure is defined in the schema.
    verbose : int
        The verbosity level. Default is 0.
    """
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
        if reg["doi"]:
            doi = doi_processor(reg["doi"])
            if doi:
                entry["external_ids"].append({"provenance": "scholar",
                                              "source": "doi", "id": doi})
    if "cid" in reg.keys():
        entry["external_ids"] = [{"provenance": "scholar",
                                  "source": "scholar", "id": reg["cid"]}]
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
                if entry["bibliographic_info"]["start_page"].isdigit() and entry["bibliographic_info"]["end_page"].isdigit():
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
                            f"Malformed start_page or end_page in source database for {reg['doi']}. Setting 'pages' to the original value.")
                    entry["bibliographic_info"]["pages"] = reg["pages"]
            else:
                if verbose > 4:
                    print(
                        f"""Malformed pages in source database for {reg["doi"]}. Inserting anyway""")
                entry["bibliographic_info"]["pages"] = reg["pages"]
                entry["bibliographic_info"]["start_page"] = reg["pages"]
    if "bibtex" in reg.keys():
        entry["bibliographic_info"]["bibtex"] = reg["bibtex"]
        typ = reg["bibtex"].split("{")[0].replace("@", "")
        entry["types"].append({"provenance": "scholar",
                               "source": "scholar", "type": typ, "level": None})
    if "cites" in reg.keys():
        entry["citations_count"].append(
            {"provenance": "scholar",
             "source": "scholar", "count": int(reg["cites"])})
    if "cites_link" in reg.keys():
        entry["external_urls"].append(
            {"provenance": "scholar",
             "source": "scholar citations", "url": reg["cites_link"]})
    if "pdf" in reg.keys():
        entry["external_urls"].append({"provenance": "scholar",
                                       "source": "pdf", "url": reg["pdf"]})

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
                            {"provenance": "scholar",
                             "source": "scholar", "id": reg["profiles"][name]}]
                        break
                    elif score > 70:
                        score = fuzz.partial_ratio(name, author)
                        if score >= 90:
                            entry["authors"][i]["external_ids"] = [
                                {"provenance": "scholar",
                                 "source": "scholar", "id": reg["profiles"][name]}]
                            break

    return entry
