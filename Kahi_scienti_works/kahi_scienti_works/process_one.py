from kahi_scienti_works.parser import parse_scienti
from kahi_impactu_utils.Utils import lang_poll, doi_processor
from time import time
from bson import ObjectId
from networkx import volume


def process_one_update(scienti_reg, colav_reg, db, collection, empty_work, verbose=0):
    entry = parse_scienti(
        scienti_reg, empty_work.copy(), verbose=verbose)
    # updated
    for upd in colav_reg["updated"]:
        if upd["source"] == "scienti":
            # adding new author and affiliations to the register
            if "openalex" in [upd["source"] for upd in colav_reg["updated"]]:
                return None
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
                    ids = [ext["id"]
                           for ext in author_db["external_ids"]]
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
                        ids = [ext["id"]
                               for ext in author_db["external_ids"]]
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

            collection.update_one(
                {"_id": colav_reg["_id"]},
                {"$push": {"authors": entry['authors'][0]}, "$inc": {
                    "author_count": 1}}
            )

            return None  # Register already on db
            # Could be updated with new information when scienti database changes
    colav_reg["updated"].append(
        {"source": "scienti", "time": int(time())})
    # titles
    if 'scienti' not in [title['source'] for title in colav_reg["titles"]]:
        lang = lang_poll(entry["titles"][0]["title"])
        colav_reg["titles"].append(
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


def process_one_insert(scienti_reg, db, collection, empty_work, es_handler, verbose=0):
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
                        f'Register with RH: {scienti_reg["COD_RH"]} and COD_PROD: {scienti_reg["COD_PRODUCTO"]} could not be linked to a source with {entry["source"]["external_ids"][0]["source"]}: {entry["source"]["external_ids"][0]["id"]}')  # noqa: E501
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
    response = collection.insert_one(entry)
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


def process_one(scienti_reg, db, collection, empty_work, es_handler, similarity, verbose=0):
    doi = None
    # register has doi
    if "TXT_DOI" in scienti_reg.keys():
        if scienti_reg["TXT_DOI"]:
            doi = doi_processor(scienti_reg["TXT_DOI"])
    if doi:
        # is the doi in colavdb?
        colav_reg = collection.find_one({"external_ids.id": doi})

        if colav_reg:  # update the register
            process_one_update(
                scienti_reg, colav_reg, db, collection, empty_work, verbose=verbose)
        else:  # insert a new register
            process_one_insert(
                scienti_reg, db, collection, empty_work, es_handler, verbose=verbose)
    elif similarity:  # does not have a doi identifier
        # elasticsearch section
        if es_handler:
            # Search in elasticsearch
            authors = []
            for author in scienti_reg["author_others"]:
                authors.append(author["TXT_TOTAL_NAMES_FILTRO"])
            source = ""
            volume = ""
            issue = ""
            page_start = ""
            page_end = ""
            if "article" in scienti_reg["details"][0].keys():
                if "journal" in scienti_reg["details"][0]["article"][0].keys():
                    source = scienti_reg["details"][0]["article"][0]["journal"][0]["TXT_NME_REVISTA"]
                if "TXT_VOLUMEN_REVISTA" in scienti_reg["details"][0]["article"][0].keys():
                    volume = scienti_reg["details"][0]["article"][0]["TXT_VOLUMEN_REVISTA"]
                if "TXT_FASCICULO_REVISTA" in scienti_reg["details"][0]["article"][0].keys():
                    issue = scienti_reg["details"][0]["article"][0]["TXT_FASCICULO_REVISTA"]
                if "TXT_PAGINA_INICIAL" in scienti_reg["details"][0]["article"][0].keys():
                    page_start = scienti_reg["details"][0]["article"][0]["TXT_PAGINA_INICIAL"]
                if "TXT_PAGINA_FINAL" in scienti_reg["details"][0]["article"][0].keys():
                    page_end = scienti_reg["details"][0]["article"][0]["TXT_PAGINA_FINAL"]
            response = es_handler.search_work(
                title=scienti_reg["TXT_NME_PROD_FILTRO"],
                # source=scienti_reg["details"][0]["article"][0]["journal"][0]["TXT_NME_REVISTA"],
                source=source,
                year=scienti_reg["NRO_ANO_PRESENTA"],
                authors=authors,
                # volume=scienti_reg["details"][0]["article"][0]["TXT_VOLUMEN_REVISTA"],
                volume=volume,
                # issue=scienti_reg["details"][0]["article"][0]["TXT_FASCICULO_REVISTA"],
                issue=issue,
                # page_start=scienti_reg["details"][0]["article"][0]["TXT_PAGINA_INICIAL"],
                page_start=page_start,
                # page_end=scienti_reg["details"][0]["article"][0]["TXT_PAGINA_FINAL"],
                page_end=page_end
            )

            if response:  # register already on db... update accordingly
                colav_reg = collection.find_one(
                    {"_id": ObjectId(response["_id"])})
                if colav_reg:
                    process_one_update(scienti_reg, colav_reg, db,
                                       collection, empty_work, verbose)
                else:
                    if verbose > 4:
                        print("Register with {} not found in mongodb".format(
                            response["_id"]))
                        print(response)
            else:  # insert new register
                process_one_insert(scienti_reg, db, collection,
                                   empty_work, es_handler, verbose)
        else:
            if verbose > 4:
                print("No elasticsearch index provided")
