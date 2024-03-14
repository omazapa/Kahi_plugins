from kahi_minciencias_opendata_works.parser import parse_minciencias_opendata
from kahi_impactu_utils.Utils import compare_author
from time import time
from bson import ObjectId
from re import search


def process_one_update(openadata_reg, colav_reg, db, collection, empty_work, verbose=0):
    """
    Method to update a register in the kahi database from minciencias opendata database if it is found.
    This means that the register is already on the kahi database and it is being updated with new information.


    Parameters
    ----------
    openadata_reg : dict
        Register from the minciencias opendata database
    colav_reg : dict
        Register from the colav database (kahi database for impactu)
    db : pymongo.database.Database
        Database where the colav collections are stored, used to search for authors and affiliations.
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    entry = parse_minciencias_opendata(
        openadata_reg, empty_work.copy(), verbose=verbose)
    # updated
    for upd in colav_reg["updated"]:
        if upd["source"] == "minciencias":
            return None  # Register already on db
    colav_reg["updated"].append(
        {"source": "minciencias", "time": int(time())})
    # titles
    if 'minciencias' not in [title['source'] for title in colav_reg["titles"]]:
        lang = entry["titles"][0]["lang"]
        colav_reg["titles"].append(
            {"title": entry["titles"][0]["title"], "lang": lang, "source": "minciencias"})
    # external_ids
    exts = [ext_ for ext_ in colav_reg["external_ids"]]
    for ext in entry["external_ids"]:
        if ext not in exts:
            colav_reg["external_ids"].append(ext)
            exts.append(ext["id"])
    # types
    types = [ext["source"] for ext in colav_reg["types"]]
    for typ in entry["types"]:
        if typ["source"] not in types:
            colav_reg["types"].append(typ)
    # authors
    minciencias_author = ""
    if "authors" in entry.keys():
        if entry["authors"]:
            minciencias_author = entry["authors"][0]

    if minciencias_author:
        author_found = False
        if "external_ids" in minciencias_author.keys() and minciencias_author["affiliations"]:
            for ext in minciencias_author["external_ids"]:

                author_db = db["person"].find_one(
                    {"external_ids.source": "scienti", "external_ids.id": ext["id"]})
                if not author_db:
                    author_db = db["person"].find_one({"external_ids.id": ext["id"]})
                if author_db:
                    group_id = minciencias_author["affiliations"][0]['external_ids'][0]['id']

                    affiliations_db = db["affiliations"].find_one(
                        {"external_ids.source": "scienti", "external_ids.id": group_id})
                    if not affiliations_db:
                        affiliations_db = db["affiliations"].find_one(
                            {"external_ids.id": group_id})

                    if affiliations_db:
                        for i, author in enumerate(colav_reg["authors"]):
                            if author_db["_id"] != author["id"]:
                                continue
                            author_affiliations = [str(aff['id'])
                                                   for aff in author['affiliations']]
                            if str(affiliations_db["_id"]) not in author_affiliations:
                                author["affiliations"].append(
                                    {
                                        "id": affiliations_db["_id"],
                                        "name": affiliations_db["names"][0]["name"],
                                        "types": affiliations_db["types"]})
                                author_found = True
                                if verbose > 4:
                                    print("group added to author: {}".format(affiliations_db["names"][0]["name"]))
                                break
                            else:
                                author_found = True
                                if verbose > 4:
                                    print("group already in author")
                                break

                        if not author_found:
                            affiliation_match = False
                            for i, author in enumerate(colav_reg["authors"]):
                                name_match = compare_author(
                                    author['id'], author['full_name'], author_db['full_name'])
                                if author['affiliations']:
                                    affiliations_person = [str(aff['id'])
                                                           for aff in author_db['affiliations']]
                                    author_affiliations = [str(aff['id'])
                                                           for aff in author['affiliations']]
                                    affiliation_match = any(affil in author_affiliations for affil in affiliations_person)
                                if name_match and affiliation_match:
                                    author["affiliations"].append({
                                        "id": affiliations_db["_id"],
                                        "name": affiliations_db["names"][0]["name"].strip(),
                                        "types": affiliations_db["types"]})
                                    break
                else:
                    if verbose > 4:
                        print("No author in db with external id")
        else:
            if verbose > 4:
                print("No author data")

    collection.update_one(
        {"_id": colav_reg["_id"]},
        {"$set": {
            "updated": colav_reg["updated"],
            "titles": colav_reg["titles"],
            "external_ids": colav_reg["external_ids"],
            "types": colav_reg["types"],
            "authors": colav_reg["authors"]
        }}
    )


def process_one_insert(openadata_reg, db, collection, empty_work, es_handler, verbose=0):
    """
    Function to insert a new register in the database if it is not found in the colav(kahi works) database.
    This means that the register is not on the database and it is being inserted.

    For similarity purposes, the register is also inserted in the elasticsearch index,
    all the elastic search fields are filled with the information from the register and it is
    handled by Mohan's Similarity class.

    The register is also linked to the source of the register, and the authors and affiliations are searched in the database.

    Parameters
    ----------
    openadata_reg : dict
        Register from the minciencias opendata database
    db : pymongo.database.Database
        Database where the colav collections are stored, used to search for authors and affiliations.
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    es_handler : Similarity
        Elasticsearch handler to insert the register in the elasticsearch index, Mohan's Similarity class.
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    # parse
    entry = parse_minciencias_opendata(openadata_reg, empty_work.copy())
    # search authors and affiliations in db
    for i, author in enumerate(entry["authors"]):
        author_db = None
        for ext in author["external_ids"]:
            author_db = db["person"].find_one(
                {
                    "external_ids.source": ext["source"],
                    "external_ids.id": ext["id"]})
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
            if "external_ids" in author.keys():
                del (author["external_ids"])

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


def process_one(openadata_reg, db, collection, empty_work, es_handler, similarity, insert_all, verbose=0):
    """
    Function to process a single register from the minciencias opendata database.
    This function is used to insert or update a register in the colav(kahi works) database.

    Parameters
    ----------
    openadata_reg : dict
        Register from the minciencias opendata database
    db : pymongo.database.Database
        Database where the colav collections are stored, used to search for authors and affiliations.
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    es_handler : Similarity
        Elasticsearch handler to insert the register in the elasticsearch index, Mohan's Similarity class.
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    # type id verification
    if "id_producto_pd" in openadata_reg.keys():
        if openadata_reg["id_producto_pd"]:
            COD_RH = ""
            COD_PROD = ""
            product_id = openadata_reg["id_producto_pd"]
            match = search(r'(\d{9,11})-(\d{1,7})$', product_id)
            if match:
                COD_RH = match.group(1)
                COD_PROD = match.group(2)

                if COD_RH and COD_PROD:
                    colav_reg = collection.find_one(
                        {"external_ids.id": {"$all": [COD_RH, COD_PROD]}})
                    if colav_reg:
                        process_one_update(openadata_reg, colav_reg, db, collection, empty_work, verbose)
                        return

    if similarity:  # does not have a doi identifier
        # elasticsearch section
        if es_handler:
            # Search in elasticsearch
            authors = []
            title_work = ""
            if 'id_persona_pd' in openadata_reg.keys():
                if openadata_reg["id_persona_pd"]:
                    author_bd = db["person"].find_one(
                        {"external_ids.id": openadata_reg["id_persona_pd"]})
                    if author_bd:
                        authors.append(author_bd["full_name"])
            if 'nme_producto_pd' in openadata_reg.keys():
                if openadata_reg["nme_producto_pd"]:
                    title_work = openadata_reg["nme_producto_pd"]
            if authors and title_work != "":
                response = es_handler.search_work(
                    title=title_work,
                    source="",
                    year="0",
                    authors=authors,
                    volume="",
                    issue="",
                    page_start="",
                    page_end="",
                )
                if response:  # register already on db... update accordingly
                    colav_reg = collection.find_one(
                        {"_id": ObjectId(response["_id"])})
                    if colav_reg:
                        process_one_update(openadata_reg, colav_reg, db, collection, empty_work, verbose)
                    else:
                        if verbose > 4:
                            print("Register with {} not found in mongodb".format(
                                response["_id"]))
                else:  # insert new register
                    if insert_all:
                        process_one_insert(openadata_reg, db, collection, empty_work, es_handler, verbose)
            else:
                if verbose > 4:
                    if not authors:
                        print(f"Not authors data for search with elasticsearch with {openadata_reg['id_persona_pd']} in {openadata_reg['_id']}")
                    else:
                        print(f"Not title data for search with elasticsearch in {openadata_reg['_id']}")
        else:
            if verbose > 4:
                print("No elasticsearch index provided")
