from time import time
from kahi_impactu_utils.Utils import doi_processor
from kahi_ciarp_works.parser import parse_ciarp
from bson import ObjectId
from pandas import isna


def process_one_update(ciarp_reg, colav_reg, db, collection, affiliation, empty_work):
    """
    Method to update a register in the kahi database from ciarp database if it is found.
    This means that the register is already on the kahi database and it is being updated with new information.


    Parameters
    ----------
    ciarp_reg : dict
        Register from the ciarp database
    colav_reg : dict
        Register from the colav database (kahi database for impactu)
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    affiliation : dict
        Affiliation of the author
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    """
    # updated
    entry = parse_ciarp(
        ciarp_reg, affiliation, empty_work.copy())
    # Add updated time
    colav_reg["updated"].append(
        {"source": "ciarp", "time": int(time())})
    # titles
    colav_reg["titles"].extend(entry["titles"])
    # external_ids
    ext_ids = [ext["id"] for ext in colav_reg["external_ids"]]
    for ext in entry["external_ids"]:
        if ext["id"] not in ext_ids:
            colav_reg["external_ids"].append(ext)
            ext_ids.append(ext["id"])
    # Process author
    author = entry["authors"][0]
    author_db = None
    for ext in author["external_ids"]:
        author_db = db["person"].find_one(
            {"external_ids.id": ext["id"]})
        if author_db:
            break
    if author_db:
        author["id"] = author_db["_id"]
        author["full_name"] = author_db["full_name"]
    # Process affiliations
    for j, aff in enumerate(author["affiliations"]):
        aff_db = None
        if "external_ids" in aff.keys():
            for ext in aff["external_ids"]:
                aff_db = db["affiliations"].find_one(
                    {"_id": ext["id"]})
                if aff_db:
                    break
            aff.pop("external_ids", None)
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
            author["affiliations"][j] = {
                "id": aff_db["_id"],
                "name": name,
                "types": aff_db["types"]
            }
    # Check if author is already in the register
    colav_reg_author_ids = [auth["id"] for auth in colav_reg["authors"]]
    for author in entry["authors"]:
        for key in ["external_ids", "types"]:
            author.pop(key, None)
        if author["id"] not in colav_reg_author_ids:
            colav_reg["authors"].append(author)

    collection.update_one(
        {"_id": colav_reg["_id"]},
        {"$set": {
            "updated": colav_reg["updated"],
            "titles": colav_reg["titles"],
            "external_ids": colav_reg["external_ids"],
            "authors": colav_reg["authors"]
        }}
    )


def process_one_insert(ciarp_reg, db, collection, affiliation, empty_work, es_handler, verbose=0):
    """
    Function to insert a new register in the database if it is not found in the colav(kahi works) database.
    This means that the register is not on the database and it is being inserted.

    For similarity purposes, the register is also inserted in the elasticsearch index,
    all the elastic search fields are filled with the information from the register and it is
    handled by Mohan's Similarity class.

    Parameters:
    ----------
    ciarp_reg: dict
        Register from the ciarp database
    db: pymongo.database.Database
        Database where the collection is stored (kahi database)
    collection: pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    affiliation: dict
        Affiliation of the author
    empty_work: dict
        Empty dictionary with the structure of a register in the database
    es_handler: Mohan.Similarity.Similarity
        Handler for the elasticsearch index
    verbose: int
        Verbosity level
    """
    # parse
    entry = parse_ciarp(
        ciarp_reg, affiliation, empty_work.copy())
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
                    f'Register with doi: {ciarp_reg["doi"]} does not provide a source')
        else:
            if verbose > 4:
                print("No source found for\n\t",
                      entry["source"]["external_ids"])
        if isna(entry["source"]["name"]):
            entry["source"] = {}
        else:
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
    # insert in elasticsearch
    if es_handler:
        work = {}
        work["title"] = entry["titles"][0]["title"]
        work["source"] = entry["source"]["name"] if "name" in entry["source"].keys() else ""
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
        work["provenance"] = "ciarp"

        es_handler.insert_work(_id=str(response.inserted_id), work=work)
    else:
        if verbose > 4:
            print("No elasticsearch index provided")


def process_one(ciarp_reg, db, collection, affiliation, empty_work, similarity, es_handler, verbose=0):
    """
    Function to process a single register from the ciarp database.
    This function is used to insert or update a register in the colav(kahi works) database.

    Parameters:
    ----------
    ciarp_reg: dict
        Register from the ciarp database
    db: pymongo.database.Database
        Database where the collection is stored (kahi database)
    collection: pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    affiliation: dict
        Affiliation of the author
    empty_work: dict
        Empty dictionary with the structure of a register in the database
    similarity: bool
        Flag to indicate if the register should be inserted in the elasticsearch index
    es_handler: Mohan.Similarity.Similarity
        Handler for the elasticsearch index
    verbose: int
        Verbosity level
    """
    doi = None
    # register has doi
    if ciarp_reg["doi"]:
        if isinstance(ciarp_reg["doi"], str):
            doi = doi_processor(ciarp_reg["doi"])
    if doi:
        # is the doi in colavdb?
        colav_reg = collection.find_one({"external_ids.id": doi})
        if colav_reg:  # update the register
            process_one_update(ciarp_reg, colav_reg, db,
                               collection, affiliation, empty_work)
        else:  # insert a new register
            process_one_insert(ciarp_reg, db, collection,
                               affiliation, empty_work, es_handler, verbose)
    elif similarity:  # does not have a doi identifier
        # elasticsearch section
        entry = parse_ciarp(ciarp_reg, affiliation, empty_work)
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
                    # Find author in person collection
                    author_db = db["person"].find_one({
                        "external_ids.id": author["external_ids"][0]["id"]})
                    if author_db:
                        author["full_name"] = author_db["full_name"]
                    authors.append(author["full_name"])
            work["authors"] = authors
            response = es_handler.search_work(
                title=work["title"],
                source=work["source"],
                year=work["year"],
                authors=authors,
                volume=work["volume"],
                issue=work["issue"],
                page_start=work["first_page"],
                page_end=work["last_page"]
            )

            if response:  # register already on db... update accordingly
                colav_reg = collection.find_one(
                    {"_id": ObjectId(response["_id"])})
                if colav_reg:
                    process_one_update(
                        ciarp_reg, colav_reg, db, collection, affiliation, empty_work)
                else:
                    if verbose > 4:
                        print("Register with {} not found in mongodb".format(
                            response["_id"]))
            else:  # insert new register
                # print("Inserting new register")
                process_one_insert(
                    ciarp_reg, db, collection, affiliation, empty_work, es_handler, verbose)
        else:
            if verbose > 4:
                print("No elasticsearch index provided")
