from kahi_impactu_utils.Utils import compare_author
from kahi_dspace_works.parser import parse_dspace
from kahi_dspace_works.utils import process_source, get_doi
from bson import ObjectId


def process_one_update(entry, colav_reg, db, collection, verbose):
    """
    Update the entry in the works collection.

    Parameters:
    -----------
    entry : dict
        entry from dspace.
    colav_reg : dict
        entry to be updated.
    db : pymongo.database.Database
        database object to kahi(ETL) database.
    collection : pymongo.collection.Collection
        collection object to kahi(ETL) database.
    verbose : int
        verbosity level.
    """
    # merging the two entries
    colav_reg["updated"].append(entry["updated"])

    for title in entry["titles"]:
        colav_reg["titles"].append(title)

    if entry["year_published"] and not colav_reg["year_published"]:
        colav_reg["year_published"] = entry["year_published"]

    if entry["doi"] and not colav_reg["doi"]:
        colav_reg["doi"] = entry["doi"]

    for abstract in entry["abstracts"]:
        if abstract not in colav_reg["abstracts"]:
            colav_reg["abstracts"].append(abstract)

    for dtype in entry["types"]:
        if dtype not in colav_reg["types"]:
            colav_reg["types"].append(dtype)

    for did in entry["external_ids"]:
        if did not in colav_reg["external_ids"]:
            colav_reg["external_ids"].append(did)

    for url in entry["external_urls"]:
        if url not in colav_reg["external_urls"]:
            colav_reg["external_urls"].append(url)

    if entry["source"] and not colav_reg["source"]:
        colav_reg["source"] = entry["source"]

    # merging autorship only to add the type
    # affiliation is not considered in this merge
    for author_reg in entry["authors"]:
        name_match = False
        for author in colav_reg["authors"]:
            if author['id'] == "":
                if verbose >= 4:
                    print(
                        f"WARNING: author with id '' found in colav register: {author}")
                continue
            # only the name can be compared, because we dont have the affiliation of the author from the paper in author_others
            author_db = db['person'].find_one(
                # this is required to get  first_names and last_names
                {'_id': author['id']}, {"_id": 1, "full_name": 1, "first_names": 1, "last_names": 1, "initials": 1})

            name_match = compare_author(
                author_reg, author_db, len(colav_reg["authors"]))
            if name_match:
                author["type"] = author_reg["type"]
        if not name_match:
            del author_reg["first_names"]
            del author_reg["last_names"]
            del author_reg["initials"]
            colav_reg["authors"].append(author_reg)
    colav_reg["author_count"] = len(author_reg["authors"])
    collection.update_one(
        {"_id": colav_reg["_id"]},
        {"$set": {
            "updated": colav_reg["updated"],
            "abstracts": colav_reg["abstracts"],
            "doi": colav_reg["doi"],
            "year_published": colav_reg["year_published"],
            "titles": colav_reg["titles"],
            "external_ids": colav_reg["external_ids"],
            "extrnal_urls": colav_reg["external_urls"],
            "types": colav_reg["types"],
            "authors": colav_reg["authors"],
            "author_count": colav_reg["author_count"],
            "source": colav_reg["source"]

        }}
    )


def process_one_insert(entry, collection, es_handler, verbose):
    """
    Insert the entry in the works collection.

    Parameters:
    -----------
    entry : dict
        entry to be inserted.
    collection : pymongo.collection.Collection
        collection object to kahi(ETL) database.
    es_handler : elasticsearch.Elasticsearch
        elasticsearch handler.
    verbose : int
        verbosity level.
    """
    # removing unnecessary fields in the work
    for author in entry['authors']:
        del author["first_names"]
        del author["last_names"]
        del author["initials"]
    # inserting the entry
    response = collection.insert_one(entry)
    # insert in elasticsearch
    authors = []
    if es_handler:
        work = {}
        work["title"] = entry["titles"][0]["title"]
        work["source"] = ""
        work["year"] = "0"
        work["volume"] = ""
        work["issue"] = ""
        work["first_page"] = ""
        work["last_page"] = ""
        for author in entry['authors']:
            if "full_name" in author.keys():
                if author["full_name"]:
                    authors.append(author["full_name"])
        work["authors"] = authors
        work["provenance"] = "dspace"
        if work["title"]:
            es_handler.insert_work(_id=str(response.inserted_id), work=work)
        else:
            if verbose > 4:
                print("Not enough data for insert in elasticsearch index")
    else:
        if verbose > 4:
            print("No elasticsearch index provided")


def process_one(dspace_reg, affiliation, base_url, db, collection, empty_work, es_handler, similarity, verbose=0):
    """
    Dspace record is parsed and processed to be inserted/updated in the works collection.

    Parameters:
    -----------
    dspace_reg : dict
        dspace record.
    affiliation : dict
        affiliation record.
    base_url : str
        base url of the dspace instance.
    db : pymongo.database.Database
        database object to kahi(ETL) database.
    collection : pymongo.collection.Collection
        collection object to kahi(ETL) database.
    empty_work : dict
        empty work record.
    es_handler : elasticsearch.Elasticsearch
        elasticsearch handler.
    similarity : bool
        if True, it will search for similar works in the works collection.
    verbose : int
        verbosity level.
    """
    # processing bad dois as well
    entry = parse_dspace(dspace_reg, empty_work,
                         base_url, affiliation, verbose)
    process_source(entry, db)  # setting source to entry

    if similarity:
        work = {}
        work["title"] = entry["titles"][0]["title"]
        work["source"] = (
            entry["source"]["name"] if "name" in entry["source"].keys() else ""
        )
        work["year"] = entry["year_published"] if entry["year_published"] else "0"
        work["volume"] = (
            entry["bibliographic_info"]["volume"]
            if "volume" in entry["bibliographic_info"].keys()
            else ""
        )
        work["issue"] = (
            entry["bibliographic_info"]["issue"]
            if "issue" in entry["bibliographic_info"].keys()
            else ""
        )
        work["first_page"] = (
            entry["bibliographic_info"]["first_page"]
            if "first_page" in entry["bibliographic_info"].keys()
            else ""
        )
        work["last_page"] = (
            entry["bibliographic_info"]["last_page"]
            if "last_page" in entry["bibliographic_info"].keys()
            else ""
        )
        authors = []
        for author in entry["authors"]:
            if len(authors) >= 5:
                break
            if "full_name" in author.keys():
                authors.append(author["full_name"])
        work["authors"] = authors
        work["provenance"] = "dspace"
        response = es_handler.search_work(
            title=work["title"],
            source=work["source"],
            year=str(work["year"]),
            authors=authors,
            volume=work["volume"],
            issue=work["issue"],
            page_start=work["first_page"],
            page_end=work["last_page"],
        )
        if response:  # register already on db... update accordingly
            colav_reg = db["works"].find_one(
                {"_id": ObjectId(response["_id"])})
            if colav_reg:
                process_one_update(entry, colav_reg, db, collection, verbose)
            else:
                print(
                    "ERROR: Register with id {} found in elasticsaerch not found in mongodb".format(
                        response["_id"])
                )
                from sys import exit
                exit(1)
        else:  # insert new register
            process_one_insert(entry, collection, es_handler, verbose)
    else:  # if doi
        doi = get_doi(dspace_reg)
        if doi:
            colav_reg = collection.find_one({"external_ids.id": doi})
            if colav_reg:
                process_one_update(entry, colav_reg, db, collection, verbose)
            else:
                process_one_insert(entry, collection, es_handler, verbose)
