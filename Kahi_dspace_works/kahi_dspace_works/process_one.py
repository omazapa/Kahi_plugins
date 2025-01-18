from kahi_dspace_works.parser import parse_dspace
from kahi_dspace_works.utils import process_source, get_doi
from bson import ObjectId


def process_one_update(entry, colav_reg, db, collection, verbose):
    # merging the two entries
    pass


def process_one_insert(entry, db, collection, verbose):
    # inserting the entry
    pass


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
            process_one_insert(entry, db, collection, verbose)
    else:  # if doi
        doi = get_doi(dspace_reg)
        if doi:
            colav_reg = collection.find_one({"external_ids.id": doi})
            if colav_reg:
                process_one_update(entry, colav_reg, db, collection, verbose)
            else:
                process_one_insert(entry, db, collection, verbose)
