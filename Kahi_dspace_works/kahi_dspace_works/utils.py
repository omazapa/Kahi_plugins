from kahi_impactu_utils.Utils import doi_processor


def get_doi(reg):
    """
    helps to get the doi from the dspace record and process it with the doi_processor.

    Parameters
    ----------
    reg : dict
        dspace record.

    Returns
    -------
    str | None
        doi processed or None if there is no doi or it is invalid.
    """
    for field in reg["OAI-PMH"]["GetRecord"]["record"]["metadata"]["dim:dim"][
        "dim:field"
    ]:
        if field["@element"] == "identifier" and "@qualifier" in field:
            if field["@qualifier"] == "doi":
                doi = doi_processor(field["#text"])
                return doi
    return None


def process_source(reg, db):
    """
    Given the issn, eissn or other external ids of the source in the dspace record, it tries to find the source in the sources collection.
    
    If the source is found, it creates a dict to put the source id and name in the parsed entry.
    It also deletes the external ids from the source dict in the parsed entry.

    Parameters
    ----------
    reg : dict
        parsed dspace record.
    db : pymongo.database.Database
        database object to kahi(ETL) database.
    """
    if reg["source"] != {}:
        for source in reg["source"]["external_ids"]:
            found = db["sources"].find_one({"external_ids.id": source["id"]})
            if found:
                del reg["source"]["external_ids"]
                reg["source"]["id"] = found["_id"]
                reg["source"]["name"] = found["names"][0]["name"]
                return
        del reg["source"]["external_ids"]


def process_affiliation(ror_id, db):
    """
    helps to get the affiliation from the ror_id and process it.

    Parameters
    ----------
    ror_id : str
        ror id.
    db : pymongo.database.Database
        database object to kahi(ETL) database.
    
    Returns
    -------
    dict
        affiliation processed.
    """
    aff_rec = db["affiliations"].find_one({"external_ids.id": ror_id})
    aff = {}
    aff["id"] = aff_rec["_id"]
    aff["name"] = next((i["name"] for i in aff_rec["names"] if i["lang"] == "es"), "")
    aff["types"] = aff_rec["types"]
    if aff_rec["addresses"]:
        if "country" in aff_rec["addresses"][0]:
            aff["country"] = aff_rec["addresses"][0]["country"]
        if "country_code" in aff_rec["addresses"][0]:
            aff["country_code"] = aff_rec["addresses"][0]["country_code"]
    return aff
