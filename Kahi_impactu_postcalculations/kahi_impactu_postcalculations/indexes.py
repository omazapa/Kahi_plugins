

def create_indexes(db):
    """
    Indexes creation required for Backend to work properly.

    Parameters
    ----------
    db : pymongo.database.Database
        Database object to create indexes on. (kahi)
    """
    db.works.createIndex({ "groups.id": 1 });
    db.works.createIndex({ "source.id": 1 });
    db.works.createIndex({"citations_count.source": 1, "citations_count.count": 1});
    db.works.createIndex({"titles.source": 1, "titles.title": 1});
    db.afilliations.createIndex({"products_count": -1});
    db.person.createIndex({"products_count": -1});
    db.works.createIndex({"types.source": 1, "types.type": 1, "types.code": 1});
    db.works.createIndex({"open_access.open_access_status": 1});