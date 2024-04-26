from kahi_scholar_person.parser import parse_scholar

def process_one(paper, db, collection, empty_person, verbose):
    author_db = None
    authors = parse_scholar(paper, empty_person)
    if authors:
        for author in authors:
            if author["external_ids"]:
                if "profile" in [idx.values() for idx in author["external_ids"]][0]:
                    author_db = collection.find_one({"external_ids.id": author["external_ids"][0]["id"]})
                if author_db:
                    already_updated = False
                    # Update existing
                    for upd in author["updated"]:
                        if upd["source"] == "scholar":
                            already_updated = True
                    if not already_updated:
                        author_db["updated"].append({"source": "scholar", "time": int(time())})
                    if author["related_works"]:
                        db_author_set = set(tuple(d.items()) for d in author_db["related_works"])
                        author_set = set(tuple(d.items()) for d in author["related_works"])
                        combined_set = db_author_set.union(author_set)
                        author_db["related_works"] = [dict(t) for t in combined_set]

                    # print(f"Updated {author_db['_id']}")
                    collection.update_one({"_id": author_db["_id"]}, {"$set": {
                        "updated": author_db["updated"],
                        "external_ids": author_db["external_ids"],
                        "related_works": author_db["related_works"]}})
                    return
            # Iserting author
            collection.insert_one(author)
            pass
    return