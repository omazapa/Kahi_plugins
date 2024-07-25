from math import log, exp


def count_works_one(db, author_id):
    """
    Count the number of works for an author.

    Parameters:
    ----------
    author : dict
        Author information.

    Returns:
        str: The author identifier.
    """
    count = db["works"].count_documents({"authors.id": author_id})
    if count != 0:
        return author_id


def network_creation_affiliations(colombia, impactu, idx, author_count):
    already = impactu["affiliations"].find_one({"_id": idx})
    if already:
        return None
    aff_info = colombia["affiliations"].find_one({"_id": idx})
    name = aff_info["names"][0]["name"]
    for n in aff_info["names"]:
        if n["lang"] == "es":
            name = n["name"]
            break
        elif n["lang"] == "en":
            name = n["name"]
    nodes = [idx]
    nodes_labels = [name]
    edges = []
    edges_coauthorships = {}
    works_count = 0
    for work in colombia["works"].find({"authors.affiliations.id": idx, "author_count": {"$lte": author_count}}):
        works_count += 1
        work_nodes = [idx]
        work_edges = []
        for author in work["authors"]:
            for aff in author["affiliations"]:
                if not aff["id"]:
                    continue
                if aff["id"] == "":
                    continue
                if aff["id"] == idx:
                    continue
                if not aff["id"] in nodes:
                    nodes.append(aff["id"])
                    name = aff["name"]

                    nodes_labels.append(name)
                if not aff["id"] in work_nodes:
                    for node in work_nodes:
                        edge_found = False
                        if (idx, aff["id"]) in work_edges:
                            edge_found = True
                        elif (aff["id"], idx) in edges:
                            edge_found = True
                        if edge_found is False:
                            work_edges.append((idx, aff["id"]))
                    work_nodes.append(aff["id"])
        # Connecting all the nodes in the work among them
        # checking if the connection already exists to add one to the count of coauthorships
        for node in work_nodes:
            if node not in nodes:
                nodes.append(node)
        for nodea, nodeb in work_edges:
            edge_found = False
            if (nodea, nodeb) in edges:
                edges_coauthorships[str(nodea) + str(nodeb)] += 1
                edge_found = True
            elif (nodeb, nodea) in edges:
                edges_coauthorships[str(nodeb) + str(nodea)] += 1
                edge_found = True
            if edge_found is False:
                edges_coauthorships[str(nodea) + str(nodeb)] = 1
                edges.append((nodea, nodeb))
    # adding the connections between the coauthoring institutions
    for node in nodes:
        if node == idx:
            continue
        for work in colombia["works"].find({"$and": [{"authors.affiliations.id": node}, {"authors.affiliations.id": {"$ne": idx}}], "author_count": {"$lte": author_count}}):
            for author in work["authors"]:
                for aff in author["affiliations"]:
                    if aff["id"] == idx:
                        print("Problem found")
                        continue
                    if not aff["id"] in nodes:
                        continue
                    if node == aff["id"]:
                        continue
                    if (node, aff["id"]) in edges:
                        edges_coauthorships[str(node) + str(aff["id"])] += 1
                    elif (aff["id"], node) in edges:
                        edges_coauthorships[str(aff["id"]) + str(node)] += 1
                    else:
                        edges_coauthorships[str(node) + str(aff["id"])] = 1
                        edges.append((node, aff["id"]))
    # Constructing the actual format to insrt in db
    num_nodes = len(nodes)
    nodes_db = []
    for i, node in enumerate(nodes):
        degree = len([1 for i, j in edges if i == node or j == node])
        size = 50 * log(1 + degree / (num_nodes - 1), 2) if num_nodes > 1 else 1
        nodes_db.append(
            {
                "id": str(node),
                "label": nodes_labels[i],
                "degree": degree,
                "size": size
            }
        )
    edges_db = []
    for nodea, nodeb in edges:
        coauthorships = 0
        if str(nodea) + str(nodeb) in edges_coauthorships.keys():
            coauthorships = edges_coauthorships[str(nodea) + str(nodeb)]
        elif str(nodeb) + str(nodea) in edges_coauthorships.keys():
            coauthorships = edges_coauthorships[str(nodeb) + str(nodea)]
        edges_db.append({
            "source": str(nodea),
            "target": str(nodeb),
            "coauthorships": coauthorships,
            "size": coauthorships,
        })
    top = max([e["coauthorships"]
              for e in edges_db]) if len(edges_db) > 0 else 1
    bot = min([e["coauthorships"]
              for e in edges_db]) if len(edges_db) > 0 else 1
    for edge in edges_db:
        if abs(top - edge["coauthorships"]) < 0.01:
            edge["size"] = 10
        elif abs(bot - edge["coauthorships"]) < 0.01:
            edge["size"] = 1
        else:
            size = 10 / (1 + exp(6 - 10 * edge["coauthorships"] / top))
            edge["size"] = size if size >= 1 else 1
    record = {
        "_id": idx,
        "coauthorship_network": {
            "nodes": nodes_db,
            "edges": edges_db
        }
    }
    try:
        record_edges = {
            "_id": idx,
            "coauthorship_network": {
                "edges": edges_db
            }
        }
        nedges = int(len(record["coauthorship_network"]["edges"]) / 2)

        record["coauthorship_network"]["edges"] = record["coauthorship_network"]["edges"][0:nedges]

        record_edges["coauthorship_network"]["nodes"] = record["coauthorship_network"]["edges"][nedges:]
        impactu["affiliations"].insert_one(record)
        impactu["affiliations_edges"].insert_one(record_edges)
    except Exception as e:
        print(f"too big network for id {record['_id']}", e)


def network_creation_person(colombia, impactu, idx):
    already = impactu["person"].find_one(
        {"_id": idx, "coauthorship_network": {"$exists": True}})
    if already:
        return None
    aff_info = colombia["person"].find_one({"_id": idx})
    name = aff_info["full_name"]
    nodes = [idx]
    nodes_labels = [name]
    edges = []
    edges_coauthorships = {}
    works_count = 0
    for work in colombia["works"].find({"authors.id": idx, "author_count": {"$lte": 10}}):
        works_count += 1
        work_nodes = [idx]
        work_edges = []
        for author in work["authors"]:
            if not author["id"]:
                continue
            if author["id"] == "":
                continue
            if author["id"] == idx:
                continue
            if not author["id"] in nodes:
                nodes.append(author["id"])
                name = author["full_name"]
                nodes_labels.append(name)
            if not author["id"] in work_nodes:
                for node in work_nodes:
                    edge_found = False
                    if (idx, author["id"]) in work_edges:
                        edge_found = True
                    elif (author["id"], idx) in edges:
                        edge_found = True
                    if edge_found is False:
                        work_edges.append((idx, author["id"]))
                work_nodes.append(author["id"])
        # Connecting all the nodes in the work among them
        # checking if the connection already exists to add one to the count of coauthorships
        for node in work_nodes:
            if node not in nodes:
                nodes.append(node)
        for nodea, nodeb in work_edges:
            edge_found = False
            if (nodea, nodeb) in edges:
                edges_coauthorships[str(nodea) + str(nodeb)] += 1
                edge_found = True
            elif (nodeb, nodea) in edges:
                edges_coauthorships[str(nodeb) + str(nodea)] += 1
                edge_found = True
            if edge_found is False:
                edges_coauthorships[str(nodea) + str(nodeb)] = 1
                edges.append((nodea, nodeb))
    # adding the connections between the coauthoring institutions
    for node in nodes:
        if node == idx:
            continue
        for work in colombia["works"].find({"$and": [{"authors.id": node}, {"authors.id": {"$ne": idx}}], "author_count": {"$lte": 10}}):
            for author in work["authors"]:
                if author["id"] == idx:
                    print("Problem found")
                    continue
                if not author["id"] in nodes:
                    continue
                if node == author["id"]:
                    continue
                if (node, author["id"]) in edges:
                    edges_coauthorships[str(node) + str(author["id"])] += 1
                elif (author["id"], node) in edges:
                    edges_coauthorships[str(author["id"]) + str(node)] += 1
                else:
                    edges_coauthorships[str(node) + str(author["id"])] = 1
                    edges.append((node, author["id"]))
    # Constructing the actual format to insrt in db
    num_nodes = len(nodes)
    nodes_db = []
    for i, node in enumerate(nodes):
        degree = len([1 for i, j in edges if i == node or j == node])
        size = 50 * log(1 + degree / (num_nodes - 1), 2) if num_nodes > 1 else 1
        nodes_db.append(
            {
                "id": str(node),
                "label": nodes_labels[i],
                "degree": degree,
                "size": size
            }
        )
    edges_db = []
    for nodea, nodeb in edges:
        coauthorships = 0
        if str(nodea) + str(nodeb) in edges_coauthorships.keys():
            coauthorships = edges_coauthorships[str(nodea) + str(nodeb)]
        elif str(nodeb) + str(nodea) in edges_coauthorships.keys():
            coauthorships = edges_coauthorships[str(nodeb) + str(nodea)]
        edges_db.append({
            "source": str(nodea),
            "target": str(nodeb),
            "coauthorships": coauthorships,
            "size": coauthorships,
        })
    top = max([e["coauthorships"]
              for e in edges_db]) if len(edges_db) > 0 else 1
    bot = min([e["coauthorships"]
              for e in edges_db]) if len(edges_db) > 0 else 1
    # avg=mean([e["coauthorships"] for e in edges])
    for edge in edges_db:
        if abs(top - edge["coauthorships"]) < 0.01:
            edge["size"] = 10
        elif abs(bot - edge["coauthorships"]) < 0.01:
            edge["size"] = 1
        else:
            size = 10 / (1 + exp(6 - 10 * edge["coauthorships"] / top))
            edge["size"] = size if size >= 1 else 1
    impactu["person"].update_one({"_id": idx}, {"$set": {"coauthorship_network": {
                                 "nodes": nodes_db, "edges": edges_db}}}, upsert=True)


def top_words_affiliations(colombia, impactu, aff, es_model, en_model, stopwords):
    aff_db = impactu["affiliations"].find_one(
        {"_id": aff["_id"], "top_words": {"$exists": 1}})
    if aff_db:
        return
    results = {}
    for work in colombia["works"].find({"authors.affiliations.id": aff["_id"], "titles.title": {"$exists": 1}}, {"titles": 1}):
        title = work["titles"][0]["title"].lower()
        lang = work["titles"][0]["lang"]
        if lang == "es":
            model = es_model
        else:
            model = en_model
        title = model(title)
        for token in title:
            if token.lemma_.isnumeric():
                continue
            if token.lemma_ in stopwords:
                continue
            if len(token.lemma_) < 4:
                continue
            if token.lemma_ in results.keys():
                results[token.lemma_] += 1
            else:
                results[token.lemma_] = 1
    topN = sorted(results.items(), key=lambda x: x[1], reverse=True)[:20]
    results = []
    for top in topN:
        results.append({"name": top[0], "value": top[1]})
    aff_db = impactu["affiliations"].find_one({"_id": aff["_id"]})
    if aff_db:
        impactu["affiliations"].update_one(
            {"_id": aff["_id"]}, {"$set": {"top_words": results}})
    else:
        impactu["affiliations"].insert_one(
            {"_id": aff["_id"], "top_words": results})


def top_words_affiliations_others(colombia, impactu, aff, es_model, en_model, stopwords):
    aff_db = impactu["affiliations"].find_one(
        {"_id": aff["_id"], "top_words": {"$exists": 1}})
    if aff_db:
        results = {}
        for author in colombia["person"].find({"affiliations.id": aff["_id"]}):
            for work in colombia["works"].find({"authors.id": author["_id"], "titles.title": {"$exists": 1}}):
                title = work["titles"][0]["title"].lower()
                lang = work["titles"][0]["lang"]
                if lang == "es":
                    model = es_model
                else:
                    model = en_model
                title = model(title)
                for token in title:
                    if token.lemma_.isnumeric():
                        continue
                    if token.lemma_ in stopwords:
                        continue
                    if len(token.lemma_) < 4:
                        continue
                    if token.lemma_ in results.keys():
                        results[token.lemma_] += 1
                    else:
                        results[token.lemma_] = 1
        topN = sorted(results.items(), key=lambda x: x[1], reverse=True)[:20]
        results = []
        for top in topN:
            results.append({"name": top[0], "value": top[1]})
        impactu["affiliations"].update_one(
            {"_id": aff["_id"]}, {"$set": {"top_words": results}})


def top_words_person(colombia, impactu, aff, es_model, en_model, stopwords):
    aff_db = impactu["person"].find_one(
        {"_id": aff["_id"], "top_words": {"$exists": 1}})
    if aff_db:
        return
    results = {}
    for work in colombia["works"].find({"authors.id": aff["_id"], "titles.title": {"$exists": 1}}, {"titles": 1}):
        title = work["titles"][0]["title"].lower()
        lang = work["titles"][0]["lang"]
        if lang == "es":
            model = es_model
        else:
            model = en_model
        title = model(title)
        for token in title:
            if token.lemma_.isnumeric():
                continue
            if token.lemma_ in stopwords:
                continue
            if len(token.lemma_) < 4:
                continue
            if token.lemma_ in results.keys():
                results[token.lemma_] += 1
            else:
                results[token.lemma_] = 1
    topN = sorted(results.items(), key=lambda x: x[1], reverse=True)[:20]
    results = []
    for top in topN:
        results.append({"name": top[0], "value": top[1]})
    aff_db = impactu["person"].find_one({"_id": aff["_id"]})
    if aff_db:
        impactu["person"].update_one(
            {"_id": aff["_id"]}, {"$set": {"top_words": results}})
    else:
        impactu["person"].insert_one({"_id": aff["_id"], "top_words": results})
