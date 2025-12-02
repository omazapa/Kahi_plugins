def set_works_authors_affiliations_country(collection) -> None:
    """
    Method to set the country of the affiliations of the authors of the works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$lookup": {
                "from": "affiliations",
                "localField": "authors.affiliations.id",
                "foreignField": "_id",
                "as": "affiliations_data",
                "pipeline": [{"$project": {"_id": 1, "addresses.country": 1}}],
            }
        },
        {
            "$addFields": {
                "authors": {
                    "$map": {
                        "input": "$authors",
                        "as": "author",
                        "in": {
                            "$mergeObjects": [
                                "$$author",
                                {
                                    "affiliations": {
                                        "$map": {
                                            "input": "$$author.affiliations",
                                            "as": "affiliation",
                                            "in": {
                                                "$mergeObjects": [
                                                    "$$affiliation",
                                                    {
                                                        "country": {
                                                            "$let": {
                                                                "vars": {
                                                                    "matchedAffiliation": {
                                                                        "$arrayElemAt": [
                                                                            {
                                                                                "$filter": {
                                                                                    "input": "$affiliations_data",
                                                                                    "as": "affiliation_data",
                                                                                    "cond": {
                                                                                        "$eq": [
                                                                                            "$$affiliation.id",
                                                                                            "$$affiliation_data._id",
                                                                                        ]
                                                                                    },
                                                                                }
                                                                            },
                                                                            0,
                                                                        ]
                                                                    }
                                                                },
                                                                "in": {
                                                                    "$ifNull": [
                                                                        {
                                                                            "$arrayElemAt": [
                                                                                "$$matchedAffiliation.addresses.country",
                                                                                0,
                                                                            ]
                                                                        },
                                                                        None,
                                                                    ]
                                                                },
                                                            }
                                                        }
                                                    },
                                                ]
                                            },
                                        }
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        },
        {"$project": {"affiliations_data": 0}},
        {
            "$merge": {
                "into": "works",
                "whenMatched": "merge",
                "whenNotMatched": "fail",
            }
        },
    ]
    collection.aggregate(pipeline)  # works collections


def set_works_authors_affiliations_country_code(collection) -> None:
    """
    Method to set the country code of the affiliations of the authors of the works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$lookup": {
                "from": "affiliations",
                "localField": "authors.affiliations.id",
                "foreignField": "_id",
                "as": "affiliations_data",
                "pipeline": [{"$project": {"_id": 1, "addresses.country_code": 1}}],
            }
        },
        {
            "$addFields": {
                "authors": {
                    "$map": {
                        "input": "$authors",
                        "as": "author",
                        "in": {
                            "$mergeObjects": [
                                "$$author",
                                {
                                    "affiliations": {
                                        "$map": {
                                            "input": "$$author.affiliations",
                                            "as": "affiliation",
                                            "in": {
                                                "$mergeObjects": [
                                                    "$$affiliation",
                                                    {
                                                        "country_code": {
                                                            "$let": {
                                                                "vars": {
                                                                    "matchedAffiliation": {
                                                                        "$arrayElemAt": [
                                                                            {
                                                                                "$filter": {
                                                                                    "input": "$affiliations_data",
                                                                                    "as": "affiliation_data",
                                                                                    "cond": {
                                                                                        "$eq": [
                                                                                            "$$affiliation.id",
                                                                                            "$$affiliation_data._id",
                                                                                        ]
                                                                                    },
                                                                                }
                                                                            },
                                                                            0,
                                                                        ]
                                                                    }
                                                                },
                                                                "in": {
                                                                    "$ifNull": [
                                                                        {
                                                                            "$arrayElemAt": [
                                                                                "$$matchedAffiliation.addresses.country_code",
                                                                                0,
                                                                            ]
                                                                        },
                                                                        None,
                                                                    ]
                                                                },
                                                            }
                                                        }
                                                    },
                                                ]
                                            },
                                        }
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        },
        {"$project": {"affiliations_data": 0}},
        {
            "$merge": {
                "into": "works",
                "whenMatched": "merge",
                "whenNotMatched": "fail",
            }
        },
    ]
    collection.aggregate(pipeline)


def set_works_groups_ranking(collection) -> None:
    """
    Function to set the ranking of the groups of the works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$lookup": {
                "from": "affiliations",
                "localField": "groups.id",
                "foreignField": "_id",
                "as": "groups_data",
                "pipeline": [{"$match": {"ranking.source": "minciencias"}}, {"$project": {"_id": 1, "ranking": 1}}],
            }
        },
        {
            "$addFields": {
                "groups": {
                    "$map": {
                        "input": "$groups",
                        "as": "group",
                        "in": {
                            "$mergeObjects": [
                                "$$group",
                                {
                                    "ranking": {
                                        "$let": {
                                            "vars": {
                                                "matchedGroup": {
                                                    "$arrayElemAt": [
                                                        {
                                                            "$filter": {
                                                                "input": "$groups_data",
                                                                "as": "group_data",
                                                                "cond": {
                                                                    "$eq": [
                                                                        "$$group.id",
                                                                        "$$group_data._id",
                                                                    ]
                                                                },
                                                            }
                                                        },
                                                        0,
                                                    ]
                                                }
                                            },
                                            "in": {
                                                "$ifNull": [
                                                    {
                                                        "$arrayElemAt": [
                                                            {
                                                                "$map": {
                                                                    "input": {
                                                                        "$filter": {
                                                                            "input": "$$matchedGroup.ranking",
                                                                            "as": "rankData",
                                                                            "cond": {
                                                                                "$eq": [
                                                                                    "$$rankData.source",
                                                                                    "minciencias",
                                                                                ]
                                                                            },
                                                                        }
                                                                    },
                                                                    "as": "filteredRank",
                                                                    "in": "$$filteredRank.rank",
                                                                }
                                                            },
                                                            0,
                                                        ]
                                                    },
                                                    None,
                                                ]
                                            },
                                        }
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        },
        {"$project": {"groups_data": 0}},
        {
            "$merge": {
                "into": "works",
                "whenMatched": "merge",
                "whenNotMatched": "fail",
            }
        },
    ]
    collection.aggregate(pipeline)


def set_works_authors_ranking(collection) -> None:
    """
    Function to set the ranking of the authors

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$lookup": {
                "from": "person",
                "localField": "authors.id",
                "foreignField": "_id",
                "as": "authors_data",
                "pipeline": [{"$match": {"ranking.source": "minciencias"}}, {"$project": {"_id": 1, "ranking": 1}}],
            }
        },
        {
            "$addFields": {
                "authors": {
                    "$map": {
                        "input": "$authors",
                        "as": "author",
                        "in": {
                            "$mergeObjects": [
                                "$$author",
                                {
                                    "ranking": {
                                        "$let": {
                                            "vars": {
                                                "matchedAuthor": {
                                                    "$arrayElemAt": [
                                                        {
                                                            "$filter": {
                                                                "input": "$authors_data",
                                                                "as": "author_data",
                                                                "cond": {
                                                                    "$eq": [
                                                                        "$$author.id",
                                                                        "$$author_data._id",
                                                                    ]
                                                                },
                                                            }
                                                        },
                                                        0,
                                                    ]
                                                }
                                            },
                                            "in": {
                                                "$ifNull": [
                                                    {
                                                        "$arrayElemAt": [
                                                            {
                                                                "$map": {
                                                                    "input": {
                                                                        "$filter": {
                                                                            "input": "$$matchedAuthor.ranking",
                                                                            "as": "rankData",
                                                                            "cond": {
                                                                                "$eq": [
                                                                                    "$$rankData.source",
                                                                                    "minciencias",
                                                                                ]
                                                                            },
                                                                        }
                                                                    },
                                                                    "as": "filteredRank",
                                                                    "in": "$$filteredRank.rank",
                                                                }
                                                            },
                                                            0,
                                                        ]
                                                    },
                                                    None,
                                                ]
                                            },
                                        }
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        },
        {"$project": {"authors_data": 0}},
        {
            "$merge": {
                "into": "works",
                "whenMatched": "merge",
                "whenNotMatched": "fail",
            }
        },
    ]
    collection.aggregate(pipeline)


def set_works_citations_count_openalex(collection) -> None:
    """
    Function to set the OpenAlex citations count in works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$set": {
                "citations_count_openalex": {
                    "$ifNull": [
                        {
                            "$getField": {
                                "field": "count",
                                "input": {
                                    "$first": {
                                        "$filter": {
                                            "input": "$citations_count",
                                            "as": "c",
                                            "cond": {
                                                "$eq": ["$$c.source", "openalex"]
                                            },
                                        }
                                    }
                                },
                            }
                        },
                        0,
                    ]
                }
            }
        }
    ]

    collection.update_many({}, pipeline)


def set_works_authors_full_data(collection) -> None:
    """
    Function to enrich works authors with full person data

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$lookup": {
                "from": "person",
                "localField": "authors.id",
                "foreignField": "_id",
                "as": "authors_data",
                "pipeline": [
                    {
                        "$project": {
                            "_id": 0,
                            "id": "$_id",
                            "sex": 1,
                            "full_name": 1,
                            "first_names": 1,
                            "last_names": 1,
                            "ranking": 1,
                            "external_ids": 1,
                        }
                    }
                ],
            }
        },
        {
            "$addFields": {
                "authors_map": {
                    "$arrayToObject": {
                        "$map": {
                            "input": "$authors_data",
                            "as": "a",
                            "in": {"k": "$$a.id", "v": "$$a"},
                        }
                    }
                }
            }
        },
        {
            "$addFields": {
                "authors": {
                    "$map": {
                        "input": "$authors",
                        "as": "author",
                        "in": {
                            "$mergeObjects": [
                                "$$author",
                                {
                                    "$ifNull": [
                                        {
                                            "$getField": {
                                                "field": "$$author.id",
                                                "input": "$authors_map",
                                            }
                                        },
                                        {},
                                    ]
                                },
                            ]
                        },
                    }
                }
            }
        },
        {"$unset": ["authors_data", "authors_map"]},
        {
            "$merge": {
                "into": "works",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }
        },
    ]

    collection.aggregate(pipeline)


def set_works_authors_affiliations_dates(collection) -> None:
    """
    Function to set authors affiliations start and end dates in works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$lookup": {
                "from": "person",
                "let": {"authorIds": "$authors.id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$in": ["$_id", "$$authorIds"]
                            }
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "id": "$_id",
                            "affiliations.id": 1,
                            "affiliations.start_date": 1,
                            "affiliations.end_date": 1,
                        }
                    },
                ],
                "as": "authors_data",
            }
        },
        {
            "$addFields": {
                "authors_map": {
                    "$arrayToObject": {
                        "$map": {
                            "input": "$authors_data",
                            "as": "a",
                            "in": {"k": "$$a.id", "v": "$$a"},
                        }
                    }
                }
            }
        },
        {
            "$addFields": {
                "authors": {
                    "$map": {
                        "input": "$authors",
                        "as": "author",
                        "in": {
                            "$let": {
                                "vars": {
                                    "personData": {
                                        "$getField": {
                                            "field": "$$author.id",
                                            "input": "$authors_map",
                                        }
                                    }
                                },
                                "in": {
                                    "$mergeObjects": [
                                        "$$author",
                                        {
                                            "affiliations": {
                                                "$map": {
                                                    "input": {
                                                        "$ifNull": [
                                                            "$$author.affiliations",
                                                            [],
                                                        ]
                                                    },
                                                    "as": "aff",
                                                    "in": {
                                                        "$let": {
                                                            "vars": {
                                                                "matchAff": {
                                                                    "$first": {
                                                                        "$filter": {
                                                                            "input": {
                                                                                "$ifNull": [
                                                                                    "$$personData.affiliations",
                                                                                    [],
                                                                                ]
                                                                            },
                                                                            "cond": {
                                                                                "$eq": [
                                                                                    "$$this.id",
                                                                                    "$$aff.id",
                                                                                ]
                                                                            },
                                                                        }
                                                                    }
                                                                }
                                                            },
                                                            "in": {
                                                                "$mergeObjects": [
                                                                    "$$aff",
                                                                    {
                                                                        "start_date": {
                                                                            "$ifNull": [
                                                                                "$$matchAff.start_date",
                                                                                "$$aff.start_date",
                                                                            ]
                                                                        },
                                                                        "end_date": {
                                                                            "$ifNull": [
                                                                                "$$matchAff.end_date",
                                                                                "$$aff.end_date",
                                                                            ]
                                                                        },
                                                                    },
                                                                ]
                                                            },
                                                        }
                                                    },
                                                }
                                            }
                                        },
                                    ]
                                },
                            }
                        },
                    }
                }
            }
        },
        {"$unset": ["authors_data", "authors_map"]},
        {
            "$merge": {
                "into": "works",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }
        },
    ]

    collection.aggregate(pipeline)


def set_works_source_full_data(collection) -> None:
    """
    Function to enrich works source with full source data

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$lookup": {
                "from": "sources",
                "localField": "source.id",
                "foreignField": "_id",
                "as": "src",
                "pipeline": [
                    {
                        "$project": {
                            "_id": 0,
                            "names": 1,
                            "types": 1,
                            "external_ids": 1,
                            "updated": 1,
                            "publisher": 1,
                            "ranking": 1,
                            "apc": 1,
                            "external_urls": 1,
                        }
                    }
                ],
            }
        },
        {"$unwind": "$src"},
        {
            "$set": {
                "source": {
                    "$mergeObjects": ["$source", "$src"]
                }
            }
        },
        {"$unset": "src"},
        {
            "$merge": {
                "into": "works",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }
        },
    ]

    collection.aggregate(pipeline)


def set_person_affiliations_relations(collection) -> None:
    """
    Function to set relations inside affiliations for person collection

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the persons are stored
    """
    pipeline = [
        {
            "$unwind": {
                "path": "$affiliations",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$lookup": {
                "from": "affiliations",
                "localField": "affiliations.id",
                "foreignField": "_id",
                "as": "aff_doc",
                "pipeline": [
                    {
                        "$project": {
                            "_id": 0,
                            "relations.id": 1,
                        }
                    }
                ],
            }
        },
        {
            "$unwind": {
                "path": "$aff_doc",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$addFields": {
                "affiliations.relations": "$aff_doc.relations"
            }
        },
        {
            "$group": {
                "_id": "$_id",
                "doc": {"$first": "$$ROOT"},
                "affiliations": {"$push": "$affiliations"},
            }
        },
        {
            "$addFields": {
                "affiliations": {
                    "$filter": {
                        "input": "$affiliations",
                        "as": "a",
                        "cond": {
                            "$and": [
                                {"$ne": ["$$a", None]},
                                {
                                    "$gt": [
                                        {
                                            "$size": {
                                                "$objectToArray": "$$a"
                                            }
                                        },
                                        0,
                                    ]
                                },
                            ]
                        },
                    }
                }
            }
        },
        {
            "$replaceRoot": {
                "newRoot": {
                    "$mergeObjects": [
                        "$doc",
                        {"affiliations": "$affiliations"},
                    ]
                }
            }
        },
        {
            "$project": {
                "aff_doc": 0,
                "doc": 0,
            }
        },
        {
            "$merge": {
                "into": "person",
                "whenMatched": "replace",
                "whenNotMatched": "discard",
            }
        },
    ]

    collection.aggregate(pipeline)


def set_works_authors_affiliations_external_data(collection) -> None:
    """
    Function to enrich works authors affiliations with external_ids and addresses data

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$lookup": {
                "from": "affiliations",
                "let": {
                    "aff_ids": {
                        "$reduce": {
                            "input": "$authors",
                            "initialValue": [],
                            "in": {
                                "$setUnion": [
                                    "$$value",
                                    "$$this.affiliations.id",
                                ]
                            },
                        }
                    }
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$in": ["$_id", "$$aff_ids"]
                            }
                        }
                    },
                    {
                        "$project": {
                            "id": "$_id",
                            "_id": 0,
                            "external_ids": 1,
                            "addresses": {
                                "$map": {
                                    "input": {
                                        "$ifNull": ["$addresses", []]
                                    },
                                    "as": "addr",
                                    "in": {
                                        "latitude": "$$addr.lat",
                                        "longitude": "$$addr.lng",
                                        "city": "$$addr.city",
                                        "country": "$$addr.country",
                                        "country_code": "$$addr.country_code",
                                    },
                                }
                            },
                        }
                    },
                ],
                "as": "aff_data",
            }
        },
        {
            "$addFields": {
                "aff_map": {
                    "$arrayToObject": {
                        "$map": {
                            "input": "$aff_data",
                            "as": "a",
                            "in": {
                                "k": "$$a.id",
                                "v": "$$a",
                            },
                        }
                    }
                }
            }
        },
        {
            "$addFields": {
                "authors": {
                    "$map": {
                        "input": "$authors",
                        "as": "author",
                        "in": {
                            "$mergeObjects": [
                                "$$author",
                                {
                                    "affiliations": {
                                        "$map": {
                                            "input": {
                                                "$ifNull": [
                                                    "$$author.affiliations",
                                                    [],
                                                ]
                                            },
                                            "as": "aff",
                                            "in": {
                                                "$let": {
                                                    "vars": {
                                                        "affData": {
                                                            "$getField": {
                                                                "field": "$$aff.id",
                                                                "input": "$aff_map",
                                                            }
                                                        }
                                                    },
                                                    "in": {
                                                        "$mergeObjects": [
                                                            "$$aff",
                                                            {
                                                                "external_ids": {
                                                                    "$ifNull": [
                                                                        "$$affData.external_ids",
                                                                        "$$aff.external_ids",
                                                                    ]
                                                                },
                                                                "addresses": {
                                                                    "$ifNull": [
                                                                        "$$affData.addresses",
                                                                        "$$aff.addresses",
                                                                    ]
                                                                },
                                                            },
                                                        ]
                                                    },
                                                }
                                            },
                                        }
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        },
        {"$unset": ["aff_data", "aff_map"]},
        {
            "$merge": {
                "into": "works",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }
        },
    ]

    collection.aggregate(pipeline)


def set_works_groups_citations_count(collection) -> None:
    """
    Function to set citations count for groups inside works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$match": {
                "groups": {
                    "$exists": True,
                    "$ne": [],
                }
            }
        },
        {"$unwind": "$groups"},
        {
            "$lookup": {
                "from": "affiliations",
                "let": {
                    "groupId": "$groups.id",
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$eq": ["$_id", "$$groupId"],
                            }
                        }
                    },
                    {
                        "$match": {
                            "types.type": "group",
                        }
                    },
                    {
                        "$project": {
                            "citations_count": 1,
                            "_id": 0,
                        }
                    },
                ],
                "as": "affiliation",
            }
        },
        {
            "$unwind": {
                "path": "$affiliation",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$addFields": {
                "groups.citations_count": "$affiliation.citations_count"
            }
        },
        {
            "$project": {
                "affiliation": 0
            }
        },
        {
            "$group": {
                "_id": "$_id",
                "groups": {"$push": "$groups"},
                "doc": {"$first": "$$ROOT"},
            }
        },
        {
            "$replaceRoot": {
                "newRoot": {
                    "$mergeObjects": [
                        "$doc",
                        {"groups": "$groups"},
                    ]
                }
            }
        },
        {
            "$merge": {
                "into": "works",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }
        },
    ]

    collection.aggregate(pipeline)


def set_works_groups_ranking_to_works_collection(collection) -> None:
    """
    Function to set ranking data for groups inside works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$lookup": {
                "from": "affiliations",
                "let": {
                    "group_ids": {
                        "$map": {
                            "input": "$groups",
                            "as": "g",
                            "in": "$$g.id",
                        }
                    }
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$in": ["$_id", "$$group_ids"]
                            }
                        }
                    },
                    {
                        "$project": {
                            "id": "$_id",
                            "_id": 0,
                            "ranking": 1,
                        }
                    },
                ],
                "as": "group_rankings",
            }
        },
        {
            "$addFields": {
                "group_rank_map": {
                    "$arrayToObject": {
                        "$map": {
                            "input": "$group_rankings",
                            "as": "gr",
                            "in": {
                                "k": "$$gr.id",
                                "v": {
                                    "$ifNull": [
                                        "$$gr.ranking",
                                        [],
                                    ]
                                },
                            },
                        }
                    }
                }
            }
        },
        {
            "$addFields": {
                "groups": {
                    "$map": {
                        "input": "$groups",
                        "as": "g",
                        "in": {
                            "$mergeObjects": [
                                "$$g",
                                {
                                    "ranking": {
                                        "$ifNull": [
                                            {
                                                "$getField": {
                                                    "field": "$$g.id",
                                                    "input": "$group_rank_map",
                                                }
                                            },
                                            [],
                                        ]
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        },
        {"$unset": ["group_rankings", "group_rank_map"]},
        {
            "$merge": {
                "into": "works",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }
        },
    ]

    collection.aggregate(pipeline)


def clean_works_authors_affiliations_country_fields(collection) -> None:
    """
    Function to remove country and country_code fields from authors affiliations in works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$set": {
                "authors": {
                    "$map": {
                        "input": "$authors",
                        "as": "author",
                        "in": {
                            "$mergeObjects": [
                                "$$author",
                                {
                                    "affiliations": {
                                        "$map": {
                                            "input": {
                                                "$ifNull": [
                                                    "$$author.affiliations",
                                                    [],
                                                ]
                                            },
                                            "as": "aff",
                                            "in": {
                                                "$arrayToObject": {
                                                    "$filter": {
                                                        "input": {
                                                            "$objectToArray": "$$aff"
                                                        },
                                                        "as": "field",
                                                        "cond": {
                                                            "$not": {
                                                                "$in": [
                                                                    "$$field.k",
                                                                    [
                                                                        "country",
                                                                        "country_code",
                                                                    ],
                                                                ]
                                                            }
                                                        },
                                                    }
                                                }
                                            },
                                        }
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        }
    ]

    collection.update_many({}, pipeline)


def normalize_works_authors_ranking_empty_list(collection) -> None:
    """
    Function to replace null authors ranking with empty list in works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$set": {
                "authors": {
                    "$map": {
                        "input": "$authors",
                        "as": "author",
                        "in": {
                            "$mergeObjects": [
                                "$$author",
                                {
                                    "ranking": {
                                        "$cond": [
                                            {
                                                "$eq": [
                                                    "$$author.ranking",
                                                    None,
                                                ]
                                            },
                                            [],
                                            "$$author.ranking",
                                        ]
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        }
    ]

    collection.update_many(
        {"authors.ranking": None},
        pipeline,
    )


def set_affiliations_citations_count_openalex(collection) -> None:
    """
    Function to set the OpenAlex citations count in affiliations

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the affiliations are stored
    """
    pipeline = [
        {
            "$set": {
                "citations_count_openalex": {
                    "$ifNull": [
                        {
                            "$getField": {
                                "field": "count",
                                "input": {
                                    "$first": {
                                        "$filter": {
                                            "input": "$citations_count",
                                            "as": "c",
                                            "cond": {
                                                "$eq": [
                                                    "$$c.source",
                                                    "openalex",
                                                ]
                                            },
                                        }
                                    }
                                },
                            }
                        },
                        0,
                    ]
                }
            }
        }
    ]

    collection.update_many({}, pipeline)


def set_sources_products_count(collection) -> None:
    """
    Function to set products count in sources from works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Sources collection
    """
    pipeline = [
        {
            "$group": {
                "_id": "$source.id",
                "products_count": {"$sum": 1},
            }
        },
        {
            "$merge": {
                "into": "sources",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }
        },
    ]

    collection.aggregate(pipeline)


def normalize_sources_products_count(collection) -> None:
    """
    Function to set default products_count to 0 in sources

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Sources collection
    """
    collection.update_many(
        {"products_count": {"$exists": False}},
        {"$set": {"products_count": 0}},
    )


def set_sources_citations_count_openalex(collection) -> None:
    """
    Function to set total OpenAlex citations count in sources

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Sources collection
    """
    pipeline = [
        {
            "$match": {
                "citations_count": {
                    "$elemMatch": {"source": "openalex"}
                }
            }
        },
        {
            "$project": {
                "source.id": 1,
                "citations_count": 1,
            }
        },
        {"$unwind": "$citations_count"},
        {
            "$match": {
                "citations_count.source": "openalex"
            }
        },
        {
            "$group": {
                "_id": "$source.id",
                "total_citations": {
                    "$sum": "$citations_count.count"
                },
            }
        },
        {
            "$project": {
                "_id": 1,
                "citations_count": [
                    {
                        "source": "openalex",
                        "count": "$total_citations",
                    }
                ],
            }
        },
        {
            "$merge": {
                "into": "sources",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }
        },
    ]

    collection.aggregate(pipeline)


def normalize_sources_citations_count(collection) -> None:
    """
    Function to set default OpenAlex citations_count in sources

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Sources collection
    """
    collection.update_many(
        {"citations_count": {"$exists": False}},
        {
            "$set": {
                "citations_count": [
                    {
                        "source": "openalex",
                        "count": 0,
                    }
                ]
            }
        },
    )


def clean_person_empty_affiliations_array(collection) -> None:
    """
    Function to replace affiliations equal to [{}]
    with an empty list in person collection

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the persons are stored
    """
    collection.update_many(
        {"affiliations": [{}]},
        {"$set": {"affiliations": []}},
    )


DENORMALIZATION_PIPELINES = {
    "works": [
        set_works_authors_affiliations_country,
        set_works_authors_affiliations_country_code,
        set_works_groups_ranking,
        set_works_authors_ranking,
        set_works_citations_count_openalex,
        set_works_authors_full_data,
        set_works_authors_affiliations_dates,
        set_works_source_full_data,
        set_works_authors_affiliations_external_data,
        set_works_groups_citations_count,
        set_works_groups_ranking_to_works_collection,
        clean_works_authors_affiliations_country_fields,
        normalize_works_authors_ranking_empty_list,
    ],
    "sources": [
        set_sources_products_count,
        normalize_sources_products_count,
        set_sources_citations_count_openalex,
        normalize_sources_citations_count,
    ],
    "person": [
        set_person_affiliations_relations,
        clean_person_empty_affiliations_array,
    ],
    "affiliations": [
        set_affiliations_citations_count_openalex,
    ],
}


def denormalize(db):
    """
    Denormalize the data in all configured collections

    Parameters
    ----------
    db : pymongo.database.Database
        Database object to denormalize
    """
    for collection_name, pipelines in DENORMALIZATION_PIPELINES.items():
        collection = db[collection_name]

        print(f"INFO: Denormalizing data in {collection_name}")

        for pipeline_func in pipelines:
            pipeline_func(collection)
