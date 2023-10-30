from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from time import time
import requests
from urllib.parse import unquote
from thefuzz import fuzz
from joblib import Parallel, delayed


def get_wikipedia_names(url="", name="", lang="en", verbose=0):
    '''
    Find the different possible names of a wikipedia entity.
    Right now it is only tested on organizations gotten from ror db

    Parameters
    ----------
    url : str
        The wikipedia url if is available
    name : str
        The name of keywords to do the search over wikipedia api
    lang : str
        The iso-639 lang code to fix the language endpooint of the search language

    Returns
    -------
    data : dict
        The response of the wikipedia requests with the langlinks of the prop params

    '''
    if url:
        subject = unquote(url.split("/")[-1].replace("_", " "))
    elif name:
        subject = name
    else:
        return {"response": [], "names": []}

    base = 'https://' + lang + '.wikipedia.org/w/api.php'
    # searching entire wikipedia
    if verbose > 4:
        print("Searching ", subject)
    params = {
        'action': 'query',
        'format': 'json',
        'list': 'search',
        'srsearch': subject
    }

    data = requests.get(base, params=params).json()
    entry = ""
    pageid = ""
    if "query" not in data.keys():
        return None
    # searching among the results and checking twice with fuzzywuzzy
    for reg in data["query"]["search"]:
        score = fuzz.ratio(reg["title"].lower(), subject.lower())
        if score > 90:
            entry = reg
            pageid = int(reg["pageid"])
        elif score > 50:
            score = fuzz.partial_ratio(reg["title"].lower(), subject.lower())
            if score > 95:
                entry = reg
                pageid = int(reg["pageid"])
            elif score > 80:
                score = fuzz.token_set_ratio(
                    reg["title"].lower(), subject.lower())
                if score > 98:
                    entry = reg
                    pageid = int(reg["pageid"])
        if entry != "":
            break

    if pageid != "":  # if the page id is available
        # retrieving the actual page's langlinks
        params = {
            'action': 'query',
            'format': 'json',
            'pageids': pageid,
            'prop': 'langlinks',
            'lllimit': 500,
            # 'exintro': True,
            # 'explaintext': True,
        }

        response = requests.get(base, params=params)
        data = response.json()
        return data
    else:
        return None


def get_logo_wikipedia(url="", name="", lang="en", verbose=5):
    '''
    Find and image of a wikipedia page.
    Right now it is only tested for the logos of organizations gotten from ror db

    Parameters
    ----------
    url : str
        The wikipedia url if is available
    name : str
        The name of keywords to do the search over wikipedia api
    lang : str
        The iso-639 lang code to fix the language endpooint of the search language

    Returns
    -------
    data : dict
        The response of the wikipedia request

    '''
    if url:
        subject = unquote(url.split("/")[-1].replace("_", " "))
    elif name:
        subject = name
    else:
        return {"response": [], "names": []}

    base = 'https://' + lang + '.wikipedia.org/w/api.php'
    # searching entire wikipedia
    if verbose > 5:
        print("Searching ", subject)
    params = {
        'action': 'query',
        'format': 'json',
        'list': 'search',
        'srsearch': subject
    }

    data = requests.get(base, params=params).json()
    # print(data)
    entry = ""
    pageid = ""
    if "query" not in data.keys():
        return None
    for reg in data["query"]["search"]:
        score = fuzz.ratio(reg["title"].lower(), subject.lower())
        if score > 90:
            entry = reg
            pageid = int(reg["pageid"])
        elif score > 50:
            score = fuzz.partial_ratio(reg["title"].lower(), subject.lower())
            if score > 95:
                entry = reg
                pageid = int(reg["pageid"])
            elif score > 80:
                score = fuzz.token_set_ratio(
                    reg["title"].lower(), subject.lower())
                if score > 98:
                    entry = reg
                    pageid = int(reg["pageid"])
        if entry != "":
            break

    if pageid != "":
        # retrieveing the actual page
        params = {
            'action': 'query',
            'format': 'json',
            'pageids': pageid,
            'prop': 'images'
        }

        response = requests.get(base, params=params)
        data = response.json()
        try:
            title = ""
            for img in data["query"]["pages"][str(pageid)]["images"]:
                if "commons" in img["title"].lower():  # avoid the wikipedia logo
                    continue
                for keyword in ["flag", "escudo", "logo", "shield", "bandera"]:
                    if keyword in img["title"].lower():
                        title = img["title"]
                        break
                if title != "":
                    break
            if verbose > 5:
                print("title: ", title)
            params = {
                'action': 'query',
                'format': 'json',
                'titles': title,
                'prop': 'imageinfo',
                'iiprop': "url"
            }
            response = requests.get(base, params=params)
            data = response.json()
            return data
        except Exception as e:
            if verbose > 5:
                print("@@@@")
                print("Function error: ", e)
                print(data)
                print("@@@@")
    else:
        return None


def process_one_wikipedia_name(inst, url, db_name, verbose=0):
    client = MongoClient(url)

    db = client[db_name]
    collection = db["affiliations"]
    wikipedia_url = ""
    wikipedia_name = ""
    for ext in inst["external_urls"]:
        if ext["source"] == "wikipedia":
            wikipedia_url = ext["url"]
            break
    if wikipedia_url:
        if "wiki" not in wikipedia_url:
            return
    else:
        if verbose > 4:
            print(
                "No information could be used for wikipedia API query in ", inst["_id"])
        return
    result = {}
    res = []
    if wikipedia_url:
        res = get_wikipedia_names(url=wikipedia_url, verbose=verbose)
    elif wikipedia_name:
        res = get_wikipedia_names(name=wikipedia_name, verbose=verbose)
    try:
        k = list(res["query"]["pages"].keys())[0]
        result = {"response": res,
                  "names": res["query"]["pages"][k]["langlinks"]}

    except Exception as e:
        if verbose > 4:
            print(e)
            if wikipedia_url:
                print("Something went wrong processing ", wikipedia_url)
            elif wikipedia_name:
                print("Something went wrong processing ", wikipedia_name)
        result = {"response": res, "names": []}
        if wikipedia_url:
            res = get_wikipedia_names(
                url=wikipedia_url, lang="es", verbose=verbose)
        elif wikipedia_name:
            res = get_wikipedia_names(
                name=wikipedia_name, lang="es", verbose=verbose)
        try:
            k = list(res["query"]["pages"].keys())[0]
            result = {"response": res,
                      "names": res["query"]["pages"][k]["langlinks"]}

        except Exception as e:
            if verbose > 4:
                print(e)
                if wikipedia_url:
                    print("Something went wrong processing ", wikipedia_url)
                elif wikipedia_name:
                    print("Something went wrong processing ", wikipedia_name)
            result = {"response": res, "names": []}

    if not result["names"]:
        return
    else:
        names = inst["names"]
        for nam in result["names"]:
            if nam["lang"] != "en":
                names.append(
                    {"name": nam["*"], "lang": nam["lang"], "source": "wikipedia"})
        inst["updated"].append({"source": "wikipedia", "time": int(time())})
        collection.update_one({"_id": inst["_id"]}, {
                              "$set": {"names": names, "updated": inst["updated"]}})


def process_one_wikipedia_logo(inst, url, db_name, verbose=0):
    client = MongoClient(url)

    db = client[db_name]
    collection = db["affiliations"]

    logo_url = None
    url = None
    for ext in inst["external_urls"]:
        if ext["source"] == "wikipedia":
            url = ext["url"]
    if url:
        logo_url = get_logo_wikipedia(url=url)
    else:
        name = None
        lang = None
        for n in inst["names"]:
            if n["lang"] == "en":
                name = n["name"]
                lang = n["lang"]
                break
        if name and lang:
            logo_url = get_logo_wikipedia(name=name, lang=lang)
        if not logo_url:
            for n in inst["names"]:
                if n["lang"] == "es":
                    name = n["name"]
                    lang = n["lang"]
                    break
            if name and lang:
                logo_url = get_logo_wikipedia(name=name, lang=lang)
    if logo_url:
        try:
            logo_url = logo_url["query"]["pages"][list(logo_url["query"]["pages"].keys())[
                0]]["imageinfo"][0]["url"]
            collection.update_one({"_id": inst["_id"]}, {
                                  "$push": {"external_urls": {"source": "logo", "url": logo_url}}})
        except Exception as e:
            if verbose > 4:
                print(inst["_id"])
                print(e)


class Kahi_wikipedia_affiliations(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["affiliations"]

        self.n_jobs = config["wikipedia_affiliations"]["num_jobs"]

        self.tasks = config["wikipedia_affiliations"]["tasks"]

        self.verbose = config["wikipedia_affiliations"]["verbose"] if "verbose" in config["wikipedia_affiliations"].keys(
        ) else 0

        self.wikipedia_updated = []

    def process_wikipedia(self):
        for task in self.tasks:
            if task == "names":
                if self.verbose > 0:
                    print("Getting names from wikipedia")
                institutions = list(self.collection.find(
                    {"updated.source": "ror", "_id": {"$nin": self.wikipedia_updated}}))
                Parallel(
                    n_jobs=self.n_jobs,
                    backend="multiprocessing",
                    verbose=10
                )(delayed(process_one_wikipedia_name)(
                    inst,
                    self.config["database_url"],
                    self.config["database_name"],
                    self.verbose
                ) for inst in institutions)
            elif task == "logos":
                if self.verbose > 0:
                    print("Getting logos from wikipedia")
                institutions = list(self.collection.find(
                    {"updated.source": "ror", "_id": {"$nin": self.wikipedia_updated}}))
                Parallel(
                    n_jobs=self.n_jobs,
                    backend="multiprocessing",
                    verbose=10
                )(delayed(process_one_wikipedia_logo)(
                    inst,
                    self.config["database_url"],
                    self.config["database_name"],
                    self.verbose
                ) for inst in institutions)

    def run(self):
        self.process_wikipedia()
        return 0
