from kahi.KahiBase import KahiBase
from pymongo import MongoClient
import datetime as dt
import time


class Kahi_doaj_sources(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["mongodb_url"]
        
        self.client = MongoClient(self.mongodb_url)
        
        self.db = client[config["database_name"]]
        self.collection = db["sources"]

        self.ror_client = MongoClient(config["plugin"]["mongodb_url"])

        self.doaj_db = client[config["plugin"]["database_name"]]
        self.doaj_collection = db[config["plugin"]["collection_name"]]

        self.already_in_db = []

    def empty_source(self):
        return {
            "updated" : [],
            "names" : [],
            "abbreviations" : [],
            "types" : [],
            "keywords" : [],
            "languages" : [],
            "publisher" : "",
            "relations" : [],
            "addresses" : [],
            "external_ids" : [],
            "external_urls" : [],
            "review_processes" : [],
            "waiver" : {},
            "plagiarism_detection" : False,
            "open_access_start_year" : None,
            "publication_time_weeks" : None,
            "apc":{},
            "copyright" : {},
            "licenses" : [],
            "subjects" : [],
            "ranking" : []
        }

    def process_doaj(self,verbose=0):
        with client.start_session() as session:
            old=dt.datetime.now()
            for oldreg in self.doaj_collection.find():
                reg=oldreg["bibjson"]
                if "eissn" in reg.keys():
                    reg_db=self.collection.find_one({"external_ids.id":reg["eissn"]})
                    if reg_db:
                        self.already_in_db.append(reg["eissn"])
                        if verbose>4:
                            print("Already in db: "+reg["eissn"])
                        if verbose >0:
                            print("Total already found: "+len(self.already_in_db))
                        continue
                        #may be updatable, check accordingly
                if "pissn" in reg.keys():
                    reg_db=self.collection.find_one({"external_ids.id":reg["pissn"]})
                    if reg_db:
                        self.already_in_db.append(reg["pissn"])
                        if verbose>4:
                            print("Already in db: "+reg["pissn"])
                        if verbose >0:
                            print("Total already found: "+len(self.already_in_db))
                        continue
                        #may be updatable, check accordingly

                entry = empty_source()
                entry["updated"]=[{"source":"doaj","time":int(time())}]
                entry["names"]=[{"lang":"en","name":reg["title"]}]
                entry["keywords"]=reg["keywords"]
                entry["languages"]=reg["language"]
                entry["publisher"]={"country_code":reg["publisher"]["country"],"name":reg["publisher"]["name"],"id":""}
                entry["open_access_start_year"]=reg["oa_start"] if "oa_start" in reg.keys() else None
                entry["external_urls"]=[{"source":ref,"url":url} for ref,url in reg["ref"].items()]
                entry["review_process"]=reg["editorial"]["review_process"]
                entry["plagiarism_detection"]=reg["plagiarism"]["detection"]
                entry["publication_time_weeks"]=reg["publication_time_weeks"]
                entry["copyright"]=reg["copyright"]
                entry["licenses"]=reg["license"]

                if "apc" in reg.keys():
                    if reg["apc"]["has_apc"]:
                        entry["apc"]={"charges":reg["apc"]["max"][-1]["price"],"currency":reg["apc"]["max"][-1]["currency"]}

                subjects_source={}
                if "subject" in reg.keys():
                    if reg["subject"]:
                        for sub in reg["subject"]:
                            sub_entry={
                                "id":"",
                                "name":sub["term"],
                                "external_ids":[{"source":sub["scheme"],"id":sub["code"]}]
                            }
                            if sub["scheme"] in subjects_source.keys():
                                subjects_source[sub["scheme"]].append(sub_entry)
                            else:
                                subjects_source[sub["scheme"]]=[sub_entry]
                for source,subs in subjects_source.items():
                    entry["subjects"].append({
                        "source":source,
                        "subjects":subs
                    })

                if "eissn" in reg.keys():
                    entry["external_ids"].append({"source":"eissn","id":reg["eissn"]})
                if "pissn" in reg.keys():
                    entry["external_ids"].append({"source":"pissn","id":reg["pissn"]})

                entry["waiver"]=reg["waiver"]

                response = self.collection.insert_one(entry)
                for ext in entry["external_ids"]:
                    self.already_in_db.append(ext["id"])
                if verbose>4:
                    print("Inserted: "+response.inserted_id)
                if verbose >0:
                    print("Total inserted: "+len(self.already_in_db))

                if delta.seconds>240:
                    client.admin.command('refreshSessions', [session.session_id], session=session)
                    old=dt.datetime.now()


    def run(self):
        process_ror(verbose=5)

