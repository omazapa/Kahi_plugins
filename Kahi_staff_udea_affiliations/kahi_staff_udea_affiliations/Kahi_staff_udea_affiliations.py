from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from bson.objectid import ObjectId
from pandas import read_excel
import time

class Kahi_staff_udea_affiliations(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.client=MongoClient(self.mongodb_url)
        
        self.db=client[config["database_name"]]
        self.collection=db["affiliations"]

        self.file_path=config["file_path"]
        self.data=read_excel(self.file_path)

        #logs for higher verbosity
        self.already_in_db=[]
        self.facs_inserted={}
        self.deps_inserted={}
        self.fac_dep=[]

        udea_reg=self.collection.find_one({"names.name":"Universidad de Antioquia"})
        if not udea_reg:
            raise Exception("University of Antioquia not found in database")

    def empty_affiliations(self):
        entry = {
            "updated":[],
            "names":[],
            "aliases":[],
            "abbreviations":[],
            "types":[],
            "year_established":None,
            "status":[],
            "relations":[],
            "addresses":[],
            "external_urls":[],
            "external_ids":[],
            "subjects":[],
            "ranking":[],
            "description":[]
        }
        return entry

    def fix_names(self,name): #reg["Nombre fac"]
        name=name.strip()
        if name=='Vic Docencia':
            name="Vicerrectoría de Docencia"
        if name=="Exactas":
            name="Facultad de Ciencias Exactas y Naturales"
        if name=="Sociales":
            name="Facultad de Ciencias Sociales y Humanas"
        if name=="Derecho":
            name="Facultad de Derecho y Ciencias Políticas"
        if name=="Agrarias":
            name="Facultad de Ciencias Agrarias"
        if name=="Est. Políticos":
            name="Institutio de Estudios Políticos"
        if name=="Artes":
            name="Facultad de Artes"
        if name=="Odontología":
            name="Facultad de Odontología"
        if name=="Comunicaciones":
            name="Facultad de Comunicaciones y Filología"
        if name=="Educación":
            name="Facultad de Educación"
        if name=="Idiomas":
            name="Escuela de Idiomas"
        if name=="Filosofía":
            name="Instituto de Filosofía"
        if name=="Económicas":
            name="Facultad de Ciencias Económicas"
        if name=="Ingeniería":
            name="Facultad de Ingeniería"
        if name=="Medicina":
            name="Facultad de Medicina"
        if name=="Farmacéuticas":
            name="Facultad de Ciencias Farmacéuticas y Alimentarias"
        if name=="Microbiología":
            name="Escuela de Microbiología"
        if name=="Salud Pública":
            name="Facultad de Salud Pública"
        if name=="Agrarias":
            name="Facultad de Ciecias Agrarias"
        if name=="Bibliotecología":
            name="Escuela Interamericana de Bibliotecología"
        if name=="Enfermería":
            name="Facultad de Enfermería"
        if name=="Educación Física":
            name="Instituto Universitario de Educación Física y Deporte"
        if name=="Nutrición":
            name="Escuela de Nutrición y Dietética"
        if name=="Corp Ambiental":
            name="Corporación Ambiental"
        if name=="Est. Regionales":
            name="Instituto de Estudios Regionales"
        return name

    def run(self):
        #inserting faculties and departments
        for idx, reg in data.iterrows():
            name=fix_names(reg["Nombre fac"])
            if not name in self.already_in_db:
                is_in_db=self.collection.find_one({"names.name":name})
                if is_in_db:
                    self.already_in_db.append(name)
                    #continue
                    #may be updatable, check accordingly
                else:
                    entry=empty_affiliations()
                    entry["updated"].append({"time":int(time()),"source":"staff"})
                    entry["names"].append({"name":reg["Nombre fac"],"lang":"es"})
                    entry["types"].append({"source":"staff","type":"faculty"})
                    entry["relations"].append({"id":udea_reg["_id"],"name":"Universidad de Antioquia","types":udea_reg["types"]})
                    
                    fac=self.collection.insert_one(entry)
                    already_in_db.append(name)
                    facs_inserted[reg["Nombre cencos"]]=name

            if not reg["Nombre cencos"] in self.already_in_db:
                is_in_db=self.collection.find_one({"names.name":reg["Nombre cencos"]})
                if is_in_db:
                    self.already_in_db.append(name)
                    #continue
                    #may be updatable, check accordingly
                else:
                    entry=empty_affiliations()
                    entry["updated"].append({"time":int(time()),"source":"staff"})
                    entry["names"].append({"name":reg["Nombre cencos"],"lang":"es"})
                    entry["types"].append({"source":"staff","type":"department"})
                    entry["relations"].append({"id":udea_reg["_id"],"name":"Universidad de Antioquia","types":udea_reg["types"]})

                    dep=self.collection.insert_one(entry)
                    already_in_db.append(name)
                    deps_inserted[reg["Nombre cencos"]]=response.inserted_id
            
            if not (name,reg["Nombre cencos"]) in fac_dep_pair:
                fac_dep_pair.append((name,reg["Nombre cencos"]))

        #Creating relations between faculties and departments
        for fac,dep in self.fac_dep:
            fac_id=self.facs_inserted[fac]
            dep_id=self.deps_inserted[dep]
            dep_reg=self.collection.find_one({"_id":ObjectId(dep_id)})
            fac_reg=self.collection.find_one({"_id":ObjectId(fac_id)})
            self.collection.update_one({"_id":fac_reg["_id"]},
                                                {"$push":{
                                                    "relations":{
                                                        "id":dep_reg["_id"],
                                                        "name":dep_reg["names"][0]["name"],"types":dep_reg["types"]}}})
            self.collection.update_one({"_id":dep_reg["_id"]},
                                                {"$push":{
                                                    "relations":{
                                                        "id":fac_reg["_id"],
                                                        "name":fac_reg["names"][0]["name"],"types":fac_reg["types"]}}})
