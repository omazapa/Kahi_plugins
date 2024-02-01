from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from pandas import read_excel
from time import time
from datetime import datetime as dt
import re


class Kahi_staff_udea_person(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.client = MongoClient(config["database_url"])

        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])

        self.file_path = config["staff_udea_person"]["file_path"]
        self.data = read_excel(self.file_path, dtype={
                               "cedula": str, "codfac": str, "ccosto": str, "fecha_nac": str, "fecha_vin": str})

        # logs for higher verbosity
        self.facs_inserted = {}
        self.deps_inserted = {}
        self.fac_dep = []

        self.cedula_dep = {}
        self.cedula_fac = {}
        for idx, reg in self.data.iterrows():
            self.cedula_fac[reg["cedula"]] = reg["Nombre fac"]
            self.cedula_dep[reg["cedula"]] = reg["Nombre cencos"]

        self.udea_reg = self.db["affiliations"].find_one(
            {"names.name": "University of Antioquia"})
        if not self.udea_reg:
            print(
                "University of Antioquia not found in database. Creating it with minimal information...")
            udea_reg = self.empty_affiliation()
            udea_reg["updated"].append(
                {"time": int(time()), "source": "manual"})
            udea_reg["names"] = [
                {"name": 'University of Antioquia', "lang": 'en'},
                {"name": "Universitat d'Antioquia", "lang": 'ca'},
                {"name": 'University of Antioquia', "lang": 'ceb'},
                {"name": 'Universidad de Antioquia', "lang": 'de'},
                {"name": 'Universitato de Antjokio', "lang": 'eo'},
                {"name": 'Universidad de Antioquia', "lang": 'es'},
                {"name": 'Antioquia Ülikool', "lang": 'et'},
                {"name": "Université d'Antioquia", "lang": 'fr'},
                {"name": 'Oilthigh Antioquia', "lang": 'gd'},
                {"name": 'アンティオキア大学', "lang": 'ja'},
                {"name": 'Universiteit van Antioquia', "lang": 'nl'},
                {"name": 'Uniwersytet Antioquia', "lang": 'pl'},
                {"name": 'Antioquias universitet', "lang": 'sv'},
                {"name": 'Unibersidad ng Antioquia', "lang": 'tl'},
                {"name": 'Antiokiya universiteti', "lang": 'uz'}
            ]
            udea_reg["abbreviations"] = ['UdeA']
            udea_reg["year_established"] = 1803
            udea_reg["addresses"] = [
                {
                    "lat": 6.267417,
                    "lng": -75.568389,
                    "postcode": '',
                    "state": "Antioquia",
                    "city": 'Medellín',
                    "country": 'Colombia',
                    "country_code": 'CO'
                }
            ]
            udea_reg["external_ids"] = [
                {"source": 'isni', "id": '0000 0000 8882 5269'},
                {"source": 'fundref', "id": '501100005278'},
                {"source": 'orgref', "id": '2696975'},
                {"source": 'wikidata', "id": 'Q1258413'},
                {"source": 'ror', "id": 'https://ror.org/03bp5hc83'},
                {"source": 'minciencias', "id": '007300000887'},
                {"source": 'nit', "id": '890980040-8'}
            ]
            self.db["affiliations"].insert_one(udea_reg)
            self.udea_reg = self.db["affiliations"].find_one(
                {"names.name": "University of Antioquia"})

    # noqa: W605
    def split_names(self, s, exceptions=['GIL', 'LEW', 'LIZ', 'PAZ', 'REY', 'RIO', 'ROA', 'RUA', 'SUS', 'ZEA']):
        """
        Extract the parts of the full name `s` in the format ([] → optional):

        [SMALL_CONECTORS] FIRST_LAST_NAME [SMALL_CONECTORS] [SECOND_LAST_NAME] NAMES

        * If len(s) == 2 → Foreign name assumed with single last name on it
        * If len(s) == 3 → Colombian name assumed two last mames and one first name

        Add short last names to `exceptions` list if necessary

        Works with:
        ----
            s='LA ROTTA FORERO DANIEL ANDRES'
            s='MONTES RAMIREZ MARIA DEL CONSUELO'
            s='CALLEJAS POSADA RICARDO DE LA MERCED'
            s='DE LA CUESTA BENJUMEA MARIA DEL CARMEN'
            s='JARAMILLO OCAMPO NICOLAS CARLOS MARTI'
            s='RESTREPO QUINTERO DIEGO ALEJANDRO'
            s='RESTREPO ZEA JAIRO HUMBERTO'
            s='JIMENEZ DEL RIO MARLEN'
            s='RESTREPO FERNÁNDEZ SARA' # Colombian: two LAST_NAMES NAME
            s='NARDI ENRICO' # Foreing
        Fails:
        ----
            s='RANGEL MARTINEZ VILLAL ANDRES MAURICIO' # more than 2 last names
            s='ROMANO ANTONIO ENEA' # Foreing → LAST_NAME NAMES
        """
        s = s.title()
        exceptions = [e.title() for e in exceptions]
        sl = re.sub('(\s\w{1,3})\s', r'\1-', s, re.UNICODE)  # noqa: W605
        sl = re.sub('(\s\w{1,3}\-\w{1,3})\s', r'\1-', sl, re.UNICODE)  # noqa: W605
        sl = re.sub('^(\w{1,3})\s', r'\1-', sl, re.UNICODE)  # noqa: W605
        # Clean exceptions
        # Extract short names list
        lst = [s for s in re.split(
            '(\w{1,3})\-', sl) if len(s) >= 1 and len(s) <= 3]  # noqa: W605
        # intersection with exceptions list
        exc = [value for value in exceptions if value in lst]
        if exc:
            for e in exc:
                sl = sl.replace('{}-'.format(e), '{} '.format(e))

        # if sl.find('-')>-1:
        # print(sl)
        sll = [s.replace('-', ' ') for s in sl.split()]
        if len(s.split()) == 2:
            sll = [s.split()[0]] + [''] + [s.split()[1]]
        #
        d = {'NOMBRE COMPLETO': ' '.join(sll[2:] + sll[:2]),
             'PRIMER APELLIDO': sll[0],
             'SEGUNDO APELLIDO': sll[1],
             'NOMBRES': ' '.join(sll[2:]),
             'INICIALES': ' '.join([i[0] + '.' for i in ' '.join(sll[2:]).split()])
             }
        return d

    def fix_names(self, name):  # reg["Nombre fac"]
        name = name.strip()
        if name == 'Vic Docencia':
            name = "Vicerrectoría de Docencia"
        if name == "Exactas":
            name = "Facultad de Ciencias Exactas y Naturales"
        if name == "Sociales":
            name = "Facultad de Ciencias Sociales y Humanas"
        if name == "Derecho":
            name = "Facultad de Derecho y Ciencias Políticas"
        if name == "Agrarias":
            name = "Facultad de Ciencias Agrarias"
        if name == "Est. Políticos":
            name = "Institutio de Estudios Políticos"
        if name == "Artes":
            name = "Facultad de Artes"
        if name == "Odontología":
            name = "Facultad de Odontología"
        if name == "Comunicaciones":
            name = "Facultad de Comunicaciones y Filología"
        if name == "Educación":
            name = "Facultad de Educación"
        if name == "Idiomas":
            name = "Escuela de Idiomas"
        if name == "Filosofía":
            name = "Instituto de Filosofía"
        if name == "Económicas":
            name = "Facultad de Ciencias Económicas"
        if name == "Ingeniería":
            name = "Facultad de Ingeniería"
        if name == "Medicina":
            name = "Facultad de Medicina"
        if name == "Farmacéuticas":
            name = "Facultad de Ciencias Farmacéuticas y Alimentarias"
        if name == "Microbiología":
            name = "Escuela de Microbiología"
        if name == "Salud Pública":
            name = "Facultad de Salud Pública"
        if name == "Agrarias":
            name = "Facultad de Ciecias Agrarias"
        if name == "Bibliotecología":
            name = "Escuela Interamericana de Bibliotecología"
        if name == "Enfermería":
            name = "Facultad de Enfermería"
        if name == "Educación Física":
            name = "Instituto Universitario de Educación Física y Deporte"
        if name == "Nutrición":
            name = "Escuela de Nutrición y Dietética"
        if name == "Corp Ambiental":
            name = "Corporación Ambiental"
        if name == "Est. Regionales":
            name = "Instituto de Estudios Regionales"
        return name

    def process_staff(self):
        for idx in list(self.cedula_dep.keys()):
            check_db = self.collection.find_one({"external_ids.id": idx})
            if check_db:
                continue
            entry = self.empty_person()
            entry["updated"].append({"time": int(time()), "source": "staff"})
            names = self.split_names(
                self.data[self.data["cedula"] == idx].iloc[0]["nombre"])
            entry["full_name"] = names["NOMBRE COMPLETO"]
            entry["first_names"] = names["NOMBRES"].split()
            entry["last_names"].append(names["PRIMER APELLIDO"])
            if names["SEGUNDO APELLIDO"]:
                entry["last_names"].append(names["SEGUNDO APELLIDO"])

            for i, reg in self.data[self.data["cedula"] == idx].iterrows():
                aff_time = int(dt.strptime(
                    reg["fecha_vin"], "%Y-%m-%d %H:%M:%S").timestamp())
                name = self.udea_reg["names"][0]["name"]
                for n in self.udea_reg["names"]:
                    if n["lang"] == "es":
                        name = n["name"]
                        break
                    elif n["lang"] == "en":
                        name = n["name"]
                udea_aff = {"id": self.udea_reg["_id"], "name": name,
                            "types": self.udea_reg["types"], "start_date": aff_time, "end_date": -1}
                if udea_aff not in entry["affiliations"]:
                    entry["affiliations"].append(udea_aff)
                if reg["tipo_doc"] == "CC":
                    id_entry = {"source": "Cédula de Ciudadanía", "id": idx}
                    if id_entry not in entry["external_ids"]:
                        entry["external_ids"].append(id_entry)
                elif reg["tipo_doc"] == "CE":
                    id_entry = {"source": "Cédula de Extranjería", "id": idx}
                    if id_entry not in entry["external_ids"]:
                        entry["external_ids"].append(id_entry)
                if reg["nombre"].lower() not in entry["aliases"]:
                    entry["aliases"].append(reg["nombre"].lower())
                dep = self.db["affiliations"].find_one(
                    {"names.name": reg["Nombre cencos"]})
                if dep:
                    name = dep["names"][0]["name"]
                    for n in dep["names"]:
                        if n["lang"] == "es":
                            name = n["name"]
                            break
                        elif n["lang"] == "en":
                            name = n["name"]
                    dep_affiliation = {
                        "id": dep["_id"], "name": name, "types": dep["types"], "start_date": aff_time, "end_date": -1}
                    if dep_affiliation not in entry["affiliations"]:
                        entry["affiliations"].append(dep_affiliation)
                fac = self.db["affiliations"].find_one(
                    {"names.name": self.fix_names(reg["Nombre fac"])})
                if fac:
                    name = fac["names"][0]["name"]
                    for n in fac["names"]:
                        if n["lang"] == "es":
                            name = n["name"]
                            break
                        elif n["lang"] == "en":
                            name = n["name"]
                    fac_affiliation = {
                        "id": fac["_id"], "name": name, "types": fac["types"], "start_date": aff_time, "end_date": -1}
                    if fac_affiliation not in entry["affiliations"]:
                        entry["affiliations"].append(fac_affiliation)
                entry["birthdate"] = int(dt.strptime(
                    reg["fecha_nac"], "%Y-%m-%d %H:%M:%S").timestamp())
                entry["sex"] = reg["sexo"].lower()
                degree = {"date": "", "degree": reg["nivelacad"], "id": "", "institutions": [
                ], "source": "staff"}
                if degree not in entry["degrees"]:
                    entry["degrees"].append(degree)
                ranking = {"date": "",
                           "rank": reg["categoria"], "source": "staff"}
                if ranking not in entry["ranking"]:
                    entry["ranking"].append(ranking)

            self.collection.insert_one(entry)

    def run(self):
        self.process_staff()
        return 0
