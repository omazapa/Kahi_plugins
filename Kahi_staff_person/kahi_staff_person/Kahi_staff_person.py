from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from pandas import read_excel, isna
from time import time
import re
from kahi_impactu_utils.Utils import check_date_format, parse_sex
from kahi_impactu_utils.String import title_case


class Kahi_staff_person(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.client = MongoClient(config["database_url"])

        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])

        self.verbose = config["verbose"] if "verbose" in config else 0

        self.required_columns = ['cedula', 'codfac', 'Nombre fac', 'ccosto', 'Nombre cencos', 'tipo_doc',
                                 'nombre', 'clase_emp', 'vincula', 'fecha_vin',
                                 'Tiempo en la universidad', 'fecha_nac', 'Edad', 'nivelacad',
                                 'categoria', 'sexo',
                                 'nombres', 'apellidos']

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
        s = title_case(s)
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
                if isinstance(reg["fecha_vin"], str):
                    aff_time = check_date_format(reg["fecha_vin"])
                else:
                    aff_time = None
                name = self.staff_reg["names"][0]["name"]
                for n in self.staff_reg["names"]:
                    if n["lang"] == "es":
                        name = n["name"]
                        break
                    elif n["lang"] == "en":
                        name = n["name"]
                name = title_case(name)
                udea_aff = {"id": self.staff_reg["_id"], "name": name,
                            "types": self.staff_reg["types"], "start_date": aff_time, "end_date": -1}
                if udea_aff not in entry["affiliations"]:
                    entry["affiliations"].append(udea_aff)
                if reg["tipo_doc"].strip() == "CC":
                    id_entry = {"provenance": "staff",
                                "source": "Cédula de Ciudadanía", "id": idx}
                    if id_entry not in entry["external_ids"]:
                        entry["external_ids"].append(id_entry)
                elif reg["tipo_doc"].strip() == "CE":
                    id_entry = {"provenance": "staff",
                                "source": "Cédula de Extranjería", "id": idx}
                    if id_entry not in entry["external_ids"]:
                        entry["external_ids"].append(id_entry)
                else:
                    print(
                        f"ERROR: tipo_doc have to be CC or CE not {reg['tipo_doc']}")
                if reg["nombre"].lower() not in entry["aliases"]:
                    entry["aliases"].append(reg["nombre"].lower())
                dep = self.db["affiliations"].find_one(
                    {"names.name": title_case(reg["Nombre cencos"]), "relations.id": self.staff_reg["_id"]})
                if dep:
                    name = dep["names"][0]["name"]
                    for n in dep["names"]:
                        if n["lang"] == "es":
                            name = n["name"]
                            break
                        elif n["lang"] == "en":
                            name = n["name"]
                    name = title_case(name)
                    dep_affiliation = {
                        "id": dep["_id"], "name": name, "types": dep["types"], "start_date": aff_time, "end_date": -1}
                    if dep_affiliation not in entry["affiliations"]:
                        entry["affiliations"].append(dep_affiliation)
                fac = self.db["affiliations"].find_one(
                    {"names.name": title_case(reg["Nombre fac"]), "relations.id": self.staff_reg["_id"]})
                if fac:
                    name = fac["names"][0]["name"]
                    for n in fac["names"]:
                        if n["lang"] == "es":
                            name = n["name"]
                            break
                        elif n["lang"] == "en":
                            name = n["name"]
                    name = title_case(name)
                    fac_affiliation = {
                        "id": fac["_id"], "name": name, "types": fac["types"], "start_date": aff_time, "end_date": -1}
                    if fac_affiliation not in entry["affiliations"]:
                        entry["affiliations"].append(fac_affiliation)
                if isinstance(reg["fecha_nac"], str):
                    entry["birthdate"] = check_date_format(reg["fecha_nac"])
                else:
                    entry["birthdate"] = None
                entry["sex"] = parse_sex(reg["sexo"].lower())
                if not isna(reg["nivelacad"]):
                    degree = {"date": "", "degree": reg["nivelacad"], "id": "", "institutions": [
                    ], "source": "staff"}
                    if degree not in entry["degrees"]:
                        entry["degrees"].append(degree)
                if not isna(reg["categoria"]):
                    ranking = {"date": "",
                               "rank": reg["categoria"], "source": "staff"}
                    if ranking not in entry["ranking"]:
                        entry["ranking"].append(ranking)

            self.collection.insert_one(entry)

    def run(self):
        if self.verbose > 4:
            start_time = time()

        for config in self.config["staff_person"]["databases"]:
            if self.verbose > 0:
                print("Processing {} database".format(
                    config["institution_id"]))

            institution_id = config["institution_id"]

            self.staff_reg = self.db["affiliations"].find_one(
                {"external_ids.id": institution_id})
            if not self.staff_reg:
                print("Institution not found in database")
                raise ValueError(
                    f"Institution {institution_id} not found in database")

            file_path = config["file_path"]
            self.data = read_excel(file_path, dtype={
                "cedula": str, "codfac": str, "ccosto": str, "fecha_nac": str, "fecha_vin": str, "nivelacad": str, "categoria": str, "sexo": str})

            # logs for higher verbosity
            self.facs_inserted = {}
            self.deps_inserted = {}
            self.fac_dep = []

            self.cedula_dep = {}
            self.cedula_fac = {}
            for idx, reg in self.data.iterrows():
                self.cedula_fac[reg["cedula"]] = title_case(reg["Nombre fac"])
                self.cedula_dep[reg["cedula"]] = title_case(
                    reg["Nombre cencos"])

            self.facs_inserted = {}
            self.deps_inserted = {}
            self.fac_dep = []

            for aff in self.required_columns:
                if aff not in self.data.columns:
                    print(
                        f"Column {aff} not found in file {file_path}, and it is required.")
                    raise ValueError(
                        f"Column {aff} not found in file {file_path}")

            self.process_staff()

        if self.verbose > 4:
            print("Execution time: {} minutes".format(
                round((time() - start_time) / 60, 2)))
        return 0
