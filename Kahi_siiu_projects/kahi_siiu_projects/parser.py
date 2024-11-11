from kahi_impactu_utils.Utils import lang_poll, check_date_format
from time import time


def parse_siiu(reg, empty_project, verbose=0):
    """
    Parse a record from the SIIU database into a project entry, using the empty_project as template.

    Parameters
    ----------
    reg : dict
        The record to be parsed from siiu
    empty_project : dict
        A template for the work entry. Structure is defined in the schema.
    verbose : int
        The verbosity level. Default is 0.
    """
    entry = empty_project.copy()
    entry["updated"] = [{"source": "siiu", "time": int(time())}]
    if reg["NOMBRE_COMPLETO"]:
        lang = lang_poll(reg["NOMBRE_COMPLETO"], verbose=verbose)
    entry["titles"].append(
        {"title": reg["NOMBRE_COMPLETO"], "lang": lang, "source": "siiu"})
    entry["external_ids"].append(
        {"provenance": "siiu", "source": "codigo", "id": reg["CODIGO"]})

    for author in reg["project_participant"]:
        # solo investigador principar de momento
        if author["project_participant_role"][0]["IDENTIFICADOR"] == 307:
            break

    affiliations = []
    for group in author["group"]:
        grec = {
            "external_ids": [{"provenance": "siiu", "source": "scienti", "id": group["CODIGO_COLCIENCIAS"]}],
            "name": group["NOMBRE_COMPLETO"]
        }

        affiliations.append(grec)
        # hay que hacer un siiu affiliations y crozar los grupos apra ver si obtenemos mas NRO_ID_GRUPO
        # affiliations.append(
        #     {
        #         "external_ids": [{"provenance": "siiu", "source": "scienti", "id": group["NRO_ID_GRUPO"]}],
        #         "name": group["NOMBRE_COMPLETO"]
        #     }
        # )
        entry["groups"].append(grec)

    author_entry = {
        "full_name": "",
        "affiliations": affiliations,
        "external_ids": [{"provenance": 'siiu', "source": 'Cédula de Ciudadanía', "id": author["PERSONA_NATURAL"]}]
    }
    entry["authors"] = [author_entry]
    return entry
