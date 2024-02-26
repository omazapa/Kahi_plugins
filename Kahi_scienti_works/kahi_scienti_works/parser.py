from kahi_impactu_utils.Utils import lang_poll, doi_processor, check_date_format
from time import time


def parse_scienti(reg, empty_work, verbose=0):
    entry = empty_work.copy()
    entry["updated"] = [{"source": "scienti", "time": int(time())}]
    lang = lang_poll(reg["TXT_NME_PROD"], verbose=verbose)
    entry["titles"].append(
        {"title": reg["TXT_NME_PROD"], "lang": lang, "source": "scienti"})
    entry["external_ids"].append(
        {"provenance": "scienti", "source": "COD_RH", "id": reg["COD_RH"]})
    entry["external_ids"].append(
        {"provenance": "scienti", "source": "COD_PRODUCTO", "id": reg["COD_PRODUCTO"]})
    if "TXT_DOI" in reg.keys():
        entry["external_ids"].append(
            {"source": "doi", "id": doi_processor(reg["TXT_DOI"])})
    if "TXT_WEB_PRODUCTO" in reg.keys():
        entry["external_urls"].append(
            {"source": "scienti", "url": reg["TXT_WEB_PRODUCTO"]})
    if "NRO_ANO_PRESENTA" in reg.keys():
        year = reg["NRO_ANO_PRESENTA"]
    if "NRO_MES_PRESENTA" in reg.keys():
        month = reg["NRO_MES_PRESENTA"]
        if len(str(month)) == 1:
            month = f'0{month}'
    if year and month:
        entry["date_published"] = check_date_format(
            f'{month}-{year}')
        entry["year_published"] = int(year)
    if "SGL_CATEGORIA" in reg.keys():
        entry["ranking"].append(
            {"provenance": "scienti", "date": "", "rank": reg["SGL_CATEGORIA"], "source": "scienti"})
    entry["types"].append(
        {"provenance": "scienti", "source": "scienti", "type": reg["product_type"][0]["TXT_NME_TIPO_PRODUCTO"]})
    if "product_type" in reg["product_type"][0].keys():
        typ = reg["product_type"][0]["product_type"][0]["TXT_NME_TIPO_PRODUCTO"]
        entry["types"].append(
            {"provenance": "scienti", "source": "scienti", "type": typ})

    # details only for articles
    if "details" in reg.keys() and len(reg["details"]) > 0 and "article" in reg["details"][0].keys():
        details = reg["details"][0]["article"][0]
        try:
            if "TXT_PAGINA_INICIAL" in details.keys():
                entry["bibliographic_info"]["start_page"] = details["TXT_PAGINA_INICIAL"]
        except Exception as e:
            if verbose > 4:
                print(
                    f'Error parsing start page on RH:{reg["COD_RH"]} and COD_PROD:{reg["COD_PRODUCTO"]}')
                print(e)
        try:
            if "TXT_PAGINA_FINAL" in details.keys():
                entry["bibliographic_info"]["end_page"] = details["TXT_PAGINA_FINAL"]
        except Exception as e:
            if verbose > 4:
                print(
                    f'Error parsing end page on RH:{reg["COD_RH"]} and COD_PROD:{reg["COD_PRODUCTO"]}')
                print(e)
        try:
            if "TXT_VOLUMEN_REVISTA" in details.keys():
                entry["bibliographic_info"]["volume"] = details["TXT_VOLUMEN_REVISTA"]
        except Exception as e:
            if verbose > 4:
                print(
                    f'Error parsing volume on RH:{reg["COD_RH"]} and COD_PROD:{reg["COD_PRODUCTO"]}')
                print(e)
        try:
            if "TXT_FASCICULO_REVISTA" in details.keys():
                entry["bibliographic_info"]["issue"] = details["TXT_FASCICULO_REVISTA"]
        except Exception as e:
            if verbose > 4:
                print(
                    f'Error parsing issue on RH:{reg["COD_RH"]} and COD_PROD:{reg["COD_PRODUCTO"]}')
                print(e)

        # source section
        source = {"external_ids": [], "title": ""}
        if "journal" in details.keys():
            journal = details["journal"][0]
            source["title"] = journal["TXT_NME_REVISTA"]
            if "TXT_ISSN_REF_SEP" in journal.keys():
                source["external_ids"].append(
                    {"source": "issn", "id": journal["TXT_ISSN_REF_SEP"]})
            if "COD_REVISTA" in journal.keys():
                source["external_ids"].append(
                    {"source": "scienti", "id": journal["COD_REVISTA"]})
        elif "journal_others" in details.keys():
            journal = details["journal_others"][0]
            source["title"] = journal["TXT_NME_REVISTA"]
            if "TXT_ISSN_REF_SEP" in journal.keys():
                source["external_ids"].append(
                    {"source": "issn", "id": journal["TXT_ISSN_REF_SEP"]})
            if "COD_REVISTA" in journal.keys():
                source["external_ids"].append(
                    {"source": "scienti", "id": journal["COD_REVISTA"]})

        entry["source"] = source

    # authors section
    affiliations = []
    if "group" in reg.keys():
        group = reg["group"][0]
        affiliations.append({
            "external_ids": [{"provenance": "scienti", "source": "scienti", "id": group["COD_ID_GRUPO"]}],
            "name": group["NME_GRUPO"]
        })
        if "institution" in group.keys():
            inst = group["institution"][0]
            affiliations.append({
                "external_ids": [{"provenance": "scienti", "source": "scienti", "id": inst["COD_INST"]}],
                "name": inst["NME_INST"]
            })
    author = reg["author"][0]
    author_entry = {
        "full_name": author["TXT_TOTAL_NAMES"],
        "types": [],
        "affiliations": affiliations,
        "external_ids": [{"provenance": "scienti", "source": "scienti", "id": author["COD_RH"]}]
    }
    if author["TPO_DOCUMENTO_IDENT"] == "P":
        author_entry["external_ids"].append(
            {"provenance": "scienti", "source": "Passport", "id": author["NRO_DOCUMENTO_IDENT"]})
    if author["TPO_DOCUMENTO_IDENT"] == "C":
        author_entry["external_ids"].append(
            {"provenance": "scienti", "source": "Cédula de Ciudadanía", "id": author["NRO_DOCUMENTO_IDENT"]})
    if author["TPO_DOCUMENTO_IDENT"] == "E":
        author_entry["external_ids"].append(
            {"provenance": "scienti", "source": "Cédula de Extranjería", "id": author["NRO_DOCUMENTO_IDENT"]})
    entry["authors"] = [author_entry]
    return entry
