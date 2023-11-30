<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi works plugin 
Kahi will use this plugin to insert or update the works information.

# Description
In order to insert a work first, the plugin must find its occurrence on either of the following sources:
* OpenAlex
* Scopus
* Web of Science
* Google Scholar
* Scienti
* UdeA's ranking table
THe plugin then will aggregate all the information in CoLav's database schema and link the resulting entities to the ones already available in the database from the provious tasks within the workflow.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_works
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
For the data dependencies the user must have:
* A copy of the openalex dumpwith the collection of works of interest (take a subset since this database is huge) which can be downloaded at [OpenAlex data dump website](https://docs.openalex.org/download-all-data/openalex-snapshot "OpenAlex data dump website") and import it on a mongodb database.
* A subset of [Scopus papers' metadata]() loaded on a mongodb database.
* A subset of [Web of Science papers' metadata]() loaded on a mongodb database.
* The output of [Moai's](https://github.com/colav/Moai) scrapping of google scholar.
* The dump from minciencias scienti database parsed with [kayPacha](https://github.com/colav/KayPacha "KayPacha") and uploaded on a mongodb database.
* The file from UdeA's ranking office.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  works:
    num_jobs: 5
    verbose: 5
    openalex:
      database_url: localhost:27017
      database_name: openalexco
      collection_name: works
    scopus:
      database_url: localhost:27017
      database_name: scopus_colombia
      collection_name: stage
    wos:
      database_url: localhost:27017
      database_name: wos_colombia
      collection_name: stage
    scholar:
      database_url: localhost:27017
      database_name: scholar_colombia
      collection_name: stage
    scienti:
      database_url: localhost:27017
      database_name: scienti_111
      collection_name: products
    puntaje:
      file_path: /current/data/colombia/udea/produccion 2018-2022 al 27 oct 2022.xlsx
```

* WARNING *. This process could take several hours

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/

