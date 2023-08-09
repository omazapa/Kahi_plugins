<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi ROR affiliations plugin 
Kahi will use this plugin to insert or update the institutional information from ROR database.

# Description
Plugin that reads the information from a mongodb collection with ROR data dump to update or insert the information of its institutions in CoLav's database format.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_ror_affiliations
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
The user must have a copy of the DOAJ dump which can be downloaded at [ROR data](https://zenodo.org/communities/ror-data "ROR data") and import it on a mongodb database.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  ror_affiliations:
    database_url: localhost:27017
    database_name: ror
    collection_name: stage
    num_jobs: 10
```

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/