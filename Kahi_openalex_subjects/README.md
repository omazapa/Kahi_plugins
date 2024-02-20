<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi openalex subjects plugin 
Kahi will use this plugin to insert or update the subjects information from OpenAlex

# Description
Plugin that reads the information from a mongodb collection with openalex dump to update or insert the information of the subjects in CoLav's database format.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install Kahi_openalex_sources
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
The user must have a copy of the OpenAlex snapshot which can be downloaded at [OpenAlex snapshot website](https://docs.openalex.org/download-all-data/openalex-snapshot "OpenAlex snapshot") and import the concpetsw collection on a mongodb database.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  openalex_subjects:
    database_url: localhost:27017
    database_name: openalex
    collection_name: concepts
    num_jobs: 10
```


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



