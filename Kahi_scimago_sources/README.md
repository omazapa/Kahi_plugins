<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi scimago sources plugin 
Kahi will use this plugin to insert or update the journal information from scimago

# Description
Plungin that reads the information from scimago ranking csv file to update or insert the information of the journals in CoLav's database format.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_scimago_sources
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
The user must have at least one file fomr scimago report of journal rankings that can be downloaded from [scimago website](https://www.scimagojr.com/journalrank.php "Scimago journal rankings")

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  scimago_sources:
    file_path:
      - scimago/scimagojr 2020.csv
```
Where file_path under scimago_sources task is the full path where the scimago csv is located.

I you have several scimago files use the yaml structure as shown below
```yaml
config:
  database_url: localhost
  database_name: kahi_test
  log_database: kahi_test
  log_collection: log
workflow:
  scimago_sources:
    file_path: 
      - /current/data/scimago/scimagojr 1999.csv
      - /current/data/scimago/scimagojr 2000.csv
      - /current/data/scimago/scimagojr 2001.csv
      - /current/data/scimago/scimagojr 2002.csv
```

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



