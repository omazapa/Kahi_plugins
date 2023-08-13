<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi scienti person plugin 
Kahi will use this plugin to insert or update the people information from scienti dump

# Description
Plugin that reads the information from a scienti dump to insert or update the information of the authors of academic products in colav's database.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_scienti_person
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
The user must have at least one database obtained from minciencias and previously processed by [kayPacha](https://github.com/colav/KayPacha "KayPacha") and uploaded on a mongodb database.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  scienti_person:
    databases:
      - database_url: localhost:27017
        database_name: scienti_111
        collection_name: products
```

If you have several scienti databases use the example below
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  scienti_person:
    databases:
      - database_url: localhost:27017
        database_name: scienti_111
        collection_name: products
      - database_url: localhost:27017
        database_name: scienti_uec_2022
        collection_name: product
    verbose: 2
```

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



