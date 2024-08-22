<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi minciencias opendata plugin 
Kahi will use this plugin to insert or update the projects information from the minciencias opendata database.

# Description
Plugin that reads the information from minciencias opendata database to insert or update the information of the of academic venets in colav's database.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_minciencias_opendata_projects
```


# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
  minciencias_opendata_projects:
    database_url: localhost:27017
    database_name: yuku
    collection_name: gruplac_production_data
    num_jobs: 6
    verbose: 1
```

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/

