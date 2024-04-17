<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi minciencias opendata plugin 
Kahi will use this plugin to insert or update the works information from the minciencias opendata database.

# Description
Plugin that reads the information from minciencias opendata database to insert or update the information of the of academic products in colav's database.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_minciencias_opendata_works
```
# Similarity support
This plugin only process works without doi. Then a elastic search server must be running. The plugin will use the server to find the most similar works in the database. To deply it please read https://github.com/colav/Chia/tree/main/elasticsaerch and follow the instructions.

Docker and docker-compose are required to deploy the server.


# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
  minciencias_opendata_works:
    es_index: kahi_es
    es_url: http://localhost:9200
    es_user: elastic
    es_password: colav
    database_url: localhost:27017
    database_name: yuku
    collection_name: gruplac_production_data
    insert_all: False
    thresholds: [65, 90, 95]
    num_jobs: 6
    verbose: 1
```
* WARNING *. This process can take more than an hour.

Note: 
-In case you want to insert all documents that fail to be associated through the similarity processes as new documents, you need to change the value of the insert_all flag to True in the workflow
-The thresholds parameter only accepts a list of three corresponding values for: A threshold for author names, a low threshold for works and a high threshold for works.

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/

